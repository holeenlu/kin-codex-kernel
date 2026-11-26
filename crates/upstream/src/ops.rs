//! 帳號操作代發（vm2api 設計第 4.4 節）：額度、帳號檢查、模型目錄、重置點數、搜尋與圖片。
//!
//! 每一種操作都照官方 CLI 0.171.0 送出（私有基準 `fidelity/baseline/codex-cli-0.171.0-linux-x64/ops-and-cookies.json`）：
//! - `usage`、`accounts_check`、`reset_credits`、`consume_credit` 由官方的 backend-client 送出：
//!   標頭依序 `user-agent`、`authorization`、`chatgpt-account-id`，沒有 `originator`；`usage` 另帶
//!   `x-openai-codex-luna-reserve: 1`；`consume_credit` 帶 `content-type` 與 `{redeem_request_id, credit_id?}`
//!   （這一項沒有錄到：TUI 只在使用者操作時消費，標頭順序由同一個 client 的查詢請求與原始碼推出）。
//! - `models`、`search`、`images_generate`、`images_edit` 由 codex-api 送出：`version` 在最前；
//!   圖片有 `x-codex-image-turn-id`，搜尋與圖片有 `x-codex-turn-metadata`（都在 `authorization` 之前）；
//!   之後 `authorization`、`chatgpt-account-id`、（POST 的）`content-type`、`originator`、`user-agent`。
//!   搜尋與圖片在官方是模型呼叫工具時由 CLI 自己送出（以假模型回工具呼叫錄得）。
//! - 之後都是 `accept: */*`，再由 reqwest 加 `accept-encoding`、`cookie`（有的話），hyper 加 `host`。
//! - 身分與這個帳號當下的推理請求相同（會話中為 `codex-tui`）。
//!
//! 搜尋與圖片的本體由控制面組好，這裡**原樣**轉送：重新序列化會改變欄位順序，而順序本身就是可辨識的差異。
//! 回應原樣轉給控制面：狀態碼、本體、`content-type` 與上游的 `x-codex-*` 標頭。

use bytes::Bytes;
use futures_util::StreamExt;
use kin_contract::config::looks_like_uuid;
use kin_contract::error::KernelError;
use kin_contract::send::UpstreamSent;
use kin_credentials::Credential;
use kin_identity::Profile;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderName, HeaderValue, USER_AGENT};
use serde::Serialize;
use serde_json::Value;
use serde_json::value::RawValue;
use tokio_util::sync::CancellationToken;

use crate::HttpUpstream;

/// 帳號操作回應本體的上限；圖片另有較大的上限（一張 1024×1536 的 PNG 以 base64 約數 MB）。
const BODY_LIMIT: usize = 8 * 1024 * 1024;
const IMAGE_BODY_LIMIT: usize = 64 * 1024 * 1024;

/// 可代發的帳號操作。只接受這些列舉值，不接受任意 URL。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Usage,
    AccountsCheck,
    Models,
    ResetCredits,
    ConsumeCredit,
    Search,
    ImagesGenerate,
    ImagesEdit,
}

impl Op {
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "usage" => Self::Usage,
            "accounts_check" => Self::AccountsCheck,
            "models" => Self::Models,
            "reset_credits" => Self::ResetCredits,
            "consume_credit" => Self::ConsumeCredit,
            "search" => Self::Search,
            "images_generate" => Self::ImagesGenerate,
            "images_edit" => Self::ImagesEdit,
            _ => return None,
        })
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Usage => "usage",
            Self::AccountsCheck => "accounts_check",
            Self::Models => "models",
            Self::ResetCredits => "reset_credits",
            Self::ConsumeCredit => "consume_credit",
            Self::Search => "search",
            Self::ImagesGenerate => "images_generate",
            Self::ImagesEdit => "images_edit",
        }
    }

    pub const fn images(self) -> bool {
        matches!(self, Self::ImagesGenerate | Self::ImagesEdit)
    }
}

/// 控制面為搜尋與圖片帶來的下游標頭（只有這兩個，其他一律不轉）。
#[derive(Debug, Clone, Default)]
pub struct OpHeaders {
    pub turn_metadata: Option<String>,
    pub image_turn_id: Option<String>,
}

/// 上游對帳號操作的回應。
#[derive(Debug)]
pub struct OpResponse {
    pub status: u16,
    /// `content-type` 與上游的 `x-codex-*` 標頭。
    pub headers: Vec<(HeaderName, HeaderValue)>,
    pub body: Bytes,
}

#[derive(Serialize)]
struct ConsumeRequest<'a> {
    redeem_request_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    credit_id: Option<&'a str>,
}

/// 官方 `ConsumeRateLimitResetCreditRequest` 的本體。`redeem_request_id` 由控制面產生並原樣轉送：
/// 結果不明時控制面只能用同一個 id 重試或查詢，不會重複消耗。
pub fn consume_body(body: Option<&RawValue>) -> Result<Vec<u8>, KernelError> {
    let value: Option<Value> = body.and_then(|raw| serde_json::from_str(raw.get()).ok());
    let text = |name: &str| {
        value
            .as_ref()
            .and_then(|body| body.get(name))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
    };
    let redeem_request_id = text("redeem_request_id")
        .ok_or_else(|| KernelError::InvalidBody("consume_credit needs redeem_request_id".to_owned()))?;
    serde_json::to_vec(&ConsumeRequest {
        redeem_request_id,
        credit_id: text("credit_id"),
    })
    .map_err(|err| KernelError::InvalidBody(err.to_string()))
}

/// 搜尋與圖片的本體：必須是 JSON 物件，原樣轉送。
fn passthrough_body(op: Op, body: Option<&RawValue>) -> Result<Vec<u8>, KernelError> {
    let raw = body
        .map(RawValue::get)
        .filter(|raw| raw.trim_start().starts_with('{'))
        .ok_or_else(|| KernelError::InvalidBody(format!("{} needs a JSON object body", op.name())))?;
    Ok(raw.as_bytes().to_vec())
}

/// 下游 turn metadata 裡的安裝 id 換成這個帳號的（與推理相同：只認 UUID，位元組替換，不改其他內容與順序）。
pub fn rewrite_turn_metadata(raw: &str, device_id: &str) -> String {
    let installation = serde_json::from_str::<Value>(raw)
        .ok()
        .and_then(|value| {
            value
                .get("installation_id")?
                .as_str()
                .map(|id| id.trim().to_owned())
        })
        .filter(|id| looks_like_uuid(id) && !id.eq_ignore_ascii_case(device_id));
    match installation {
        Some(id) => raw.replace(&id, device_id),
        None => raw.to_owned(),
    }
}

/// backend-client 的位址：Responses 的位址去掉 `/codex`（`https://chatgpt.com/backend-api`）。
fn backend_base(base_url: &str) -> &str {
    base_url.strip_suffix("/codex").unwrap_or(base_url)
}

fn header(value: &str) -> Result<HeaderValue, KernelError> {
    HeaderValue::from_str(value).map_err(|err| KernelError::InvalidBody(err.to_string()))
}

impl HttpUpstream {
    /// 代發一個帳號操作。`profile` 是這個帳號最近一次推理的身分輸入（帳號環境與自訂 UA），
    /// 帳號操作與推理才對上游呈現同一個身分。
    pub async fn operate(
        &self,
        op: Op,
        credential: &Credential,
        profile: &Profile,
        body: Option<&RawValue>,
        headers: &OpHeaders,
        cancel: &CancellationToken,
    ) -> Result<OpResponse, KernelError> {
        let identity = profile.identity(&self.device_id);
        let user_agent = header(&identity.user_agent)?;
        let mut authorization = header(&format!("Bearer {}", credential.access_token))?;
        authorization.set_sensitive(true);
        let account = credential.chatgpt_account_id.as_deref().map(header).transpose()?;
        let with_account = |request: reqwest::RequestBuilder| match &account {
            Some(account) => request.header("chatgpt-account-id", account.clone()),
            None => request,
        };
        let backend = backend_base(&self.base_url);
        let accept = HeaderValue::from_static("*/*");
        let backend_client = |request: reqwest::RequestBuilder| {
            with_account(
                request
                    .header(USER_AGENT, user_agent.clone())
                    .header(AUTHORIZATION, authorization.clone()),
            )
        };
        // codex-api 的請求：version、（圖片的 image turn id、turn metadata）、authorization、帳號。
        let codex_api = |request: reqwest::RequestBuilder,
                         turn_headers: bool|
         -> Result<reqwest::RequestBuilder, KernelError> {
            let mut request = request.header("version", identity.version.as_str());
            if turn_headers {
                if op.images()
                    && let Some(id) = &headers.image_turn_id
                {
                    request = request.header("x-codex-image-turn-id", header(id)?);
                }
                if let Some(metadata) = &headers.turn_metadata {
                    request = request.header(
                        "x-codex-turn-metadata",
                        header(&rewrite_turn_metadata(metadata, &self.device_id))?,
                    );
                }
            }
            Ok(with_account(request.header(AUTHORIZATION, authorization.clone())))
        };
        let identity_tail = |request: reqwest::RequestBuilder| {
            request
                .header("originator", identity.originator.as_str())
                .header(USER_AGENT, user_agent.clone())
                .header(ACCEPT, accept.clone())
        };
        let json_post = |path: &str| -> Result<reqwest::RequestBuilder, KernelError> {
            Ok(identity_tail(
                codex_api(self.client.post(format!("{}/{path}", self.base_url)), true)?
                    .header(CONTENT_TYPE, HeaderValue::from_static("application/json")),
            )
            .body(passthrough_body(op, body)?))
        };
        let request = match op {
            Op::Usage => backend_client(self.client.get(format!("{backend}/wham/usage")))
                .header("x-openai-codex-luna-reserve", HeaderValue::from_static("1"))
                .header(ACCEPT, accept.clone()),
            Op::AccountsCheck => backend_client(self.client.get(format!("{backend}/wham/accounts/check")))
                .header(ACCEPT, accept.clone()),
            Op::ResetCredits => backend_client(
                self.client
                    .get(format!("{backend}/wham/rate-limit-reset-credits")),
            )
            .header(ACCEPT, accept.clone()),
            Op::ConsumeCredit => backend_client(
                self.client
                    .post(format!("{backend}/wham/rate-limit-reset-credits/consume")),
            )
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .header(ACCEPT, accept.clone())
            .body(consume_body(body)?),
            Op::Models => identity_tail(codex_api(
                self.client.get(format!(
                    "{}/models?client_version={}",
                    self.base_url,
                    kin_identity::TARGET_VERSION
                )),
                false,
            )?),
            Op::Search => json_post("alpha/search")?,
            Op::ImagesGenerate => json_post("images/generations")?,
            Op::ImagesEdit => json_post("images/edits")?,
        };
        let response = self.send_with_headers_deadline(request, cancel).await?;
        let status = response.status().as_u16();
        let passthrough = response
            .headers()
            .iter()
            .filter(|(name, _)| *name == CONTENT_TYPE || name.as_str().starts_with("x-codex-"))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        // 已收到上游的回應；本體讀到一半失敗時結果不明（圖片是付費操作，控制面不得因此再生成）。
        let unreadable = |message: String| KernelError::UpstreamTransport {
            message,
            sent: UpstreamSent::Unknown,
        };
        let limit = if op.images() { IMAGE_BODY_LIMIT } else { BODY_LIMIT };
        let mut answer = Vec::new();
        let mut stream = super::http_chunks(response, self.read_idle);
        loop {
            let chunk = tokio::select! {
                chunk = stream.next() => chunk,
                () = cancel.cancelled() => return Err(unreadable("request cancelled".to_owned())),
            };
            let Some(chunk) = chunk else { break };
            let chunk = chunk.map_err(|err| unreadable(err.to_string()))?;
            if answer.len() + chunk.len() > limit {
                return Err(unreadable(format!(
                    "{} response exceeds {limit} bytes",
                    op.name()
                )));
            }
            answer.extend_from_slice(&chunk);
        }
        Ok(OpResponse {
            status,
            headers: passthrough,
            body: Bytes::from(answer),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(text: &str) -> Box<RawValue> {
        RawValue::from_string(text.to_owned()).unwrap()
    }

    #[test]
    fn only_listed_operations_are_accepted() {
        for name in [
            "usage",
            "accounts_check",
            "models",
            "reset_credits",
            "consume_credit",
            "search",
            "images_generate",
            "images_edit",
        ] {
            assert_eq!(Op::parse(name).map(Op::name), Some(name));
        }
        for name in ["refresh", "images", "../wham/usage", ""] {
            assert!(Op::parse(name).is_none(), "{name}");
        }
    }

    #[test]
    fn the_consume_body_matches_the_official_request() {
        let body = raw(r#"{"credit_id": "c-1", "redeem_request_id": "r-1", "extra": 1}"#);
        assert_eq!(
            String::from_utf8(consume_body(Some(&body)).unwrap_or_default()).unwrap_or_default(),
            r#"{"redeem_request_id":"r-1","credit_id":"c-1"}"#
        );
        assert_eq!(
            consume_body(None).map(|_| ()).unwrap_err().code(),
            "invalid_request"
        );
        assert_eq!(
            backend_base("https://chatgpt.com/backend-api/codex"),
            "https://chatgpt.com/backend-api"
        );
    }

    #[test]
    fn search_and_image_bodies_keep_their_bytes() {
        let original = r#"{"prompt":"a red square","background":"opaque","model":"gpt-image-2","quality":"auto","size":"auto"}"#;
        let body = raw(original);
        assert_eq!(
            passthrough_body(Op::ImagesGenerate, Some(&body)).unwrap_or_default(),
            original.as_bytes()
        );
        assert!(passthrough_body(Op::Search, Some(&raw("[1]"))).is_err());
        assert!(passthrough_body(Op::Search, None).is_err());
    }

    #[test]
    fn the_downstream_installation_id_in_turn_metadata_is_replaced() {
        let down = "11111111-2222-4333-8444-555555555555";
        let slot = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
        let metadata = format!(r#"{{"session_id":"s","installation_id":"{down}","turn_id":"t"}}"#);
        assert_eq!(
            rewrite_turn_metadata(&metadata, slot),
            format!(r#"{{"session_id":"s","installation_id":"{slot}","turn_id":"t"}}"#)
        );
        let without = r#"{"session_id":"s","turn_id":"t"}"#;
        assert_eq!(rewrite_turn_metadata(without, slot), without);
        // 不是 UUID 的 id 不替換，否則會誤換 metadata 裡相同的子字串
        let short = r#"{"session_id":"s1","installation_id":"s","turn_id":"ts"}"#;
        assert_eq!(rewrite_turn_metadata(short, slot), short);
    }
}
