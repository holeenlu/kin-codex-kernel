//! 換票：照官方 CLI 0.171.0（`codex-rs/login/src/oauth/client.rs` 與 `auth/manager.rs`），只在共享模式使用。
//!
//! 換票請求基準（`fidelity/baseline/codex-cli-0.171.0-linux-x64/refresh.json`）：
//! - TLS 與推理的 HTTP 請求相同，所以走這個帳號的 HTTP client（同一個出口、同一個 native-tls 設定）。
//! - 標頭依序 `content-type`、`originator`、`user-agent`、`accept`、`accept-encoding`，之後由 hyper 加 `host`、`content-length`；
//!   沒有 `version` 或 `authorization`。身分與當下的推理請求相同（會話中為 `codex-tui`）。
//! - 本體是 JSON，欄位依字母排序：`client_id`、`grant_type`、`refresh_reason`、`refresh_token`。逾時 45 秒。
//! - 失敗分類同官方：`refresh_token_expired`／`refresh_token_reused`／`refresh_token_invalidated`、
//!   401、400 加 `invalid_grant` 是永久失敗（需要重新授權）；其他都是暫時失敗。

use std::collections::BTreeMap;
use std::time::Duration;

use kin_contract::send::UpstreamSent;
use kin_credentials::stored::Refreshed;
use kin_identity::Profile;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderValue, USER_AGENT};
use serde::Deserialize;
use serde_json::Value;

use crate::HttpUpstream;

/// 官方 CLI 的 OAuth client id（`codex-rs/login/src/auth/manager.rs` 的 `CLIENT_ID`）。
pub const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const TIMEOUT: Duration = Duration::from_secs(45);
/// 錯誤本體最多讀這麼多，避免異常回應占用記憶體；只用來解析錯誤碼，不轉給控制面。
const ERROR_BODY_LIMIT: usize = 16 * 1024;
const TOKEN_BODY_LIMIT: usize = 256 * 1024;

/// 換票原因，照官方送出（只供 token 端點診斷，不是 token 是否有效的依據）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshReason {
    /// access token 在 5 分鐘內到期。
    ProactiveExpiry,
    /// 讀不到到期時間，且距上次刷新超過 8 天。
    Scheduled,
    /// 上游回 401 之後的復原。
    UnauthorizedRecovery,
    /// 控制面要求的換票（`ops/refresh`）。
    Forced,
}

impl RefreshReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProactiveExpiry => "proactive_expiry",
            Self::Scheduled => "scheduled",
            Self::UnauthorizedRecovery => "unauthorized_recovery",
            Self::Forced => "forced",
        }
    }
}

/// 換票失敗。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefreshError {
    /// 需要重新授權；`code` 是 token 端點的錯誤碼（沒有時為狀態碼的描述）。
    Permanent { code: String },
    /// 可以退避後再試。`sent` 表示換票請求是否可能已被 token 端點處理。
    Transient { message: String, sent: UpstreamSent },
}

#[derive(Deserialize)]
struct TokenResponse {
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    access_token: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
}

/// 官方的取法（`oauth/error.rs` 的 `TokenErrorDetail::parse`）：`error` 字串、`error.code`、頂層 `code`。
fn error_code(body: &str) -> Option<String> {
    let json: Value = serde_json::from_str(body.trim()).ok()?;
    let text = |value: Option<&Value>| {
        value
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_owned)
    };
    text(json.get("error"))
        .or_else(|| text(json.get("error").and_then(|error| error.get("code"))))
        .or_else(|| text(json.get("code")))
}

/// 官方 `request_chatgpt_token_refresh` 與 `classify_refresh_token_failure` 的分類。
pub fn classify(status: u16, body: &str) -> RefreshError {
    let code = error_code(body);
    let normalized = code.as_deref().map(str::to_ascii_lowercase);
    let known = matches!(
        normalized.as_deref(),
        Some("refresh_token_expired" | "refresh_token_reused" | "refresh_token_invalidated")
    );
    let invalid_grant = status == 400 && normalized.as_deref() == Some("invalid_grant");
    if status == 401 || known || invalid_grant {
        RefreshError::Permanent {
            code: normalized
                .filter(|_| known || invalid_grant)
                .unwrap_or_else(|| format!("http_{status}")),
        }
    } else {
        RefreshError::Transient {
            message: format!("token endpoint returned status {status}"),
            sent: UpstreamSent::Yes,
        }
    }
}

/// 送給 token 端點的本體。`BTreeMap` 讓欄位依字母排序，與官方相同。
pub fn request_body(refresh_token: &str, reason: RefreshReason) -> Vec<u8> {
    let body: BTreeMap<&str, &str> = BTreeMap::from([
        ("client_id", CLIENT_ID),
        ("grant_type", "refresh_token"),
        ("refresh_reason", reason.as_str()),
        ("refresh_token", refresh_token),
    ]);
    serde_json::to_vec(&body).unwrap_or_default()
}

impl HttpUpstream {
    /// 用 refresh token 換新的 token。身分取自這次（或最近一次）推理請求的身分輸入（帳號環境與自訂 UA）。
    pub async fn refresh(
        &self,
        refresh_token: &str,
        reason: RefreshReason,
        profile: &Profile,
    ) -> Result<Refreshed, RefreshError> {
        let identity = profile.identity(&self.device_id);
        let user_agent =
            HeaderValue::from_str(&identity.user_agent).map_err(|err| RefreshError::Transient {
                message: err.to_string(),
                sent: UpstreamSent::No,
            })?;
        let mut response = self
            .client
            .post(&self.token_url)
            .timeout(TIMEOUT)
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .header("originator", identity.originator.as_str())
            .header(USER_AGENT, user_agent)
            .header(ACCEPT, HeaderValue::from_static("*/*"))
            .body(request_body(refresh_token, reason))
            .send()
            .await
            .map_err(|err| RefreshError::Transient {
                sent: if err.is_connect() || err.is_builder() {
                    UpstreamSent::No
                } else {
                    UpstreamSent::Unknown
                },
                message: err.without_url().to_string(),
            })?;
        let status = response.status().as_u16();
        let success = response.status().is_success();
        let limit = if success {
            TOKEN_BODY_LIMIT
        } else {
            ERROR_BODY_LIMIT
        };
        let mut raw = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|err| RefreshError::Transient {
            message: err.without_url().to_string(),
            sent: UpstreamSent::Unknown,
        })? {
            let remaining = limit - raw.len();
            if success && chunk.len() > remaining {
                return Err(RefreshError::Transient {
                    message: "token endpoint response exceeds the size limit".to_owned(),
                    sent: UpstreamSent::Yes,
                });
            }
            raw.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
            if !success && raw.len() == limit {
                break;
            }
        }
        if !success {
            return Err(classify(status, &String::from_utf8_lossy(&raw)));
        }
        let parsed: TokenResponse = serde_json::from_slice(&raw).map_err(|_| RefreshError::Transient {
            message: "token endpoint answered an unreadable body".to_owned(),
            sent: UpstreamSent::Yes,
        })?;
        let clean = |value: Option<String>| {
            value
                .map(|text| text.trim().to_owned())
                .filter(|text| !text.is_empty())
        };
        Ok(Refreshed {
            access_token: clean(parsed.access_token),
            refresh_token: clean(parsed.refresh_token),
            id_token: clean(parsed.id_token),
            expires_in: parsed.expires_in,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_body_matches_the_recorded_official_request() {
        let body =
            String::from_utf8(request_body("rt-1", RefreshReason::UnauthorizedRecovery)).unwrap_or_default();
        assert_eq!(
            body,
            r#"{"client_id":"app_EMoamEEZ73f0CkXaXp7hrann","grant_type":"refresh_token","refresh_reason":"unauthorized_recovery","refresh_token":"rt-1"}"#
        );
        assert_eq!(RefreshReason::ProactiveExpiry.as_str(), "proactive_expiry");
        assert_eq!(RefreshReason::Scheduled.as_str(), "scheduled");
        assert_eq!(RefreshReason::Forced.as_str(), "forced");
    }

    #[test]
    fn failures_are_classified_like_the_official_client() {
        let permanent = |code: &str| RefreshError::Permanent {
            code: code.to_owned(),
        };
        assert_eq!(
            classify(400, r#"{"error":"refresh_token_reused"}"#),
            permanent("refresh_token_reused")
        );
        assert_eq!(
            classify(400, r#"{"error":{"code":"refresh_token_expired","message":"x"}}"#),
            permanent("refresh_token_expired")
        );
        assert_eq!(
            classify(403, r#"{"code":"Refresh_Token_Invalidated"}"#),
            permanent("refresh_token_invalidated")
        );
        assert_eq!(
            classify(400, r#"{"error":"invalid_grant"}"#),
            permanent("invalid_grant")
        );
        assert_eq!(classify(401, "not json"), permanent("http_401"));
        assert_eq!(
            classify(401, r#"{"error":"private upstream diagnostic"}"#),
            permanent("http_401")
        );
        // 其他錯誤：暫時失敗，退避後再試
        for (status, body) in [
            (500, ""),
            (429, r#"{"error":"rate_limited"}"#),
            (403, r#"{"error":"invalid_grant"}"#),
        ] {
            assert!(
                matches!(classify(status, body), RefreshError::Transient { .. }),
                "{status} {body}"
            );
        }
    }
}
