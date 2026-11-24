//! 出站標頭的組裝（設計第 5.3 節；基準見 `fidelity/baseline/codex-cli-0.171.0-linux-x64/expected-identity.json`）。
//!
//! 順序照官方互動式 TUI 在 HTTP 上送出的順序。錄得的部分逐字對照；只在特定情境出現的標頭
//! （turn-state、parent-thread-id、subagent、memgen）依官方原始碼（`core/src/client.rs`、
//! `responses_metadata.rs`、`codex-api/src/endpoint/responses.rs`）的插入順序擺放。
//! `host` 與 `content-length` 由 HTTP 層附在最後，與官方相同。
//!
//! 下游標頭只取控制面白名單中、官方 HTTP 路徑也會送的名稱；`openai-beta` 與
//! `x-codex-installation-id` 官方在 HTTP 上不送，所以丟掉。授權與身分一律由核心附加。
//!
//! WebSocket 升級：`openai-beta` 固定為 v2 的值。這裡給的是**插入順序**，不是線上順序：
//! tungstenite（官方分支）寫出請求時，先以 `swap_remove` 取出 `host`、`connection`、`upgrade`、
//! `sec-websocket-version`、`sec-websocket-key` 放在最前面，於是最後插入的五個標頭會倒序移到前面，
//! `sec-websocket-extensions`（permessage-deflate）附在最後。用同一個分支、照官方的插入順序，
//! 線上順序就與官方錄得的一致：`chatgpt-account-id, authorization, user-agent, originator, openai-beta,
//! version, x-codex-beta-features, x-client-request-id, session-id, thread-id, x-codex-window-id,
//! x-codex-turn-metadata, x-codex-routing-hint`。
//! `x-openai-internal-codex-responses-lite` 與 `x-codex-turn-state` 官方在 WebSocket 握手不送。

use reqwest::header::{HeaderName, HeaderValue};
use tap_contract::envelope::ResponsesEnvelope;
use tap_credentials::Credential;

pub(super) const MODELS_ETAG: &str = "x-models-etag";
use tap_identity::Identity;

/// 一個出站標頭的來源。
enum Slot {
    /// 下游客戶端提供才送。
    Downstream(&'static str),
    /// 下游沒有提供時，以控制面的 session id 補上（官方每次都送）。
    Conversation(&'static str),
    Version,
    RoutingHint,
    Fixed(&'static str, &'static str),
    Authorization,
    AccountId,
    Originator,
    UserAgent,
}

const PLAN: &[Slot] = &[
    Slot::Version,
    Slot::Downstream("x-codex-beta-features"),
    Slot::Downstream("x-codex-turn-state"),
    Slot::Downstream("x-codex-window-id"),
    Slot::Downstream("x-codex-turn-metadata"),
    Slot::Downstream("x-codex-parent-thread-id"),
    Slot::Downstream("x-openai-subagent"),
    Slot::Downstream("x-openai-memgen-request"),
    Slot::Downstream("x-openai-internal-codex-responses-lite"),
    Slot::RoutingHint,
    Slot::Conversation("x-client-request-id"),
    Slot::Conversation("session-id"),
    Slot::Conversation("thread-id"),
    Slot::Fixed("accept", "text/event-stream"),
    Slot::Fixed("content-encoding", "zstd"),
    Slot::Fixed("content-type", "application/json"),
    Slot::Authorization,
    Slot::AccountId,
    Slot::Originator,
    Slot::UserAgent,
];

/// 官方 WebSocket v2 的 beta 值（`core/src/client.rs` 的 `RESPONSES_WEBSOCKETS_V2_BETA_HEADER_VALUE`）。
pub const WEBSOCKET_BETA: &str = "responses_websockets=2026-02-06";

const WEBSOCKET_PLAN: &[Slot] = &[
    Slot::Version,
    Slot::Downstream("x-codex-beta-features"),
    Slot::Conversation("x-client-request-id"),
    Slot::Conversation("session-id"),
    Slot::Conversation("thread-id"),
    Slot::Downstream("x-codex-window-id"),
    Slot::Downstream("x-codex-turn-metadata"),
    Slot::Downstream("x-codex-parent-thread-id"),
    Slot::Downstream("x-openai-subagent"),
    Slot::Downstream("x-openai-memgen-request"),
    Slot::RoutingHint,
    Slot::Fixed("openai-beta", WEBSOCKET_BETA),
    Slot::Originator,
    Slot::UserAgent,
    Slot::Authorization,
    Slot::AccountId,
];

/// 依固定順序組出 Responses（HTTP）的出站標頭。值不合法的下游標頭直接略過。
pub fn responses_http(
    envelope: &ResponsesEnvelope,
    model: Option<&str>,
    service_tier: Option<&str>,
    turn_metadata: Option<&str>,
    credential: &Credential,
    identity: &Identity,
) -> Vec<(HeaderName, HeaderValue)> {
    assemble(
        PLAN,
        envelope,
        model,
        service_tier,
        turn_metadata,
        credential,
        identity,
    )
}

/// WebSocket 升級請求上由核心附加的標頭，依官方的插入順序（線上順序見模組說明）。
pub fn responses_websocket(
    envelope: &ResponsesEnvelope,
    model: Option<&str>,
    service_tier: Option<&str>,
    turn_metadata: Option<&str>,
    credential: &Credential,
    identity: &Identity,
) -> Vec<(HeaderName, HeaderValue)> {
    assemble(
        WEBSOCKET_PLAN,
        envelope,
        model,
        service_tier,
        turn_metadata,
        credential,
        identity,
    )
}

/// `turn_metadata` 是改寫過安裝 id 的 `x-codex-turn-metadata`（見 [`crate::scrub`]），取代下游原值。
fn assemble(
    plan: &[Slot],
    envelope: &ResponsesEnvelope,
    model: Option<&str>,
    service_tier: Option<&str>,
    turn_metadata: Option<&str>,
    credential: &Credential,
    identity: &Identity,
) -> Vec<(HeaderName, HeaderValue)> {
    let conversation = envelope.session_id();
    let mut out = Vec::with_capacity(plan.len());
    for slot in plan {
        let (name, value): (&str, Option<String>) = match slot {
            Slot::Version => ("version", Some(identity.version.to_owned())),
            Slot::Downstream("x-codex-turn-metadata") => {
                ("x-codex-turn-metadata", turn_metadata.map(str::to_owned))
            }
            Slot::Downstream(name) => (name, envelope.header(name).map(str::to_owned)),
            Slot::Conversation(name) => (name, envelope.header(name).or(conversation).map(str::to_owned)),
            Slot::RoutingHint => (
                "x-codex-routing-hint",
                model.and_then(|model| routing_hint(model, service_tier)),
            ),
            Slot::Fixed(name, value) => (name, Some((*value).to_owned())),
            Slot::Authorization => (
                "authorization",
                Some(format!("Bearer {}", credential.access_token)),
            ),
            Slot::AccountId => ("chatgpt-account-id", credential.chatgpt_account_id.clone()),
            Slot::Originator => ("originator", Some(identity.originator.to_owned())),
            Slot::UserAgent => ("user-agent", Some(identity.user_agent.clone())),
        };
        let Some(value) = value else { continue };
        if let (Ok(name), Ok(mut value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            if name == "authorization" {
                value.set_sensitive(true);
            }
            out.push((name, value));
        }
    }
    out
}

/// 只用請求本體已選定的模型與等級組提示，不讓欄位值引入其他路由參數。
fn routing_hint(model: &str, service_tier: Option<&str>) -> Option<String> {
    let model = model.trim_matches(' ');
    if model.is_empty() || model.contains([';', '=']) || model.bytes().any(|b| b.is_ascii_control()) {
        return None;
    }
    let tier = service_tier
        .map(|value| value.trim_matches(' '))
        .filter(|value| {
            (1..=64).contains(&value.len())
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        })
        .map(str::to_ascii_lowercase);
    Some(match tier.as_deref() {
        None | Some("auto" | "standard" | "default") => format!("model={model}"),
        Some(tier) => format!("model={model};tier={tier}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tap_contract::envelope::ResponsesEnvelope;
    use tap_identity::{IdentitySource, ORIGINATOR, TARGET_VERSION};

    fn identity() -> Identity {
        Identity {
            originator: ORIGINATOR.into(),
            version: TARGET_VERSION.into(),
            user_agent: "codex-tui/0.171.0 (Debian 12.0.0; x86_64) xterm-256color (codex-tui; 0.171.0)"
                .to_owned(),
            source: IdentitySource::Environment,
        }
    }

    fn credential() -> Credential {
        Credential {
            access_token: "at".to_owned(),
            chatgpt_account_id: Some("acct".to_owned()),
        }
    }

    fn names(headers: &[(HeaderName, HeaderValue)]) -> Vec<&str> {
        headers.iter().map(|(name, _)| name.as_str()).collect()
    }

    #[test]
    fn follows_the_recorded_official_tui_order() {
        // 官方 TUI 經 HTTP 送出的推理請求（錄得）：這些名稱、這個順序。
        let envelope: ResponsesEnvelope = serde_json::from_str(
            r#"{"body":{},"session":{"session_id":"s"},"headers":{
                "thread-id":"t","session-id":"s","x-client-request-id":"t","x-codex-window-id":"t:0",
                "x-codex-turn-metadata":"{}","x-codex-beta-features":"remote_compaction_v2",
                "x-openai-internal-codex-responses-lite":"true","openai-beta":"responses_websockets=2026-02-06",
                "x-codex-installation-id":"i","authorization":"Bearer evil","user-agent":"evil","x-other":"no"}}"#,
        )
        .unwrap();
        let headers = responses_http(
            &envelope,
            Some("gpt-6.1-sol"),
            None,
            envelope.header("x-codex-turn-metadata"),
            &credential(),
            &identity(),
        );
        assert_eq!(
            names(&headers),
            [
                "version",
                "x-codex-beta-features",
                "x-codex-window-id",
                "x-codex-turn-metadata",
                "x-openai-internal-codex-responses-lite",
                "x-codex-routing-hint",
                "x-client-request-id",
                "session-id",
                "thread-id",
                "accept",
                "content-encoding",
                "content-type",
                "authorization",
                "chatgpt-account-id",
                "originator",
                "user-agent",
            ]
        );
        let value = |name: &str| {
            headers
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.to_str().unwrap())
        };
        assert_eq!(value("authorization"), Some("Bearer at"));
        assert_eq!(value("x-codex-routing-hint"), Some("model=gpt-6.1-sol"));
        assert_eq!(value("originator"), Some("codex-tui"));
        assert_eq!(value("version"), Some("0.171.0"));
    }

    #[test]
    fn websocket_headers_use_the_official_insertion_order() {
        // 插入順序；經 tungstenite 的 swap_remove 後，線上順序等於官方錄得的（見整合測試）。
        let envelope: ResponsesEnvelope = serde_json::from_str(
            r#"{"body":{},"session":{"session_id":"s"},"headers":{
                "thread-id":"t","session-id":"s","x-client-request-id":"t","x-codex-window-id":"t:0",
                "x-codex-turn-metadata":"{}","x-codex-beta-features":"remote_compaction_v2",
                "x-openai-internal-codex-responses-lite":"true","x-codex-turn-state":"ts",
                "openai-beta":"downstream-value"}}"#,
        )
        .unwrap();
        let headers = responses_websocket(
            &envelope,
            Some("gpt-6.1-sol"),
            None,
            envelope.header("x-codex-turn-metadata"),
            &credential(),
            &identity(),
        );
        assert_eq!(
            names(&headers),
            [
                "version",
                "x-codex-beta-features",
                "x-client-request-id",
                "session-id",
                "thread-id",
                "x-codex-window-id",
                "x-codex-turn-metadata",
                "x-codex-routing-hint",
                "openai-beta",
                "originator",
                "user-agent",
                "authorization",
                "chatgpt-account-id",
            ]
        );
        let beta = headers
            .iter()
            .find(|(n, _)| n == "openai-beta")
            .map(|(_, v)| v.to_str().unwrap());
        assert_eq!(beta, Some(WEBSOCKET_BETA));
    }

    #[test]
    fn conversation_ids_fall_back_to_the_control_plane_session() {
        let envelope: ResponsesEnvelope =
            serde_json::from_str(r#"{"body":{},"session":{"session_id":"sess-1"},"headers":{}}"#).unwrap();
        let headers = responses_http(
            &envelope,
            None,
            None,
            envelope.header("x-codex-turn-metadata"),
            &credential(),
            &identity(),
        );
        for name in ["x-client-request-id", "session-id", "thread-id"] {
            let value = headers
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.to_str().unwrap());
            assert_eq!(value, Some("sess-1"), "{name}");
        }
        assert!(!names(&headers).contains(&"x-codex-routing-hint"));
    }
}
