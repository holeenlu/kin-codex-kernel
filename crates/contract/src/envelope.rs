//! `POST /internal/v1/codex/responses` 的請求格式（vm2api `handle-codex.mjs` 組出的 envelope）。
//!
//! `body` 保留控制面送來的原始 JSON 位元組，不重新序列化：欄位順序是下游客戶端的原樣，
//! 重新排序本身就是可辨識的差異。

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::value::RawValue;

use crate::config::{TelemetryRuntime, UpstreamTransport};

/// 共享模式（ABI 2）下這次請求要用的帳號。單帳號模式沒有這個欄位。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AccountBinding {
    #[serde(rename = "ref")]
    pub account_ref: String,
    /// 必須等於登錄中的環境代次，否則回 `account_generation_mismatch`，確定沒送出。
    pub env_generation: u64,
}

#[derive(Debug, Deserialize)]
pub struct ResponsesEnvelope {
    /// 要送給上游的 Responses 請求本體，原樣保留。
    pub body: Box<RawValue>,
    /// 下游客戶端的官方標頭（控制面已依白名單過濾）；核心只取認得的名稱。
    #[serde(default)]
    pub headers: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub session: Option<Session>,
    #[serde(default)]
    pub options: RequestOptions,
    /// 控制面為這次請求產生的 id，`POST /internal/v1/cancel` 以它取消。
    #[serde(default)]
    pub request_id: Option<String>,
    /// 共享模式必填；單帳號模式忽略。
    #[serde(default)]
    pub account: Option<AccountBinding>,
}

#[derive(Debug, Default, Deserialize)]
pub struct Session {
    #[serde(default)]
    pub session_id: Option<String>,
}

/// 每次請求的策略（vm2api `codexKernelRequestOptions`）。未知欄位忽略。
#[derive(Debug, Default, Deserialize)]
pub struct RequestOptions {
    /// 只由入口 WS 的逐連線歷史快取提供；body 仍是完整、可獨立回放的快照。
    #[serde(default)]
    pub ws_replay: Option<WsReplay>,
    #[serde(default)]
    pub upstream_transport: Option<UpstreamTransport>,
    #[serde(default)]
    pub user_agent: Option<String>,
    #[serde(default)]
    pub client_features: Option<bool>,
    #[serde(default)]
    pub analytics: Option<bool>,
    /// 帳號環境；沒有自訂 UA 時依它組出預設身分。
    #[serde(default)]
    pub client_environment: Option<ClientEnvironment>,
    /// 共享模式只拿來與登錄值比對，不一致回 409；實際使用的值以登錄為準（設計第 4.4 節）。
    #[serde(default)]
    pub installation_id: Option<String>,
    #[serde(default)]
    pub telemetry_runtime: Option<TelemetryRuntime>,
}

#[derive(Debug, Default, Deserialize)]
pub struct WsReplay {
    #[serde(default)]
    pub previous_response_id: Option<String>,
}

/// 帳號所在機器的原始事實，不先格式化。格式化規則重現官方客戶端（`kin-identity`）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ClientEnvironment {
    #[serde(default)]
    pub os_release: Option<OsRelease>,
    /// `uname -m`，例如 `x86_64`。
    #[serde(default)]
    pub machine: Option<String>,
    #[serde(default)]
    pub term: Option<String>,
    #[serde(default)]
    pub term_program: Option<String>,
    #[serde(default)]
    pub term_program_version: Option<String>,
}

/// `/etc/os-release` 的 `ID` 與 `VERSION_ID`。
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct OsRelease {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub version_id: Option<String>,
}

impl ResponsesEnvelope {
    /// 下游標頭的字串值；空字串視為沒有。
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(name)
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }

    pub fn session_id(&self) -> Option<&str> {
        self.session
            .as_ref()
            .and_then(|session| session.session_id.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_the_body_bytes_and_reads_known_fields() {
        let raw = br#"{"body":{"model":"gpt-5.5","input":[],"stream":true},"headers":{"session-id":"s1","x-n":1},
            "stream":true,"session":{"session_id":"s1"},"request_id":"r1",
            "options":{"upstream_transport":"http","user_agent":"ignored","client_environment":{"os_release":{"id":"ubuntu","version_id":"24.04"},"machine":"x86_64","term":"xterm-256color"}}}"#;
        let envelope: ResponsesEnvelope = serde_json::from_slice(raw).unwrap();
        assert_eq!(
            envelope.body.get(),
            r#"{"model":"gpt-5.5","input":[],"stream":true}"#
        );
        assert_eq!(envelope.header("session-id"), Some("s1"));
        assert_eq!(envelope.header("x-n"), None);
        assert_eq!(envelope.session_id(), Some("s1"));
        assert_eq!(envelope.options.upstream_transport, Some(UpstreamTransport::Http));
        let env = envelope.options.client_environment.unwrap();
        assert_eq!(env.os_release.unwrap().id.as_deref(), Some("ubuntu"));
    }

    #[test]
    fn missing_optional_parts_default() {
        let envelope: ResponsesEnvelope = serde_json::from_slice(br#"{"body":{}}"#).unwrap();
        assert!(!envelope.stream);
        assert!(envelope.request_id.is_none());
        assert!(envelope.options.client_environment.is_none());
        assert!(envelope.account.is_none());
    }

    #[test]
    fn reads_the_shared_mode_account_binding() {
        let envelope: ResponsesEnvelope = serde_json::from_slice(
            br#"{"body":{},"account":{"ref":"vm-25","env_generation":8},
            "options":{"installation_id":"d-1","telemetry_runtime":{"runtime_os":"linux","runtime_os_version":"24.04","runtime_arch":"x86_64"}}}"#,
        )
        .unwrap();
        assert_eq!(
            envelope.account,
            Some(AccountBinding {
                account_ref: "vm-25".to_owned(),
                env_generation: 8
            })
        );
        assert_eq!(envelope.options.installation_id.as_deref(), Some("d-1"));
        assert_eq!(
            envelope.options.telemetry_runtime.map(|t| t.runtime_arch),
            Some("x86_64".to_owned())
        );
    }
}
