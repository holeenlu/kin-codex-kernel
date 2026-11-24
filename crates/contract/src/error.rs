//! 核心回給控制面的錯誤。
//!
//! 每一種錯誤在這裡集中定義 HTTP 狀態、錯誤碼、錯誤類型與送出狀態，
//! 處理程式只回傳 [`KernelError`]，不自行拼 JSON，避免同一個錯誤在不同端點長得不一樣。
//! 錯誤碼與訊息要與 vm2api 依賴的值相容（例如 `body_too_large`）。

use serde::Serialize;

use crate::send::UpstreamSent;

/// 錯誤類型，對應回應中的 `error.type`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    AuthenticationError,
    InvalidRequestError,
    ApiError,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KernelError {
    #[error("internal token required")]
    Unauthorized,
    #[error("request body exceeds max_request_bytes")]
    BodyTooLarge,
    #[error("request body is not valid: {0}")]
    InvalidBody(String),
    #[error("unknown internal endpoint")]
    NotFound,
    #[error("{0} is not implemented yet")]
    NotImplemented(&'static str),
    /// 憑據檔沒有可用的 access token（上游核心同樣回 503 `no_credential`）。
    #[error("no schedulable credential")]
    NoCredential,
    /// 憑據檔讀不到或格式不對（上游核心回 502 `invalid_json`）。
    #[error("{0}")]
    InvalidCredential(String),
    /// 連不上上游或傳輸中斷，訊息沿用 HTTP client 的原文。
    #[error("{message}")]
    UpstreamTransport { message: String, sent: UpstreamSent },
    #[error("{message}")]
    WebsocketPolicy {
        code: &'static str,
        message: &'static str,
        sent: UpstreamSent,
    },
    /// 握手遭明示限流。重連可能發生在前一次 create 已寫出之後，不能一律回未送出。
    #[error("upstream websocket handshake rate limited")]
    WebsocketHandshakeRateLimited {
        facts: Box<UpstreamFacts>,
        headers: Vec<(String, String)>,
        sent: UpstreamSent,
    },
    /// 上游回非 2xx。
    #[error("upstream status {}: {}", .0.status, .0.body)]
    Upstream(UpstreamFailure),
    /// WebSocket 上的第一個事件就是 `error`（上游核心同樣轉成 JSON 錯誤，一般錯誤訊息取 `error.message`）。
    /// `headers` 是握手時拿到的 `x-codex-*`，與成功時一樣轉給控制面。
    #[error("{message}")]
    UpstreamEvent {
        /// 明確的輸入錯誤保留欄位路徑及擴充資訊，讓呼叫端能修正原請求。
        request_error: Option<Box<serde_json::Value>>,
        facts: Box<UpstreamFacts>,
        status: u16,
        message: String,
        headers: Vec<(String, String)>,
    },
    // --- ABI 2 共享模式（vm2api 設計第 4.5 節）。除了 `ProxyUnavailable`，都在送上游之前判定。 ---
    /// 登錄尚未對帳（`empty` 或 `reconciling`）：等對帳完成再派。
    #[error("account registry is not ready")]
    RegistryNotReady,
    /// 帳號不在登錄中：控制面重新同步帳號後才可重派。
    #[error("account {0} is not registered")]
    AccountUnknown(String),
    /// 帳號已登錄但停用、或正在刪除排空。
    #[error("account {0} is disabled")]
    AccountDisabled(String),
    /// 推理信封帶的代次或身分與登錄不一致：控制面重新同步後重派。
    #[error("account {account_ref} expects generation {expected}, request carries {actual}")]
    AccountGenerationMismatch {
        account_ref: String,
        expected: u64,
        actual: u64,
    },
    /// 推理信封帶的 `installation_id` 或 `telemetry_runtime` 與登錄不同。設計只寫「回 409」，
    /// 錯誤碼沿用代次不符（處置相同：控制面重新同步後重派），見 `docs/ABI2.md`。
    #[error("account {account_ref}: request {field} differs from the registered account")]
    AccountIdentityMismatch {
        account_ref: String,
        field: &'static str,
    },
    /// PUT 的 CAS 前提不成立，或同代次但內容不同。
    #[error("generation conflict")]
    GenerationConflict,
    /// 代次小於現有代次或墓碑代次（含晚到的舊 DELETE）；不套用。
    #[error("stale generation")]
    StaleGeneration,
    /// 超過該帳號的並發上限；視為容量拒絕，控制面可換槽。
    #[error("account {0} is busy")]
    AccountBusy(String),
    /// 憑據已吊銷、`refresh_token_reused` 或帳號停用（K2c 啟用）。
    #[error("account {0} requires reauthorization")]
    ReauthRequired(String),
    /// 換票暫時失敗（網路、token 端點 5xx、回應無法解析）。帳號不標為需要重新授權，退避後再試。
    /// 換票請求可能已送出而回應遺失，所以帶實際的送出狀態：控制面不得在 `unknown` 時再要求換票，
    /// 否則會用掉已輪換的 refresh token（設計第 4.4 節：以帳號健康的憑據代次確認結果）。
    #[error("token refresh failed for account {account_ref}: {message}")]
    RefreshFailed {
        account_ref: String,
        message: String,
        sent: UpstreamSent,
    },
    /// 熔斷中或代理不可用（K2d 啟用）。連線前就失敗為 `No`，連線後失敗為 `Unknown`。
    #[error("proxy unavailable for account {account_ref}")]
    ProxyUnavailable { account_ref: String, sent: UpstreamSent },
}

/// 與上游核心相同的錯誤碼對應：429 一律 `usage_limit_reached`；401、403 是 `upstream_auth`；其他 `upstream_status`。
pub const fn upstream_code(status: u16) -> &'static str {
    match status {
        429 => "usage_limit_reached",
        401 | 403 => "upstream_auth",
        _ => "upstream_status",
    }
}

/// 上游的非 2xx 回應。狀態碼照轉，錯誤碼依上游核心的對應（設計第 4.6 節）。
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct UpstreamFacts {
    pub stage: &'static str,
    pub status: Option<u16>,
    pub code: Option<String>,
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub request_id: Option<String>,
    pub retry_after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamFailure {
    pub facts: Box<UpstreamFacts>,
    pub status: u16,
    /// 上游回應本體原文（已截斷到合理長度）。控制面依其中的錯誤碼判斷停槽，所以不改寫。
    pub body: String,
    /// HTTP 429 的上游原始額度標頭，交由控制面判讀。
    pub headers: Vec<(String, String)>,
}

impl UpstreamFailure {
    pub const fn code(&self) -> &'static str {
        upstream_code(self.status)
    }
}

impl KernelError {
    pub fn status(&self) -> u16 {
        match self {
            Self::Unauthorized => 401,
            Self::BodyTooLarge => 413,
            Self::InvalidBody(_) | Self::WebsocketPolicy { .. } => 400,
            Self::NotFound => 404,
            Self::NotImplemented(_) => 501,
            Self::NoCredential => 503,
            Self::InvalidCredential(_) | Self::UpstreamTransport { .. } => 502,
            Self::Upstream(failure) => failure.status,
            Self::UpstreamEvent { status, .. } => *status,
            Self::RegistryNotReady => 503,
            Self::AccountUnknown(_) => 404,
            Self::AccountDisabled(_)
            | Self::AccountGenerationMismatch { .. }
            | Self::AccountIdentityMismatch { .. }
            | Self::GenerationConflict
            | Self::StaleGeneration => 409,
            Self::AccountBusy(_) | Self::WebsocketHandshakeRateLimited { .. } => 429,
            Self::ReauthRequired(_) => 401,
            Self::ProxyUnavailable { .. } | Self::RefreshFailed { .. } => 502,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::Unauthorized => "internal_unauthorized",
            Self::BodyTooLarge => "body_too_large",
            Self::InvalidBody(_) => "invalid_request",
            Self::WebsocketPolicy { code, .. } => code,
            Self::WebsocketHandshakeRateLimited { .. } => "rate_limit_exceeded",
            Self::NotFound => "not_found",
            Self::NotImplemented(_) => "not_implemented",
            Self::NoCredential => "no_credential",
            Self::InvalidCredential(_) => "invalid_json",
            Self::UpstreamTransport { .. } => "upstream_transport",
            Self::Upstream(failure) => failure.code(),
            Self::UpstreamEvent { status, .. } => upstream_code(*status),
            Self::RegistryNotReady => "registry_not_ready",
            Self::AccountUnknown(_) => "account_unknown",
            Self::AccountDisabled(_) => "account_disabled",
            Self::AccountGenerationMismatch { .. } | Self::AccountIdentityMismatch { .. } => {
                "account_generation_mismatch"
            }
            Self::GenerationConflict => "generation_conflict",
            Self::StaleGeneration => "stale_generation",
            Self::AccountBusy(_) => "account_busy",
            Self::ReauthRequired(_) => "reauth_required",
            Self::ProxyUnavailable { .. } => "proxy_unavailable",
            Self::RefreshFailed { .. } => "refresh_failed",
        }
    }

    pub const fn kind(&self) -> ErrorKind {
        match self {
            Self::Unauthorized | Self::ReauthRequired(_) => ErrorKind::AuthenticationError,
            Self::WebsocketPolicy { .. }
            | Self::BodyTooLarge
            | Self::InvalidBody(_)
            | Self::NotFound
            | Self::AccountUnknown(_) => ErrorKind::InvalidRequestError,
            Self::NotImplemented(_)
            | Self::NoCredential
            | Self::InvalidCredential(_)
            | Self::UpstreamTransport { .. }
            | Self::WebsocketHandshakeRateLimited { .. }
            | Self::Upstream(_)
            | Self::UpstreamEvent { .. }
            | Self::RegistryNotReady
            | Self::AccountDisabled(_)
            | Self::AccountGenerationMismatch { .. }
            | Self::AccountIdentityMismatch { .. }
            | Self::GenerationConflict
            | Self::StaleGeneration
            | Self::AccountBusy(_)
            | Self::ProxyUnavailable { .. }
            | Self::RefreshFailed { .. } => ErrorKind::ApiError,
        }
    }

    /// 每一種錯誤都明確決定送出狀態；新增錯誤時編譯器會要求在這裡補上。
    pub const fn upstream_sent(&self) -> UpstreamSent {
        match self {
            Self::Unauthorized
            | Self::BodyTooLarge
            | Self::InvalidBody(_)
            | Self::NotFound
            | Self::NotImplemented(_)
            | Self::NoCredential
            | Self::InvalidCredential(_)
            | Self::RegistryNotReady
            | Self::AccountUnknown(_)
            | Self::AccountDisabled(_)
            | Self::AccountGenerationMismatch { .. }
            | Self::AccountIdentityMismatch { .. }
            | Self::GenerationConflict
            | Self::StaleGeneration
            | Self::AccountBusy(_)
            | Self::ReauthRequired(_) => UpstreamSent::No,
            Self::WebsocketPolicy { sent, .. }
            | Self::WebsocketHandshakeRateLimited { sent, .. }
            | Self::UpstreamTransport { sent, .. }
            | Self::ProxyUnavailable { sent, .. }
            | Self::RefreshFailed { sent, .. } => *sent,
            Self::Upstream(_) | Self::UpstreamEvent { .. } => UpstreamSent::Yes,
        }
    }

    /// 上游的 `retry-after` 照轉給控制面。
    pub fn retry_after(&self) -> Option<&str> {
        self.upstream_facts()?.retry_after.as_deref()
    }

    /// 錯誤回應上另外帶的上游標頭。
    pub fn extra_headers(&self) -> &[(String, String)] {
        match self {
            Self::Upstream(f) => &f.headers,
            Self::UpstreamEvent { headers, .. } | Self::WebsocketHandshakeRateLimited { headers, .. } => {
                headers
            }
            _ => &[],
        }
    }

    pub fn upstream_facts(&self) -> Option<&UpstreamFacts> {
        match self {
            Self::Upstream(f) => Some(&f.facts),
            Self::UpstreamEvent { facts, .. } | Self::WebsocketHandshakeRateLimited { facts, .. } => {
                Some(facts)
            }
            _ => None,
        }
    }

    pub fn body(&self) -> ErrorBody<'_> {
        let error = match self {
            Self::UpstreamEvent {
                request_error: Some(detail),
                ..
            } => ErrorDetail::Upstream(detail),
            _ => ErrorDetail::Kernel {
                code: self.code(),
                message: self.to_string(),
                kind: self.kind(),
            },
        };
        ErrorBody { error }
    }
}

/// 上游明確回報輸入錯誤時保留欄位，例如指出錯誤位置的 `param`。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ErrorBody<'a> {
    pub error: ErrorDetail<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum ErrorDetail<'a> {
    Upstream(&'a serde_json::Value),
    Kernel {
        code: &'static str,
        message: String,
        #[serde(rename = "type")]
        kind: ErrorKind,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upstream(status: u16) -> KernelError {
        KernelError::Upstream(UpstreamFailure {
            status,
            headers: Vec::new(),
            body: r#"{"error":{"code":"x"}}"#.to_owned(),
            facts: Box::new(UpstreamFacts {
                retry_after: Some("7".to_owned()),
                ..Default::default()
            }),
        })
    }

    #[test]
    fn upstream_failures_keep_the_status_and_the_upstream_text() {
        let err = upstream(429);
        assert_eq!(err.status(), 429);
        assert_eq!(err.code(), "usage_limit_reached");
        assert_eq!(err.to_string(), r#"upstream status 429: {"error":{"code":"x"}}"#);
        assert_eq!(err.retry_after(), Some("7"));
        assert_eq!(err.upstream_sent(), UpstreamSent::Yes);
        assert_eq!(upstream(401).code(), "upstream_auth");
        assert_eq!(upstream(403).code(), "upstream_auth");
        assert_eq!(upstream(503).code(), "upstream_status");
    }

    #[test]
    fn websocket_error_events_use_the_status_mapping_and_the_plain_message() {
        let err = KernelError::UpstreamEvent {
            request_error: None,
            facts: Box::default(),
            status: 400,
            message: "bad thing".to_owned(),
            headers: vec![("x-codex-primary-used-percent".to_owned(), "12".to_owned())],
        };
        assert_eq!((err.status(), err.code()), (400, "upstream_status"));
        assert_eq!(err.to_string(), "bad thing");
        assert_eq!(err.upstream_sent(), UpstreamSent::Yes);
        assert_eq!(err.extra_headers().len(), 1);
    }

    #[test]
    fn credential_errors_match_the_upstream_kernel_and_are_never_sent() {
        assert_eq!(KernelError::NoCredential.status(), 503);
        assert_eq!(KernelError::NoCredential.code(), "no_credential");
        assert_eq!(KernelError::NoCredential.to_string(), "no schedulable credential");
        let invalid = KernelError::InvalidCredential("bad".to_owned());
        assert_eq!((invalid.status(), invalid.code()), (502, "invalid_json"));
        assert_eq!(invalid.upstream_sent(), UpstreamSent::No);
    }

    /// vm2api 設計第 4.5 節的表，逐列鎖住：狀態碼、錯誤碼與送出狀態。
    #[test]
    fn shared_mode_errors_follow_the_design_table() {
        let a = || "vm-1".to_owned();
        let cases = [
            (
                KernelError::RegistryNotReady,
                503,
                "registry_not_ready",
                UpstreamSent::No,
            ),
            (
                KernelError::AccountUnknown(a()),
                404,
                "account_unknown",
                UpstreamSent::No,
            ),
            (
                KernelError::AccountDisabled(a()),
                409,
                "account_disabled",
                UpstreamSent::No,
            ),
            (
                KernelError::AccountGenerationMismatch {
                    account_ref: a(),
                    expected: 2,
                    actual: 1,
                },
                409,
                "account_generation_mismatch",
                UpstreamSent::No,
            ),
            (
                KernelError::AccountIdentityMismatch {
                    account_ref: a(),
                    field: "installation_id",
                },
                409,
                "account_generation_mismatch",
                UpstreamSent::No,
            ),
            (
                KernelError::GenerationConflict,
                409,
                "generation_conflict",
                UpstreamSent::No,
            ),
            (
                KernelError::StaleGeneration,
                409,
                "stale_generation",
                UpstreamSent::No,
            ),
            (
                KernelError::AccountBusy(a()),
                429,
                "account_busy",
                UpstreamSent::No,
            ),
            (
                KernelError::ReauthRequired(a()),
                401,
                "reauth_required",
                UpstreamSent::No,
            ),
            (
                KernelError::RefreshFailed {
                    account_ref: a(),
                    message: "timeout".to_owned(),
                    sent: UpstreamSent::Unknown,
                },
                502,
                "refresh_failed",
                UpstreamSent::Unknown,
            ),
            (
                KernelError::ProxyUnavailable {
                    account_ref: a(),
                    sent: UpstreamSent::No,
                },
                502,
                "proxy_unavailable",
                UpstreamSent::No,
            ),
            (
                KernelError::ProxyUnavailable {
                    account_ref: a(),
                    sent: UpstreamSent::Unknown,
                },
                502,
                "proxy_unavailable",
                UpstreamSent::Unknown,
            ),
        ];
        for (err, status, code, sent) in cases {
            assert_eq!(
                (err.status(), err.code(), err.upstream_sent()),
                (status, code, sent),
                "{err:?}"
            );
        }
    }

    #[test]
    fn body_uses_the_control_plane_shape() {
        let body = serde_json::to_value(KernelError::BodyTooLarge.body()).unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "error": {
                    "code": "body_too_large",
                    "message": "request body exceeds max_request_bytes",
                    "type": "invalid_request_error"
                }
            })
        );
    }
}
