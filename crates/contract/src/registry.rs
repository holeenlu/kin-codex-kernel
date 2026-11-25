//! 共享模式（ABI 2）的帳號登錄、查詢與健康型別（vm2api `docs/CODEX_SHARED_KERNEL_DESIGN.md` 第 4.2–4.4 節）。
//!
//! 兩種代次分開計數：
//! - `env_generation`：帳號環境（出口、身分、上限、啟用狀態）的代次，由控制面維護、單調遞增；
//! - `credential_generation`：憑據內容的代次，由憑據的唯一寫入者遞增（K2c）。
//!
//! 登錄不持久化：核心啟動時 [`RegistryState::Empty`]，控制面送一次 reconcile 之後才是 `Ready`。

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::config::{DEFAULT_MAX_REQUEST_BYTES, TelemetryRuntime};

/// 帳號參照最長的位元組數。
pub const MAX_ACCOUNT_REF_LEN: usize = 64;

/// 帳號參照只允許檔名安全的字元（與 vm2api 的槽 ID 相同），並限制長度。
/// `reconcile` 保留給 `POST /internal/v1/accounts/reconcile`，不能當帳號參照。
pub fn valid_account_ref(value: &str) -> bool {
    value != "reconcile"
        && !value.is_empty()
        && value.len() <= MAX_ACCOUNT_REF_LEN
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// `PUT /internal/v1/accounts/{ref}` 的本體，也是 reconcile 清單的一筆。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AccountEnv {
    /// 必須與路徑上的 `{ref}` 相同。
    #[serde(rename = "ref")]
    pub account_ref: String,
    /// 這次要套用的環境代次。
    pub env_generation: u64,
    /// CAS 前提：控制面認為核心目前的代次。建立時省略或為 0；reconcile 不看這個值。
    #[serde(default)]
    pub expected_env_generation: u64,
    /// 停用時核心保留登錄但不接新請求（回 `account_disabled`）。
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub credential_path: PathBuf,
    /// 這個帳號的安裝身分，也是 `x-codex-installation-id` 與下游 id 改寫的目標。
    pub device_id: String,
    #[serde(default)]
    pub telemetry_runtime: Option<TelemetryRuntime>,
    /// 空字串表示直連；`proxy_required` 為 true 時不可為空。
    #[serde(default)]
    pub proxy_url: String,
    #[serde(default)]
    pub proxy_required: bool,
    #[serde(default = "default_max_request_bytes")]
    pub max_request_bytes: u64,
    /// 這個帳號同時在途的推理請求上限；超過回 `account_busy`。
    pub max_concurrency: u32,
}

const fn default_true() -> bool {
    true
}

const fn default_max_request_bytes() -> u64 {
    DEFAULT_MAX_REQUEST_BYTES
}

impl AccountEnv {
    /// 冪等判斷用：除了 CAS 前提 `expected_env_generation` 之外的內容是否完全相同。
    /// ACK 遺失時控制面原樣重送，前提值不變；但兩筆前提不同、內容相同的請求也視為同一筆。
    pub fn same_content(&self, other: &Self) -> bool {
        Self {
            expected_env_generation: 0,
            ..self.clone()
        } == Self {
            expected_env_generation: 0,
            ..other.clone()
        }
    }
}

/// `PUT /internal/v1/accounts/{ref}/credential` 的本體：匯入或替換憑據，由核心寫入（唯一寫入者）。
#[derive(Debug, Clone, Deserialize)]
pub struct CredentialPut {
    /// 憑據檔 `accounts[0]` 的內容：`access_token`、`refresh_token`、`id_token`、`chatgpt_account_id`、`email` 等。
    pub credential: serde_json::Map<String, serde_json::Value>,
    /// CAS 前提：控制面認為憑據檔目前的代次；檔案不存在時為 0。
    #[serde(default)]
    pub expected_credential_generation: u64,
}

/// 憑據匯入成功的回應。不回傳任何 token。
#[derive(Debug, Clone, Serialize)]
pub struct CredentialAck {
    pub ok: bool,
    #[serde(rename = "ref")]
    pub account_ref: String,
    pub credential_generation: u64,
}

/// `POST /internal/v1/accounts/{ref}/ops/refresh` 的回應：只有結果，不回 access／refresh／id token。
#[derive(Debug, Clone, Serialize)]
pub struct RefreshAck {
    pub ok: bool,
    #[serde(rename = "ref")]
    pub account_ref: String,
    /// 新 access token 的到期時間（Unix 毫秒）；讀不到時為 `null`。
    pub expires_at: Option<u64>,
    pub credential_generation: u64,
}

/// `DELETE /internal/v1/accounts/{ref}` 的本體。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct AccountDelete {
    /// 刪除時的環境代次；小於現有代次就是晚到的舊命令，回 `stale_generation`。
    pub env_generation: u64,
}

/// `POST /internal/v1/accounts/reconcile` 的本體：完整帳號清單，核心以它取代整個登錄。
#[derive(Debug, Clone, Deserialize)]
pub struct ReconcileRequest {
    pub accounts: Vec<AccountEnv>,
}

/// 登錄的整體狀態。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistryState {
    /// 剛啟動、尚未對帳。所有推理與帳號操作回 `registry_not_ready`。
    Empty,
    /// 對帳進行中，同上。
    Reconciling,
    Ready,
}

/// 單一帳號的服務狀態。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountState {
    Ready,
    /// `enabled: false`。
    Disabled,
    /// 已收到 DELETE，等在途請求結束；不接新請求。
    Draining,
    /// 憑據需要重新授權、或代理不可用。
    Failing,
}

/// 憑據狀態（設計第 4.2 節）。K2b 只會回 `ok` 或 `missing`；刷新相關的值在 K2c 啟用。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialStatus {
    Ok,
    Refreshing,
    ReauthRequired,
    Missing,
}

/// 熔斷器狀態（設計第 5.3 節）。K2b 一律 `closed`；熔斷在 K2d 啟用。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Breakers {
    pub http: BreakerState,
    pub websocket: BreakerState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LastError {
    pub code: &'static str,
    /// Unix 毫秒。
    pub at: u64,
}

/// `PUT` 與 `DELETE` 成功時的回應。
#[derive(Debug, Clone, Serialize)]
pub struct AccountAck {
    pub ok: bool,
    #[serde(rename = "ref")]
    pub account_ref: String,
    pub env_generation: u64,
}

/// `DELETE` 成功時的回應。`inflight > 0` 表示仍在排空，結束後自動移除。
#[derive(Debug, Clone, Serialize)]
pub struct DeleteAck {
    pub ok: bool,
    #[serde(rename = "ref")]
    pub account_ref: String,
    pub tombstone_generation: u64,
    pub inflight: u32,
}

/// `GET /internal/v1/accounts` 的一筆。
#[derive(Debug, Clone, Serialize)]
pub struct AccountSummary {
    #[serde(rename = "ref")]
    pub account_ref: String,
    pub env_generation: u64,
    /// K2c 之前沒有憑據代次。
    pub credential_generation: Option<u64>,
    pub state: AccountState,
}

/// `GET /internal/v1/accounts` 的回應。
#[derive(Debug, Clone, Serialize)]
pub struct AccountList {
    pub registry_state: RegistryState,
    pub accounts: Vec<AccountSummary>,
}

/// `POST /internal/v1/accounts/reconcile` 的回應。
#[derive(Debug, Clone, Serialize)]
pub struct ReconcileAck {
    pub ok: bool,
    pub registry_state: RegistryState,
    pub accounts: usize,
}

/// `GET /internal/v1/accounts/{ref}/health` 的回應。不含任何秘密。
#[derive(Debug, Clone, Serialize)]
pub struct AccountHealth {
    pub ok: bool,
    #[serde(rename = "ref")]
    pub account_ref: String,
    pub state: AccountState,
    pub env_generation: u64,
    pub credential_generation: Option<u64>,
    pub credential: CredentialStatus,
    pub proxy_ok: bool,
    pub breaker: Breakers,
    pub inflight: u32,
    pub last_error: Option<LastError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AccountCounts {
    pub total: usize,
    pub ready: usize,
    pub failing: usize,
}

/// 共享模式的 `GET /internal/health`。刻意沒有 `status` 欄位（理由見 [`crate::status::Health`]）。
#[derive(Debug, Clone, Serialize)]
pub struct SharedHealth {
    pub ok: bool,
    pub engine: &'static str,
    pub provider: &'static str,
    pub version: &'static str,
    pub worker_version: &'static str,
    pub build_sha: &'static str,
    pub abi: crate::abi::Abi,
    pub registry_state: RegistryState,
    pub accounts: AccountCounts,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(raw: &str) -> AccountEnv {
        serde_json::from_str(raw).unwrap()
    }

    #[test]
    fn parses_the_design_example_and_fills_defaults() {
        let parsed = env(
            r#"{"ref":"vm-25","env_generation":8,"expected_env_generation":7,"enabled":true,
            "credential_path":"/data/vms/vm-25/codex-credentials.shared.json","device_id":"d-1",
            "telemetry_runtime":{"runtime_os":"linux","runtime_os_version":"24.04","runtime_arch":"x86_64"},
            "proxy_url":"socks5h://u:p@h:1","proxy_required":true,"max_request_bytes":33554432,"max_concurrency":5}"#,
        );
        assert_eq!(parsed.account_ref, "vm-25");
        assert_eq!((parsed.env_generation, parsed.expected_env_generation), (8, 7));

        let minimal = env(
            r#"{"ref":"a","env_generation":1,"credential_path":"/c","device_id":"d","max_concurrency":1}"#,
        );
        assert!(minimal.enabled);
        assert_eq!(minimal.expected_env_generation, 0);
        assert_eq!(minimal.max_request_bytes, DEFAULT_MAX_REQUEST_BYTES);
        assert!(minimal.proxy_url.is_empty());
    }

    #[test]
    fn same_content_ignores_only_the_cas_precondition() {
        let base =
            r#""ref":"a","env_generation":2,"credential_path":"/c","device_id":"d","max_concurrency":1"#;
        let first = env(&format!(r#"{{{base},"expected_env_generation":1}}"#));
        let resent = env(&format!(r#"{{{base},"expected_env_generation":1}}"#));
        let other_precondition = env(&format!(r#"{{{base},"expected_env_generation":0}}"#));
        let changed = env(&format!(
            r#"{{{base},"expected_env_generation":1,"proxy_url":"socks5h://h:1"}}"#
        ));
        assert!(first.same_content(&resent));
        assert!(first.same_content(&other_precondition));
        assert!(!first.same_content(&changed));
    }

    #[test]
    fn account_refs_are_filename_safe() {
        assert!(valid_account_ref("vm-25"));
        assert!(valid_account_ref("gw_01"));
        assert!(!valid_account_ref(""));
        assert!(!valid_account_ref("reconcile"));
        assert!(!valid_account_ref("../etc"));
        assert!(!valid_account_ref("a b"));
        assert!(!valid_account_ref(&"x".repeat(MAX_ACCOUNT_REF_LEN + 1)));
    }

    #[test]
    fn serializes_ref_and_never_a_status_field() {
        let health = AccountHealth {
            ok: true,
            account_ref: "vm-1".to_owned(),
            state: AccountState::Ready,
            env_generation: 3,
            credential_generation: None,
            credential: CredentialStatus::Ok,
            proxy_ok: true,
            breaker: Breakers {
                http: BreakerState::Closed,
                websocket: BreakerState::HalfOpen,
            },
            inflight: 0,
            last_error: None,
        };
        let value = serde_json::to_value(health).unwrap();
        assert_eq!(value["ref"], "vm-1");
        assert!(value.get("account_ref").is_none());
        assert_eq!(value["breaker"]["websocket"], "half_open");
        assert!(value.get("status").is_none());

        let shared = SharedHealth {
            ok: true,
            engine: "codex",
            provider: "kin-codex-kernel",
            version: "0",
            worker_version: "0",
            build_sha: "dev",
            abi: crate::abi::Abi::Shared,
            registry_state: RegistryState::Empty,
            accounts: AccountCounts {
                total: 0,
                ready: 0,
                failing: 0,
            },
        };
        let value = serde_json::to_value(shared).unwrap();
        assert_eq!(value["abi"], 2);
        assert_eq!(value["registry_state"], "empty");
        assert!(value.get("status").is_none());
    }
}
