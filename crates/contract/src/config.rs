//! 單帳號相容模式（ABI 1）的設定檔格式。
//!
//! 欄位與 vm2api `writeCodexKernelConfig` 寫出的 `run/codex-kernel.json` 一致，
//! 讓本核心可以直接取代上游 `kin-codex-kernel`，控制面不必改程式。
//! 未知欄位一律接受並忽略：上游日後新增欄位時，不能因此拒絕啟動。
//! 這裡只做解析與驗證，讀檔屬於執行檔的責任。

use std::path::PathBuf;

use serde::Deserialize;

/// 官方 Codex CLI 使用的 Responses 位址（ChatGPT 帳號）。
pub const CHATGPT_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";

/// 官方 Codex CLI 換票的 token 端點（`codex-rs/login/src/auth/manager.rs` 的 `REFRESH_TOKEN_URL`）。
pub const OAUTH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";

/// 控制面沒有設定請求上限時的預設值，與 vm2api `DEFAULT_MAX_REQUEST_BYTES` 相同。
pub const DEFAULT_MAX_REQUEST_BYTES: u64 = 32 * 1024 * 1024;

/// 上游傳輸方式，對應 vm2api `CODEX_UPSTREAM_TRANSPORTS`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamTransport {
    #[default]
    PreferWebsocket,
    Http,
}

/// 遙測用的執行環境身分（vm2api `codexKernelRequestOptions` 的 `telemetry_runtime`）。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TelemetryRuntime {
    pub runtime_os: String,
    pub runtime_os_version: String,
    pub runtime_arch: String,
}

/// 實體代理的資源限制。0 表示不限制；部署依共用出口容量設定，不限制帳號准入。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct WsLimits {
    pub proxy_connecting: u32,
    pub proxy_connections: u32,
    /// 可獨立重放的完整請求，建連到此期限仍未寫入就使用 HTTP；0 保持原有等待。
    pub fallback_after_ms: u32,
    pub read_idle_ms: u32,
}

impl Default for WsLimits {
    fn default() -> Self {
        Self {
            proxy_connecting: 0,
            proxy_connections: 0,
            fallback_after_ms: 0,
            read_idle_ms: 300_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SingleAccountConfig {
    pub vm_id: String,
    pub device_id: String,
    pub socket_path: PathBuf,
    pub credential_path: PathBuf,
    #[serde(default)]
    pub proxy_url: String,
    #[serde(default)]
    pub proxy_required: bool,
    pub internal_token: String,
    #[serde(default)]
    pub ws_limits: WsLimits,
    #[serde(default)]
    pub max_request_bytes: u64,
    #[serde(default)]
    pub upstream_transport: UpstreamTransport,
    #[serde(default)]
    pub user_agent: Option<String>,
    #[serde(default)]
    pub client_features: bool,
    #[serde(default)]
    pub analytics: bool,
    #[serde(default)]
    pub telemetry_runtime: Option<TelemetryRuntime>,
    #[serde(default)]
    pub test_endpoints: bool,
    /// 只在 `test_endpoints` 開啟時生效，讓契約測試把上游指向受控的假上游（與上游核心相同的欄位）。
    #[serde(default)]
    pub chatgpt_base_url: Option<String>,
    #[serde(default)]
    pub oauth_token_url: Option<String>,
}

/// 自訂 User-Agent 的長度上限（位元組）。
pub const MAX_USER_AGENT_BYTES: usize = 512;

/// `user_agent`（設定檔或請求選項）的三種狀態。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserAgentChoice<'a> {
    /// 缺欄位、空字串或只有空白：沿用下一層的設定。
    Unset,
    /// `auto`（不分大小寫）：依帳號環境產生官方身分。
    Auto,
    /// 自訂值（已去掉頭尾空白）。
    Custom(&'a str),
}

impl<'a> UserAgentChoice<'a> {
    pub fn parse(value: Option<&'a str>) -> Self {
        match value.map(str::trim) {
            None | Some("") => Self::Unset,
            Some(value) if value.eq_ignore_ascii_case("auto") => Self::Auto,
            Some(value) => Self::Custom(value),
        }
    }
}

/// 自訂 User-Agent 能否原樣放進 HTTP 標頭：不超過上限，位元組規則與 `http::HeaderValue` 相同（不含控制字元）。
pub fn valid_user_agent(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_USER_AGENT_BYTES
        && value.bytes().all(|b| b == b'\t' || (b >= 0x20 && b != 0x7f))
}

/// 官方的安裝 id 是 UUID（8-4-4-4-12 的十六進位）。
pub fn looks_like_uuid(value: &str) -> bool {
    let groups: Vec<&str> = value.split('-').collect();
    groups.len() == 5
        && groups
            .iter()
            .zip([8, 4, 4, 4, 12])
            .all(|(group, len)| group.len() == len && group.bytes().all(|b| b.is_ascii_hexdigit()))
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("設定檔不是合法的 JSON：{0}")]
    Parse(#[from] serde_json::Error),
    #[error("設定檔欄位 {0} 不能是空值")]
    Empty(&'static str),
    /// 值無法使用（例如 User-Agent 含控制字元、`client_features` 需要 UUID 形式的 `device_id`）。
    /// 在啟動時拒絕，不讓每個請求才各自失敗。
    #[error("設定檔欄位 {0} 不合法")]
    Invalid(&'static str),
    /// 與上游核心相同：要求走代理卻沒有代理時拒絕啟動，不退回直連。
    #[error("proxy required")]
    ProxyRequired,
}

/// 共享模式（ABI 2）的程序層設定：`kin-codex-kernel --shared <shared.json>`。
///
/// 只有程序本身的東西；帳號一律由控制面經 `/internal/v1/accounts` 登錄，不寫在這裡。
#[derive(Debug, Clone, Deserialize)]
pub struct SharedConfig {
    pub socket_path: PathBuf,
    pub internal_token: String,
    #[serde(default)]
    pub ws_limits: WsLimits,
    /// 登錄的帳號數上限，超過時 PUT 與 reconcile 回 `invalid_request`。
    #[serde(default = "default_max_accounts")]
    pub max_accounts: usize,
    #[serde(default)]
    pub test_endpoints: bool,
    /// 帳號 × 出口熔斷的冷卻時間（毫秒）；斷開後經過這麼久才放一個探測請求。
    #[serde(default = "default_breaker_cooldown_ms")]
    pub breaker_cooldown_ms: u64,
    /// 與單帳號模式相同，只在 `test_endpoints` 開啟時生效。
    #[serde(default)]
    pub chatgpt_base_url: Option<String>,
    #[serde(default)]
    pub oauth_token_url: Option<String>,
}

/// 帳號數的預設上限：遠大於一台控制面實際的 Codex 槽數，只用來防止資源耗盡。
pub const DEFAULT_MAX_ACCOUNTS: usize = 1024;

const fn default_max_accounts() -> usize {
    DEFAULT_MAX_ACCOUNTS
}

/// 熔斷冷卻時間的預設值。
pub const DEFAULT_BREAKER_COOLDOWN_MS: u64 = 30_000;

const fn default_breaker_cooldown_ms() -> u64 {
    DEFAULT_BREAKER_COOLDOWN_MS
}

impl SharedConfig {
    pub fn from_slice(raw: &[u8]) -> Result<Self, ConfigError> {
        let config: Self = serde_json::from_slice(raw)?;
        if config.internal_token.trim().is_empty() {
            return Err(ConfigError::Empty("internal_token"));
        }
        if config.socket_path.as_os_str().is_empty() {
            return Err(ConfigError::Empty("socket_path"));
        }
        if config.max_accounts == 0 {
            return Err(ConfigError::Empty("max_accounts"));
        }
        Ok(config)
    }

    pub fn chatgpt_base_url(&self) -> &str {
        base_url(self.test_endpoints, self.chatgpt_base_url.as_deref())
    }

    /// 換票端點。正式設定一律是 auth.openai.com；`test_endpoints` 開啟時才允許覆寫。
    pub fn oauth_token_url(&self) -> &str {
        match self.oauth_token_url.as_deref().map(str::trim) {
            Some(url) if self.test_endpoints && !url.is_empty() => url,
            _ => OAUTH_TOKEN_URL,
        }
    }
}

/// 正式設定一律是 chatgpt.com；`test_endpoints` 開啟時才允許覆寫。
fn base_url(test_endpoints: bool, configured: Option<&str>) -> &str {
    match configured.map(str::trim) {
        Some(url) if test_endpoints && !url.is_empty() => url.trim_end_matches('/'),
        _ => CHATGPT_BASE_URL,
    }
}

impl SingleAccountConfig {
    pub fn from_slice(raw: &[u8]) -> Result<Self, ConfigError> {
        let config: Self = serde_json::from_slice(raw)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let required = [
            ("vm_id", self.vm_id.as_str()),
            ("device_id", self.device_id.as_str()),
            ("internal_token", self.internal_token.as_str()),
        ];
        for (name, value) in required {
            if value.trim().is_empty() {
                return Err(ConfigError::Empty(name));
            }
        }
        if self.socket_path.as_os_str().is_empty() {
            return Err(ConfigError::Empty("socket_path"));
        }
        if self.proxy_required && self.proxy_url.trim().is_empty() {
            return Err(ConfigError::ProxyRequired);
        }
        if let UserAgentChoice::Custom(user_agent) = UserAgentChoice::parse(self.user_agent.as_deref())
            && !valid_user_agent(user_agent)
        {
            return Err(ConfigError::Invalid("user_agent"));
        }
        // 合成的 client_metadata 以 device_id 當安裝 id；官方的安裝 id 是 UUID。
        if self.client_features && !looks_like_uuid(&self.device_id) {
            return Err(ConfigError::Invalid("device_id"));
        }
        Ok(())
    }

    /// 設定檔的自訂 User-Agent；`auto`、空值或缺欄位時為 `None`（依帳號環境產生）。
    pub fn custom_user_agent(&self) -> Option<&str> {
        match UserAgentChoice::parse(self.user_agent.as_deref()) {
            UserAgentChoice::Custom(user_agent) => Some(user_agent),
            UserAgentChoice::Unset | UserAgentChoice::Auto => None,
        }
    }

    /// Responses 的上游位址。正式設定一律是 chatgpt.com；`test_endpoints` 開啟時才允許覆寫。
    pub fn chatgpt_base_url(&self) -> &str {
        base_url(self.test_endpoints, self.chatgpt_base_url.as_deref())
    }

    /// `max_request_bytes` 為 0 表示控制面沒有設定，沿用控制面的預設值。
    pub fn request_limit(&self) -> usize {
        let limit = if self.max_request_bytes == 0 {
            DEFAULT_MAX_REQUEST_BYTES
        } else {
            self.max_request_bytes
        };
        usize::try_from(limit).unwrap_or(usize::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample() -> serde_json::Value {
        json!({
            "vm_id": "vm-test",
            "device_id": "0f3c2b1a-1111-4222-8333-444455556666",
            "socket_path": "/tmp/codex-kernel.sock",
            "credential_path": "/tmp/codex-credentials.json",
            "proxy_url": "socks5h://127.0.0.1:1080",
            "proxy_required": true,
            "internal_token": "token",
            "max_request_bytes": 1024,
            "upstream_transport": "http",
            "user_agent": "codex_cli_rs/0.167.1 (linux x86_64)",
            "client_features": false,
            "analytics": true,
            "telemetry_runtime": {
                "runtime_os": "linux",
                "runtime_os_version": "24.04",
                "runtime_arch": "x86_64"
            },
            "test_endpoints": false,
            "future_field_from_upstream": { "anything": 1 }
        })
    }

    #[test]
    fn parses_the_upstream_config_shape_and_ignores_unknown_fields() {
        let config = SingleAccountConfig::from_slice(&serde_json::to_vec(&sample()).unwrap()).unwrap();
        assert_eq!(config.vm_id, "vm-test");
        assert_eq!(config.upstream_transport, UpstreamTransport::Http);
        assert!(config.proxy_required);
        assert_eq!(config.telemetry_runtime.as_ref().unwrap().runtime_arch, "x86_64");
        assert_eq!(config.request_limit(), 1024);
    }

    #[test]
    fn rejects_empty_required_fields() {
        let mut raw = sample();
        raw["internal_token"] = json!("  ");
        let err = SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap_err();
        assert!(err.to_string().contains("internal_token"));
    }

    #[test]
    fn zero_limit_falls_back_to_the_control_plane_default() {
        let mut raw = sample();
        raw["max_request_bytes"] = json!(0);
        let config = SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap();
        assert_eq!(config.request_limit(), 32 * 1024 * 1024);
    }

    #[test]
    fn a_required_proxy_must_be_configured() {
        let mut raw = sample();
        raw["proxy_url"] = json!(" ");
        let err = SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap_err();
        assert_eq!(err.to_string(), "proxy required");
        raw["proxy_required"] = json!(false);
        assert!(SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).is_ok());
    }

    #[test]
    fn test_endpoints_are_honoured_only_when_enabled() {
        let mut raw = sample();
        raw["chatgpt_base_url"] = json!("http://127.0.0.1:9/backend-api/codex/");
        let config = SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap();
        assert_eq!(config.chatgpt_base_url(), CHATGPT_BASE_URL);
        raw["test_endpoints"] = json!(true);
        let config = SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap();
        assert_eq!(config.chatgpt_base_url(), "http://127.0.0.1:9/backend-api/codex");
    }

    #[test]
    fn shared_config_needs_a_socket_and_a_token_and_bounds_the_accounts() {
        let parsed =
            SharedConfig::from_slice(br#"{"socket_path":"/run/k.sock","internal_token":"t"}"#).unwrap();
        assert_eq!(parsed.max_accounts, DEFAULT_MAX_ACCOUNTS);
        assert_eq!(parsed.chatgpt_base_url(), CHATGPT_BASE_URL);
        let err =
            SharedConfig::from_slice(br#"{"socket_path":"/run/k.sock","internal_token":" "}"#).unwrap_err();
        assert!(err.to_string().contains("internal_token"));
        assert!(
            SharedConfig::from_slice(
                br#"{"socket_path":"/run/k.sock","internal_token":"t","max_accounts":0}"#
            )
            .is_err()
        );
        let test = SharedConfig::from_slice(
            br#"{"socket_path":"/s","internal_token":"t","test_endpoints":true,"chatgpt_base_url":"http://127.0.0.1:9/x/"}"#,
        )
        .unwrap();
        assert_eq!(test.chatgpt_base_url(), "http://127.0.0.1:9/x");
    }

    #[test]
    fn user_agent_choices_and_startup_validation() {
        assert_eq!(UserAgentChoice::parse(None), UserAgentChoice::Unset);
        assert_eq!(UserAgentChoice::parse(Some("  ")), UserAgentChoice::Unset);
        assert_eq!(UserAgentChoice::parse(Some(" AUTO ")), UserAgentChoice::Auto);
        assert_eq!(
            UserAgentChoice::parse(Some(" project/1 ")),
            UserAgentChoice::Custom("project/1")
        );
        assert!(valid_user_agent("project/1 (x; y)\tz"));
        assert!(!valid_user_agent("bad\r\nheader"));
        assert!(!valid_user_agent(&"a".repeat(MAX_USER_AGENT_BYTES + 1)));

        let mut raw = sample();
        raw["user_agent"] = json!("bad\nua");
        let err = SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap_err();
        assert!(err.to_string().contains("user_agent"));
        raw["user_agent"] = json!("Auto");
        let config = SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap();
        assert_eq!(config.custom_user_agent(), None);
        raw["user_agent"] = json!(" project/1 ");
        let config = SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap();
        assert_eq!(config.custom_user_agent(), Some("project/1"));
    }

    #[test]
    fn client_features_need_a_uuid_device_id() {
        let mut raw = sample();
        raw["client_features"] = json!(true);
        assert!(SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).is_ok());
        raw["device_id"] = json!("vm-25");
        let err = SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap_err();
        assert!(err.to_string().contains("device_id"));
        raw["client_features"] = json!(false);
        assert!(SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).is_ok());
    }

    #[test]
    fn missing_transport_defaults_to_prefer_websocket() {
        let mut raw = sample();
        raw.as_object_mut().unwrap().remove("upstream_transport");
        let config = SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap();
        assert_eq!(config.upstream_transport, UpstreamTransport::PreferWebsocket);
    }
}
