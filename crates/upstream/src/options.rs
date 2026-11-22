//! 平台的逐請求設定：自訂 User-Agent、`client_metadata` 合成，以及會話的分析事件。
//!
//! - 本體與 `x-codex-turn-metadata` 只做位元組級編修（[`crate::json_object`]）：其他欄位的位元組與順序不變。
//! - 分析事件只用該帳號的 client 與出口，與推理獨立，絕不重試推理；熔斷斷開時不撥號。
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderValue, USER_AGENT};
use serde::Serialize;
use serde_json::value::RawValue;
use tap_contract::config::{
    SingleAccountConfig, TelemetryRuntime, UserAgentChoice, looks_like_uuid, valid_user_agent,
};
use tap_contract::envelope::ResponsesEnvelope;
use tap_contract::error::KernelError;
use tap_credentials::Credential;
use tap_identity::{Identity, Profile};
use tokio::sync::Semaphore;

use crate::breaker::{Breaker, Ticket};
use crate::scrub::Scrubbed;
use crate::{HttpUpstream, json_object};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct Defaults {
    pub(super) runtime: Option<TelemetryRuntime>,
    /// 設定檔的自訂 UA（啟動時已驗證）；`None` 表示依帳號環境產生。
    pub(super) user_agent: Option<String>,
    pub(super) features: bool,
    pub(super) analytics: bool,
}

impl Defaults {
    pub(super) fn single(c: &SingleAccountConfig) -> Self {
        Self {
            runtime: c.telemetry_runtime.clone(),
            user_agent: c.custom_user_agent().map(str::to_owned),
            features: c.client_features,
            analytics: c.analytics,
        }
    }

    /// 這次請求的身分輸入。請求的 `user_agent` 優先：自訂值覆寫設定、`auto` 改依帳號環境產生，
    /// 缺欄位或空字串沿用設定。
    pub(super) fn profile(&self, e: &ResponsesEnvelope) -> Result<Profile, KernelError> {
        let user_agent = match UserAgentChoice::parse(e.options.user_agent.as_deref()) {
            UserAgentChoice::Custom(user_agent) if valid_user_agent(user_agent) => {
                Some(user_agent.to_owned())
            }
            UserAgentChoice::Custom(_) => {
                return Err(KernelError::InvalidBody("invalid options.user_agent".into()));
            }
            UserAgentChoice::Auto => None,
            UserAgentChoice::Unset => self.user_agent.clone(),
        };
        Ok(Profile {
            environment: e.options.client_environment.clone(),
            user_agent,
        })
    }

    pub(super) fn metadata(
        &self,
        e: &ResponsesEnvelope,
        device: &str,
        wire: &mut Scrubbed,
    ) -> Result<(), KernelError> {
        let enabled = e.options.client_features.unwrap_or(self.features);
        if !enabled {
            // 平台轉換器已經刪除了客戶端本體的 client_metadata；直接的 ABI 客戶仍可能帶入。
            if e.options.client_features == Some(false) {
                if let Some(turn) = wire
                    .turn_metadata
                    .as_deref()
                    .and_then(|raw| json_object::remove(raw, "installation_id"))
                {
                    wire.turn_metadata = Some(turn);
                }
                // 掃描時沒看到 client_metadata 就不再解析整份本體。
                if wire.client_metadata
                    && let Some(body) = json_object::remove(&wire.body, "client_metadata")
                {
                    wire.body = body;
                    wire.client_metadata = false;
                }
            }
            return Ok(());
        }
        // 帳號設定是安裝身分的唯一來源；共享模式已先比對 envelope 的宣告值。
        if !looks_like_uuid(device) {
            return Err(KernelError::InvalidBody(
                "client_features needs a UUID device_id for this account".into(),
            ));
        }
        let installation = json_object::string(device);
        let body = {
            // 下游原有的 client_metadata：保留它的欄位與順序，只設定安裝身分與會話、輪次。
            let object = if wire.client_metadata {
                Some(json_object::Object::parse(&wire.body).ok_or_else(not_an_object)?)
            } else {
                None
            };
            let mut metadata = object
                .as_ref()
                .and_then(|object| object.get("client_metadata"))
                .map(RawValue::get)
                .filter(|raw| raw.trim_start().starts_with('{'))
                .unwrap_or("{}")
                .to_owned();
            metadata = put(&metadata, "x-codex-installation-id", &installation)?;
            if let Some(session) = e.session_id() {
                metadata = put(&metadata, "session_id", &json_object::string(session))?;
            }
            if let Some(turn) = e.header("x-codex-turn-id").or(e.request_id.as_deref()) {
                metadata = put(&metadata, "turn_id", &json_object::string(turn))?;
            }
            let updated = match &object {
                Some(object) => object.set("client_metadata", &metadata),
                // 標準推理路徑沒有此欄位：加在最後，不解析也不改動既有的位元組。
                None => json_object::append(&wire.body, "client_metadata", &metadata),
            };
            updated.ok_or_else(not_an_object)?
        };
        wire.body = body;
        wire.client_metadata = true;
        // 標頭：就地設定 installation_id，其餘欄位與順序不動；沒有（或不是物件）時只帶安裝身分。
        let turn = wire
            .turn_metadata
            .as_deref()
            .and_then(|raw| json_object::set(raw, "installation_id", &installation))
            .unwrap_or_else(|| format!(r#"{{"installation_id":{installation}}}"#));
        wire.turn_metadata = Some(turn);
        Ok(())
    }
}

fn not_an_object() -> KernelError {
    KernelError::InvalidBody("request body and client_metadata must be JSON objects".into())
}

fn put(object: &str, key: &str, value: &str) -> Result<String, KernelError> {
    json_object::set(object, key, value).ok_or_else(not_an_object)
}

/// 已送過事件的「帳號 × 會話」，依插入順序淘汰最舊的。
#[derive(Debug, Default)]
pub(crate) struct Observed {
    sessions: Mutex<Sessions>,
}

#[derive(Debug, Default)]
struct Sessions {
    keys: HashSet<String>,
    order: VecDeque<String>,
}

/// 每個帳號記得的會話數上限。
const MAX_OBSERVED: usize = 4096;
/// 全程序同時在送的分析事件上限；滿了就略過這次（不記為已送，下一個請求再試）。
static UPLOADS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(256)));
const ANALYTICS_TIMEOUT: Duration = Duration::from_secs(10);

/// 官方 `codex_thread_initialized` 的欄位與順序（`fidelity/baseline/codex-cli-0.167.1-linux-x64/session-tui.json`、
/// `session-exec.json`，2026-11-16 錄得）。
#[derive(Serialize)]
struct Events<'a> {
    events: [Event<'a>; 1],
}

#[derive(Serialize)]
struct Event<'a> {
    event_type: &'static str,
    event_params: ThreadInitialized<'a>,
}

#[derive(Serialize)]
struct ThreadInitialized<'a> {
    thread_id: &'a str,
    session_id: &'a str,
    app_server_client: AppServerClient<'a>,
    runtime: Runtime<'a>,
    model: &'a str,
    ephemeral: bool,
    is_worktree: Option<bool>,
    thread_source: &'static str,
    initialization_mode: &'static str,
    subagent_source: Option<&'static str>,
    parent_thread_id: Option<&'static str>,
    forked_from_thread_id: Option<&'static str>,
    created_at: u64,
}

#[derive(Serialize)]
struct AppServerClient<'a> {
    product_client_id: &'a str,
    client_name: &'a str,
    client_version: &'a str,
    rpc_transport: &'static str,
    experimental_api_enabled: bool,
}

#[derive(Serialize)]
struct Runtime<'a> {
    codex_rs_version: &'a str,
    runtime_os: &'a str,
    runtime_os_version: &'a str,
    runtime_arch: &'a str,
}

impl<'a> Events<'a> {
    /// client 欄位與這次請求的身分一致：名稱是 originator、版本是 `version` 標頭的版本。
    fn thread_initialized(
        session: &'a str,
        model: &'a str,
        identity: &'a Identity,
        runtime: &'a TelemetryRuntime,
        created_at: u64,
    ) -> Self {
        Self {
            events: [Event {
                event_type: "codex_thread_initialized",
                event_params: ThreadInitialized {
                    thread_id: session,
                    session_id: session,
                    app_server_client: AppServerClient {
                        product_client_id: &identity.originator,
                        client_name: &identity.originator,
                        client_version: &identity.version,
                        rpc_transport: rpc_transport(&identity.originator),
                        experimental_api_enabled: true,
                    },
                    runtime: Runtime {
                        codex_rs_version: &identity.version,
                        runtime_os: &runtime.runtime_os,
                        runtime_os_version: &runtime.runtime_os_version,
                        runtime_arch: &runtime.runtime_arch,
                    },
                    model,
                    ephemeral: false,
                    is_worktree: None,
                    thread_source: "user",
                    initialization_mode: "new",
                    subagent_source: None,
                    parent_thread_id: None,
                    forked_from_thread_id: None,
                    created_at,
                },
            }],
        }
    }
}

/// 官方 client 與 app-server 之間的傳輸：exec 在程序內，TUI 經 WebSocket。
fn rpc_transport(originator: &str) -> &'static str {
    if originator == "codex_exec" {
        "in_process"
    } else {
        "websocket"
    }
}

/// 分析事件的撥號准入。熔斷斷開時不撥號；半開時的探測名額留給推理（丟棄通行證即釋放）。
/// 外層 `None` 表示這次不送。
fn admit_analytics(breaker: Option<&Breaker>) -> Option<Option<Ticket<'_>>> {
    match breaker.map(Breaker::admit) {
        None => Some(None),
        Some(Some(ticket)) if !ticket.is_probe() => Some(Some(ticket)),
        Some(_) => None,
    }
}

impl HttpUpstream {
    pub(crate) fn observe_session(
        &self,
        e: &ResponsesEnvelope,
        credential: &Credential,
        identity: &Identity,
        model: Option<&str>,
    ) {
        let (Some(session), Some(model), Some(runtime)) = (
            e.session_id(),
            model,
            e.options
                .telemetry_runtime
                .as_ref()
                .or(self.options.runtime.as_ref()),
        ) else {
            return;
        };
        if [
            session,
            model,
            &runtime.runtime_os,
            &runtime.runtime_os_version,
            &runtime.runtime_arch,
        ]
        .iter()
        .any(|s| s.trim().is_empty() || s.len() > 256)
        {
            return;
        }
        let Ok(permit) = UPLOADS.clone().try_acquire_owned() else {
            return;
        };
        let key = format!(
            "{}\0{session}",
            credential.chatgpt_account_id.as_deref().unwrap_or("")
        );
        {
            let Ok(mut seen) = self.observed.sessions.lock() else {
                return;
            };
            if !seen.keys.insert(key.clone()) {
                return;
            }
            seen.order.push_back(key);
            if seen.order.len() > MAX_OBSERVED
                && let Some(old) = seen.order.pop_front()
            {
                seen.keys.remove(&old);
            }
        }
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs());
        let Ok(body) = serde_json::to_vec(&Events::thread_initialized(
            session, model, identity, runtime, created_at,
        )) else {
            return;
        };
        // 標頭順序同官方：authorization、chatgpt-account-id、content-type、originator、user-agent、accept。
        let mut request = self
            .client
            .post(format!(
                "{}/analytics-events/events",
                self.base_url.trim_end_matches('/')
            ))
            .timeout(ANALYTICS_TIMEOUT)
            .bearer_auth(&credential.access_token);
        if let Some(id) = &credential.chatgpt_account_id {
            request = request.header("chatgpt-account-id", id);
        }
        let request = request
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .header("originator", identity.originator.as_str())
            .header(USER_AGENT, identity.user_agent.as_str())
            .header(ACCEPT, HeaderValue::from_static("*/*"))
            .body(body);
        let breaker = self.http_breaker.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let Some(ticket) = admit_analytics(breaker.as_deref()) else {
                tracing::debug!("session analytics skipped while the exit breaker is not closed");
                return;
            };
            let result = request.send().await;
            if let Some(ticket) = ticket {
                match &result {
                    Err(err) if err.is_connect() => ticket.failure(),
                    _ => ticket.success(),
                }
            }
            match result {
                Ok(r) if r.status().is_success() => {}
                Ok(r) => tracing::warn!(status = r.status().as_u16(), "session analytics rejected"),
                Err(_) => tracing::warn!("session analytics unavailable"),
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;
    const DEVICE: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
    const DOWN: &str = "11111111-2222-4333-8444-555555555555";

    fn wire(raw: &str, defaults: &Defaults) -> Scrubbed {
        let e: ResponsesEnvelope = serde_json::from_str(raw).unwrap();
        let mut wire = crate::scrub::installation_ids(&e, DEVICE);
        defaults.metadata(&e, DEVICE, &mut wire).unwrap();
        wire
    }

    fn enabled() -> Defaults {
        Defaults {
            features: true,
            ..Defaults::default()
        }
    }

    /// 把 `client_metadata` 鍵換成 JSON 跳脫寫法（`m` 寫成反斜線 u006d）：解碼後同名，位元組不同。
    fn escape_key(raw: &str) -> String {
        raw.replace(
            "client_metadata",
            &format!("client_{}u006detadata", char::from(92)),
        )
    }

    #[test]
    fn metadata_handles_escaped_keys_and_keeps_the_account_identity() {
        let raw = escape_key(
            r#"{"body":{"model":"m","client_metadata":{"extra":"keep","x-codex-installation-id":"other"}},"options":{"client_features":true,"installation_id":"11111111-2222-4333-8444-555555555555"}}"#,
        );
        let result = wire(&raw, &Defaults::default());
        let body: Value = serde_json::from_str(&result.body).unwrap();
        assert_eq!(body["client_metadata"]["x-codex-installation-id"], DEVICE);
        assert_eq!(body["client_metadata"]["extra"], "keep");
        // 跳脫寫法的鍵就地改寫，不另外附加一個 client_metadata。
        assert_eq!(
            result.body,
            escape_key(&format!(
                r#"{{"model":"m","client_metadata":{{"extra":"keep","x-codex-installation-id":"{DEVICE}"}}}}"#
            ))
        );
        let turn: Value = serde_json::from_str(result.turn_metadata.as_deref().unwrap()).unwrap();
        assert_eq!(turn["installation_id"], DEVICE);
    }

    #[test]
    fn explicit_disable_removes_metadata_even_with_escaped_keys() {
        let raw = escape_key(
            r#"{"body":{"model":"m","client_metadata":{"secret":"remove"},"input": [ 1, 2 ]},"options":{"client_features":false},"headers":{"x-codex-turn-metadata":"{\"turn_id\":\"keep\",\"installation_id\":\"remove\",\"z\":1}"}}"#,
        );
        assert!(!raw.contains("client_metadata"));
        let result = wire(&raw, &enabled());
        // 其他欄位的位元組與順序不變。
        assert_eq!(result.body, r#"{"model":"m","input": [ 1, 2 ]}"#);
        assert_eq!(
            result.turn_metadata.as_deref(),
            Some(r#"{"turn_id":"keep","z":1}"#)
        );
    }

    #[test]
    fn enabling_keeps_the_body_and_metadata_order() {
        let raw = format!(
            r#"{{"body":{{"model":"m","client_metadata":{{"z":1,"x-codex-installation-id":"{DOWN}","a":2}},"input":[ ]}},"session":{{"session_id":"s"}},"request_id":"t","headers":{{"x-codex-turn-metadata":"{{\"turn_id\":\"t\",\"installation_id\":\"{DOWN}\",\"sandbox\":\"x\"}}"}}}}"#
        );
        let result = wire(&raw, &enabled());
        assert_eq!(
            result.body,
            format!(
                r#"{{"model":"m","client_metadata":{{"z":1,"x-codex-installation-id":"{DEVICE}","a":2,"session_id":"s","turn_id":"t"}},"input":[ ]}}"#
            )
        );
        assert_eq!(
            result.turn_metadata,
            Some(format!(
                r#"{{"turn_id":"t","installation_id":"{DEVICE}","sandbox":"x"}}"#
            ))
        );
    }

    #[test]
    fn metadata_append_preserves_existing_body_bytes_and_options_override_defaults() {
        let raw = r#"{"body":{"model":"m", "input":[ { "text":"client_metadata" } ]},"session":{"session_id":"s"},"request_id":"t"}"#;
        let e: ResponsesEnvelope = serde_json::from_str(raw).unwrap();
        let untouched = wire(raw, &Defaults::default());
        assert_eq!(untouched.body, e.body.get());
        assert_eq!(untouched.turn_metadata, None);
        let enriched = wire(raw, &enabled());
        assert!(enriched.body.starts_with(e.body.get().strip_suffix('}').unwrap()));
        let body: Value = serde_json::from_str(&enriched.body).unwrap();
        assert_eq!(body["client_metadata"]["session_id"], "s");
        assert_eq!(body["client_metadata"]["turn_id"], "t");
        assert_eq!(
            enriched.turn_metadata,
            Some(format!(r#"{{"installation_id":"{DEVICE}"}}"#))
        );
    }

    #[test]
    fn enabling_needs_a_uuid_device_and_an_object_body() {
        let e: ResponsesEnvelope =
            serde_json::from_str(r#"{"body":{"model":"m"},"options":{"client_features":true}}"#).unwrap();
        let mut scrubbed = crate::scrub::installation_ids(&e, "device-1");
        assert!(
            Defaults::default()
                .metadata(&e, "device-1", &mut scrubbed)
                .is_err()
        );
        let e: ResponsesEnvelope =
            serde_json::from_str(r#"{"body":[1],"options":{"client_features":true}}"#).unwrap();
        let mut scrubbed = crate::scrub::installation_ids(&e, DEVICE);
        assert!(Defaults::default().metadata(&e, DEVICE, &mut scrubbed).is_err());
    }

    #[test]
    fn user_agent_precedence_request_over_config_and_empty_falls_through() {
        let defaults = Defaults {
            user_agent: Some("custom/1".into()),
            ..Defaults::default()
        };
        let identity = |options: Value| {
            let raw = serde_json::json!({"body":{}, "options": options}).to_string();
            let e: ResponsesEnvelope = serde_json::from_str(&raw).unwrap();
            defaults.profile(&e).map(|profile| profile.identity(DEVICE))
        };
        for options in [
            serde_json::json!({}),
            serde_json::json!({"user_agent": ""}),
            serde_json::json!({"user_agent": "  "}),
        ] {
            let id = identity(options.clone()).unwrap();
            assert_eq!(
                (
                    id.user_agent.as_str(),
                    id.originator.as_str(),
                    id.version.as_str()
                ),
                ("custom/1", "custom", "1"),
                "{options}"
            );
        }
        for auto in ["auto", "AUTO", " Auto "] {
            let id = identity(serde_json::json!({ "user_agent": auto })).unwrap();
            assert_eq!(id, tap_identity::compose(None, DEVICE), "{auto}");
        }
        let id = identity(serde_json::json!({"user_agent": "project/2 (x)"})).unwrap();
        assert_eq!(
            (id.user_agent.as_str(), id.originator.as_str()),
            ("project/2 (x)", "project")
        );
        let bad = ["bad", "header"].join(&char::from(10).to_string());
        assert!(identity(serde_json::json!({ "user_agent": bad })).is_err());
        assert!(identity(serde_json::json!({ "user_agent": "x".repeat(513) })).is_err());
    }

    #[test]
    fn the_thread_initialized_event_matches_the_official_shape() {
        let identity = tap_identity::compose(None, DEVICE);
        let runtime = TelemetryRuntime {
            runtime_os: "linux".into(),
            runtime_os_version: "12.0.0".into(),
            runtime_arch: "x86_64".into(),
        };
        let event = Events::thread_initialized("s-1", "gpt-6.1-sol", &identity, &runtime, 1_794_801_135);
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            concat!(
                r#"{"events":[{"event_type":"codex_thread_initialized","event_params":{"#,
                r#""thread_id":"s-1","session_id":"s-1","#,
                r#""app_server_client":{"product_client_id":"codex-tui","client_name":"codex-tui","client_version":"0.171.0","rpc_transport":"websocket","experimental_api_enabled":true},"#,
                r#""runtime":{"codex_rs_version":"0.171.0","runtime_os":"linux","runtime_os_version":"12.0.0","runtime_arch":"x86_64"},"#,
                r#""model":"gpt-6.1-sol","ephemeral":false,"is_worktree":null,"thread_source":"user","initialization_mode":"new","#,
                r#""subagent_source":null,"parent_thread_id":null,"forked_from_thread_id":null,"created_at":1794801135}}]}"#
            )
        );
        assert_eq!(rpc_transport("codex_exec"), "in_process");
    }

    #[test]
    fn analytics_never_dial_through_an_open_breaker_or_take_the_probe() {
        assert!(matches!(admit_analytics(None), Some(None)));
        let breaker = Breaker::new(Duration::ZERO);
        assert!(matches!(admit_analytics(Some(&breaker)), Some(Some(_))));
        for _ in 0..crate::breaker::THRESHOLD {
            breaker.admit().unwrap().failure();
        }
        // 冷卻為 0：下一個准入就是半開探測。分析事件不拿，名額仍留給推理。
        assert!(admit_analytics(Some(&breaker)).is_none());
        assert!(breaker.admit().is_some_and(|ticket| ticket.is_probe()));
    }
}
