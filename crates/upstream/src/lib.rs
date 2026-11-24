//! 出站 Responses。一個 [`HttpUpstream`] 就是一個帳號的上游執行環境：HTTP client、WebSocket 連線池、
//! 出口代理與安裝身分都屬於它，帳號之間不共用任何可變狀態。單帳號模式整個程序一份，共享模式每個帳號一份。
//!
//! 一次請求的流程：依帳號環境組身分（`tap-identity`）→ 依傳輸方式選路：
//! - `prefer_websocket`：[`websocket`]，握手失敗兩次或上游要求升級時改走 HTTP；
//! - `http`：依官方順序組標頭（[`headers`]），原樣保留請求本體並以 zstd（等級 3，與官方相同）壓縮，
//!   經 native-tls 送出，依上游核心的對應把非 2xx 轉成 [`KernelError`]。
//!
//! 兩種傳輸的串流本體都以位元組交給 API 層轉發。

pub mod breaker;
mod continuation;
pub mod cookies;
mod failure;
pub mod headers;
pub mod interrupt;
mod json_object;
pub mod ops;
mod options;
pub mod proxy;
pub mod refresh;
pub mod scrub;
pub mod sse;
pub mod timing;
pub mod websocket;
mod ws_capacity;
mod ws_policy;

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, RETRY_AFTER};
use tap_contract::config::{SharedConfig, SingleAccountConfig, UpstreamTransport};
use tap_contract::envelope::ResponsesEnvelope;
use tap_contract::error::{KernelError, UpstreamFailure};
use tap_contract::registry::AccountEnv;
use tap_contract::send::UpstreamSent;
use tap_credentials::Credential;
use tap_identity::IdentitySource;
use timing::{SseObserver, TurnTiming};
use tokio_util::sync::CancellationToken;

/// 上游錯誤本體最多保留這麼多位元組，避免異常的大回應進到控制面的錯誤訊息。
const ERROR_BODY_LIMIT: usize = 64 * 1024;
/// 官方 http-client 的 zstd 壓縮等級（`http-client/src/request.rs`）。
const ZSTD_LEVEL: i32 = 3;
/// 退回身分的警告只記一次。
static FALLBACK_WARNED: AtomicBool = AtomicBool::new(false);
/// 代理檢查結果的有效時間：健康檢查很頻繁，不必每次都連一次代理。
const PROXY_PROBE_TTL: Duration = Duration::from_secs(30);

/// 建立一個帳號執行環境所需的全部設定。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamSettings {
    options: options::Defaults,
    pub ws_limits: tap_contract::config::WsLimits,
    /// 這個帳號的安裝身分：退回身分的挑選、下游安裝 id 的改寫目標。
    pub device_id: String,
    /// 空字串表示直連。
    pub proxy_url: String,
    /// Responses 的上游位址（不含 `/responses`）。
    pub base_url: String,
    /// 請求沒有指定傳輸方式時使用。
    pub default_transport: UpstreamTransport,
    /// 換票端點；只有共享模式會用到（單帳號模式不自行刷新）。
    pub token_url: String,
    /// 測試端點開啟時，cookie 也適用於受控的 http 假上游。
    pub test_endpoints: bool,
    /// 錯誤訊息與日誌用的帳號參照（單帳號模式是槽 ID）。
    pub account_ref: String,
    /// 帳號 × 出口熔斷的冷卻時間；`None` 表示不熔斷（單帳號模式，與上游核心一致）。
    pub breaker_cooldown: Option<Duration>,
}

impl UpstreamSettings {
    /// 單帳號模式：整份設定檔就是這個帳號。
    pub fn single(config: &SingleAccountConfig) -> Self {
        Self {
            options: options::Defaults::single(config),
            ws_limits: config.ws_limits,
            device_id: config.device_id.clone(),
            proxy_url: config.proxy_url.trim().to_owned(),
            base_url: config.chatgpt_base_url().to_owned(),
            default_transport: config.upstream_transport,
            token_url: tap_contract::config::OAUTH_TOKEN_URL.to_owned(),
            test_endpoints: config.test_endpoints,
            account_ref: config.vm_id.clone(),
            breaker_cooldown: None,
        }
    }

    /// 共享模式：帳號環境加上程序層的上游位址。傳輸方式由每個請求的 options 決定，預設與控制面相同。
    pub fn account(env: &AccountEnv, config: &SharedConfig) -> Self {
        Self {
            options: options::Defaults {
                runtime: env.telemetry_runtime.clone(),
                ..Default::default()
            },
            ws_limits: config.ws_limits,
            device_id: env.device_id.clone(),
            proxy_url: env.proxy_url.trim().to_owned(),
            base_url: config.chatgpt_base_url().to_owned(),
            default_transport: UpstreamTransport::default(),
            token_url: config.oauth_token_url().to_owned(),
            test_endpoints: config.test_endpoints,
            account_ref: env.account_ref.clone(),
            breaker_cooldown: Some(Duration::from_millis(config.breaker_cooldown_ms)),
        }
    }
}

/// 一個帳號的上游連線。HTTP client 與 WebSocket 傳輸建立一次，之後這個帳號的所有請求共用。
#[derive(Debug, Clone)]
pub struct HttpUpstream {
    options: options::Defaults,
    observed: Arc<options::Observed>,
    client: reqwest::Client,
    endpoint: String,
    device_id: String,
    default_transport: UpstreamTransport,
    read_idle: Option<Duration>,
    websocket: Arc<websocket::WebsocketTransport>,
    token_url: String,
    /// Responses 的上游位址（不含 `/responses`），帳號操作由它推出其他端點。
    base_url: String,
    account_ref: String,
    /// HTTP 的帳號 × 出口熔斷（WebSocket 的在 [`websocket::WebsocketTransport`] 裡）。
    http_breaker: Option<Arc<breaker::Breaker>>,
    /// 代理的位址（健康檢查用）；沒有代理時為 `None`。
    proxy_endpoint: Option<(String, u16)>,
    proxy_probe: Arc<Mutex<Option<(Instant, bool)>>>,
}

/// 上游接受的請求：要轉給控制面的標頭，以及尚未讀取的本體。
#[derive(Debug)]
pub struct Accepted {
    /// 請求本體裡的模型，作為 `x-model`。
    pub model: Option<String>,
    /// 本輪上游的 `x-codex-*` 回應標頭；WebSocket 握手標頭僅在新連線的首輪轉交。
    pub passthrough: Vec<(HeaderName, HeaderValue)>,
    body: AcceptedBody,
    timing: TurnTiming,
}

enum AcceptedBody {
    Http(ByteStream),
    Websocket(Box<websocket::Turn>),
}

impl std::fmt::Debug for AcceptedBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Http(_) => "Http",
            Self::Websocket(_) => "Websocket",
        })
    }
}

/// 下游取得下一塊時才起算，背壓與整輪長度都不是上游讀取閒置。
fn http_chunks(response: reqwest::Response, idle: Option<Duration>) -> ByteStream {
    Box::pin(futures_util::stream::unfold(
        Some(response.bytes_stream()),
        move |state| async move {
            let mut stream = state?;
            let chunk = match idle {
                Some(d) => match tokio::time::timeout(d, stream.next()).await {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        return Some((
                            Err(io::Error::new(
                                io::ErrorKind::TimedOut,
                                "upstream HTTP read idle timeout",
                            )),
                            None,
                        ));
                    }
                },
                None => stream.next().await,
            }?;
            Some((chunk.map_err(io::Error::other), Some(stream)))
        },
    ))
}
/// 轉給控制面的串流本體。
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>>;

impl HttpUpstream {
    pub fn new(settings: &UpstreamSettings) -> Result<Self, UpstreamInitError> {
        // 不讀環境變數裡的代理；只用設定的出口。
        // 這個帳號的 Cloudflare／路由 cookie，HTTP 與 WebSocket 共用（見 [`cookies`]）。
        let cookies = Arc::new(cookies::AccountCookies::new(settings.test_endpoints));
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(90))
            .cookie_provider(Arc::clone(&cookies));
        let proxy = settings.proxy_url.as_str();
        if !proxy.is_empty() {
            builder = builder.proxy(reqwest::Proxy::all(proxy)?);
        }
        let base = settings.base_url.as_str();
        let mut websocket = websocket::WebsocketTransport::new(
            base,
            proxy,
            cookies,
            settings.breaker_cooldown.map(breaker::Breaker::new),
        )
        .map_err(|err| UpstreamInitError::Websocket(err.to_string()))?;
        websocket.configure(proxy, settings.ws_limits);
        let websocket = Arc::new(websocket);
        websocket.register_capacity();
        let proxy_endpoint = match proxy::parse(proxy) {
            proxy::Parsed::None => None,
            proxy::Parsed::Supported(p) => Some((p.host, p.port)),
            proxy::Parsed::Unsupported { host, port } => Some((host, port)),
        };
        Ok(Self {
            client: builder.build()?,
            endpoint: format!("{base}/responses"),
            options: settings.options.clone(),
            observed: Arc::default(),
            device_id: settings.device_id.clone(),
            default_transport: settings.default_transport,
            read_idle: (settings.ws_limits.read_idle_ms != 0)
                .then(|| Duration::from_millis(settings.ws_limits.read_idle_ms.into())),
            websocket,
            token_url: settings.token_url.clone(),
            base_url: base.to_owned(),
            account_ref: settings.account_ref.clone(),
            http_breaker: settings
                .breaker_cooldown
                .map(|cooldown| Arc::new(breaker::Breaker::new(cooldown))),
            proxy_endpoint,
            proxy_probe: Arc::new(Mutex::new(None)),
        })
    }

    /// 經這個帳號的 HTTP client 送出。共享模式套用 HTTP 熔斷：斷開時不撥號，直接回 `proxy_unavailable`。
    pub(crate) async fn send_http(
        &self,
        request: reqwest::RequestBuilder,
        cancel: &CancellationToken,
    ) -> Result<reqwest::Response, KernelError> {
        // 通行證被丟棄而沒有回報（取消、future 被丟棄）時自動釋放探測名額。
        let ticket = match self.http_breaker.as_deref().map(breaker::Breaker::admit) {
            Some(None) => {
                return Err(KernelError::ProxyUnavailable {
                    account_ref: self.account_ref.clone(),
                    sent: UpstreamSent::No,
                });
            }
            Some(Some(ticket)) => Some(ticket),
            None => None,
        };
        let result = tokio::select! {
            result = request.send() => result,
            () = cancel.cancelled() => {
                return Err(KernelError::UpstreamTransport {
                    message: "request cancelled".to_owned(),
                    sent: UpstreamSent::Unknown,
                });
            }
        };
        if let Some(ticket) = ticket {
            match &result {
                Err(err) if err.is_connect() => ticket.failure(),
                _ => ticket.success(),
            }
        }
        result.map_err(|err| transport_error(&err))
    }

    /// 即使帳號不再收到請求，仍應釋放到期的閒置 socket 與代理名額。
    pub fn reap_idle(&self) {
        self.websocket.reap_idle();
    }

    /// 熔斷狀態（HTTP、WebSocket），供帳號健康回報。單帳號模式恆為 `closed`。
    pub fn breakers(
        &self,
    ) -> (
        tap_contract::registry::BreakerState,
        tap_contract::registry::BreakerState,
    ) {
        let state = |breaker: Option<&breaker::Breaker>| {
            breaker.map_or(
                tap_contract::registry::BreakerState::Closed,
                breaker::Breaker::state,
            )
        };
        (
            state(self.http_breaker.as_deref()),
            state(self.websocket.breaker()),
        )
    }

    /// HTTP 熔斷是否斷開（不含半開）；控制面據此避開這個槽。
    pub fn exit_down(&self) -> bool {
        self.http_breaker
            .as_ref()
            .is_some_and(|breaker| breaker.state() == tap_contract::registry::BreakerState::Open)
    }

    /// 這個執行環境的安裝身分（共享模式拿來與請求比對）。
    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    /// 這次推理請求的身分輸入（帳號環境與自訂 UA，含設定檔的預設）。共享模式記下它，
    /// 帳號操作與換票才用與推理相同的身分。請求的 `user_agent` 不能放進標頭時回 `invalid_request`。
    pub fn profile(&self, envelope: &ResponsesEnvelope) -> Result<tap_identity::Profile, KernelError> {
        self.options.profile(envelope)
    }

    /// 健康回報的 `proxy_ok`：沒有代理時為 true；有代理時是最近一次（30 秒內）連到代理的結果。
    pub async fn proxy_ok(&self) -> bool {
        let Some((host, port)) = &self.proxy_endpoint else {
            return true;
        };
        if let Ok(cached) = self.proxy_probe.lock()
            && let Some((at, ok)) = *cached
            && at.elapsed() < PROXY_PROBE_TTL
        {
            return ok;
        }
        let ok = proxy::reachable(host, *port).await;
        if let Ok(mut cached) = self.proxy_probe.lock() {
            *cached = Some((Instant::now(), ok));
        }
        ok
    }

    /// 送出一次 Responses 請求。`cancel` 觸發時中止並回傳結果不明。
    pub async fn send(
        &self,
        envelope: &ResponsesEnvelope,
        credential: &Credential,
        cancel: &CancellationToken,
        timing: &TurnTiming,
    ) -> Result<Accepted, KernelError> {
        timing.begin_attempt();
        let mut policy = crate::ws_policy::Policy::parse(envelope.body.get());
        let model = policy.model.take().filter(|model| !model.trim().is_empty());
        let identity = self.profile(envelope)?.identity(&self.device_id);
        if identity.source == IdentitySource::Fallback {
            // 控制面補上帳號環境之前，每個請求都會走到這裡；只警告一次，避免灌爆每槽日誌。
            if !FALLBACK_WARNED.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    "client_environment missing or not representable; using the fallback identity"
                );
            }
            tracing::debug!("fallback identity in use");
        }
        // 下游的安裝 id 換成本帳號的（本體與 x-codex-turn-metadata），其餘一個位元組都不動。
        let (mut scrubbed, service_tier) = {
            let fields = json_object::Object::parse(envelope.body.get());
            let service_tier: Option<String> = fields
                .as_ref()
                .and_then(|fields| fields.get("service_tier"))
                .and_then(|raw| serde_json::from_str(raw.get()).ok());
            (
                scrub::installation_ids_from_object(envelope, &self.device_id, fields.as_ref()),
                service_tier,
            )
        };
        self.options.metadata(envelope, &self.device_id, &mut scrubbed)?;
        if scrubbed.replaced > 0 {
            tracing::debug!(
                replaced = scrubbed.replaced,
                "downstream installation ids rewritten"
            );
        }
        let turn_metadata = scrubbed.turn_metadata.as_deref();
        let transport = envelope
            .options
            .upstream_transport
            .unwrap_or(self.default_transport);
        timing.update(|f| f.full_body_bytes = scrubbed.body.len());
        if cancel.is_cancelled() {
            return Err(KernelError::UpstreamTransport {
                message: "request cancelled".to_owned(),
                sent: UpstreamSent::No,
            });
        }
        if transport == UpstreamTransport::Http
            && (policy.warmup() || (envelope.options.ws_replay.is_some() && !policy.full()))
        {
            return Err(policy.rejection(false, UpstreamSent::No));
        }
        if envelope.options.analytics.unwrap_or(self.options.analytics) {
            self.observe_session(envelope, credential, &identity, model.as_deref());
        }
        let prior_send = if transport == UpstreamTransport::PreferWebsocket {
            let headers = headers::responses_websocket(
                envelope,
                model.as_deref(),
                service_tier.as_deref(),
                turn_metadata,
                credential,
                &identity,
            );
            let key = envelope.header("session-id").or_else(|| envelope.session_id());
            match self
                .websocket
                .send(
                    key,
                    headers,
                    &scrubbed.body,
                    (envelope.options.ws_replay.as_ref(), &policy),
                    timing,
                    cancel,
                )
                .await?
            {
                websocket::Outcome::Accepted(mut turn) => {
                    return Ok(Accepted {
                        model,
                        passthrough: std::mem::take(&mut turn.passthrough),
                        body: AcceptedBody::Websocket(turn),
                        timing: timing.clone(),
                    });
                }
                websocket::Outcome::Fallback(sent) => {
                    tracing::debug!("websocket unavailable; falling back to HTTP");
                    sent
                }
            }
        } else {
            UpstreamSent::No
        };
        timing.update(|f| {
            f.transport = Some("http");
            f.request_bytes = Some(scrubbed.body.len());
            f.request_mode = Some(
                if envelope
                    .options
                    .ws_replay
                    .as_ref()
                    .is_some_and(|h| h.previous_response_id.is_some())
                {
                    "full_replay"
                } else {
                    "as_received"
                },
            );
        });
        let body = compress(scrubbed.body.into_bytes())
            .await
            .map_err(|e| http_fallback_error(e, prior_send))?;
        timing.update(|f| f.http_zstd_bytes = Some(body.len()));
        let mut request = self.client.post(&self.endpoint);
        for (name, value) in headers::responses_http(
            envelope,
            model.as_deref(),
            service_tier.as_deref(),
            turn_metadata,
            credential,
            &identity,
        ) {
            request = request.header(name, value);
        }
        let send = self.send_http(request.body(body), cancel);
        let response = match self.read_idle {
            Some(d) => tokio::time::timeout(d, send)
                .await
                .map_err(|_| KernelError::UpstreamTransport {
                    message: "upstream HTTP headers timeout".to_owned(),
                    sent: UpstreamSent::Unknown,
                })?,
            None => send.await,
        }
        .map_err(|e| http_fallback_error(e, prior_send))?;
        timing.update(|f| {
            f.http_headers_ms = Some(timing.ms());
            f.sent = Some("1");
        });
        let status = response.status();
        if !status.is_success() {
            return Err(failure(response, self.read_idle, cancel).await);
        }
        let mut passthrough: Vec<_> = response
            .headers()
            .iter()
            .filter(|(name, _)| name.as_str().starts_with("x-codex-"))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        if let Some(etag) = response
            .headers()
            .get(headers::MODELS_ETAG)
            .filter(|v| v.to_str().is_ok())
        {
            passthrough.push((HeaderName::from_static(headers::MODELS_ETAG), etag.clone()));
        }
        Ok(Accepted {
            model,
            passthrough,
            body: AcceptedBody::Http(http_chunks(response, self.read_idle)),
            timing: timing.clone(),
        })
    }
}

/// 建立上游連線設定時的錯誤。
#[derive(Debug, thiserror::Error)]
pub enum UpstreamInitError {
    #[error("http client: {0}")]
    Http(#[from] reqwest::Error),
    #[error("websocket transport: {0}")]
    Websocket(String),
}

impl Accepted {
    pub fn interrupt_channel(&mut self) -> Option<interrupt::InterruptHandle> {
        match &mut self.body {
            AcceptedBody::Websocket(turn) => Some(turn.interrupt_channel()),
            AcceptedBody::Http(_) => None,
        }
    }

    /// 串流本體。HTTP 原樣轉發（不重新切分事件，跨 chunk 的 UTF-8 與 CRLF 都保留）；
    /// WebSocket 每個訊框轉成一個 SSE 事件。
    pub fn into_stream(self) -> ByteStream {
        match self.body {
            AcceptedBody::Http(response) => {
                let mut observer = SseObserver::default();
                Box::pin(response.map(move |chunk| {
                    let bytes = chunk?;
                    observer.push(&bytes, &self.timing);
                    Ok(bytes)
                }))
            }
            AcceptedBody::Websocket(turn) => Box::pin(turn.into_stream()),
        }
    }

    /// 非串流請求：讀到終態事件，回傳它。`cancel` 觸發時中止（上游已收到請求，送出狀態為已送出）。
    pub async fn into_terminal_event(
        self,
        cancel: &CancellationToken,
    ) -> Result<serde_json::Value, KernelError> {
        tokio::select! {
            result = self.read_terminal_event() => result,
            () = cancel.cancelled() => Err(KernelError::UpstreamTransport {
                message: "request cancelled".to_owned(),
                sent: UpstreamSent::Yes,
            }),
        }
    }

    async fn read_terminal_event(self) -> Result<serde_json::Value, KernelError> {
        let response = match self.body {
            AcceptedBody::Http(response) => response,
            AcceptedBody::Websocket(turn) => return turn.into_terminal_event().await,
        };
        let mut chunks = response;
        let mut decoder = sse::TerminalDecoder::default();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|e| KernelError::UpstreamTransport {
                message: e.to_string(),
                sent: UpstreamSent::Yes,
            })?;
            decoder
                .push(&chunk, &self.timing)
                .map_err(|e| KernelError::UpstreamTransport {
                    message: e.to_string(),
                    sent: UpstreamSent::Yes,
                })?;
            if let Some(event) = decoder.take_terminal() {
                return Ok(event);
            }
        }
        decoder.finish().ok_or_else(|| KernelError::UpstreamTransport {
            message: "upstream stream ended without a terminal event".to_owned(),
            sent: UpstreamSent::Yes,
        })
    }
}

/// 1009 已證實 WS 請求送達。HTTP 的連線前失敗不能把整輪標為未送出。
fn http_fallback_error(mut error: KernelError, prior_send: UpstreamSent) -> KernelError {
    if prior_send == UpstreamSent::Yes && error.upstream_sent() == UpstreamSent::No {
        match &mut error {
            KernelError::UpstreamTransport { sent, .. } | KernelError::ProxyUnavailable { sent, .. } => {
                *sent = prior_send
            }
            _ => {
                return KernelError::UpstreamTransport {
                    message: error.to_string(),
                    sent: prior_send,
                };
            }
        }
    }
    error
}

async fn compress(raw: Vec<u8>) -> Result<Vec<u8>, KernelError> {
    let encoded = tokio::task::spawn_blocking(move || {
        // 與舊版 encode_all 一樣串流寫入。預告短本體的長度會改變 zstd 訊框標頭。
        let mut encoder = zstd::stream::Encoder::new(Vec::new(), ZSTD_LEVEL)?;
        io::copy(&mut raw.as_slice(), &mut encoder)?;
        encoder.finish()
    })
    .await
    .map_err(|err| KernelError::InvalidBody(format!("compress request body: {err}")))?;
    encoded.map_err(|err| KernelError::InvalidBody(format!("compress request body: {err}")))
}

/// 連線建立前就失敗的錯誤確定沒有送出；其他情況（已開始送出）結果不明。
fn transport_error(err: &reqwest::Error) -> KernelError {
    let sent = if err.is_connect() || err.is_builder() {
        UpstreamSent::No
    } else {
        UpstreamSent::Unknown
    };
    KernelError::UpstreamTransport {
        message: err.to_string(),
        sent,
    }
}

async fn failure(
    response: reqwest::Response,
    idle: Option<Duration>,
    cancel: &CancellationToken,
) -> KernelError {
    let status = response.status().as_u16();
    let headers = if status == 429 {
        failure::quota_headers(
            response
                .headers()
                .iter()
                .filter_map(|(n, v)| Some((n.as_str(), v.to_str().ok()?))),
        )
    } else {
        Vec::new()
    };
    let retry_after = header_text(response.headers(), &RETRY_AFTER);
    let id = response
        .headers()
        .get("x-request-id")
        .or_else(|| response.headers().get("x-oai-request-id"))
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let mut raw = Vec::new();
    let mut stream = http_chunks(response, idle);
    while raw.len() < ERROR_BODY_LIMIT {
        let chunk = tokio::select! { () = cancel.cancelled() => break, chunk = stream.next() => chunk };
        let Some(Ok(chunk)) = chunk else { break };
        raw.extend_from_slice(&chunk[..chunk.len().min(ERROR_BODY_LIMIT - raw.len())]);
    }
    let fields = serde_json::from_slice(&raw).unwrap_or_default();
    KernelError::Upstream(UpstreamFailure {
        status,
        headers,
        facts: failure::facts(
            "http",
            Some(status),
            &fields,
            id.as_deref(),
            retry_after.as_deref(),
        ),
        body: String::from_utf8_lossy(&raw).into_owned(),
    })
}

fn header_text(headers: &HeaderMap, name: &HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::compress;
    use sha2::{Digest, Sha256};

    #[tokio::test]
    async fn outbound_zstd_matches_legacy_frames() {
        // 預期值取自 zstd 0.13.4 的 encode_all(level 3)，涵蓋 0.14 的 128 KiB 緩衝邊界。
        for (len, expected) in [
            (
                0,
                "6fb8543883dafcc2ed0934866b8b3f775231f5b0933add08bf502e9e84944cbd",
            ),
            (
                2,
                "e809bb6edfbcfaab2278faf3b4362d5e932d8255eaccfd30a57221abf24c4a61",
            ),
            (
                131_071,
                "87d34acdd70475b6722be41299bae8604e7d1a510ddf913f76a6153f1acc4a3d",
            ),
            (
                131_072,
                "85740499112da643a98994b2c2fbec9b1545dfa7d07788c137e4a28fded1a320",
            ),
            (
                131_073,
                "963186036a9129e0c55249800a1a72de57a2fd165116409d7d892a24d18a5e09",
            ),
            (
                1_048_577,
                "7390df617ce513826d56b5298578e304cfec6dac4e3547c7ff16cc4c7a89fbdd",
            ),
        ] {
            let body = (0..len).map(|i| (i % 251) as u8).collect();
            let frame = compress(body).await.unwrap();
            let digest: String = Sha256::digest(&frame)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            assert_eq!(digest, expected, "input length {len}");
        }
    }
}
