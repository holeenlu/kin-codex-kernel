//! Responses 的 WebSocket 傳輸（`upstream_transport: prefer_websocket`）。
//!
//! 對 Node 的行為照上游核心（2026-11-16 錄得）：
//! - 第一個事件若是 `error`，改回 JSON 錯誤：狀態取事件的 `status`（缺少時 400），輸入錯誤保留上游 detail，其他錯誤訊息取 `error.message`；
//! - 否則回 200 串流，每個 WebSocket 文字訊框轉成 `event: response` 加 `data: <原文>`；
//! - 完整請求可在送出前改用 HTTP；預熱與只有增量的請求不可降級。
//!
//! 對上游的行為照官方互動式 TUI：rustls 加 aws-lc-rs、permessage-deflate、官方的升級標頭順序；
//! 同一個對話（session）結束一輪後把連線留著給下一輪用，不另外發預熱（下游客戶端自己的預熱會原樣轉送）。
//! 上游要求升級（426）時記住同一握手範圍，可降級的請求改用 HTTP。

use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::continuation::{self, Pending, Snapshot};
use crate::timing::{Event, TurnTiming, millis};
use bytes::Bytes;
use futures_util::{SinkExt, Stream, StreamExt};
use reqwest::header::{HeaderName, HeaderValue};
use rustls::ClientConfig;
use serde_json::Value;
use tap_contract::envelope::WsReplay;
use tap_contract::error::{KernelError, UpstreamFacts};
use tap_contract::send::UpstreamSent;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[cfg(test)]
use tokio_tungstenite::client_async_tls_with_config;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName as WsHeaderName, HeaderValue as WsHeaderValue};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, client_async_with_config};
use tokio_util::sync::CancellationToken;
use tungstenite::extensions::ExtensionsConfig;
use tungstenite::extensions::compression::deflate::DeflateConfig;
use tungstenite::protocol::WebSocketConfig;

use crate::breaker::Breaker;
use crate::cookies::AccountCookies;
use crate::proxy;
use crate::sse::TERMINAL;

/// 閒置連線保留多久；超過就不再重用。
const IDLE_TTL: Duration = Duration::from_secs(240);
/// 最多保留多少個對話的閒置連線。
const IDLE_LIMIT: usize = 256;
/// 同一個會話鍵最多保留幾條閒置連線。官方 CLI 每條執行緒各有一條連線；多代理的子執行緒
/// 共用同一個 `session-id`，只留一條時後放回的會把先放回的關掉，下一輪並行就得重新握手。
const IDLE_PER_KEY: usize = 8;
/// 記住多少個「已改走 HTTP」的對話。
const DISABLED_LIMIT: usize = 4096;
/// 可重試的握手失敗最多試兩次；上游明示 429 則立即回報。
const HANDSHAKE_ATTEMPTS: usize = 2;
/// 撥號加握手的上限，與 HTTP 的 `connect_timeout` 相同：代理接受了 TCP 卻不回應時不能無限期等待。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
type Conn = WebSocketStream<MaybeTlsStream<Box<dyn Io>>>;
type Passthrough = Vec<(HeaderName, HeaderValue)>;

pub struct WebsocketTransport {
    url: String,
    host: String,
    port: u16,
    tls: Arc<ClientConfig>,
    /// 出口：直連、socks5／socks5h，或 http 代理的 CONNECT。其他代理（例如 https）只走 HTTP 傳輸。
    egress: proxy::Parsed,
    /// 與 HTTP client 共用的帳號 cookie 罐。
    cookies: Arc<AccountCookies>,
    /// WebSocket 的帳號 × 出口熔斷；斷開時改走 HTTP。
    breaker: Option<Breaker>,
    capacity: Option<Arc<crate::ws_capacity::Capacity>>,
    fast_budget: Option<Duration>,
    read_idle: Option<Duration>,
    idle: Mutex<HashMap<String, Vec<(Socket, Instant)>>>,
    disabled: Mutex<VecDeque<String>>,
}

/// `send` 的結果：取得串流，或改走 HTTP。
#[derive(Debug)]
pub enum Outcome {
    Accepted(Box<Turn>),
    Fallback(UpstreamSent),
}

struct Socket {
    conn: Conn,
    /// 握手回應頭只交給開啟這條連線的那一輪；協商值另存於 deflate 與 max_message。
    opening_headers: Passthrough,
    deflate: bool,
    previous: Option<Snapshot>,
    owner: Option<String>,
    max_message: Option<usize>,
    _capacity: Option<tokio::sync::OwnedSemaphorePermit>,
}

/// 一輪進行中的 WebSocket 請求。
pub struct Turn {
    pub passthrough: Passthrough,
    first: String,
    socket: Socket,
    pending: Option<Pending>,
    timing: TurnTiming,
    interrupt: Option<tokio::sync::mpsc::Receiver<crate::interrupt::Command>>,
    response_id: Option<String>,
    awaiting_ack: bool,
    deferred_terminal: Option<String>,
    key: Option<String>,
    transport: Arc<WebsocketTransport>,
}

enum HandshakeError {
    /// 上游回了非 101 的 HTTP 狀態。
    Rejected(u16),
    RateLimited(Box<UpstreamFacts>),
    Failed(String),
}

impl std::fmt::Debug for WebsocketTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketTransport")
            .field("url", &self.url)
            .field("egress", &self.egress)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for Turn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Turn")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl WebsocketTransport {
    pub(super) fn configure(&mut self, proxy: &str, limits: tap_contract::config::WsLimits) {
        self.capacity = crate::ws_capacity::Capacity::for_proxy(proxy, limits);
        self.read_idle =
            (limits.read_idle_ms != 0).then(|| Duration::from_millis(limits.read_idle_ms.into()));
        self.fast_budget =
            (limits.fallback_after_ms != 0).then(|| Duration::from_millis(limits.fallback_after_ms.into()));
    }

    pub(crate) fn breaker(&self) -> Option<&Breaker> {
        self.breaker.as_ref()
    }

    /// `base` 是 Responses 的 HTTP 位址；`https` 對應 `wss`，`http`（只在測試端點）對應 `ws`。
    pub fn new(
        base: &str,
        proxy_url: &str,
        cookies: Arc<AccountCookies>,
        breaker: Option<Breaker>,
    ) -> io::Result<Self> {
        let url = if let Some(rest) = base.strip_prefix("https://") {
            format!("wss://{rest}/responses")
        } else if let Some(rest) = base.strip_prefix("http://") {
            format!("ws://{rest}/responses")
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unsupported upstream URL",
            ));
        };
        let (host, port) = authority(&url)?;
        Ok(Self {
            url,
            host,
            port,
            tls: tls_config(),
            egress: proxy::parse(proxy_url),
            cookies,
            breaker,
            capacity: None,
            fast_budget: None,
            read_idle: None,
            idle: Mutex::new(HashMap::new()),
            disabled: Mutex::new(VecDeque::new()),
        })
    }

    /// 送出一輪。握手失敗兩次、上游要求升級，或出口不支援時回 [`Outcome::Fallback`]。
    pub(super) async fn send(
        self: &Arc<Self>,
        key: Option<&str>,
        headers: Vec<(HeaderName, HeaderValue)>,
        body: &str,
        plan: (Option<&WsReplay>, &crate::ws_policy::Policy),
        timing: &TurnTiming,
        cancel: &CancellationToken,
    ) -> Result<Outcome, KernelError> {
        let (hint, policy) = plan;
        let fallback_or_reject = |reason, oversized| {
            timing.update(|f| f.fallback = Some(reason));
            if policy.full() {
                Ok(Outcome::Fallback(UpstreamSent::No))
            } else {
                Err(policy.rejection(oversized, UpstreamSent::No))
            }
        };
        let semantics = header_semantics(&headers);
        let idle_key = key.map(|key| format!("{key}\u{1f}{semantics:x?}"));
        let fallback = if matches!(self.egress, proxy::Parsed::Unsupported { .. }) {
            Some("unsupported_proxy")
        } else if idle_key.as_deref().is_some_and(|key| self.is_disabled(key)) {
            Some("session_426")
        } else {
            None
        };
        if let Some(reason) = fallback {
            return fallback_or_reject(reason, false);
        }
        // 只在送出前探測；Close 與部分寫入都不能證明送出的請求沒有執行。
        let exact = policy.previous_response_id.as_deref();
        let want = exact.or_else(|| hint.and_then(|hint| hint.previous_response_id.as_deref()));
        let idle = idle_key
            .as_deref()
            .and_then(|k| self.take_idle(k, want, exact.is_some()));
        // 原生 WS 的增量不能脫離原線。外部 HTTP 呼叫者自帶的未知 ID 仍可在同帳號新線原樣詢問，不借用另一條鏈。
        if exact.is_some() && hint.is_some() && idle.is_none() {
            return Err(policy.rejection(false, UpstreamSent::No));
        }
        let reuse = idle.is_some();
        let mut socket = if let Some(s) = idle {
            s
        } else {
            let opening = self.connect_with_retry(&headers, cancel, timing, UpstreamSent::No);
            let opened = if let Some(budget) = self.fast_budget.filter(|_| policy.full()) {
                match tokio::time::timeout(budget, opening).await {
                    Ok(r) => r?,
                    Err(_) => return fallback_or_reject("handshake_budget", false),
                }
            } else {
                opening.await?
            };
            match opened {
                Ok(s) => s,
                Err((status, reason)) => {
                    if status == 426
                        && let Some(k) = idle_key.as_deref()
                    {
                        self.disable(k);
                    }
                    timing.update(|f| {
                        f.fallback = Some(reason);
                        f.handshake_status = (status != 0).then_some(status);
                    });
                    return fallback_or_reject(reason, false);
                }
            }
        };
        timing.update(|f| {
            f.transport = Some("websocket");
            f.ws_reused = Some(reuse);
            f.ws_deflate = Some(socket.deflate);
        });
        let (mut message, pending, mut mode) = continuation::prepare(body, hint, socket.previous.as_ref());
        // 首個文字事件前可恢復的情況見 docs/RESPONSES_TIMING.md。
        // - 上游明確拒絕由核心產生的續接 ID（`previous_response_not_found`）：同一條連線補送完整本體。
        // - 上游送來 1009：完整輸入改走 HTTP 一次；其他 Close：與上游核心、官方 CLI 相同，換一條新連線送完整本體。沒有 Close 的斷線、
        //   寫入失敗不重送（`unknown`），交給控制面與客戶端判斷。
        let mut cache_rejected = false;
        let mut closed_resent = false;
        loop {
            if cancel.is_cancelled() {
                return Err(cancelled(if cache_rejected {
                    UpstreamSent::Yes
                } else if closed_resent {
                    UpstreamSent::Unknown
                } else {
                    UpstreamSent::No
                }));
            }
            if socket.max_message.is_some_and(|limit| message.len() > limit) {
                if cache_rejected || closed_resent {
                    return Err(policy.rejection(
                        true,
                        if cache_rejected {
                            UpstreamSent::Yes
                        } else {
                            UpstreamSent::Unknown
                        },
                    ));
                }
                return fallback_or_reject("websocket_message_too_large", true);
            }
            timing.update(|f| {
                f.ws_send_attempts += 1;
                f.request_mode = Some(mode);
                f.request_bytes = Some(message.len());
                f.sent = Some("unknown");
            });
            tokio::select! {
                biased;
                ()=cancel.cancelled()=>return Err(cancelled(UpstreamSent::Unknown)),
                result=send_create(&mut socket.conn, message)=>result.map_err(|err| KernelError::UpstreamTransport{message:err.to_string(),sent:UpstreamSent::Unknown})?,
            }
            timing.update(|f| {
                f.send_complete_ms = Some(timing.ms());
                f.ws_frame_compressed = Some(socket.deflate);
            });
            let first = tokio::select! {
                biased;
                ()=cancel.cancelled()=>return Err(cancelled(UpstreamSent::Unknown)),
                frame=read_with_idle(self.read_idle, next_frame(&mut socket.conn))=>match frame {
                    Err(())=>return Err(KernelError::UpstreamTransport{message:"upstream websocket read idle timeout".to_owned(),sent:UpstreamSent::Unknown}),
                    Ok(Next::Text(t))=>t,
                    Ok(Next::Oversized) => {
                        timing.update(|f| f.fallback = Some("websocket_message_too_large"));
                        if policy.full() && !closed_resent && !cache_rejected {
                            return Ok(Outcome::Fallback(UpstreamSent::Yes));
                        }
                        let sent = if closed_resent { UpstreamSent::Unknown } else { UpstreamSent::Yes };
                        return Err(policy.rejection(true, sent));
                    }
                    Ok(Next::Closed) if policy.full() && !closed_resent && !cache_rejected => {
                        closed_resent = true;
                        drop(socket); // 先歸還已關閉連線持有的代理名額。
                        socket = match self.connect_with_retry(&headers, cancel, timing, UpstreamSent::Unknown).await? {
                            Ok(fresh) => fresh,
                            // 第一次已經寫出：換線失敗不改走 HTTP（那會是第三次送出），結果不明。
                            Err(_) => return Err(KernelError::UpstreamTransport{message:"websocket closed before the first event".to_owned(),sent:UpstreamSent::Unknown}),
                        };
                        mode = "full_replay";
                        message = create_message(body);
                        timing.update(|f| {
                            f.ws_reused = Some(false);
                            f.ws_deflate = Some(socket.deflate);
                        });
                        continue;
                    }
                    Ok(Next::Closed|Next::Broken)=>return Err(KernelError::UpstreamTransport{message:"websocket closed before the first event".to_owned(),sent:UpstreamSent::Unknown}),
                }
            };
            if mode == "incremental" && previous_missing(&first) {
                cache_rejected = true;
                mode = "full_replay";
                message = create_message(body);
                timing.update(|f| {
                    f.cache_miss = true;
                    f.sent = Some("1");
                });
                continue;
            }
            timing.update(|f| f.sent = Some("1"));
            if let Ok(event) = serde_json::from_str::<Event>(&first) {
                timing.event(&event);
            }
            let passthrough = std::mem::take(&mut socket.opening_headers);
            if let Some(err) = first_event_error(&first, &passthrough) {
                return Err(err);
            }
            return Ok(Outcome::Accepted(Box::new(Turn {
                passthrough,
                first,
                socket,
                pending,
                timing: timing.clone(),
                interrupt: None,
                response_id: None,
                awaiting_ack: false,
                deferred_terminal: None,
                key: idle_key,
                transport: Arc::clone(self),
            })));
        }
    }

    async fn connect_with_retry(
        &self,
        headers: &[(HeaderName, HeaderValue)],
        cancel: &CancellationToken,
        timing: &TurnTiming,
        already_sent: UpstreamSent,
    ) -> Result<Result<Socket, (u16, &'static str)>, KernelError> {
        let mut last_status = 0;
        for _ in 0..HANDSHAKE_ATTEMPTS {
            let (_connecting, live) = match self.capacity.as_ref().map(|c| c.reserve()).transpose() {
                Ok(permits) => permits.unwrap_or_default(),
                Err(()) => return Ok(Err((0, "proxy_ws_capacity"))),
            };
            // 熔斷斷開時不撥號，改走 HTTP（狀態 0 表示沒有握手回應）。
            let ticket = match self.breaker.as_ref().map(Breaker::admit) {
                Some(None) => return Ok(Err((last_status, "breaker_open"))),
                Some(Some(ticket)) => Some(ticket),
                None => None,
            };
            timing.update(|f| f.ws_handshakes += 1);
            let attempt = tokio::select! {
                attempt = tokio::time::timeout(CONNECT_TIMEOUT, self.connect(headers,timing)) => attempt
                    .unwrap_or_else(|_| Err(HandshakeError::Failed("websocket connect timed out".to_owned()))),
                () = cancel.cancelled() => return Err(cancelled(already_sent)),
            };
            if let Some(ticket) = ticket {
                match &attempt {
                    Err(HandshakeError::Failed(_)) => ticket.failure(),
                    _ => ticket.success(),
                }
            }
            match attempt {
                Ok(mut connected) => {
                    connected._capacity = live;
                    return Ok(Ok(connected));
                }
                Err(HandshakeError::Rejected(426)) => return Ok(Err((426, "upgrade_required"))),
                Err(HandshakeError::RateLimited(facts)) => {
                    timing.update(|f| {
                        f.transport = Some("websocket");
                        f.handshake_status = Some(429);
                    });
                    return Err(KernelError::WebsocketHandshakeRateLimited {
                        facts,
                        sent: already_sent,
                    });
                }
                Err(HandshakeError::Rejected(status)) => last_status = status,
                Err(HandshakeError::Failed(message)) => {
                    last_status = 0;
                    tracing::debug!(%message, "websocket handshake failed");
                }
            }
        }
        Ok(Err((
            last_status,
            if last_status == 0 {
                "handshake_failed"
            } else {
                "handshake_rejected"
            },
        )))
    }

    async fn connect(
        &self,
        headers: &[(HeaderName, HeaderValue)],
        timing: &TurnTiming,
    ) -> Result<Socket, HandshakeError> {
        let mut request = self
            .url
            .as_str()
            .into_client_request()
            .map_err(|err| HandshakeError::Failed(err.to_string()))?;
        for (name, value) in headers {
            if let (Ok(name), Ok(value)) = (
                WsHeaderName::from_bytes(name.as_str().as_bytes()),
                WsHeaderValue::from_bytes(value.as_bytes()),
            ) {
                request.headers_mut().append(name, value);
            }
        }
        // 與官方相同：cookie 在插入順序的最後，分支把最後五個標頭倒序搬到前面後，
        // 線上順序是緊接在 Sec-WebSocket-Key 之後。
        if let Some(cookie) = self
            .cookies
            .header_for(&self.url)
            .and_then(|cookie| WsHeaderValue::from_bytes(cookie.as_bytes()).ok())
        {
            request
                .headers_mut()
                .append(WsHeaderName::from_static("cookie"), cookie);
        }
        let stage = Instant::now();
        let io = self.dial().await;
        timing.update(|f| f.dial_ms = Some(millis(stage.elapsed())));
        let io = io.map_err(|err| HandshakeError::Failed(err.to_string()))?;
        let io = if self.url.starts_with("wss://") {
            let name = rustls::pki_types::ServerName::try_from(self.host.clone())
                .map_err(|e| HandshakeError::Failed(e.to_string()))?;
            let stage = Instant::now();
            let tls = tokio_rustls::TlsConnector::from(Arc::clone(&self.tls))
                .connect(name, io)
                .await;
            timing.update(|f| f.tls_ms = Some(millis(stage.elapsed())));
            MaybeTlsStream::Rustls(tls.map_err(|e| HandshakeError::Failed(e.to_string()))?)
        } else {
            MaybeTlsStream::Plain(io)
        };
        let stage = Instant::now();
        let upgraded = client_async_with_config(request, io, Some(websocket_config())).await;
        timing.update(|f| f.upgrade_ms = Some(millis(stage.elapsed())));
        match upgraded {
            Ok((conn, response)) => {
                let set_cookies: Vec<HeaderValue> = response
                    .headers()
                    .get_all("set-cookie")
                    .iter()
                    .filter_map(|value| HeaderValue::from_bytes(value.as_bytes()).ok())
                    .collect();
                self.cookies.store_from(&self.url, set_cookies.iter());
                let opening_headers = response
                    .headers()
                    .iter()
                    .filter(|(name, _)| name.as_str().starts_with("x-codex-"))
                    .filter_map(|(name, value)| {
                        Some((
                            HeaderName::from_bytes(name.as_str().as_bytes()).ok()?,
                            HeaderValue::from_bytes(value.as_bytes()).ok()?,
                        ))
                    })
                    .collect();
                let deflate = response
                    .headers()
                    .get_all("sec-websocket-extensions")
                    .iter()
                    .filter_map(|v| v.to_str().ok())
                    .flat_map(|s| s.split(','))
                    .any(|s| {
                        s.split(';')
                            .next()
                            .is_some_and(|v| v.trim().eq_ignore_ascii_case("permessage-deflate"))
                    });
                Ok(Socket {
                    conn,
                    opening_headers,
                    deflate,
                    max_message: response
                        .headers()
                        .get("x-codex-websocket-max-message-bytes")
                        .and_then(|v| v.to_str().ok()?.trim().parse().ok()),
                    _capacity: None,
                    previous: None,
                    owner: None,
                })
            }
            Err(tungstenite::Error::Http(response)) if response.status().as_u16() == 429 => {
                let header = |name: &str| response.headers().get(name).and_then(|v| v.to_str().ok());
                // tungstenite 只交付握手標頭後已讀到的位元組；正文缺漏或過大時只回報標頭。
                let fields = response
                    .body()
                    .as_deref()
                    .filter(|body| body.len() <= crate::ERROR_BODY_LIMIT)
                    .and_then(|body| serde_json::from_slice(body).ok())
                    .unwrap_or_default();
                Err(HandshakeError::RateLimited(crate::failure::facts(
                    "websocket_handshake",
                    Some(429),
                    &fields,
                    header("x-request-id").or_else(|| header("x-oai-request-id")),
                    header("retry-after"),
                )))
            }
            Err(tungstenite::Error::Http(response)) => {
                Err(HandshakeError::Rejected(response.status().as_u16()))
            }
            Err(err) => Err(HandshakeError::Failed(err.to_string())),
        }
    }

    async fn dial(&self) -> io::Result<Box<dyn Io>> {
        let proxy = match &self.egress {
            proxy::Parsed::Supported(proxy) => proxy,
            proxy::Parsed::None | proxy::Parsed::Unsupported { .. } => {
                return Ok(Box::new(
                    TcpStream::connect((self.host.as_str(), self.port)).await?,
                ));
            }
        };
        match proxy.kind {
            proxy::Kind::HttpConnect => {
                Ok(Box::new(proxy::http_connect(proxy, &self.host, self.port).await?))
            }
            proxy::Kind::Socks5 { remote_dns } => {
                use tokio_socks::tcp::Socks5Stream;
                let address = (proxy.host.as_str(), proxy.port);
                // socks5h：主機名交給代理解析（與 reqwest 相同）；socks5：本機解析後送位址。
                let stream = if remote_dns {
                    let target = (self.host.as_str(), self.port);
                    match &proxy.credentials {
                        Some((user, pass)) => {
                            Socks5Stream::connect_with_password(address, target, user, pass).await
                        }
                        None => Socks5Stream::connect(address, target).await,
                    }
                } else {
                    let target = tokio::net::lookup_host((self.host.as_str(), self.port))
                        .await?
                        .next()
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::NotFound, "upstream host did not resolve")
                        })?;
                    match &proxy.credentials {
                        Some((user, pass)) => {
                            Socks5Stream::connect_with_password(address, target, user, pass).await
                        }
                        None => Socks5Stream::connect(address, target).await,
                    }
                }
                .map_err(io::Error::other)?;
                Ok(Box::new(stream))
            }
        }
    }

    /// 取一條仍開著的閒置連線。有續接提示時優先拿上一輪回應對得上的那條（才送得了增量），
    /// 否則拿最近放回的；閒置期間被上游關掉的丟掉、再試下一條。
    fn take_idle(&self, key: &str, want: Option<&str>, exact: bool) -> Option<Socket> {
        loop {
            let mut socket = {
                let mut idle = self.idle.lock().ok()?;
                let list = idle.get_mut(key)?;
                list.retain(|(_, since)| since.elapsed() < IDLE_TTL);
                let index = want
                    .and_then(|id| list.iter().rposition(|(s, _)| s.owner.as_deref() == Some(id)))
                    .or_else(|| (!exact).then(|| list.len().checked_sub(1)).flatten());
                let taken = index.map(|i| list.remove(i).0);
                if list.is_empty() {
                    idle.remove(key);
                }
                taken?
            };
            if still_open(&mut socket.conn) {
                return Some(socket);
            }
        }
    }

    pub(super) fn register_capacity(self: &Arc<Self>) {
        if let Some(capacity) = &self.capacity {
            capacity.attach(self);
        }
    }

    pub(super) fn oldest_idle(&self) -> Option<(String, Instant)> {
        self.idle
            .lock()
            .ok()?
            .iter()
            .filter_map(|(key, list)| list.first().map(|(_, at)| (key.clone(), *at)))
            .min_by_key(|(_, at)| *at)
    }
    pub(super) fn remove_idle(&self, key: &str, at: Instant) -> bool {
        let Ok(mut idle) = self.idle.lock() else {
            return false;
        };
        let Some(list) = idle.get_mut(key) else {
            return false;
        };
        if list.first().is_none_or(|(_, since)| *since != at) {
            return false;
        }
        list.remove(0);
        if list.is_empty() {
            idle.remove(key);
        }
        true
    }

    pub(super) fn reap_idle(&self) {
        if let Ok(mut idle) = self.idle.lock() {
            idle.retain(|_, list| {
                list.retain(|(_, at)| at.elapsed() < IDLE_TTL);
                !list.is_empty()
            });
        }
    }

    fn put_idle(&self, key: String, socket: Socket) {
        let Ok(mut idle) = self.idle.lock() else { return };
        idle.retain(|_, list| {
            list.retain(|(_, since)| since.elapsed() < IDLE_TTL);
            !list.is_empty()
        });
        if idle.values().map(Vec::len).sum::<usize>() >= IDLE_LIMIT
            && let Some(oldest) = idle
                .iter()
                .filter_map(|(key, list)| list.first().map(|(_, since)| (key.clone(), *since)))
                .min_by_key(|(_, since)| *since)
                .map(|(key, _)| key)
            && let Some(list) = idle.get_mut(&oldest)
        {
            list.remove(0);
            if list.is_empty() {
                idle.remove(&oldest);
            }
        }
        let list = idle.entry(key).or_default();
        if list.len() >= IDLE_PER_KEY {
            list.remove(0);
        }
        list.push((socket, Instant::now()));
    }

    fn is_disabled(&self, key: &str) -> bool {
        self.disabled
            .lock()
            .is_ok_and(|disabled| disabled.iter().any(|k| k == key))
    }

    fn disable(&self, key: &str) {
        if let Ok(mut disabled) = self.disabled.lock() {
            if disabled.len() >= DISABLED_LIMIT {
                disabled.pop_front();
            }
            disabled.push_back(key.to_owned());
        }
    }
}

impl Turn {
    pub(super) fn interrupt_channel(&mut self) -> crate::interrupt::InterruptHandle {
        let (handle, rx) = crate::interrupt::channel();
        self.interrupt = Some(rx);
        handle
    }

    async fn next_controlled(&mut self) -> Option<String> {
        let deadline = self.transport.read_idle.map(|d| tokio::time::Instant::now() + d);
        if let Some(terminal) = self.deferred_terminal.take() {
            return Some(terminal);
        }
        loop {
            let Some(rx) = &mut self.interrupt else {
                return read_with_idle(self.transport.read_idle, next_text(&mut self.socket.conn))
                    .await
                    .unwrap_or(None);
            };
            tokio::select! {
                message = rx.recv() => {
                    let Some(cmd) = message else { self.interrupt = None; continue };
                    if cmd.written.is_closed() || self.awaiting_ack || self.response_id.as_deref() != Some(&cmd.response_id) {
                        let _ = cmd.written.send(false); continue;
                    }
                    let payload = serde_json::json!({"type":"response.interrupt","mode":"discard_partial_items","response_id":cmd.response_id});
                    let written = matches!(tokio::time::timeout(Duration::from_secs(2), self.socket.conn.send(Message::text(payload.to_string()))).await, Ok(Ok(())));
                    let _ = cmd.written.send(written);
                    if !written { return None }
                    self.awaiting_ack = true;
                }
                next = async {
                    let read = next_text(&mut self.socket.conn);
                    match deadline {
                        Some(at) => tokio::time::timeout_at(at, read).await.unwrap_or(None),
                        None => read.await,
                    }
                } => {
                    let text = next?;
                    if self.awaiting_ack {
                        if self.is_ack(&text) { self.awaiting_ack = false; }
                        else if event_type(&text).is_some_and(|kind| TERMINAL.contains(&kind.as_str())) {
                            // 官方允許終態先到。等確認時不可把這條線交給下一輪。
                            if let Ok(Some(ack)) = tokio::time::timeout(Duration::from_secs(3), next_text(&mut self.socket.conn)).await
                                && self.is_ack(&ack) {
                                self.awaiting_ack = false; self.deferred_terminal = Some(text); return Some(ack);
                            }
                            self.key = None;
                        }
                    }
                    return Some(text);
                }
            }
        }
    }

    fn is_ack(&self, text: &str) -> bool {
        serde_json::from_str::<Value>(text).is_ok_and(|e| {
            matches!(
                e["type"].as_str(),
                Some("response.interrupt.accepted" | "response.interrupt.failed")
            ) && e["response_id"].as_str() == self.response_id.as_deref()
        })
    }

    /// 串流：每個文字訊框轉成 `event: response` 的 SSE 事件。終態事件之後把連線留給同一個對話的下一輪。
    pub fn into_stream(self) -> impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static {
        futures_util::stream::unfold(Some((self, true)), |state| async move {
            let (mut turn, first) = state?;
            let text = if first {
                std::mem::take(&mut turn.first)
            } else {
                match turn.next_controlled().await {
                    Some(text) => text,
                    None => {
                        turn.timing.end("interrupted", Some("1"));
                        return Some((
                            Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "websocket ended before a terminal event",
                            )),
                            None,
                        ));
                    }
                }
            };
            let done = turn.observe(&text, first);
            let frame = Bytes::from(format!("event: response\ndata: {text}\n\n"));
            if done {
                turn.finish();
                return Some((Ok(frame), None));
            }
            Some((Ok(frame), Some((turn, false))))
        })
    }

    pub async fn into_terminal_event(mut self) -> Result<Value, KernelError> {
        let mut text = std::mem::take(&mut self.first);
        let mut first = true;
        loop {
            if self.observe(&text, first) {
                let event = serde_json::from_str(&text).map_err(|e| KernelError::UpstreamTransport {
                    message: format!("{e}"),
                    sent: UpstreamSent::Yes,
                })?;
                self.finish();
                return Ok(event);
            }
            first = false;
            text = read_with_idle(self.transport.read_idle, next_text(&mut self.socket.conn))
                .await
                .unwrap_or(None)
                .ok_or_else(|| KernelError::UpstreamTransport {
                    message: "upstream stream ended without a terminal event".to_owned(),
                    sent: UpstreamSent::Yes,
                })?;
        }
    }

    fn observe(&mut self, text: &str, first: bool) -> bool {
        let Ok(event) = serde_json::from_str::<Event>(text) else {
            self.pending = None;
            return false;
        };
        if event.kind == "response.created" && self.interrupt.is_some() {
            self.response_id = serde_json::from_str::<Value>(text).ok().and_then(|e| {
                e.pointer("/response/id")?
                    .as_str()
                    .filter(|id| !id.is_empty() && id.len() <= 256)
                    .map(str::to_owned)
            });
        }
        if !first {
            self.timing.event(&event);
        }
        if let Some(p) = &mut self.pending {
            p.observe(event.kind, text);
        }
        if !TERMINAL.contains(&event.kind) {
            return false;
        }
        self.socket.owner = None;
        self.socket.previous = if matches!(event.kind, "response.completed" | "response.incomplete") {
            self.pending.take().and_then(|p| p.complete(text))
        } else {
            None
        };
        if let Some(snapshot) = &self.socket.previous {
            self.socket.owner = Some(snapshot.id().to_owned());
        } else if event.kind == "response.completed" {
            #[derive(serde::Deserialize)]
            struct Id<'a> {
                id: &'a str,
            }
            #[derive(serde::Deserialize)]
            struct Done<'a> {
                #[serde(borrow)]
                response: Id<'a>,
            }
            self.socket.owner = serde_json::from_str::<Done>(text)
                .ok()
                .map(|d| d.response.id)
                .filter(|id| !id.is_empty() && id.len() <= 256)
                .map(str::to_owned);
        }
        true
    }
    fn finish(self) {
        if let Some(key) = self.key {
            self.transport.put_idle(key, self.socket);
        }
    }
}

fn cancelled(sent: UpstreamSent) -> KernelError {
    KernelError::UpstreamTransport {
        message: "request cancelled".to_owned(),
        sent,
    }
}
fn previous_missing(text: &str) -> bool {
    if event_type(text).as_deref() != Some("error") {
        return false;
    }
    serde_json::from_str::<Value>(text).is_ok_and(|e| {
        e.pointer("/error/code").and_then(Value::as_str) == Some("previous_response_not_found")
            && e.get("status")
                .is_none_or(|s| matches!(s.as_u64(), Some(400 | 404)))
    })
}

/// 讀到的下一個訊框。Ping 由 tungstenite 自動回 Pong。
enum Next {
    Text(String),
    /// 對方明確拒絕過大的訊息（1009）。
    Oversized,
    /// 對方送了其他 Close。
    Closed,
    /// 連線被重設、中斷或沒有 Close 就結束。
    Broken,
}

async fn read_with_idle<F: std::future::Future>(
    duration: Option<Duration>,
    read: F,
) -> Result<F::Output, ()> {
    match duration {
        Some(d) => tokio::time::timeout(d, read).await.map_err(|_| ()),
        None => Ok(read.await),
    }
}

async fn next_frame(conn: &mut Conn) -> Next {
    loop {
        match conn.next().await {
            Some(Ok(Message::Text(text))) => return Next::Text(text.to_string()),
            Some(Ok(Message::Close(Some(frame))))
                if frame.code == tungstenite::protocol::frame::coding::CloseCode::Size =>
            {
                return Next::Oversized;
            }
            Some(Ok(Message::Close(_))) => return Next::Closed,
            Some(Err(_)) | None | Some(Ok(Message::Binary(_) | Message::Frame(_))) => return Next::Broken,
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
        }
    }
}

async fn next_text(conn: &mut Conn) -> Option<String> {
    match next_frame(conn).await {
        Next::Text(text) => Some(text),
        Next::Closed | Next::Oversized | Next::Broken => None,
    }
}

/// 閒置期間連線可能已被上游或代理關閉：不阻塞地看一眼，已結束、出錯或有殘留訊框就不重用。
fn still_open(conn: &mut Conn) -> bool {
    use futures_util::FutureExt;
    conn.next().now_or_never().is_none()
}

/// 路由、beta、thread/window、身分等握手設定都必須相容才可依舊 ID 續接。
fn header_semantics(headers: &[(HeaderName, HeaderValue)]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    for (k, v) in headers {
        // 診斷請求 ID 每輪可變，不決定實體連線歸屬。
        if k.as_str() == "x-client-request-id" {
            continue;
        }
        h.update((k.as_str().len() as u64).to_be_bytes());
        h.update(k.as_str().as_bytes());
        let mut normalized = Vec::new();
        let bytes = if k.as_str() == "x-codex-turn-metadata"
            && let Ok(Value::Object(mut map)) = serde_json::from_slice(v.as_bytes())
        {
            // 這些逐輪追蹤值在官方 TUI 每一輪都會換；其餘已知與未知設定照比。
            for name in ["turn_id", "root_turn_id", "turn_started_at_unix_ms"] {
                map.remove(name);
            }
            serde_json::to_writer(&mut normalized, &map).ok();
            normalized.as_slice()
        } else {
            v.as_bytes()
        };
        h.update((bytes.len() as u64).to_be_bytes());
        h.update(bytes);
    }
    h.finalize().into()
}

fn event_type(text: &str) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct Typed {
        #[serde(rename = "type")]
        kind: Option<String>,
    }
    serde_json::from_str::<Typed>(text).ok()?.kind
}

fn first_event_error(text: &str, passthrough: &Passthrough) -> Option<KernelError> {
    if event_type(text).as_deref() != Some("error") {
        return None;
    }
    let mut event: Value = serde_json::from_str(text).ok()?;
    let status = event
        .get("status")
        .and_then(Value::as_u64)
        .and_then(|status| u16::try_from(status).ok())
        .filter(|s| (400..600).contains(s));
    let message = event
        .pointer("/error/message")
        .and_then(Value::as_str)
        .map_or_else(|| "Upstream WebSocket request failed.".to_owned(), str::to_owned);
    let facts = crate::failure::facts("websocket_first_event", status, &event, None, None);
    Some(KernelError::UpstreamEvent {
        facts,
        // 只採信上游明示的輸入錯誤；429、伺服器錯誤及非結構化事件維持原本契約。
        request_error: if event.pointer("/error/type").and_then(Value::as_str)
            == Some("invalid_request_error")
            && event.get("status").is_none_or(|s| s.as_u64() == Some(400))
        {
            event.get_mut("error").map(|e| Box::new(e.take()))
        } else {
            None
        },
        status: status.unwrap_or(400),
        message,
        headers: passthrough
            .iter()
            .filter_map(|(name, value)| Some((name.as_str().to_owned(), value.to_str().ok()?.to_owned())))
            .collect(),
    })
}

/// `{"type":"response.create", <請求本體的欄位，原順序>}`，與官方相同把 `type` 放在最前面。
pub fn create_message(body: &str) -> String {
    let inner = body.trim();
    match inner.strip_prefix('{').map(str::trim_start) {
        Some("}") | None => r#"{"type":"response.create"}"#.to_owned(),
        Some(rest) => format!(r#"{{"type":"response.create",{rest}"#),
    }
}

/// 一個 create 只有一次送出回執；大文字的切點是位元組，UTF-8 字元可跨幀。
async fn send_create(conn: &mut Conn, text: String) -> Result<(), tungstenite::Error> {
    use tungstenite::protocol::frame::{Fragment, Frame, coding::Data};
    const LIMIT: usize = 1024 * 1024;
    if text.len() <= LIMIT {
        return conn.send(Message::Text(text.into())).await;
    }
    let mut bytes = Bytes::from(text);
    let mut opcode = Data::Text;
    while !bytes.is_empty() {
        let part = bytes.split_to(bytes.len().min(LIMIT));
        let fragment = if bytes.is_empty() {
            Fragment::Final
        } else {
            Fragment::More
        };
        conn.send(Message::Frame(Frame::message_fragment(part, opcode, fragment)))
            .await?;
        opcode = Data::Continue;
    }
    Ok(())
}

/// 官方的 WebSocket 設定：開啟 permessage-deflate（`codex-api/src/endpoint/responses_websocket.rs`）。
fn websocket_config() -> WebSocketConfig {
    let mut extensions = ExtensionsConfig::default();
    extensions.permessage_deflate = Some(DeflateConfig::default());
    let mut config = WebSocketConfig::default();
    // 合法的圖片事件可能是單一 20 MiB 文字幀；單幀仍受整則訊息的 64 MiB 上限約束。
    config.max_frame_size = config.max_message_size;
    config.extensions = extensions;
    config
}

/// 與官方相同（`http-client/src/custom_ca.rs` 的 `build_rustls_client_config`）：先明確安裝 aws-lc-rs provider，
/// 再以預設的 builder 建立；系統根憑證，不帶客戶端憑證，不設 ALPN。
fn tls_config() -> Arc<ClientConfig> {
    // 已經安裝過時回 Err，不影響使用。
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    let (added, ignored) = roots.add_parsable_certificates(native.certs);
    tracing::debug!(added, ignored, "loaded native root certificates");
    Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

fn authority(url: &str) -> io::Result<(String, u16)> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidInput, "invalid websocket URL");
    let (scheme, rest) = url.split_once("://").ok_or_else(invalid)?;
    let hostport = rest.split('/').next().ok_or_else(invalid)?;
    let default_port = if scheme == "wss" { 443 } else { 80 };
    match hostport.rsplit_once(':') {
        Some((host, port)) if !host.ends_with(']') || hostport.starts_with('[') => {
            let port = port.parse().map_err(|_| invalid())?;
            Ok((host.trim_matches(['[', ']']).to_owned(), port))
        }
        _ => Ok((hostport.trim_matches(['[', ']']).to_owned(), default_port)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_create_message_puts_type_first_and_keeps_the_body_order() {
        assert_eq!(
            create_message(r#"{"model":"m","input":[],"stream":true}"#),
            r#"{"type":"response.create","model":"m","input":[],"stream":true}"#
        );
        assert_eq!(create_message("{}"), r#"{"type":"response.create"}"#);
        assert_eq!(create_message(" { } "), r#"{"type":"response.create"}"#);
    }

    async fn connect_to(addr: std::net::SocketAddr) -> Conn {
        let tcp = TcpStream::connect(addr).await.unwrap();
        let io: Box<dyn Io> = Box::new(tcp);
        client_async_tls_with_config(format!("ws://{addr}/"), io, None, None)
            .await
            .unwrap()
            .0
    }

    #[tokio::test]
    async fn the_wire_frame_limit_does_not_split_a_text_message_or_a_unicode_character() {
        const LIMIT: usize = 1024 * 1024;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let first = "a".repeat(LIMIT);
        let second = format!("{}界{}", "b".repeat(LIMIT - 1), "c".repeat(LIMIT));
        let expected = [first.clone(), second.clone()];
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let config = WebSocketConfig::default().max_frame_size(Some(LIMIT));
            let mut peer = tokio_tungstenite::accept_async_with_config(tcp, Some(config))
                .await
                .unwrap();
            for text in expected {
                assert_eq!(
                    peer.next().await.unwrap().unwrap().into_text().unwrap().as_str(),
                    text
                );
            }
        });
        let mut conn = connect_to(addr).await;
        send_create(&mut conn, first).await.unwrap();
        send_create(&mut conn, second).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn a_close_frame_and_a_dropped_connection_are_told_apart() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            // 第一條：送 Close；單憑 Close 不知道有沒有處理過請求。
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            ws.close(None).await.ok();
            // 第二條：不送 Close 就斷線（被重設，無法得知是否已處理）
            let (tcp, _) = listener.accept().await.unwrap();
            drop(tokio_tungstenite::accept_async(tcp).await.unwrap());
        });
        let mut closed = connect_to(addr).await;
        assert!(matches!(next_frame(&mut closed).await, Next::Closed));
        let mut broken = connect_to(addr).await;
        assert!(matches!(next_frame(&mut broken).await, Next::Broken));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn an_idle_connection_closed_meanwhile_is_not_reused() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (close, closing) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            closing.await.ok();
            ws.close(None).await.ok();
        });
        let mut conn = connect_to(addr).await;
        assert!(
            still_open(&mut conn),
            "nothing has arrived on an open idle connection"
        );
        close.send(()).unwrap();
        server.await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !still_open(&mut conn),
            "a connection closed while idle is dropped"
        );
    }

    #[tokio::test]
    async fn every_idle_connection_of_a_session_is_kept_and_the_continuation_owner_is_picked() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (close_third, closing) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let mut open = Vec::new();
            for _ in 0..2 {
                let (tcp, _) = listener.accept().await.unwrap();
                open.push(tokio_tungstenite::accept_async(tcp).await.unwrap());
            }
            let (tcp, _) = listener.accept().await.unwrap();
            let mut third = tokio_tungstenite::accept_async(tcp).await.unwrap();
            closing.await.ok();
            third.close(None).await.ok();
            open
        });
        let socket = |conn: Conn, previous: Option<&str>| Socket {
            conn,
            opening_headers: Vec::new(),
            deflate: false,
            previous: previous.map(Snapshot::for_test),
            owner: previous.map(str::to_owned),
            max_message: None,
            _capacity: None,
        };
        let transport =
            WebsocketTransport::new("http://127.0.0.1:9/backend-api/codex", "", Arc::default(), None)
                .unwrap();
        // 多代理：三條執行緒共用同一個 session-id，各自完成一輪後把連線放回
        transport.put_idle("s1".to_owned(), socket(connect_to(addr).await, Some("resp_a")));
        transport.put_idle("s1".to_owned(), socket(connect_to(addr).await, Some("resp_b")));
        transport.put_idle("s1".to_owned(), socket(connect_to(addr).await, Some("resp_c")));
        close_third.send(()).unwrap();
        let open = server.await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let owner = transport
            .take_idle("s1", Some("resp_a"), true)
            .expect("the owner's connection");
        assert_eq!(owner.previous.as_ref().map(Snapshot::id), Some("resp_a"));
        // 最近放回的（resp_c）閒置時被上游關了：跳過，拿下一條
        let next = transport
            .take_idle("s1", None, false)
            .expect("another open connection");
        assert_eq!(next.previous.as_ref().map(Snapshot::id), Some("resp_b"));
        assert!(transport.take_idle("s1", None, false).is_none(), "nothing left");
        transport.put_idle("retired".into(), next);
        transport.idle.lock().unwrap().get_mut("retired").unwrap()[0].1 = Instant::now() - IDLE_TTL;
        transport.reap_idle();
        assert!(
            transport.oldest_idle().is_none(),
            "reclaims idle sockets even without a new request"
        );
        drop(open);
    }

    #[test]
    fn urls_and_proxies_are_parsed() {
        let ws = WebsocketTransport::new("https://chatgpt.com/backend-api/codex", "", Arc::default(), None)
            .unwrap();
        assert_eq!(ws.url, "wss://chatgpt.com/backend-api/codex/responses");
        assert_eq!((ws.host.as_str(), ws.port), ("chatgpt.com", 443));
        let local = WebsocketTransport::new("http://127.0.0.1:9/backend-api/codex", "", Arc::default(), None)
            .unwrap();
        assert_eq!(local.url, "ws://127.0.0.1:9/backend-api/codex/responses");
        assert_eq!(local.port, 9);
    }

    #[test]
    fn a_first_error_event_maps_like_the_upstream_kernel() {
        let headers = vec![(
            HeaderName::from_static("x-codex-primary-used-percent"),
            HeaderValue::from_static("12"),
        )];
        let err = first_event_error(
            r#"{"type":"error","status":429,"error":{"code":"x","message":"limit"}}"#,
            &headers,
        )
        .unwrap();
        assert_eq!(
            (err.status(), err.code(), err.to_string()),
            (429, "usage_limit_reached", "limit".to_owned())
        );
        assert_eq!(err.extra_headers().len(), 1);
        let no_status = first_event_error(r#"{"type":"error","error":{"message":"y"}}"#, &headers).unwrap();
        assert_eq!((no_status.status(), no_status.code()), (400, "upstream_status"));
        assert!(first_event_error(r#"{"type":"response.created"}"#, &headers).is_none());
    }
}
