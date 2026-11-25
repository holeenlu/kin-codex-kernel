//! 路由與處理程式。處理程式只負責 HTTP 邊界，回傳 [`ApiError`] 而不自行拼錯誤 JSON。

use axum::body::{Body, Bytes, to_bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::HeaderMap;
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router, middleware};
use futures_util::{StreamExt, stream};
use http_body::Frame;
use http_body_util::StreamBody;
use kin_contract::abi::{Abi, Capability};
use kin_contract::envelope::ResponsesEnvelope;
use kin_contract::error::KernelError;
use kin_contract::headers::TURN_TIMING;
use kin_contract::registry::SharedHealth;
use kin_contract::status::{Describe, Health, Impersonate};
use kin_upstream::timing::TurnTiming;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::accounts;
use crate::auth::require_token;
use crate::credential;
use crate::error::ApiError;
use crate::state::{AppState, Mode};

pub fn router(state: AppState) -> Router {
    let mut router = Router::new()
        .route("/internal/health", get(health))
        .route("/internal/v1/describe", get(describe))
        .route(
            "/internal/v1/codex/responses",
            // 大小上限由處理程式依設定判斷，才能回控制面認得的 `body_too_large`。
            post(responses).layer(DefaultBodyLimit::disable()),
        )
        .route("/internal/v1/interrupt", post(interrupt))
        .route("/internal/v1/cancel", post(cancel));
    if matches!(state.mode(), Mode::Single { .. }) {
        router = router.route(
            "/internal/v1/codex/responses/packed",
            post(crate::packed::responses).layer(DefaultBodyLimit::disable()),
        );
    }
    if matches!(state.mode(), Mode::Shared { .. }) {
        router = accounts::routes(router);
    }
    router
        .fallback(not_found)
        .layer(middleware::from_fn_with_state(state.clone(), require_token))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Response {
    let build = state.build();
    match state.mode() {
        Mode::Single { config, upstream } => Json(Health {
            uds_packed_v1: true,
            ok: true,
            engine: "codex",
            provider: build.name,
            version: build.version,
            worker_version: build.version,
            build_sha: build.build_sha,
            abi: Abi::SingleAccount,
            vm_id: config.vm_id.clone(),
            device_id: config.device_id.clone(),
            accounts: kin_credentials::account_count(&config.credential_path).await,
            reachable: true,
            proxy_ok: Some(upstream.proxy_ok().await),
        })
        .into_response(),
        // 登錄還沒對帳時程序仍在服務（回 200）；控制面看 `registry_state` 決定何時對帳、何時派單。
        Mode::Shared { registry, .. } => Json(SharedHealth {
            ok: true,
            engine: "codex",
            provider: build.name,
            version: build.version,
            worker_version: build.version,
            build_sha: build.build_sha,
            abi: Abi::Shared,
            registry_state: registry.state(),
            // 需要重新授權，或這個帳號的出口 HTTP 熔斷中。
            accounts: registry.counts_with(|runtime, auth| auth.failing() || runtime.exit_down()),
        })
        .into_response(),
    }
}

async fn describe(State(state): State<AppState>) -> Json<Describe> {
    let build = state.build();
    // 每完成一項上游能力就加進來。
    let mut capabilities = vec![
        Capability::Responses,
        Capability::ResponsesWebsocket,
        Capability::Cancel,
        Capability::ResponsesInterrupt,
        Capability::ResponsesWsReplay,
        Capability::ResponsesTurnTiming,
        Capability::Analytics,
    ];
    let (abi, user_agent) = match state.mode() {
        Mode::Single { config, .. } => {
            capabilities.push(Capability::UdsPackedV1);
            (Abi::SingleAccount, config.custom_user_agent())
        }
        Mode::Shared { .. } => {
            capabilities.extend([Capability::MultiAccount, Capability::AccountOps]);
            (Abi::Shared, None)
        }
    };
    // 與出站請求同一個規則（`kin_identity::resolve`）：自訂 UA 帶出的 originator 與版本。
    let (originator, version) = kin_identity::advertised(user_agent);
    Json(Describe {
        abi,
        kernel: build.name,
        kernel_version: build.version,
        build_sha: build.build_sha,
        capabilities,
        impersonate: Some(Impersonate {
            originator: originator.to_owned(),
            version: version.to_owned(),
        }),
    })
}

async fn responses(State(state): State<AppState>, body: Body) -> Result<Response, ApiError> {
    // 先讀完並檢查大小：超過上限一律回 413，不論內容為何。
    let raw = to_bytes(body, state.body_limit())
        .await
        .map_err(|_| KernelError::BodyTooLarge)?;
    respond(state, raw).await
}

pub(crate) async fn respond(state: AppState, raw: Bytes) -> Result<Response, ApiError> {
    let envelope: ResponsesEnvelope =
        serde_json::from_slice(&raw).map_err(|err| KernelError::InvalidBody(err.to_string()))?;
    let target = state.target(&envelope, raw.len())?;
    // 取得帳號之後的失敗都記到該帳號，供帳號健康回報。
    let failed = |err: KernelError| {
        target.note_error(&err);
        ApiError(err)
    };
    // 單帳號模式每次重讀、不自行刷新；共享模式在這裡做請求前的刷新與 401 復原。
    let credential = credential::for_request(&target, &envelope)
        .await
        .map_err(failed)?;
    let inflight = state.register(envelope.request_id.as_deref(), target.cancel_parent());
    let timing = TurnTiming::default();
    let receipt = Receipt {
        timing: timing.clone(),
        request: envelope.request_id.clone(),
        account: target
            .lease()
            .map_or_else(|| state.label().to_owned(), |l| l.env.account_ref.clone()),
        cancel: inflight.token().clone(),
    };
    let rejected = |err: KernelError| {
        timing.end(
            if inflight.token().is_cancelled() {
                "cancelled"
            } else {
                "error"
            },
            Some(err.upstream_sent().header_value()),
        );
        let mut response = failed(err).into_response();
        response.headers_mut().extend(timing_header(&timing));
        response
    };
    let mut accepted = match Box::pin(credential::send(
        &target,
        &envelope,
        credential,
        inflight.token(),
        &timing,
    ))
    .await
    {
        Ok(accepted) => accepted,
        Err(e) => return Ok(rejected(e)),
    };

    let passthrough = accepted.passthrough.clone();
    if !envelope.stream {
        let event = match accepted.into_terminal_event(inflight.token()).await {
            Ok(e) => e,
            Err(e) => return Ok(rejected(e)),
        };
        let mut response = Json(event).into_response();
        response.headers_mut().extend(passthrough);
        response.headers_mut().extend(timing_header(&timing));
        return Ok(response);
    }

    let control = accepted
        .interrupt_channel()
        .is_some_and(|handle| inflight.bind(handle));
    let model = accepted.model.clone();
    let cancel = inflight.token().clone();
    let chunks = accepted
        .into_stream()
        .take_until(cancel.cancelled_owned())
        .map(|r| r.map(Frame::data));
    // 只在最後附內部 trailer，不插 SSE 事件、不提早放行；drop 時保留取消或中斷的事實。
    let end = stream::once(async move {
        let _keep = (inflight, target);
        Ok(Frame::trailers(receipt.headers()))
    });
    let mut response = Response::new(Body::new(StreamBody::new(chunks.chain(end))));
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(
        "x-tap-interrupt",
        HeaderValue::from_static(if control { "websocket" } else { "unavailable" }),
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    if let Some(model) = model
        .as_deref()
        .and_then(|model| HeaderValue::from_str(model).ok())
    {
        headers.insert("x-model", model);
    }
    // 與上游核心相同：標頭在串流開始時送出，值固定為 verified；終態由控制面依事件判定。
    headers.insert("x-terminal-state", HeaderValue::from_static("verified"));
    headers.extend(passthrough);
    headers.extend(timing_header(&timing));
    headers.insert("trailer", HeaderValue::from_static(TURN_TIMING));
    Ok(response)
}

fn timing_header(timing: &TurnTiming) -> HeaderMap {
    let mut h = HeaderMap::new();
    if let Some(v) = timing.json().and_then(|s| HeaderValue::from_str(&s).ok()) {
        h.insert(TURN_TIMING, v);
    }
    h
}

struct Receipt {
    timing: TurnTiming,
    request: Option<String>,
    account: String,
    cancel: CancellationToken,
}
impl Receipt {
    fn finish(&self) {
        self.timing.finish_if_open(if self.cancel.is_cancelled() {
            "cancelled"
        } else {
            "interrupted"
        });
    }
    fn headers(&self) -> HeaderMap {
        self.timing.finish_if_open(if self.cancel.is_cancelled() {
            "cancelled"
        } else {
            "end_without_observed_terminal"
        });
        timing_header(&self.timing)
    }
}
impl Drop for Receipt {
    fn drop(&mut self) {
        self.finish();
        tracing::info!(
            request_id = self.request.as_deref().unwrap_or(""),
            account_ref = self.account,
            turn = self.timing.json(),
            "responses turn"
        );
    }
}

#[derive(Deserialize)]
struct InterruptRequest {
    request_id: String,
    response_id: String,
}

async fn interrupt(State(state): State<AppState>, body: Body) -> Response {
    let Ok(raw) = to_bytes(body, 4096).await else {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    };
    let Ok(req) = serde_json::from_slice::<InterruptRequest>(&raw) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if req.response_id.is_empty() || req.response_id.len() > 256 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if let Some(handle) = state.interrupt(&req.request_id)
        && tokio::time::timeout(std::time::Duration::from_secs(3), handle.forward(req.response_id)).await
            == Ok(true)
    {
        return StatusCode::NO_CONTENT.into_response();
    }
    StatusCode::CONFLICT.into_response()
}

#[derive(Deserialize)]
struct CancelRequest {
    #[serde(default)]
    request_id: Option<String>,
}

async fn cancel(State(state): State<AppState>, body: Body) -> Json<Value> {
    // 控制面在客戶端斷線時送出，容許找不到；連線關閉本身也會停止上游。
    let request_id = to_bytes(body, 64 * 1024)
        .await
        .ok()
        .and_then(|raw| serde_json::from_slice::<CancelRequest>(&raw).ok())
        .and_then(|request| request.request_id);
    let cancelled = request_id.as_deref().is_some_and(|id| state.cancel(id));
    Json(json!({ "ok": true, "cancelled": cancelled }))
}

pub(crate) async fn not_found() -> ApiError {
    KernelError::NotFound.into()
}
