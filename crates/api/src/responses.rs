//! 已還原的 Responses 請求共用同一套推理、串流與計時流程。
use axum::Json;
use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header::CONTENT_TYPE};
use axum::response::{IntoResponse, Response};
use futures_util::{StreamExt, stream};
use http_body::Frame;
use http_body_util::StreamBody;
use kin_contract::envelope::ResponsesEnvelope;
use kin_contract::error::KernelError;
use kin_contract::headers::TURN_TIMING;
use kin_upstream::timing::TurnTiming;
use tokio_util::sync::CancellationToken;

use crate::{AppState, credential, error::ApiError};

pub(crate) async fn respond(state: &AppState, raw: Bytes) -> Result<Response, ApiError> {
    let envelope: ResponsesEnvelope =
        serde_json::from_slice(&raw).map_err(|err| KernelError::InvalidBody(err.to_string()))?;
    let target = state.target(&envelope, raw.len())?;
    // 取得帳號之後的失敗都記到該帳號，供帳號健康回報。
    let failed = |err: KernelError| {
        target.note_error(&err);
        ApiError(err)
    };
    // 單帳號模式每次重讀、不自行刷新；共享模式在這裡做請求前的刷新與 401 復原。
    let inflight = state.register(envelope.request_id.as_deref(), target.cancel_parent())?;
    let credential = credential::for_request(&target, &envelope)
        .await
        .map_err(failed)?;
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
