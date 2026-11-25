//! 把契約層的 [`KernelError`] 轉成 HTTP 回應：狀態碼、JSON 本體與送出狀態標頭都在這裡決定。

use axum::Json;
use axum::http::header::RETRY_AFTER;
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use kin_contract::error::KernelError;
use kin_contract::headers::UPSTREAM_SENT;

/// 處理程式回傳的錯誤。包一層是因為 orphan rule：不能直接替外部型別實作外部 trait。
#[derive(Debug)]
pub(crate) struct ApiError(pub(crate) KernelError);

impl From<KernelError> for ApiError {
    fn from(err: KernelError) -> Self {
        Self(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.0.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut response = (status, Json(self.0.body())).into_response();
        let headers = response.headers_mut();
        headers.insert(
            UPSTREAM_SENT,
            HeaderValue::from_static(self.0.upstream_sent().header_value()),
        );
        if let Some(value) = self
            .0
            .retry_after()
            .and_then(|value| HeaderValue::from_str(value).ok())
        {
            headers.insert(RETRY_AFTER, value);
        }
        if let Some(facts) = self.0.upstream_facts()
            && let Ok(raw) = serde_json::to_string(facts)
            && let Ok(header) = HeaderValue::from_str(&raw)
        {
            headers.insert("x-kin-upstream-error", header);
        }
        for (name, value) in self.0.extra_headers() {
            if let (Ok(name), Ok(value)) = (
                HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_str(value),
            ) {
                headers.insert(name, value);
            }
        }
        response
    }
}
