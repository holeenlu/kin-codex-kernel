//! 內部 token 驗證。所有端點一律要求，沒有例外路徑。

use axum::extract::{Request, State};
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use kin_contract::error::KernelError;
use kin_contract::headers::{INTERNAL_TOKEN, LEGACY_INTERNAL_TOKEN};

use crate::error::ApiError;
use crate::state::AppState;

pub(crate) async fn require_token(State(state): State<AppState>, request: Request, next: Next) -> Response {
    if authorized(request.headers(), state.internal_token()) {
        next.run(request).await
    } else {
        ApiError(KernelError::Unauthorized).into_response()
    }
}

fn authorized(headers: &HeaderMap, expected: &str) -> bool {
    let presented = headers
        .get(INTERNAL_TOKEN)
        .or_else(|| headers.get(LEGACY_INTERNAL_TOKEN))
        .map(|value| value.as_bytes().trim_ascii());
    presented.is_some_and(|token| constant_time_eq(token, expected.trim().as_bytes()))
}

/// 比對時間不隨第一個不同的位元組位置而變，避免從回應時間推出 token。
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn accepts_either_header_name_and_rejects_everything_else() {
        let mut headers = HeaderMap::new();
        assert!(!authorized(&headers, "secret"));
        headers.insert(LEGACY_INTERNAL_TOKEN, HeaderValue::from_static("secret"));
        assert!(authorized(&headers, "secret"));
        headers.insert(INTERNAL_TOKEN, HeaderValue::from_static("wrong"));
        assert!(
            !authorized(&headers, "secret"),
            "the namespaced header takes precedence"
        );
        headers.insert(INTERNAL_TOKEN, HeaderValue::from_static(" secret "));
        assert!(authorized(&headers, "secret"));
    }

    #[test]
    fn constant_time_eq_matches_plain_equality() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }
}
