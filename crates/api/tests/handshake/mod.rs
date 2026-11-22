//! 以實際 TCP 與握手狀態核對送出邊界；拒絕回應的正文不出現在 API 錯誤訊息內。
use super::{envelope, kernel};
use axum::{
    body::Body,
    http::{Method, Request},
    routing::any,
};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tower::ServiceExt;
use tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};

#[derive(Default)]
struct Observed {
    methods: Mutex<Vec<Method>>,
    creates: Mutex<usize>,
}

/// 可在第一次 create 後送出指定 Close；之後的握手一律回指定狀態。
async fn rejecting_upstream(status: u16, close_first: Option<CloseCode>) -> (String, Arc<Observed>) {
    let seen = Arc::new(Observed::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let record = Arc::clone(&seen);
    tokio::spawn(async move {
        if let Some(code) = close_first {
            let (stream, _) = listener.accept().await.unwrap();
            record.methods.lock().unwrap().push(Method::GET);
            let mut extensions = tungstenite::extensions::ExtensionsConfig::default();
            extensions.permessage_deflate =
                Some(tungstenite::extensions::compression::deflate::DeflateConfig::default());
            let mut config = tungstenite::protocol::WebSocketConfig::default();
            config.extensions = extensions;
            let mut ws = tokio_tungstenite::accept_async_with_config(stream, Some(config))
                .await
                .unwrap();
            let frame = ws.next().await.unwrap().unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(frame.to_text().unwrap()).unwrap()["type"],
                "response.create"
            );
            *record.creates.lock().unwrap() += 1;
            // 以 0 模擬 HTTP 降級時連不上上游。
            if status == 0 {
                drop(listener);
                ws.close(Some(CloseFrame {
                    code,
                    reason: "".into(),
                }))
                .await
                .unwrap();
                return;
            }
            ws.close(Some(CloseFrame {
                code,
                reason: "".into(),
            }))
            .await
            .unwrap();
        }
        serve_rejections(listener, record, status).await;
    });
    (format!("http://{addr}/backend-api/codex"), seen)
}

async fn serve_rejections(listener: TcpListener, seen: Arc<Observed>, status: u16) {
    let app = axum::Router::new().fallback(any(move |request: Request<Body>| {
        let seen = Arc::clone(&seen);
        async move {
            seen.methods.lock().unwrap().push(request.method().clone());
            if request.method() == Method::POST {
                return axum::http::Response::builder().status(401).body(Body::empty()).unwrap();
            }
            axum::http::Response::builder()
                .status(status)
                .header("retry-after", "73")
                .header("x-oai-request-id", "upgrade-id")
                .header("set-cookie", "secret=do-not-forward")
                .body(Body::from(r#"{"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"private upstream detail"}}"#))
                .unwrap()
        }
    }));
    axum::serve(listener, app).await.unwrap();
}

#[tokio::test]
async fn explicit_opening_429_is_reported_without_http_fallback_or_replay() {
    let (base, seen) = rejecting_upstream(429, None).await;
    let dir = tempfile::tempdir().unwrap();
    let app = kernel(&base, &dir);
    for warm in [false, true] {
        let (parts, body) = envelope(false).into_parts();
        let mut input: Value = serde_json::from_slice(&body.collect().await.unwrap().to_bytes()).unwrap();
        if warm {
            input["body"]["generate"] = json!(false);
            input["body"]["store"] = json!(false);
        }
        let response = app
            .clone()
            .oneshot(Request::from_parts(parts, Body::from(input.to_string())))
            .await
            .unwrap();
        assert_eq!(response.status(), 429);
        assert_eq!(response.headers()["x-kin-upstream-sent"], "0");
        assert_eq!(response.headers()["retry-after"], "73");
        assert!(!response.headers().contains_key("set-cookie"));
        let facts: Value =
            serde_json::from_slice(response.headers()["x-tap-upstream-error"].as_bytes()).unwrap();
        assert_eq!(
            facts,
            json!({"stage":"websocket_handshake","status":429,"request_id":"upgrade-id","retry_after":"73","code":"turn_rate_limit_exceeded","type":"rate_limit_error"})
        );
        let timing: Value =
            serde_json::from_slice(response.headers()["x-tap-turn-timing"].as_bytes()).unwrap();
        assert_eq!(timing["handshake_status"], 429);
        assert_eq!(timing["ws_handshakes"], 1);
        assert_eq!(timing["ws_send_attempts"], 0);
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(body["error"]["code"], "rate_limit_exceeded");
        assert!(!body.to_string().contains("private upstream detail"));
    }
    assert_eq!(*seen.methods.lock().unwrap(), [Method::GET, Method::GET]);
    assert_eq!(*seen.creates.lock().unwrap(), 0);
}

#[tokio::test]
async fn rejected_reconnect_preserves_the_unknown_result_of_the_previous_create() {
    let (base, seen) = rejecting_upstream(429, Some(CloseCode::Normal)).await;
    let dir = tempfile::tempdir().unwrap();
    let response = kernel(&base, &dir).oneshot(envelope(false)).await.unwrap();
    assert_eq!(response.status(), 429);
    assert_eq!(response.headers()["x-kin-upstream-sent"], "unknown");
    assert_eq!(response.headers()["retry-after"], "73");
    assert_eq!(*seen.methods.lock().unwrap(), [Method::GET, Method::GET]);
    assert_eq!(*seen.creates.lock().unwrap(), 1);
}

#[tokio::test]
async fn other_handshake_statuses_keep_the_full_http_fallback_and_its_401() {
    for (rejection, initial_methods) in [(426, vec![Method::GET]), (401, vec![Method::GET, Method::GET])] {
        let (base, seen) = rejecting_upstream(rejection, None).await;
        let dir = tempfile::tempdir().unwrap();
        let response = kernel(&base, &dir).oneshot(envelope(false)).await.unwrap();
        assert_eq!(response.status(), 401);
        let facts: Value =
            serde_json::from_slice(response.headers()["x-tap-upstream-error"].as_bytes()).unwrap();
        assert_eq!(facts["stage"], "http");
        let mut expected = initial_methods;
        expected.push(Method::POST);
        assert_eq!(*seen.methods.lock().unwrap(), expected);
    }
}

#[tokio::test]
async fn first_close_1009_falls_back_only_for_a_full_turn() {
    for warmup in [false, true] {
        let (base, seen) = rejecting_upstream(500, Some(CloseCode::Size)).await;
        let dir = tempfile::tempdir().unwrap();
        let (parts, body) = envelope(false).into_parts();
        let mut input: Value = serde_json::from_slice(&body.collect().await.unwrap().to_bytes()).unwrap();
        if warmup {
            input["body"]["generate"] = json!(false);
        }
        let response = kernel(&base, &dir)
            .oneshot(Request::from_parts(parts, Body::from(input.to_string())))
            .await
            .unwrap();
        if warmup {
            assert_eq!(response.status(), 400);
            assert_eq!(response.headers()["x-kin-upstream-sent"], "1");
            assert_eq!(*seen.methods.lock().unwrap(), [Method::GET]);
            let body: Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
            assert_eq!(body["error"]["code"], "websocket_message_too_large");
        } else {
            assert_eq!(response.status(), 401);
            assert_eq!(*seen.methods.lock().unwrap(), [Method::GET, Method::POST]);
        }
        assert_eq!(*seen.creates.lock().unwrap(), 1);
    }
}

#[tokio::test]
async fn a_failed_http_connection_after_1009_keeps_the_confirmed_send() {
    let (base, seen) = rejecting_upstream(0, Some(CloseCode::Size)).await;
    let dir = tempfile::tempdir().unwrap();
    let response = kernel(&base, &dir).oneshot(envelope(false)).await.unwrap();
    assert_eq!(response.status(), 502);
    assert_eq!(response.headers()["x-kin-upstream-sent"], "1");
    assert_eq!(*seen.creates.lock().unwrap(), 1);
}
