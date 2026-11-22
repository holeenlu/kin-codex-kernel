//! 共享模式的換票與帳號操作：呼叫端被取消、換票途中匯入、登錄狀態、取消與每帳號上限。

// 整合測試檔：輔助函式也屬於測試程式碼，失敗就該讓測試直接中止。
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes, to_bytes};
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures_util::stream::{self, StreamExt};
use serde_json::{Value, json};
use tap_api::{AppState, BuildInfo, router};
use tap_contract::config::SharedConfig;
use tap_contract::headers::{INTERNAL_TOKEN, UPSTREAM_SENT};
use tower::ServiceExt;

const TOKEN: &str = "test-internal-token";

fn build() -> BuildInfo {
    BuildInfo {
        name: "tap-codex-kernel",
        version: "0.0.0-test",
        build_sha: "test",
    }
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

/// 只帶 `exp` 的 access token（核心只讀到期時間）。
fn access_token(exp: u64) -> String {
    let encode = |raw: &[u8]| {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut out = String::new();
        for chunk in raw.chunks(3) {
            let bytes = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
            let n = (u32::from(bytes[0]) << 16) | (u32::from(bytes[1]) << 8) | u32::from(bytes[2]);
            for i in 0..=chunk.len() {
                out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
            }
        }
        out
    };
    format!(
        "{}.{}.sig",
        encode(br#"{"alg":"none"}"#),
        encode(json!({ "exp": exp }).to_string().as_bytes())
    )
}

/// token 端點依序回的答案：（延遲毫秒, 狀態, 本體）。用完後回一組新的 token。
type Plan = Arc<Mutex<VecDeque<(u64, u16, Value)>>>;

#[derive(Clone)]
struct Fake {
    plan: Plan,
    token_calls: Arc<AtomicUsize>,
}

async fn token_endpoint(State(fake): State<Fake>) -> Response {
    let call = fake.token_calls.fetch_add(1, Ordering::SeqCst) + 1;
    let step = fake.plan.lock().unwrap().pop_front();
    let (delay, status, body) = step.unwrap_or_else(|| {
        (
            0,
            200,
            json!({ "access_token": access_token(now_secs() + 7200), "refresh_token": format!("rt-rotated-{call}") }),
        )
    });
    tokio::time::sleep(Duration::from_millis(delay)).await;
    (StatusCode::from_u16(status).unwrap(), axum::Json(body)).into_response()
}

/// 標頭先送出、本體永遠不結束的上游：用來測試讀本體途中的取消。
async fn stalled() -> Response {
    let first = stream::once(async { Ok::<_, std::io::Error>(Bytes::from_static(b"{")) });
    let body = Body::from_stream(first.chain(stream::pending()));
    ([("content-type", "application/json")], body).into_response()
}

async fn start_fake() -> (String, Fake) {
    let fake = Fake {
        plan: Arc::default(),
        token_calls: Arc::default(),
    };
    let app = axum::Router::new()
        .route("/oauth/token", post(token_endpoint))
        .route("/backend-api/wham/usage", get(stalled))
        .route("/backend-api/codex/responses", post(stalled))
        .with_state(fake.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), fake)
}

fn kernel(base: &str) -> axum::Router {
    let raw = json!({
        "socket_path": "/unused/kernel.sock",
        "internal_token": TOKEN,
        "test_endpoints": true,
        "chatgpt_base_url": format!("{base}/backend-api/codex"),
        "oauth_token_url": format!("{base}/oauth/token"),
    });
    let config = SharedConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap();
    router(AppState::shared(config, build()))
}

fn write_credential(path: &Path, refresh_token: &str) {
    std::fs::write(
        path,
        json!({
            "kin_credential_generation": 1,
            "accounts": [{
                "access_token": access_token(now_secs() + 3600),
                "refresh_token": refresh_token,
                "chatgpt_account_id": "acct-1",
            }],
        })
        .to_string(),
    )
    .unwrap();
}

fn stored_refresh_token(path: &Path) -> String {
    let doc: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    doc["accounts"][0]["refresh_token"].as_str().unwrap().to_owned()
}

fn account(account_ref: &str, path: &Path, extra: &Value) -> Value {
    let mut env = json!({
        "ref": account_ref,
        "env_generation": 1,
        "credential_path": path,
        "device_id": "0f3c2b1a-1111-4222-8333-444455556666",
        "max_concurrency": 4,
    });
    env.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    env
}

async fn call(app: &axum::Router, method: &str, uri: &str, body: &Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(INTERNAL_TOKEN, TOKEN)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let raw = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&raw).unwrap_or(Value::Null))
}

async fn reconcile(app: &axum::Router, accounts: Vec<Value>) {
    let (status, body) = call(
        app,
        "POST",
        "/internal/v1/accounts/reconcile",
        &json!({ "accounts": accounts }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

fn setup() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("codex-credentials.shared.json");
    write_credential(&path, "rt-1");
    (dir, path)
}

#[tokio::test]
async fn a_rate_limited_handshake_during_401_recovery_keeps_the_earlier_http_send() {
    let methods = Arc::new(Mutex::new(Vec::new()));
    let record = Arc::clone(&methods);
    let upstream = axum::Router::new().fallback(move |req: Request<Body>| async move {
        let mut path = record.lock().unwrap();
        path.push(req.method().clone());
        // 先走既有握手重試及 HTTP 的 401；重讀憑據後的首次握手才明示 429。
        if path.len() == 4 {
            StatusCode::TOO_MANY_REQUESTS
        } else {
            StatusCode::UNAUTHORIZED
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    let (_dir, path) = setup();
    let app = kernel(&base);
    reconcile(&app, vec![account("a", &path, &json!({}))]).await;
    let response = app
        .oneshot(
            Request::post("/internal/v1/codex/responses")
                .header(INTERNAL_TOKEN, TOKEN)
                .body(Body::from(
                    json!({
                        "account":{"ref":"a","env_generation":1}, "body":{"model":"m","input":[]},
                        "options":{"upstream_transport":"prefer_websocket"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 429);
    assert_eq!(response.headers()[UPSTREAM_SENT], "1");
    let receipt: Value =
        serde_json::from_slice(response.headers()["x-tap-upstream-error"].as_bytes()).unwrap();
    assert_eq!(receipt["stage"], "websocket_handshake");
    assert!(receipt["code"].is_null() && receipt["retry_after"].is_null());
    use axum::http::Method;
    assert_eq!(
        *methods.lock().unwrap(),
        [Method::GET, Method::GET, Method::POST, Method::GET]
    );
}

#[tokio::test]
async fn a_rotated_refresh_token_is_written_even_when_the_caller_goes_away() {
    let (base, fake) = start_fake().await;
    let (_dir, path) = setup();
    let app = kernel(&base);
    reconcile(&app, vec![account("a", &path, &json!({}))]).await;
    fake.plan.lock().unwrap().push_back((
        300,
        200,
        json!({ "access_token": access_token(now_secs() + 7200), "refresh_token": "rt-2" }),
    ));

    // 控制面等不及就斷線：請求的 future 被丟棄
    let gone = tokio::time::timeout(
        Duration::from_millis(50),
        call(&app, "POST", "/internal/v1/accounts/a/ops/refresh", &json!({})),
    )
    .await;
    assert!(
        gone.is_err(),
        "the caller gave up before the token endpoint answered"
    );

    for _ in 0..60 {
        if stored_refresh_token(&path) == "rt-2" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        stored_refresh_token(&path),
        "rt-2",
        "the rotated refresh token reached the file"
    );
    assert_eq!(fake.token_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_credential_imported_during_a_refresh_is_not_marked_for_reauthorization() {
    let (base, fake) = start_fake().await;
    let (_dir, path) = setup();
    let app = kernel(&base);
    reconcile(&app, vec![account("a", &path, &json!({}))]).await;
    // 舊的 refresh token 早被別處用掉
    fake.plan
        .lock()
        .unwrap()
        .push_back((300, 400, json!({ "error": { "code": "refresh_token_reused" } })));

    let refreshing = {
        let app = app.clone();
        tokio::spawn(
            async move { call(&app, "POST", "/internal/v1/accounts/a/ops/refresh", &json!({})).await },
        )
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (status, body) = call(
        &app,
        "PUT",
        "/internal/v1/accounts/a/credential",
        &json!({
            "credential": {
                "access_token": access_token(now_secs() + 3600),
                "refresh_token": "rt-imported",
                "chatgpt_account_id": "acct-1",
            },
            "expected_credential_generation": 1,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = refreshing.await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "the late rejection was for the old token: {body}"
    );
    let (_, health) = call(&app, "GET", "/internal/v1/accounts/a/health", &Value::Null).await;
    assert_eq!(health["credential"], "ok", "{health}");
    assert_eq!(stored_refresh_token(&path), "rt-imported");
}

#[tokio::test]
async fn control_plane_commands_wait_for_the_registry_and_respect_disabling() {
    let (base, fake) = start_fake().await;
    let (_dir, path) = setup();
    let app = kernel(&base);
    let import = json!({
        "credential": { "access_token": access_token(now_secs() + 3600), "refresh_token": "rt-2" },
        "expected_credential_generation": 1,
    });

    let (status, body) = call(&app, "POST", "/internal/v1/accounts/a/ops/refresh", &json!({})).await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("registry_not_ready"))
    );
    let (status, _) = call(&app, "PUT", "/internal/v1/accounts/a/credential", &import).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    // 控制面停用帳號是要核心放手憑據檔：不再換票，但仍可匯入
    reconcile(&app, vec![account("a", &path, &json!({ "enabled": false }))]).await;
    let (status, body) = call(&app, "POST", "/internal/v1/accounts/a/ops/refresh", &json!({})).await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::CONFLICT, Some("account_disabled"))
    );
    let (status, body) = call(&app, "PUT", "/internal/v1/accounts/a/credential", &import).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(fake.token_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_operation_body_is_held_to_its_own_account_limit() {
    let (base, _fake) = start_fake().await;
    let dir = tempfile::tempdir().unwrap();
    let small = dir.path().join("small.json");
    let large = dir.path().join("large.json");
    write_credential(&small, "rt-s");
    write_credential(&large, "rt-l");
    let app = kernel(&base);
    reconcile(
        &app,
        vec![
            account("small", &small, &json!({ "max_request_bytes": 1024 })),
            account("large", &large, &json!({ "max_request_bytes": 1_048_576 })),
        ],
    )
    .await;
    let body = json!({ "env_generation": 1, "body": { "query": "x".repeat(4096) } });
    let (status, answer) = call(&app, "POST", "/internal/v1/accounts/small/ops/search", &body).await;
    assert_eq!(
        (status, answer["error"]["code"].as_str()),
        (StatusCode::PAYLOAD_TOO_LARGE, Some("body_too_large"))
    );
}

/// 讀上游本體途中被取消：帳號操作與非串流推理都要立刻結束。
#[tokio::test]
async fn cancelling_stops_operations_and_non_streaming_requests_while_reading_the_answer() {
    let (base, _fake) = start_fake().await;
    let (_dir, path) = setup();
    let app = kernel(&base);
    reconcile(&app, vec![account("a", &path, &json!({}))]).await;

    let cases = [
        (
            "/internal/v1/accounts/a/ops/usage",
            json!({ "request_id": "op-1", "env_generation": 1 }),
            "op-1",
        ),
        (
            "/internal/v1/codex/responses",
            json!({
                "request_id": "turn-1",
                "stream": false,
                "account": { "ref": "a", "env_generation": 1 },
                "body": { "model": "gpt-5.5", "input": "hi", "stream": true },
            }),
            "turn-1",
        ),
    ];
    for (uri, body, request_id) in cases {
        let waiting = {
            let app = app.clone();
            tokio::spawn(async move { call(&app, "POST", uri, &body).await })
        };
        let mut cancelled = false;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let (_, answer) = call(
                &app,
                "POST",
                "/internal/v1/cancel",
                &json!({ "request_id": request_id }),
            )
            .await;
            if answer["cancelled"] == true {
                cancelled = true;
                break;
            }
        }
        assert!(cancelled, "{uri} was registered for cancellation");
        let (status, answer) = tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .expect("the request ended after the cancel")
            .unwrap();
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{uri}: {answer}");
    }
}

#[tokio::test]
async fn a_cancelled_operation_reports_an_unknown_send_state() {
    let (base, _fake) = start_fake().await;
    let (_dir, path) = setup();
    let app = kernel(&base);
    reconcile(&app, vec![account("a", &path, &json!({}))]).await;
    let request = Request::builder()
        .method("POST")
        .uri("/internal/v1/accounts/a/ops/usage")
        .header(INTERNAL_TOKEN, TOKEN)
        .body(Body::from(
            json!({ "request_id": "op-2", "env_generation": 1 }).to_string(),
        ))
        .unwrap();
    let waiting = {
        let app = app.clone();
        tokio::spawn(async move { app.oneshot(request).await.unwrap() })
    };
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let (_, answer) = call(
            &app,
            "POST",
            "/internal/v1/cancel",
            &json!({ "request_id": "op-2" }),
        )
        .await;
        if answer["cancelled"] == true {
            break;
        }
    }
    let response = waiting.await.unwrap();
    assert_eq!(
        response
            .headers()
            .get(UPSTREAM_SENT)
            .and_then(|v| v.to_str().ok()),
        Some("unknown"),
        "the upstream already answered; the result is unknown, not unsent"
    );
}
