//! 單帳號相容模式（ABI 1）的 HTTP 邊界契約。
//!
//! 鎖定的是 vm2api 控制面依賴的行為；推理本身尚未實作（K0）。

// 整合測試檔：輔助函式也屬於測試程式碼，失敗就該讓測試直接中止。
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::os::unix::fs::PermissionsExt;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use http_body_util::BodyExt;
use kin_api::{AppState, BuildInfo, router, serve};
use kin_contract::config::SingleAccountConfig;
use kin_contract::headers::{INTERNAL_TOKEN, LEGACY_INTERNAL_TOKEN, UPSTREAM_SENT};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const TOKEN: &str = "test-internal-token";

fn build() -> BuildInfo {
    BuildInfo {
        name: "kin-codex-kernel",
        version: "0.0.0-test",
        build_sha: "test",
    }
}

fn config(socket: &str, max_request_bytes: u64) -> SingleAccountConfig {
    let raw = json!({
        "vm_id": "vm-test",
        "device_id": "0f3c2b1a-1111-4222-8333-444455556666",
        "socket_path": socket,
        "credential_path": "/tmp/does-not-exist/codex-credentials.json",
        "internal_token": TOKEN,
        "max_request_bytes": max_request_bytes,
    });
    SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap()
}

fn app(max_request_bytes: u64) -> axum::Router {
    router(AppState::new(config("/tmp/unused.sock", max_request_bytes), build()).unwrap())
}

async fn call(app: axum::Router, request: Request<Body>) -> (StatusCode, HeaderMap, Value) {
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        headers,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn request(method: &str, path: &str, token: Option<(&str, &str)>, body: Body) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some((name, value)) = token {
        builder = builder.header(name, value);
    }
    builder
        .header("content-type", "application/json")
        .body(body)
        .unwrap()
}

fn authed(method: &str, path: &str, body: Body) -> Request<Body> {
    request(method, path, Some((INTERNAL_TOKEN, TOKEN)), body)
}

#[tokio::test]
async fn every_endpoint_requires_the_internal_token() {
    for (method, path) in [
        ("GET", "/internal/health"),
        ("GET", "/internal/v1/describe"),
        ("POST", "/internal/v1/codex/responses"),
        ("POST", "/internal/v1/cancel"),
        ("GET", "/anything-else"),
    ] {
        let (status, headers, body) = call(app(1024), request(method, path, None, Body::empty())).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
        assert_eq!(body["error"]["code"], "internal_unauthorized");
        assert_eq!(headers[UPSTREAM_SENT], "0");
    }
    let legacy = request(
        "GET",
        "/internal/health",
        Some((LEGACY_INTERNAL_TOKEN, TOKEN)),
        Body::empty(),
    );
    assert_eq!(call(app(1024), legacy).await.0, StatusCode::OK);
}

#[tokio::test]
async fn health_has_the_fields_the_control_plane_reads_and_no_status() {
    let (status, _, body) = call(app(1024), authed("GET", "/internal/health", Body::empty())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], true);
    assert_eq!(body["engine"], "codex");
    assert_eq!(body["provider"], "kin-codex-kernel");
    assert_eq!(body["vm_id"], "vm-test");
    assert_eq!(body["abi"], 1);
    assert_eq!(body["worker_version"], body["version"]);
    assert_eq!(body["accounts"], 0);
    assert_eq!(body["device_id"], "0f3c2b1a-1111-4222-8333-444455556666");
    assert!(body.get("status").is_none());
}

#[tokio::test]
async fn describe_reports_abi_1_with_responses_websocket_and_cancel() {
    let (status, _, body) = call(app(1024), authed("GET", "/internal/v1/describe", Body::empty())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["abi"], 1);
    assert_eq!(body["kernel"], "kin-codex-kernel");
    assert_eq!(
        body["capabilities"],
        json!([
            "responses",
            "responses_websocket",
            "cancel",
            "responses_interrupt",
            "responses_ws_replay",
            "responses_turn_timing",
            "analytics",
            "uds_packed_v1"
        ])
    );
    assert_eq!(
        body["impersonate"],
        json!({"originator": "codex-tui", "version": "0.171.0"})
    );
}

#[tokio::test]
async fn describe_reports_the_identity_of_a_configured_user_agent() {
    for (user_agent, expected) in [
        (
            "codex_exec/0.170.0 (Debian 12.0.0; x86_64) unknown",
            json!({"originator": "codex_exec", "version": "0.170.0"}),
        ),
        ("auto", json!({"originator": "codex-tui", "version": "0.171.0"})),
        ("curl", json!({"originator": "codex-tui", "version": "0.171.0"})),
    ] {
        let raw = json!({
            "vm_id": "vm-test",
            "device_id": "0f3c2b1a-1111-4222-8333-444455556666",
            "socket_path": "/tmp/unused.sock",
            "credential_path": "/tmp/does-not-exist/codex-credentials.json",
            "internal_token": TOKEN,
            "user_agent": user_agent,
        });
        let config = SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap();
        let app = router(AppState::new(config, build()).unwrap());
        let (_, _, body) = call(app, authed("GET", "/internal/v1/describe", Body::empty())).await;
        assert_eq!(body["impersonate"], expected, "{user_agent}");
    }
}

#[tokio::test]
async fn responses_enforce_the_size_limit_first() {
    let oversized = authed("POST", "/internal/v1/codex/responses", Body::from(vec![b'x'; 17]));
    let (status, headers, body) = call(app(16), oversized).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body["error"]["code"], "body_too_large");
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert_eq!(headers[UPSTREAM_SENT], "0");
}

#[tokio::test]
async fn a_malformed_envelope_is_rejected_before_anything_is_sent() {
    let envelope = authed(
        "POST",
        "/internal/v1/codex/responses",
        Body::from(r#"{"stream":true}"#),
    );
    let (status, headers, body) = call(app(1024), envelope).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "invalid_request");
    assert_eq!(headers[UPSTREAM_SENT], "0");
}

#[tokio::test]
async fn an_unreadable_credential_file_is_invalid_json_and_never_sent() {
    let envelope = authed(
        "POST",
        "/internal/v1/codex/responses",
        Body::from(r#"{"body":{"model":"m"},"stream":true}"#),
    );
    let (status, headers, body) = call(app(1024), envelope).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body["error"]["code"], "invalid_json");
    assert_eq!(headers[UPSTREAM_SENT], "0");
}

#[tokio::test]
async fn cancel_is_accepted() {
    let cancel = authed(
        "POST",
        "/internal/v1/cancel",
        Body::from(r#"{"request_id":"req-1"}"#),
    );
    let (status, _, body) = call(app(1024), cancel).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], true);
    assert_eq!(body["cancelled"], false);
}

#[tokio::test]
async fn unknown_endpoints_return_the_shared_error_shape() {
    let (status, headers, body) = call(app(1024), authed("GET", "/internal/v1/nope", Body::empty())).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "not_found");
    assert_eq!(headers[UPSTREAM_SENT], "0");
}

#[tokio::test]
async fn serves_on_a_private_unix_socket_and_cleans_up() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("run").join("codex-kernel.sock");
    std::fs::create_dir_all(socket.parent().unwrap()).unwrap();
    drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());

    let state = AppState::new(config(socket.to_str().unwrap(), 1024), build()).unwrap();
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(serve(state, shutdown.clone()));

    let mut stream = None;
    for _ in 0..100 {
        if let Ok(conn) = tokio::net::UnixStream::connect(&socket).await {
            stream = Some(conn);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let mut stream = stream.expect("kernel socket did not come up");
    assert_eq!(
        kin_api::bind(&socket).unwrap_err().kind(),
        std::io::ErrorKind::AddrInUse
    );
    let probe = tokio::net::UnixStream::connect(&socket).await.unwrap();
    drop(probe);
    assert_eq!(
        std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let request = format!(
        "GET /internal/health HTTP/1.1\r\nhost: kernel\r\n{INTERNAL_TOKEN}: {TOKEN}\r\nconnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains(r#""engine":"codex""#));

    shutdown.cancel();
    server.await.unwrap().unwrap();
    assert!(!socket.exists(), "socket must be removed on shutdown");
}

#[tokio::test]
async fn refuses_to_replace_a_directory() {
    let dir = tempfile::tempdir().unwrap();
    let err = kin_api::bind(dir.path()).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
    assert!(dir.path().is_dir());
}

#[tokio::test]
async fn bind_preserves_regular_files_and_symlinks() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("credential.json");
    let link = dir.path().join("kernel.sock");
    std::fs::write(&file, b"keep").unwrap();
    std::os::unix::fs::symlink(&file, &link).unwrap();
    for path in [&file, &link] {
        assert_eq!(
            kin_api::bind(path).unwrap_err().kind(),
            std::io::ErrorKind::AlreadyExists
        );
    }
    assert!(link.is_symlink());
    assert_eq!(std::fs::read(file).unwrap(), b"keep");
}

#[tokio::test]
async fn packed_limits_ingress_and_receipts_include_misses() {
    use kin_contract::headers::{UDS_BASE, UDS_COPY, UDS_MISS, UDS_RECEIVED};
    const PATH: &str = "/internal/v1/codex/responses/packed";
    let app = app(1024);
    let mut pending = Vec::new();
    for _ in 0..4 {
        let body = Body::from_stream(futures_util::stream::pending::<
            Result<axum::body::Bytes, std::io::Error>,
        >());
        let mut request = Box::pin(app.clone().oneshot(authed("POST", PATH, body)));
        assert!(
            request
                .as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
        pending.push(request);
    }
    let (status, headers, body) = call(app.clone(), authed("POST", PATH, Body::empty())).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body["error"]["code"], "account_busy");
    assert_eq!(headers[UPSTREAM_SENT], "0");
    drop(pending);
    let mut miss = authed("POST", PATH, Body::from("abc"));
    miss.headers_mut().insert(UDS_BASE, "absent".parse().unwrap());
    miss.headers_mut().insert(UDS_COPY, "0,0,1".parse().unwrap());
    let (status, headers, _) = call(app, miss).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(headers[UDS_MISS], "1");
    assert_eq!(headers[UDS_RECEIVED], "3");
    assert_eq!(headers[UPSTREAM_SENT], "0");
}
