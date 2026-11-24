//! HTTP 推理路徑：對本機的假上游跑完整的請求，檢查出站請求與回給控制面的格式。

// 整合測試檔：輔助函式也屬於測試程式碼，失敗就該讓測試直接中止。
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};

use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tap_api::{AppState, BuildInfo, router};
use tap_contract::config::SingleAccountConfig;
use tap_contract::headers::{INTERNAL_TOKEN, UPSTREAM_SENT};
use tower::ServiceExt;

const TOKEN: &str = "test-internal-token";
const SSE: &str = "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\"}}\n\n\
event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}\n\n";

/// 假上游收到的一個請求：依序的標頭，以及原始本體。
type Received = (Vec<(String, String)>, Vec<u8>);

#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<Received>>>);

async fn fake_upstream(State((seen, plan)): State<(Seen, u16)>, headers: HeaderMap, body: Body) -> Response {
    let raw = to_bytes(body, usize::MAX).await.unwrap().to_vec();
    let ordered = headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                value.to_str().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    seen.0.lock().unwrap().push((ordered, raw));
    if plan == 200 {
        (
            [
                ("content-type", "text/event-stream"),
                ("x-codex-primary-used-percent", "12"),
                ("x-request-id", "not-forwarded"),
            ],
            SSE,
        )
            .into_response()
    } else {
        (
            StatusCode::from_u16(plan).unwrap(),
            [("retry-after", "7")],
            r#"{"error":{"type":"usage_limit_reached","code":"usage_limit_reached","message":"limit"}}"#,
        )
            .into_response()
    }
}

async fn fake_analytics(State((seen, _)): State<(Seen, u16)>, headers: HeaderMap, body: Body) -> Response {
    fake_upstream(State((seen, 503)), headers, body).await
}

async fn start_upstream(status: u16) -> (String, Seen) {
    let seen = Seen::default();
    let app = axum::Router::new()
        .route("/backend-api/codex/responses", post(fake_upstream))
        .route("/backend-api/codex/analytics-events/events", post(fake_analytics))
        .with_state((seen.clone(), status));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}/backend-api/codex"), seen)
}

fn kernel(base: &str, dir: &tempfile::TempDir) -> axum::Router {
    configured_kernel(base, dir, &json!({}))
}

fn configured_kernel(base: &str, dir: &tempfile::TempDir, extra: &Value) -> axum::Router {
    let credential = dir.path().join("codex-credentials.json");
    std::fs::write(
        &credential,
        r#"{"accounts":[{"access_token":"at-1","chatgpt_account_id":"acct-1","expires_at":1}]}"#,
    )
    .unwrap();
    let mut raw = json!({
        "vm_id": "vm-test",
        "device_id": "device-1",
        "socket_path": dir.path().join("k.sock"),
        "credential_path": credential,
        "internal_token": TOKEN,
        "test_endpoints": true,
        "chatgpt_base_url": base,
    });
    raw.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let config = SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap();
    let build = BuildInfo {
        name: "tap-codex-kernel",
        version: "0.0.0-test",
        build_sha: "test",
    };
    router(AppState::new(config, build).unwrap())
}

fn envelope(stream: bool) -> Request<Body> {
    // 手寫 JSON：本體的欄位順序要原樣送到上游（json! 會排序，不能用）。
    let body = format!(
        r#"{{"body":{{"model":"gpt-5.5","instructions":"x","input":[],"stream":{stream}}},
        "headers":{{"session-id":"s1","x-codex-window-id":"s1:0","openai-beta":"dropped","x-evil":"no"}},
        "stream":{stream},"session":{{"session_id":"s1"}},"request_id":"req-1",
        "options":{{"user_agent":"project/1","client_environment":{{
            "os_release":{{"id":"ubuntu","version_id":"24.04"}},"machine":"x86_64","term":"xterm-256color"}}}}}}"#
    );
    Request::builder()
        .method("POST")
        .uri("/internal/v1/codex/responses")
        .header(INTERNAL_TOKEN, TOKEN)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

#[tokio::test]
async fn forced_http_accepts_a_snapshot_but_not_a_native_ws_delta() {
    let (base, seen) = start_upstream(200).await;
    let dir = tempfile::tempdir().unwrap();
    let app = kernel(&base, &dir);
    for (native, explicit_id, expected) in [(true, true, 400), (true, false, 200), (false, true, 200)] {
        let mut body = json!({"model":"m","input":["next"],"stream":true});
        if explicit_id {
            body["previous_response_id"] = "old".into();
        }
        let mut options = json!({"upstream_transport":"http"});
        if native {
            options["ws_replay"] = json!({"previous_response_id":"old"});
        }
        let mut request = envelope(true);
        *request.body_mut() = Body::from(json!({"body":body,"options":options,"stream":true}).to_string());
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), expected);
        if expected == 400 {
            assert_eq!(response.headers()["x-kin-upstream-sent"], "0");
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(
                serde_json::from_slice::<Value>(&bytes).unwrap()["error"]["code"],
                "previous_response_not_found"
            );
            assert!(seen.0.lock().unwrap().is_empty());
        } else {
            response.into_body().collect().await.unwrap();
        }
    }
    assert_eq!(seen.0.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn decodes_gzip_and_zstd_responses_before_streaming_sse() {
    use std::io::Write;
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(SSE.as_bytes()).unwrap();
    for (codec, data) in [
        ("gzip", gzip.finish().unwrap()),
        ("zstd", zstd::stream::encode_all(SSE.as_bytes(), 3).unwrap()),
    ] {
        let upstream = axum::Router::new().route(
            "/responses",
            post(move |headers: HeaderMap| async move {
                assert_eq!(headers["accept-encoding"], "zstd,gzip");
                (
                    [("content-encoding", codec), ("content-type", "text/event-stream")],
                    data,
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let response = kernel(&format!("http://{addr}"), &dir)
            .oneshot(envelope(true))
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert!(!response.headers().contains_key("content-encoding"));
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            SSE.as_bytes()
        );
    }
}

#[tokio::test]
async fn streams_the_upstream_sse_unchanged_with_the_control_plane_headers() {
    let (base, seen) = start_upstream(200).await;
    let dir = tempfile::tempdir().unwrap();
    let response = kernel(&base, &dir).oneshot(envelope(true)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers().clone();
    assert_eq!(headers["content-type"], "text/event-stream");
    assert_eq!(headers["x-model"], "gpt-5.5");
    assert_eq!(headers["x-terminal-state"], "verified");
    assert_eq!(headers["x-codex-primary-used-percent"], "12");
    assert!(headers.get("x-request-id").is_none());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, SSE.as_bytes());

    let calls = seen.0.lock().unwrap();
    let (outbound, raw) = &calls[0];
    let names: Vec<&str> = outbound.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
        [
            "version",
            "x-codex-window-id",
            "x-codex-routing-hint",
            "x-client-request-id",
            "session-id",
            "thread-id",
            "accept",
            "content-encoding",
            "content-type",
            "authorization",
            "chatgpt-account-id",
            "originator",
            "user-agent",
            "accept-encoding",
            "host",
            "content-length",
        ]
    );
    let get = |name: &str| outbound.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str());
    assert_eq!(get("authorization"), Some("Bearer at-1"));
    assert_eq!(get("chatgpt-account-id"), Some("acct-1"));
    assert_eq!(get("originator"), Some("project"));
    assert_eq!(get("user-agent"), Some("project/1"));
    // 舊版以 level 3 串流壓縮此短請求，只產生訊框標頭、一個 raw block 標頭與原文。
    let body = br#"{"model":"gpt-5.5","instructions":"x","input":[],"stream":true}"#;
    let legacy_header = [0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58, 0xf9, 0x01, 0x00];
    assert_eq!(*raw, [legacy_header.as_slice(), body.as_slice()].concat());
    assert_eq!(zstd::stream::decode_all(raw.as_slice()).unwrap(), body);
}

#[tokio::test]
async fn routing_hint_uses_the_body_tier_over_an_envelope_header() {
    let (base, seen) = start_upstream(200).await;
    let dir = tempfile::tempdir().unwrap();
    let app = kernel(&base, &dir);
    for (model, tier, expected) in [
        ("m", json!("priority"), Some("model=m;tier=priority")),
        ("m", json!(" FLEX "), Some("model=m;tier=flex")),
        ("m", json!(" DEFAULT "), Some("model=m")),
        ("m", json!("auto"), Some("model=m")),
        ("m", json!("standard"), Some("model=m")),
        ("m", json!("priority;other=value"), Some("model=m")),
        ("m", json!(7), Some("model=m")),
        ("m;tier=priority", Value::Null, None),
    ] {
        let body = json!({"model":model,"input":[],"service_tier":tier});
        let mut request = envelope(false);
        *request.body_mut() = Body::from(
            json!({
                "body":body, "headers":{"x-codex-routing-hint":"model=other;tier=priority"},
                "options":{"upstream_transport":"http"}
            })
            .to_string(),
        );
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let calls = seen.0.lock().unwrap();
        let (headers, compressed) = calls.last().unwrap();
        let hint = headers.iter().find(|(k, _)| k == "x-codex-routing-hint");
        assert_eq!(hint.map(|(_, v)| v.as_str()), expected);
        let sent = zstd::stream::decode_all(compressed.as_slice()).unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&sent).unwrap(), body);
    }
}

#[tokio::test]
async fn non_stream_requests_return_the_terminal_event() {
    let (base, _) = start_upstream(200).await;
    let dir = tempfile::tempdir().unwrap();
    let response = kernel(&base, &dir).oneshot(envelope(false)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-codex-primary-used-percent"], "12");
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["type"], "response.completed");
    assert_eq!(body["response"]["status"], "completed");
}

#[tokio::test]
async fn an_upstream_429_keeps_the_status_text_and_retry_after() {
    let (base, _) = start_upstream(429).await;
    let dir = tempfile::tempdir().unwrap();
    let response = kernel(&base, &dir).oneshot(envelope(true)).await.unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["retry-after"], "7");
    assert_eq!(response.headers()[UPSTREAM_SENT], "1");
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["error"]["code"], "usage_limit_reached");
    assert_eq!(body["error"]["type"], "api_error");
    assert_eq!(
        body["error"]["message"],
        r#"upstream status 429: {"error":{"type":"usage_limit_reached","code":"usage_limit_reached","message":"limit"}}"#
    );
}

#[tokio::test]
async fn non_stream_returns_terminal_events_before_the_upstream_closes() {
    use futures_util::{StreamExt, stream};
    use std::convert::Infallible;
    use std::time::Duration;

    let errors = [
        "api_key_spend_limit_exceeded",
        "organization_spend_limit_exceeded",
        "project_spend_limit_exceeded",
        "bio_policy",
        "cyber_policy",
        "policy_capacity_unavailable",
        "server_is_overloaded",
    ];
    let mut events = vec![json!({
        "type": "response.completed",
        "response": {"id": "r1", "usage": {"input_tokens": 4, "output_tokens": 2}}
    })];
    for code in errors {
        let detail = json!({"code": code, "message": "rejected", "headers": {"Retry-After": "7"}});
        events.push(json!({"type": "error", "error": detail}));
        events.push(json!({"type": "response.failed", "response": {"error": detail}}));
    }
    for event in events {
        // 把一個多行 SSE 事件切成數個 chunk，送完仍保持連線，驗證不依賴 EOF。
        let encoded = event.to_string();
        let split = encoded.find(',').unwrap();
        let raw = format!(
            "data: {}\r\ndata: {}\r\n\r\n",
            &encoded[..=split],
            &encoded[split + 1..]
        );
        let pieces: Vec<_> = raw
            .as_bytes()
            .chunks(7)
            .map(|b| Ok::<_, Infallible>(axum::body::Bytes::copy_from_slice(b)))
            .collect();
        let upstream = axum::Router::new().route(
            "/responses",
            post(move || async move {
                (
                    [("content-type", "text/event-stream")],
                    Body::from_stream(stream::iter(pieces).chain(stream::pending())),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            let response = kernel(&format!("http://{addr}"), &dir)
                .oneshot(envelope(false))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            serde_json::from_slice::<Value>(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap()
        })
        .await;
        server.abort();
        assert_eq!(result.expect("terminal event must not wait for EOF"), event);
    }
}

#[tokio::test]
async fn a_refused_connection_is_a_transport_error_that_was_not_sent() {
    let dir = tempfile::tempdir().unwrap();
    let response = kernel("http://127.0.0.1:9/backend-api/codex", &dir)
        .oneshot(envelope(true))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(response.headers()[UPSTREAM_SENT], "0");
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["error"]["code"], "upstream_transport");
}

#[tokio::test]
async fn analytics_respects_options_and_never_retries_inference_after_event_failure() {
    let (base, seen) = start_upstream(200).await;
    let dir = tempfile::tempdir().unwrap();
    let app = configured_kernel(
        &base,
        &dir,
        &json!({
            "analytics": true,
            "upstream_transport": "http",
            "telemetry_runtime": {"runtime_os":"linux", "runtime_os_version":"24.04", "runtime_arch":"x86_64"},
        }),
    );
    for (session, options) in [
        ("disabled", json!({"analytics":false})),
        ("observed", json!({})),
        ("observed", json!({})),
        (
            "incomplete",
            json!({"telemetry_runtime":{"runtime_os":"", "runtime_os_version":"24.04", "runtime_arch":"x86_64"}}),
        ),
    ] {
        let request = Request::builder().method("POST").uri("/internal/v1/codex/responses")
            .header(INTERNAL_TOKEN, TOKEN).header("content-type", "application/json")
            .body(Body::from(json!({"body":{"model":"m", "input":[]}, "session":{"session_id":session}, "options":options}).to_string())).unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        response.into_body().collect().await.unwrap();
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while seen.0.lock().unwrap().len() < 5 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // 事件是另起任務送的：到齊後再等一段，多送（不該送的會話、重試）的事件也會在這段時間內抵達本機假上游。
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let calls = seen.0.lock().unwrap();
    let events: Vec<_> = calls
        .iter()
        .filter_map(|(headers, raw)| {
            let body = serde_json::from_slice::<Value>(raw).ok()?;
            body.get("events")?;
            Some((headers, body))
        })
        .collect();
    assert_eq!(
        calls.len(),
        5,
        "four inference calls and one event, with no retry"
    );
    assert_eq!(events.len(), 1, "one event per account and session");
    let (headers, event) = &events[0];
    // 檢查分析請求實際送出的標頭名稱與順序。
    let names: Vec<&str> = headers.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
        [
            "authorization",
            "chatgpt-account-id",
            "content-type",
            "originator",
            "user-agent",
            "accept",
            "accept-encoding",
            "host",
            "content-length"
        ]
    );
    let get = |name: &str| headers.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str());
    assert_eq!(get("authorization"), Some("Bearer at-1"));
    assert_eq!(get("originator"), Some("codex-tui"));
    // client 欄位與這次請求的身分一致，不是核心自己的名稱與版本。
    let params = &event["events"][0]["event_params"];
    assert_eq!(params["session_id"], "observed");
    assert_eq!(params["runtime"]["runtime_os_version"], "24.04");
    assert_eq!(params["runtime"]["runtime_arch"], "x86_64");
    assert_eq!(params["runtime"]["codex_rs_version"], "0.171.0");
    assert_eq!(params["app_server_client"]["client_name"], "codex-tui");
    assert_eq!(params["app_server_client"]["product_client_id"], "codex-tui");
    assert_eq!(params["app_server_client"]["client_version"], "0.171.0");
}

/// 依序送出幾個推理請求，回傳每一個上游收到的 `user-agent` 與 `originator`。
async fn outbound_identities(
    app: &axum::Router,
    seen: &Seen,
    user_agents: &[Option<&str>],
) -> Vec<(String, String)> {
    for user_agent in user_agents {
        let mut options = json!({"client_environment":{
            "os_release":{"id":"ubuntu","version_id":"24.04"},"machine":"x86_64","term":"xterm-256color"}});
        if let Some(user_agent) = user_agent {
            options["user_agent"] = json!(user_agent);
        }
        let request = Request::builder()
            .method("POST")
            .uri("/internal/v1/codex/responses")
            .header(INTERNAL_TOKEN, TOKEN)
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"body":{"model":"m","input":[]}, "options": options}).to_string(),
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{user_agent:?}");
        response.into_body().collect().await.unwrap();
    }
    let calls = seen.0.lock().unwrap();
    calls
        .iter()
        .map(|(headers, _)| {
            let get = |name: &str| {
                headers
                    .iter()
                    .find(|(n, _)| n == name)
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default()
            };
            (get("user-agent"), get("originator"))
        })
        .collect()
}

#[tokio::test]
async fn the_default_identity_comes_from_the_account_environment() {
    let (base, seen) = start_upstream(200).await;
    let dir = tempfile::tempdir().unwrap();
    let app = configured_kernel(&base, &dir, &json!({"upstream_transport": "http"}));
    let official = "codex-tui/0.171.0 (Ubuntu 24.4.0; x86_64) xterm-256color (codex-tui; 0.171.0)";
    let seen = outbound_identities(&app, &seen, &[None, Some("auto"), Some("")]).await;
    for (user_agent, originator) in &seen {
        assert_eq!(
            (user_agent.as_str(), originator.as_str()),
            (official, "codex-tui")
        );
    }
}

#[tokio::test]
async fn a_request_user_agent_overrides_the_config_and_empty_keeps_it() {
    let (base, seen) = start_upstream(200).await;
    let dir = tempfile::tempdir().unwrap();
    let app = configured_kernel(
        &base,
        &dir,
        &json!({"upstream_transport": "http", "user_agent": "configured/2 (x)"}),
    );
    let seen = outbound_identities(&app, &seen, &[None, Some(""), Some("project/1"), Some("AUTO")]).await;
    let pairs: Vec<(&str, &str)> = seen
        .iter()
        .map(|(user_agent, originator)| (user_agent.as_str(), originator.as_str()))
        .collect();
    assert_eq!(
        pairs,
        [
            ("configured/2 (x)", "configured"),
            ("configured/2 (x)", "configured"),
            ("project/1", "project"),
            (
                "codex-tui/0.171.0 (Ubuntu 24.4.0; x86_64) xterm-256color (codex-tui; 0.171.0)",
                "codex-tui"
            ),
        ]
    );
}
