//! WebSocket 推理路徑：對本機的 WebSocket 假上游跑完整請求，檢查升級標頭、訊息、回給控制面的格式與連線重用。

// 整合測試檔：輔助函式也屬於測試程式碼，失敗就該讓測試直接中止。
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use serde_json::Value;
use tap_api::{AppState, BuildInfo, router};
use tap_contract::config::SingleAccountConfig;
use tap_contract::headers::INTERNAL_TOKEN;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request as WsRequest, Response as WsResponse};
use tower::ServiceExt;

const TOKEN: &str = "test-internal-token";

mod handshake;

/// 一條連線：升級標頭（線上順序）與收到的訊息。
type Connection = (axum::http::HeaderMap, Vec<String>);

#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<Connection>>>);

/// 假上游的行為。
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Complete,
    TurnMetadata,
    /// 第一個事件就是 429 的 error。
    FirstError,
    BadInput(Option<u16>),
    /// 完成一輪後由上游關閉連線（模擬閒置連線被上游收掉）。
    CloseAfterTurn,
    CloseBeforeReply,
    DropBeforeReply,
    HoldBeforeReply,
    CacheMiss,
    LimitedCacheMiss,
    WithOutput,
    /// 每輪延遲一下才回，讓同一會話的兩輪確實並行。
    SlowReply,
}

async fn start_ws_upstream(mode: Mode) -> (String, Seen) {
    let seen = Seen::default();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let record = seen.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let record = record.clone();
            tokio::spawn(async move {
                let index = {
                    let mut all = record.0.lock().unwrap();
                    all.push(Connection::default());
                    all.len() - 1
                };
                let headers_record = record.clone();
                #[allow(clippy::result_large_err)]
                let callback = move |request: &WsRequest, mut response: WsResponse| {
                    headers_record.0.lock().unwrap()[index].0 = request.headers().clone();
                    response
                        .headers_mut()
                        .insert("x-codex-primary-used-percent", "12".parse().unwrap());
                    if mode == Mode::TurnMetadata {
                        response
                            .headers_mut()
                            .insert("x-codex-turn-state", "opening-state".parse().unwrap());
                    }
                    if mode == Mode::LimitedCacheMiss {
                        response
                            .headers_mut()
                            .insert("x-codex-websocket-max-message-bytes", "1024".parse().unwrap());
                    }
                    Ok(response)
                };
                // 伺服端也開 permessage-deflate：分支的伺服端看到不認得的擴充提議會拒絕握手。
                let mut extensions = tungstenite::extensions::ExtensionsConfig::default();
                extensions.permessage_deflate =
                    Some(tungstenite::extensions::compression::deflate::DeflateConfig::default());
                let mut config = tungstenite::protocol::WebSocketConfig::default();
                config.extensions = extensions;
                let mut ws = tokio_tungstenite::accept_hdr_async_with_config(stream, callback, Some(config))
                    .await
                    .unwrap();
                while let Some(Ok(message)) = ws.next().await {
                    let Message::Text(text) = message else { continue };
                    let count = {
                        let mut all = record.0.lock().unwrap();
                        all[index].1.push(text.to_string());
                        all[index].1.len()
                    };
                    if mode == Mode::HoldBeforeReply {
                        continue;
                    }
                    if mode == Mode::SlowReply {
                        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                    }
                    if index == 0
                        && count == 2
                        && matches!(mode, Mode::CloseBeforeReply | Mode::DropBeforeReply)
                    {
                        if mode == Mode::CloseBeforeReply {
                            ws.close(None).await.unwrap();
                        }
                        break;
                    }
                    if matches!(mode, Mode::CacheMiss | Mode::LimitedCacheMiss) && index == 0 && count == 2 {
                        ws.send(Message::text(
                            r#"{"type":"error","status":400,"error":{"code":"previous_response_not_found"}}"#,
                        ))
                        .await
                        .unwrap();
                        continue;
                    }
                    if let Mode::BadInput(status) = mode {
                        let mut event = serde_json::json!({"type":"error","sequence_number":7,"error":{
                            "type":"invalid_request_error","code":"invalid_value","param":"input[2].id",
                            "message":"Invalid input.","extension":{"reason":"duplicate"}
                        }});
                        if let Some(status) = status {
                            event["status"] = status.into();
                        }
                        ws.send(Message::text(event.to_string())).await.unwrap();
                        continue;
                    }
                    let events: Vec<&str> = if mode == Mode::FirstError
                        || (mode == Mode::TurnMetadata && count == 4)
                    {
                        vec![
                            r#"{"type":"error","status":429,"error":{"code":"usage_limit_reached","message":"limit"}}"#,
                        ]
                    } else if mode == Mode::TurnMetadata && count == 3 {
                        vec![
                            r#"{"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":37}}}"#,
                            r#"{"type":"codex.response.metadata","headers":{"x-codex-turn-state":"third-state"}}"#,
                            r#"{"type":"response.completed","response":{"id":"r3","status":"completed"}}"#,
                        ]
                    } else if matches!(mode, Mode::CacheMiss | Mode::LimitedCacheMiss | Mode::WithOutput) {
                        vec![
                            r#"{"type":"response.created","response":{"id":"r1"}}"#,
                            r#"{"type":"response.reasoning_summary_text.delta","delta":"reason"}"#,
                            r#"{"type":"response.output_text.delta","delta":"ok"}"#,
                            r#"{"type":"response.completed","response":{"id":"r1","status":"completed","output":[{"type":"reasoning","encrypted_content":"opaque"},{"type":"function_call","call_id":"tc","arguments":"{}"}]}}"#,
                        ]
                    } else {
                        vec![
                            r#"{"type":"response.created","response":{"id":"r1"}}"#,
                            r#"{"type":"response.output_item.added","item":{"type":"reasoning","encrypted_content":"opaque"}}"#,
                            r#"{"type":"response.output_text.delta","delta":"ok","sequence_number":7}"#,
                            r#"{"type":"response.completed","response":{"id":"r1","status":"completed"}}"#,
                        ]
                    };
                    for event in events {
                        ws.send(Message::Text(event.into())).await.unwrap();
                    }
                    if mode == Mode::CloseAfterTurn {
                        let _ = ws.close(None).await;
                    }
                }
            });
        }
    });
    (format!("http://{addr}/backend-api/codex"), seen)
}

fn kernel(base: &str, dir: &tempfile::TempDir) -> axum::Router {
    let credential = dir.path().join("codex-credentials.json");
    std::fs::write(
        &credential,
        r#"{"accounts":[{"access_token":"at-1","chatgpt_account_id":"acct-1","expires_at":1}]}"#,
    )
    .unwrap();
    let raw = serde_json::json!({
        "vm_id": "vm-test",
        "device_id": "device-1",
        "socket_path": dir.path().join("k.sock"),
        "credential_path": credential,
        "internal_token": TOKEN,
        "test_endpoints": true,
        "chatgpt_base_url": base,
    });
    let config = SingleAccountConfig::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap();
    let build = BuildInfo {
        name: "tap-codex-kernel",
        version: "0.0.0-test",
        build_sha: "test",
    };
    router(AppState::new(config, build).unwrap())
}

fn envelope(stream: bool) -> Request<Body> {
    let body = format!(
        r#"{{"body":{{"model":"gpt-5.5","input":[],"stream":{stream}}},
        "headers":{{"session-id":"s1","x-codex-window-id":"s1:0",
        "x-codex-exclude-stream-fields":"delta.sequence_number,reasoning.added.encrypted_content"}},
        "stream":{stream},"session":{{"session_id":"s1"}},"request_id":"req-1",
        "options":{{"upstream_transport":"prefer_websocket"}}}}"#
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
async fn streams_websocket_events_as_sse_and_reuses_the_connection() {
    let (base, seen) = start_ws_upstream(Mode::Complete).await;
    let dir = tempfile::tempdir().unwrap();
    let app = kernel(&base, &dir);

    for i in 0..2 {
        let response = app.clone().oneshot(envelope(true)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "text/event-stream");
        assert_eq!(response.headers()["x-model"], "gpt-5.5");
        let facts: Value =
            serde_json::from_slice(response.headers()["x-tap-turn-timing"].as_bytes()).unwrap();
        assert_eq!(facts["ws_reused"], i == 1);
        assert_eq!(facts["ws_frame_compressed"], true);
        assert_eq!(facts["ws_deflate"], true);
        assert_eq!(response.headers()["x-terminal-state"], "verified");
        assert_eq!(
            response
                .headers()
                .get("x-codex-primary-used-percent")
                .map(|v| v.to_str().unwrap()),
            (i == 0).then_some("12")
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            String::from_utf8(body.to_vec()).unwrap(),
            "event: response\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\"}}\n\n\
event: response\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"reasoning\",\"encrypted_content\":\"opaque\"}}\n\n\
event: response\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\",\"sequence_number\":7}\n\n\
event: response\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}\n\n"
        );
    }

    let seen = seen.0.lock().unwrap();
    assert_eq!(
        seen.len(),
        1,
        "the second turn of the session reuses the connection"
    );
    let (headers, messages) = &seen[0];
    assert_eq!(messages.len(), 2);
    assert_eq!(
        messages[0],
        r#"{"type":"response.create","model":"gpt-5.5","input":[],"stream":true}"#
    );
    assert_eq!(
        headers.keys().map(|name| name.as_str()).collect::<Vec<_>>(),
        // 官方 TUI 錄得的線上順序（缺少的是這個請求沒有的選用標頭）。
        &[
            "host",
            "connection",
            "upgrade",
            "sec-websocket-version",
            "sec-websocket-key",
            "chatgpt-account-id",
            "authorization",
            "user-agent",
            "originator",
            "openai-beta",
            "version",
            "x-client-request-id",
            "session-id",
            "thread-id",
            "x-codex-window-id",
            "x-codex-routing-hint",
            "sec-websocket-extensions",
        ]
    );
}

#[tokio::test]
async fn routing_hint_keeps_each_service_tier_on_its_own_connection() {
    let (base, seen) = start_ws_upstream(Mode::Complete).await;
    let dir = tempfile::tempdir().unwrap();
    let app = kernel(&base, &dir);
    for (tier, reused) in [("priority", false), ("ultrafast", false), ("priority", true)] {
        let mut request = envelope(true);
        *request.body_mut() = Body::from(
            serde_json::json!({
                "body":{"model":"m","input":[],"service_tier":tier}, "stream":true,
                "session":{"session_id":"same"}, "options":{"upstream_transport":"prefer_websocket"}
            })
            .to_string(),
        );
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let facts: Value =
            serde_json::from_slice(response.headers()["x-tap-turn-timing"].as_bytes()).unwrap();
        assert_eq!(facts["ws_reused"], reused);
        response.into_body().collect().await.unwrap();
    }
    let calls = seen.0.lock().unwrap();
    assert_eq!(calls.len(), 2);
    for ((headers, messages), (tier, count)) in calls.iter().zip([("priority", 2), ("ultrafast", 1)]) {
        assert_eq!(headers["x-codex-routing-hint"], format!("model=m;tier={tier}"));
        assert_eq!(messages.len(), count);
        for message in messages {
            assert_eq!(
                serde_json::from_str::<Value>(message).unwrap()["service_tier"],
                tier
            );
        }
    }
}

#[tokio::test]
async fn opening_headers_and_live_metadata_belong_only_to_their_turn() {
    let (base, seen) = start_ws_upstream(Mode::TurnMetadata).await;
    let dir = tempfile::tempdir().unwrap();
    let app = kernel(&base, &dir);
    for round in 1..=4 {
        // 第二輪非串流，第三輪串流，第四輪首事件是錯誤；四輪使用同一條 WS。
        let response = app.clone().oneshot(envelope(round != 2)).await.unwrap();
        let headers = response.headers();
        let opening = round == 1;
        for (name, value) in [
            ("x-codex-turn-state", "opening-state"),
            ("x-codex-primary-used-percent", "12"),
        ] {
            assert_eq!(
                headers.get(name).map(|v| v.to_str().unwrap()),
                opening.then_some(value)
            );
        }
        if round == 4 {
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
            let facts: Value = serde_json::from_slice(headers["x-tap-upstream-error"].as_bytes()).unwrap();
            assert_eq!(facts["stage"], "websocket_first_event");
            assert_eq!(facts["code"], "usage_limit_reached");
        } else {
            assert_eq!(response.status(), StatusCode::OK);
        }
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body = std::str::from_utf8(&bytes).unwrap();
        assert_eq!(body.contains(r#""used_percent":37"#), round == 3);
        assert_eq!(body.contains(r#""x-codex-turn-state":"third-state""#), round == 3);
    }
    let seen = seen.0.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].1.len(), 4);
}

#[tokio::test]
async fn a_first_error_event_becomes_a_json_error_with_the_handshake_headers() {
    let (base, _) = start_ws_upstream(Mode::FirstError).await;
    let dir = tempfile::tempdir().unwrap();
    let response = kernel(&base, &dir).oneshot(envelope(true)).await.unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["x-codex-primary-used-percent"], "12");
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["error"]["code"], "usage_limit_reached");
    assert_eq!(body["error"]["message"], "limit");
    assert_eq!(body["error"]["type"], "api_error");
}

#[tokio::test]
async fn a_first_invalid_request_preserves_the_upstream_detail_but_not_transport_fields() {
    for status in [Some(400), None, Some(429)] {
        let (base, seen) = start_ws_upstream(Mode::BadInput(status)).await;
        let dir = tempfile::tempdir().unwrap();
        let response = kernel(&base, &dir).oneshot(envelope(true)).await.unwrap();
        assert_eq!(response.status().as_u16(), status.unwrap_or(400));
        assert_eq!(response.headers()["x-kin-upstream-sent"], "1");
        let facts: Value =
            serde_json::from_slice(response.headers()["x-tap-upstream-error"].as_bytes()).unwrap();
        assert_eq!(facts["status"], serde_json::json!(status));
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
        if status == Some(429) {
            assert_eq!(body["error"]["code"], "usage_limit_reached");
        } else {
            assert_eq!(
                body,
                serde_json::json!({"error":{"code":"invalid_value","type":"invalid_request_error","param":"input[2].id","message":"Invalid input.","extension":{"reason":"duplicate"}}})
            );
        }
        let seen = seen.0.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].1.len(), 1);
    }
}

#[tokio::test]
async fn a_large_compressed_create_is_received_as_one_complete_request() {
    let (base, seen) = start_ws_upstream(Mode::Complete).await;
    let dir = tempfile::tempdir().unwrap();
    let input = serde_json::json!(["界".repeat(780_000)]);
    let response = kernel(&base, &dir)
        .oneshot(replay_request(&input.to_string(), None))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let facts: Value = serde_json::from_slice(response.headers()["x-tap-turn-timing"].as_bytes()).unwrap();
    assert_eq!(facts["ws_send_attempts"], 1);
    assert_eq!(facts["ws_deflate"], true);
    response.into_body().collect().await.unwrap();
    let seen = seen.0.lock().unwrap();
    assert_eq!((seen.len(), seen[0].1.len()), (1, 1));
    assert_eq!(
        serde_json::from_str::<Value>(&seen[0].1[0]).unwrap()["input"],
        input
    );
}

#[tokio::test]
async fn non_stream_requests_over_websocket_return_the_terminal_event() {
    let (base, _) = start_ws_upstream(Mode::Complete).await;
    let dir = tempfile::tempdir().unwrap();
    let response = kernel(&base, &dir).oneshot(envelope(false)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["type"], "response.completed");
}

#[tokio::test]
async fn an_idle_connection_closed_by_the_upstream_is_replaced_transparently() {
    let (base, seen) = start_ws_upstream(Mode::CloseAfterTurn).await;
    let dir = tempfile::tempdir().unwrap();
    let app = kernel(&base, &dir);
    for i in 0..2 {
        // 等 Close 可供送出前的探測讀到；已開始寫入才遇到 Close 則是另一個契約。
        if i == 1 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let response = app.clone().oneshot(envelope(true)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-codex-primary-used-percent"], "12");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(
            String::from_utf8(body.to_vec())
                .unwrap()
                .contains("response.completed")
        );
    }
    let seen = seen.0.lock().unwrap();
    assert_eq!(seen.len(), 2, "the closed connection is replaced by a new one");
    assert_eq!(
        seen[1].1.len(),
        1,
        "the second turn is sent once, on the new connection"
    );
}

#[tokio::test]
async fn a_close_after_the_peer_read_the_create_is_sent_once_more_on_a_new_connection() {
    // 與上游核心、官方 CLI 相同：上游以 Close 結束這條連線時，換新連線送完整本體一次。
    let (base, seen) = start_ws_upstream(Mode::CloseBeforeReply).await;
    let dir = tempfile::tempdir().unwrap();
    let app = kernel(&base, &dir);
    let first = app.clone().oneshot(envelope(true)).await.unwrap();
    first.into_body().collect().await.unwrap();
    let second = app.oneshot(envelope(true)).await.unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let fact: Value = serde_json::from_slice(second.headers()["x-tap-turn-timing"].as_bytes()).unwrap();
    assert_eq!(fact["ws_reused"], false, "answered on the new connection");
    assert_eq!(fact["ws_send_attempts"], 2);
    assert_eq!(fact["request_mode"], "full_replay");
    second.into_body().collect().await.unwrap();
    let seen = seen.0.lock().unwrap();
    assert_eq!(seen.len(), 2, "one new connection");
    assert_eq!(
        seen[0].1.len(),
        2,
        "the second create reached the closed connection"
    );
    assert_eq!(seen[1].1.len(), 1, "and was sent once more, only once");
}

#[tokio::test]
async fn a_reused_socket_dropped_after_the_peer_read_the_create_never_replays() {
    for mode in [Mode::DropBeforeReply] {
        let (base, seen) = start_ws_upstream(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let app = kernel(&base, &dir);
        let first = app.clone().oneshot(envelope(true)).await.unwrap();
        first.into_body().collect().await.unwrap();
        let second = app.oneshot(envelope(true)).await.unwrap();
        assert_eq!(second.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(second.headers()["x-kin-upstream-sent"], "unknown");
        let fact: Value = serde_json::from_slice(second.headers()["x-tap-turn-timing"].as_bytes()).unwrap();
        assert_eq!(fact["ws_reused"], true);
        assert!(fact["send_complete_ms"].is_number() && fact["first_event_ms"].is_null());
        let seen = seen.0.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen[0].1.len(),
            2,
            "exactly one second create, with no reconnect or HTTP fallback"
        );
    }
}

fn replay_request(input: &str, prev: Option<&str>) -> Request<Body> {
    let mut req = envelope(true);
    let options = serde_json::json!({"upstream_transport":"prefer_websocket","ws_replay":{"previous_response_id":prev}});
    *req.body_mut() = Body::from(format!(
        r#"{{"body":{{"model":"gpt-5.5","input":{input},"stream":true}},"stream":true,"headers":{{"session-id":"seq"}},"options":{options}}}"#
    ));
    req
}

#[tokio::test]
async fn native_delta_or_explicit_cache_rejection_matches_the_full_tool_history() {
    use serde_json::json;
    for mode in [Mode::WithOutput, Mode::CacheMiss] {
        let (base, seen) = start_ws_upstream(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let app = kernel(&base, &dir);
        let user = json!({"role":"user","content":"x".repeat(300_000)});
        let tool = json!({"type":"function_call_output","call_id":"tc","output":"local"});
        let first = app
            .clone()
            .oneshot(replay_request(&json!([user]).to_string(), None))
            .await
            .unwrap()
            .into_body();
        first.collect().await.unwrap();
        // 上游輸出欄位順序和控制面 JSON 相同；json! 的 Map 在測試中會按鍵排序，這裡保留原文。
        let raw_input = format!(
            r#"[{user},{{"type":"reasoning","encrypted_content":"opaque"}},{{"type":"function_call","call_id":"tc","arguments":"{{}}"}},{tool}]"#
        );
        let req = replay_request(&raw_input, Some("r1"));
        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), 200);
        let collected = response.into_body().collect().await.unwrap();
        let facts: Value =
            serde_json::from_slice(collected.trailers().unwrap()["x-tap-turn-timing"].as_bytes()).unwrap();
        assert_eq!(facts["cache_miss"], mode == Mode::CacheMiss);
        assert_eq!(facts["first_event_type"], "response.created");
        assert_eq!(
            facts["first_generated_type"],
            "response.reasoning_summary_text.delta"
        );
        assert_eq!(facts["first_text_type"], "response.output_text.delta");
        let seen = seen.0.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let msgs = &seen[0].1;
        assert_eq!(msgs.len(), if mode == Mode::CacheMiss { 3 } else { 2 });
        let delta: Value = serde_json::from_str(&msgs[1]).unwrap();
        assert_eq!(delta["previous_response_id"], "r1");
        assert_eq!(delta["input"], json!([tool]));
        assert!(msgs[1].len() < 300);
        if mode == Mode::CacheMiss {
            let replay: Value = serde_json::from_str(&msgs[2]).unwrap();
            assert!(replay.get("previous_response_id").is_none());
            assert_eq!(
                replay["input"],
                serde_json::from_str::<Value>(&raw_input).unwrap()
            );
        }
    }
}

#[tokio::test]
async fn cancel_after_the_create_before_any_event_is_unknown_and_does_not_reconnect() {
    let (base, seen) = start_ws_upstream(Mode::HoldBeforeReply).await;
    let dir = tempfile::tempdir().unwrap();
    let app = kernel(&base, &dir);
    let request = tokio::spawn(app.clone().oneshot(envelope(true)));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !seen.0.lock().unwrap().iter().any(|c| !c.1.is_empty()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let cancel = Request::builder()
        .method("POST")
        .uri("/internal/v1/cancel")
        .header(INTERNAL_TOKEN, TOKEN)
        .body(Body::from(r#"{"request_id":"req-1"}"#))
        .unwrap();
    let result = app
        .oneshot(cancel)
        .await
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(
        serde_json::from_slice::<Value>(&result).unwrap()["cancelled"],
        true
    );
    let response = request.await.unwrap().unwrap();
    assert_eq!(response.headers()["x-kin-upstream-sent"], "unknown");
    let facts: Value = serde_json::from_slice(response.headers()["x-tap-turn-timing"].as_bytes()).unwrap();
    assert_eq!(facts["outcome"], "cancelled");
    assert_eq!(facts["ws_send_attempts"], 1);
    let seen = seen.0.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].1.len(), 1);
}

#[tokio::test]
async fn changing_accounts_before_continuation_opens_a_new_socket_with_full_input() {
    use serde_json::json;
    let (base, seen) = start_ws_upstream(Mode::WithOutput).await;
    let dir = tempfile::tempdir().unwrap();
    let app = kernel(&base, &dir);
    let first = r#"[{"role":"user","content":"one"}]"#;
    app.clone()
        .oneshot(replay_request(first, None))
        .await
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap();
    std::fs::write(
        dir.path().join("codex-credentials.json"),
        r#"{"accounts":[{"access_token":"new","chatgpt_account_id":"other","expires_at":1}]}"#,
    )
    .unwrap();
    let full = r#"[{"role":"user","content":"one"},{"type":"reasoning","encrypted_content":"opaque"},{"type":"function_call","call_id":"tc","arguments":"{}"},{"type":"function_call_output","call_id":"tc","output":"done"}]"#;
    let answer = app.oneshot(replay_request(full, Some("r1"))).await.unwrap();
    let timing: Value = serde_json::from_slice(answer.headers()["x-tap-turn-timing"].as_bytes()).unwrap();
    assert_eq!(timing["ws_reused"], false);
    assert_eq!(timing["request_mode"], "full_replay");
    answer.into_body().collect().await.unwrap();
    let seen = seen.0.lock().unwrap();
    assert_eq!(seen.len(), 2);
    let msg: Value = serde_json::from_str(&seen[1].1[0]).unwrap();
    assert_eq!(msg["input"], serde_json::from_str::<Value>(full).unwrap());
    assert_eq!(msg.get("previous_response_id"), None);
    assert_eq!(msg["type"], json!("response.create"));
}

#[tokio::test]
async fn concurrent_turns_of_one_session_keep_their_connections_for_the_next_turns() {
    // Codex 多代理：子執行緒共用同一個 session-id。兩輪並行開兩條連線，下一批兩輪都要重用，不再握手。
    let (base, seen) = start_ws_upstream(Mode::SlowReply).await;
    let dir = tempfile::tempdir().unwrap();
    let app = kernel(&base, &dir);
    for _ in 0..2 {
        let (a, b) = tokio::join!(
            app.clone().oneshot(envelope(true)),
            app.clone().oneshot(envelope(true))
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!((a.status(), b.status()), (StatusCode::OK, StatusCode::OK));
        a.into_body().collect().await.unwrap();
        b.into_body().collect().await.unwrap();
    }
    let seen = seen.0.lock().unwrap();
    assert_eq!(
        seen.len(),
        2,
        "two connections for two concurrent turns, both reused by the next pair"
    );
    assert_eq!(seen.iter().map(|c| c.1.len()).sum::<usize>(), 4);
}

#[tokio::test]
async fn an_oversized_full_replay_after_a_rejected_delta_is_not_sent_again() {
    let (base, seen) = start_ws_upstream(Mode::LimitedCacheMiss).await;
    let dir = tempfile::tempdir().unwrap();
    let app = kernel(&base, &dir);
    let user = serde_json::json!({"role":"user","content":"x".repeat(620)});
    let first = app
        .clone()
        .oneshot(replay_request(&format!("[{user}]"), None))
        .await
        .unwrap();
    assert_eq!(first.status(), 200);
    first.into_body().collect().await.unwrap();
    let tool = serde_json::json!({"type":"function_call_output","call_id":"tc","output":"y".repeat(470)});
    let history = format!(
        r#"[{user},{{"type":"reasoning","encrypted_content":"opaque"}},{{"type":"function_call","call_id":"tc","arguments":"{{}}"}},{tool}]"#
    );
    let r = app.oneshot(replay_request(&history, Some("r1"))).await.unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(r.headers()["x-kin-upstream-sent"], "1");
    let calls = seen.0.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].1.len(), 2);
    assert!(calls[0].1[1].contains("previous_response_id"));
}
