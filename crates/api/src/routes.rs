//! 路由與處理程式。處理程式只負責 HTTP 邊界，回傳 [`ApiError`] 而不自行拼錯誤 JSON。

use axum::body::{Body, to_bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router, middleware};
use kin_contract::abi::{Abi, Capability};
use kin_contract::error::KernelError;
use kin_contract::registry::SharedHealth;
use kin_contract::status::{Describe, Health, Impersonate};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::accounts;
use crate::auth::require_token;
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
            capabilities.extend([
                Capability::MultiAccount,
                Capability::AccountOps,
                Capability::AccountManagementGeneration,
            ]);
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
    let mut permit = state.admission().enter(false)?;
    let raw = permit.read(body, state.body_limit()).await?;
    Ok(permit.finish(crate::responses::respond(&state, raw).await?))
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
