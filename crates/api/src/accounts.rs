//! 共享模式（ABI 2）的帳號登錄端點（`docs/ABI2.md`）。只在共享模式掛上；單帳號模式打到這些路徑回 404。

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post, put};
use futures_util::{StreamExt, stream};
use kin_contract::config::SharedConfig;
use kin_contract::envelope::AccountBinding;
use kin_contract::error::KernelError;
use kin_contract::headers::UPSTREAM_SENT;
use kin_contract::registry::{
    AccountAck, AccountDelete, AccountEnv, AccountHealth, AccountList, AccountState, Breakers, CredentialAck,
    CredentialPut, CredentialStatus, ReconcileRequest, RefreshAck,
};
use kin_contract::send::UpstreamSent;
use kin_credentials::stored;
use kin_registry::{PutOutcome, Registry};
use kin_upstream::ops::{Op, OpHeaders};
use kin_upstream::refresh::RefreshReason;
use kin_upstream::{HttpUpstream, UpstreamSettings};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use serde_json::value::RawValue;

use crate::credential::{self, AccountAuth};
use crate::error::ApiError;
use crate::state::{AppState, Mode};

/// 單一帳號的 PUT／DELETE 本體上限。
const ACCOUNT_BODY_LIMIT: usize = 64 * 1024;
/// reconcile 的本體上限：每筆帳號環境不到 1 KiB，這裡保留到上千個帳號。
const RECONCILE_BODY_LIMIT: usize = 4 * 1024 * 1024;

pub(crate) fn routes(router: Router<AppState>) -> Router<AppState> {
    router
        .route("/internal/v1/accounts", get(list))
        // `reconcile` 不能當帳號參照；其他方法打到這個路徑時明確拒絕，不回框架預設的 405。
        .route("/internal/v1/accounts/reconcile", post(reconcile).fallback(reserved_ref))
        .route(
            "/internal/v1/accounts/{account_ref}",
            put(put_account)
                .delete(delete_account)
                .fallback(crate::routes::not_found),
        )
        .route("/internal/v1/accounts/{account_ref}/health", get(account_health))
        .route(
            "/internal/v1/accounts/{account_ref}/credential",
            put(put_credential).fallback(crate::routes::not_found),
        )
        .route(
            "/internal/v1/accounts/{account_ref}/ops/{op}",
            post(operation).fallback(crate::routes::not_found),
        )
}

type SharedRegistry = Registry<HttpUpstream, std::sync::Arc<AccountAuth>>;

/// 共享模式的登錄，以及建立執行環境要用的上游位址與換票端點。
fn registry(state: &AppState) -> Result<(&SharedRegistry, &SharedConfig), KernelError> {
    match state.mode() {
        Mode::Shared { config, registry } => Ok((registry, config)),
        Mode::Single { .. } => Err(KernelError::NotFound),
    }
}

async fn read_json<T: DeserializeOwned>(body: Body, limit: usize) -> Result<T, KernelError> {
    let raw = to_bytes(body, limit)
        .await
        .map_err(|_| KernelError::BodyTooLarge)?;
    serde_json::from_slice(&raw).map_err(|err| KernelError::InvalidBody(err.to_string()))
}

/// 建立一個帳號的上游執行環境。
fn build(config: &SharedConfig) -> impl Fn(&AccountEnv) -> Result<HttpUpstream, String> + '_ {
    move |env| HttpUpstream::new(&UpstreamSettings::account(env, config)).map_err(|err| err.to_string())
}

async fn list(State(state): State<AppState>) -> Result<Response, ApiError> {
    let (registry, _) = registry(&state)?;
    let (registry_state, entries) = registry.list_with_paths();
    // 保留排序及個別帳號代次缺失的 ABI 行為，限制同時開啟的憑據檔數。
    let accounts = stream::iter(entries)
        .map(|mut row| async move {
            if let Some(path) = row.credential_path {
                row.summary.credential_generation = stored::read_generation(&path).await.ok().flatten();
            }
            row.summary
        })
        .buffered(8)
        .collect()
        .await;
    Ok(Json(AccountList {
        registry_state,
        accounts,
    })
    .into_response())
}

async fn reconcile(State(state): State<AppState>, body: Body) -> Result<Response, ApiError> {
    let (registry, config) = registry(&state)?;
    let request: ReconcileRequest = read_json(body, RECONCILE_BODY_LIMIT).await?;
    let ack = registry.reconcile(request.accounts, build(config))?;
    tracing::info!(accounts = ack.accounts, "account registry reconciled");
    Ok(Json(ack).into_response())
}

async fn reserved_ref() -> ApiError {
    KernelError::InvalidBody("reconcile is not an account ref".to_owned()).into()
}

async fn put_account(
    State(state): State<AppState>,
    Path(account_ref): Path<String>,
    body: Body,
) -> Result<Response, ApiError> {
    let (registry, config) = registry(&state)?;
    let env: AccountEnv = read_json(body, ACCOUNT_BODY_LIMIT).await?;
    let generation = env.env_generation;
    let outcome = registry.put(&account_ref, env, build(config))?;
    let status = match outcome {
        PutOutcome::Created => StatusCode::CREATED,
        PutOutcome::Updated | PutOutcome::Unchanged => StatusCode::OK,
    };
    if outcome != PutOutcome::Unchanged {
        tracing::info!(account = %account_ref, env_generation = generation, ?outcome, "account registered");
    }
    let ack = AccountAck {
        ok: true,
        account_ref,
        env_generation: generation,
    };
    Ok((status, Json(ack)).into_response())
}

async fn delete_account(
    State(state): State<AppState>,
    Path(account_ref): Path<String>,
    RawQuery(query): RawQuery,
    body: Body,
) -> Result<Response, ApiError> {
    let (registry, _) = registry(&state)?;
    let request: AccountDelete = read_json(body, ACCOUNT_BODY_LIMIT).await?;
    let force = query.as_deref().is_some_and(|query| {
        query
            .split('&')
            .any(|pair| matches!(pair, "force=1" | "force=true"))
    });
    let ack = registry.delete(&account_ref, request.env_generation, force)?;
    tracing::info!(account = %account_ref, inflight = ack.inflight, force, "account removed");
    Ok(Json(ack).into_response())
}

async fn account_health(
    State(state): State<AppState>,
    Path(account_ref): Path<String>,
) -> Result<Response, ApiError> {
    let (registry, _) = registry(&state)?;
    let view = registry.view(&account_ref)?;
    // 不回傳任何秘密：只有狀態、代次與到期判斷。
    let current = stored::read(&view.env.credential_path).await?;
    let credential = view.ext().status(current.as_ref());
    let proxy_ok = view.runtime.proxy_ok().await;
    let (http, websocket) = view.runtime.breakers();
    let failing = matches!(
        credential,
        CredentialStatus::ReauthRequired | CredentialStatus::Missing
    ) || !proxy_ok
        || view.runtime.exit_down();
    let state = if failing && view.state == AccountState::Ready {
        AccountState::Failing
    } else {
        view.state
    };
    Ok(Json(AccountHealth {
        ok: !failing && view.state == AccountState::Ready,
        account_ref,
        state,
        env_generation: view.env.env_generation,
        credential_generation: current.map(|current| current.generation),
        credential,
        proxy_ok,
        breaker: Breakers { http, websocket },
        inflight: view.inflight,
        last_error: view.last_error,
    })
    .into_response())
}

/// `PUT /internal/v1/accounts/{ref}/credential`：匯入或替換憑據（代次 CAS），回應不含 token。
async fn put_credential(
    State(state): State<AppState>,
    Path(account_ref): Path<String>,
    body: Body,
) -> Result<Response, ApiError> {
    let (registry, _) = registry(&state)?;
    let request: CredentialPut = read_json(body, ACCOUNT_BODY_LIMIT).await?;
    let view = registry.managed(&account_ref, false)?;
    if view.env.env_generation != request.env_generation {
        return Err(KernelError::AccountGenerationMismatch {
            account_ref,
            expected: view.env.env_generation,
            actual: request.env_generation,
        }
        .into());
    }
    let generation = credential::import(
        &view.env,
        view.ext(),
        request.credential,
        request.expected_credential_generation,
    )
    .await?;
    Ok(Json(CredentialAck {
        ok: true,
        account_ref,
        credential_generation: generation,
    })
    .into_response())
}

/// 帳號操作的請求本體（設計第 4.4 節）。`body` 保留原始位元組，搜尋與圖片原樣轉送。
#[derive(Deserialize)]
struct OperationRequest {
    #[serde(default)]
    request_id: Option<String>,
    env_generation: u64,
    #[serde(default)]
    body: Option<Box<RawValue>>,
    /// 下游標頭；只取 `x-codex-turn-metadata` 與 `x-codex-image-turn-id`，其他忽略。
    #[serde(default)]
    headers: Option<serde_json::Map<String, Value>>,
}

impl OperationRequest {
    fn op_headers(&self) -> OpHeaders {
        let text = |name: &str| {
            self.headers
                .as_ref()
                .and_then(|headers| headers.get(name))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        };
        OpHeaders {
            turn_metadata: text("x-codex-turn-metadata"),
            image_turn_id: text("x-codex-image-turn-id"),
        }
    }
}

/// `POST /internal/v1/accounts/{ref}/ops/{op}`：代發帳號操作，回應原樣轉回（狀態碼、本體、`content-type`、
/// 上游的 `x-codex-*`），並帶 `x-kin-upstream-sent: 1`。`refresh` 另見 [`force_refresh`]。
async fn operation(
    State(state): State<AppState>,
    Path((account_ref, op)): Path<(String, String)>,
    body: Body,
) -> Result<Response, ApiError> {
    if op == "refresh" {
        let request: OperationRequest = read_json(body, ACCOUNT_BODY_LIMIT).await?;
        return force_refresh(
            &state,
            AccountBinding {
                account_ref,
                env_generation: request.env_generation,
            },
        )
        .await;
    }
    let op = Op::parse(&op).ok_or(KernelError::NotFound)?;
    let (registry, _) = registry(&state)?;
    // 圖片編輯帶參考圖，本體可能很大：與推理相同，先以登錄中最大的帳號上限讀入，取得帳號後再比對它自己的上限。
    let limit = usize::try_from(registry.max_request_bytes()).unwrap_or(usize::MAX);
    let mut permit = state.admission().enter(op.images())?;
    let raw = permit.read(body, limit).await?;
    let request: OperationRequest =
        serde_json::from_slice(&raw).map_err(|err| KernelError::InvalidBody(err.to_string()))?;
    let view = registry.account(&AccountBinding {
        account_ref,
        env_generation: request.env_generation,
    })?;
    if u64::try_from(raw.len()).unwrap_or(u64::MAX) > view.env.max_request_bytes {
        return Err(KernelError::BodyTooLarge.into());
    }
    // 帳號操作不算「使用過」，身分沿用這個帳號最近一次推理的身分輸入（帳號環境與自訂 UA）。
    let profile = view.ext().profile();
    let inflight = state.register(request.request_id.as_deref(), Some(view.cancel_token()))?;
    let credential = credential::prepare(&view.env, &view.runtime, view.ext(), &profile)
        .await
        .inspect_err(|err| view.note_error(err))?;
    // 掛在帳號閘門下：`/cancel` 與 `DELETE ?force=1` 都能中止帳號操作（含已收到回應、讀本體中的）。
    let answered = view
        .runtime
        .operate(
            op,
            &credential,
            &profile,
            request.body.as_deref(),
            &request.op_headers(),
            inflight.token(),
        )
        .await
        .inspect_err(|err| view.note_error(err))?;
    let mut response = Response::new(Body::from(answered.body));
    *response.status_mut() = StatusCode::from_u16(answered.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let headers = response.headers_mut();
    headers.extend(answered.headers);
    headers.insert(
        UPSTREAM_SENT,
        HeaderValue::from_static(UpstreamSent::Yes.header_value()),
    );
    Ok(permit.finish(response))
}

/// `POST /internal/v1/accounts/{ref}/ops/refresh`：控制面要求換票。只回結果，不回任何 token；
/// 結果不明時控制面以帳號健康的憑據代次確認，不得重送（設計第 4.4 節）。
async fn force_refresh(state: &AppState, binding: AccountBinding) -> Result<Response, ApiError> {
    let (registry, _) = registry(state)?;
    let view = registry.account(&binding)?;
    let profile = view.ext().profile();
    let refreshed = credential::refresh(
        &view.env,
        &view.runtime,
        view.ext(),
        None,
        RefreshReason::Forced,
        &profile,
    )
    .await
    .inspect_err(|err| view.note_error(err))?;
    Ok(Json(RefreshAck {
        ok: true,
        account_ref: binding.account_ref,
        expires_at: refreshed.expires_at_ms(),
        credential_generation: refreshed.generation,
    })
    .into_response())
}
