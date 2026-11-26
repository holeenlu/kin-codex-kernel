//! 推理請求的憑據，以及共享模式的刷新（vm2api 設計第 5.4 節、本倉庫 `docs/ABI2.md`）。
//!
//! 單帳號模式照舊：每次請求重讀憑據檔，不自行刷新（換票由 vm2api 負責）。
//!
//! 共享模式由核心當唯一寫入者並負責刷新，行為照官方 CLI 0.171.0（擬真基準 `refresh.json`）：
//! - 請求前：access token 在 5 分鐘內到期就先刷新（`proactive_expiry`）；讀不到到期時間且距上次刷新超過
//!   8 天也刷新（`scheduled`）。暫時失敗時與官方相同，照用現有的 token。
//! - 上游回 401：先重讀憑據檔，用同一個（或已被換掉的）token 重試；再 401 就以 `unauthorized_recovery`
//!   刷新後重試；第三次仍 401 就把錯誤交回控制面。
//! - 背景：只替近期用過的帳號提早刷新，到期窗口另加依帳號固定的偏移，避免大量帳號同時換票。
//!   官方沒有背景刷新；這是設計第 5.4 節的決定，閒置帳號照官方不刷新。
//! - 兩把鎖：刷新單飛鎖（網路換票只有一個）與寫入鎖（寫檔依序）。匯入只拿寫入鎖，不等刷新；
//!   晚回來的刷新在提交時發現檔案已被換掉，就放棄自己的結果。
//! - 永久失敗（`refresh_token_reused` 等）標為需要重新授權，之後的請求確定不送出、回 401；
//!   暫時失敗退避：30 分鐘內 5 次就停 10 分鐘（控制面明確要求的換票除外）；access token 已過期、且從第一次
//!   失敗起連續 2 小時換不到票，也標為需要重新授權。連不到 token 端點（確定沒送出）是出口問題，不計入。
//! - 換票狀態（兩把鎖與退避）以憑據檔為單位，整個程序只有一份：帳號被刪除後重建、對帳換新閘門時，
//!   同一個檔案仍只有一個換票者。
//! - 換票與寫回在獨立任務裡執行，呼叫端被取消也會做完：token 端點一旦輪換了 refresh token，新的必須寫進檔案。
//!   寫不進去時新 token 留在記憶體，下次換票前先補寫。程序結束前等進行中的換票寫完。

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use kin_contract::envelope::ResponsesEnvelope;
use kin_contract::error::KernelError;
use kin_contract::registry::{AccountEnv, CredentialStatus};
use kin_contract::send::UpstreamSent;
use kin_credentials::Credential;
use kin_credentials::stored::{self, CommitError, Refreshed, Stored};
use kin_identity::Profile;
use kin_upstream::refresh::{RefreshError, RefreshReason};
use kin_upstream::{Accepted, HttpUpstream};
use tokio_util::sync::CancellationToken;

use crate::state::{AppState, Mode, Target};

/// 官方 `CHATGPT_ACCESS_TOKEN_REFRESH_WINDOW_MINUTES`。
const REFRESH_WINDOW_SECS: i64 = 5 * 60;
/// 官方 `TOKEN_REFRESH_INTERVAL`（天）。
const SCHEDULED_AFTER_MS: i64 = 8 * 24 * 3600 * 1000;
const FAILURE_WINDOW: Duration = Duration::from_secs(30 * 60);
const MAX_FAILURES: usize = 5;
const BACKOFF: Duration = Duration::from_secs(10 * 60);
/// access token 已到期時，從第一次換票失敗起算的恢復窗口。
const RECOVERY_WINDOW: Duration = Duration::from_secs(2 * 3600);
/// 寫回換到的票失敗時，當場重試的次數。
const COMMIT_ATTEMPTS: usize = 3;
/// 背景刷新只看這段時間內用過的帳號。
const RECENT_USE: Duration = Duration::from_secs(15 * 60);
const BACKGROUND_TICK: Duration = Duration::from_secs(30);
/// 背景刷新的到期窗口，在官方的 5 分鐘之外另加 0–150 秒（依帳號固定）。
const BACKGROUND_SPREAD_SECS: u64 = 150;

/// 共享模式每個憑據檔的換票狀態。由 [`auth_for`] 依路徑取用，掛在 registry 的帳號閘門上；
/// 沒有任何閘門持有時消失。
#[derive(Debug, Default)]
pub(crate) struct AccountAuth {
    /// 網路換票的單飛鎖（換票任務持有，呼叫端被取消也不會提早釋放）。
    refresh: Arc<tokio::sync::Mutex<()>>,
    /// 寫憑據檔的鎖（刷新提交與匯入）。
    write: tokio::sync::Mutex<()>,
    writer: tokio::sync::Mutex<Weak<std::fs::File>>,
    state: Mutex<AuthState>,
}

#[derive(Debug, Default)]
struct AuthState {
    /// 需要重新授權的原因（token 端點的錯誤碼）。
    reauth: Option<String>,
    failures: Vec<Instant>,
    retry_after: Option<Instant>,
    last_used: Option<Instant>,
    /// 最近一次推理的身分輸入（帳號環境與自訂 UA）；帳號操作與換票用它組出與推理相同的身分。
    profile: Profile,
    /// 第一次暫時失敗的時間（成功或匯入後清掉）；恢復窗口從這裡算。
    failing_since: Option<Instant>,
    /// 換到了票卻沒寫進檔案。下次換票前先補寫。
    unwritten: Option<Unwritten>,
}

/// 換到卻沒寫進檔案的票與當時檔案的代次。不輸出內容。
struct Unwritten {
    key: Option<stored::AccountKey>,
    consumed: Option<String>,
    tokens: Refreshed,
    _owner: Arc<std::fs::File>,
}

impl std::fmt::Debug for Unwritten {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Unwritten")
    }
}

/// 每個憑據檔一份換票狀態。
static AUTHS: LazyLock<Mutex<HashMap<PathBuf, Weak<AccountAuth>>>> = LazyLock::new(Mutex::default);

/// 這個憑據檔的換票狀態；同一個檔案在整個程序只有一份。
pub(crate) fn auth_for(path: &Path) -> Arc<AccountAuth> {
    let mut auths = AUTHS.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(auth) = auths.get(path).and_then(Weak::upgrade) {
        return auth;
    }
    auths.retain(|_, auth| auth.strong_count() > 0);
    let auth = Arc::new(AccountAuth::default());
    auths.insert(path.to_path_buf(), Arc::downgrade(&auth));
    auth
}

/// 進行中的換票任務數；程序結束前等它們寫完。
static REFRESHING: AtomicUsize = AtomicUsize::new(0);
static REFRESH_DONE: LazyLock<tokio::sync::Notify> = LazyLock::new(tokio::sync::Notify::new);

struct RefreshInFlight;

impl RefreshInFlight {
    fn start() -> Self {
        REFRESHING.fetch_add(1, Ordering::AcqRel);
        Self
    }
}

impl Drop for RefreshInFlight {
    fn drop(&mut self) {
        REFRESHING.fetch_sub(1, Ordering::AcqRel);
        REFRESH_DONE.notify_waiters();
    }
}

/// 等進行中的換票寫完（最多 `limit`）。程序結束時呼叫。
pub(crate) async fn wait_for_refreshes(limit: Duration) {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        let done = REFRESH_DONE.notified();
        tokio::pin!(done);
        done.as_mut().enable();
        if REFRESHING.load(Ordering::Acquire) == 0 {
            return;
        }
        if tokio::time::timeout_at(deadline, done).await.is_err() {
            tracing::warn!(
                refreshing = REFRESHING.load(Ordering::Acquire),
                "shutting down with token refreshes still running"
            );
            return;
        }
    }
}

impl AccountAuth {
    /// 同一程序內允許匯入與換票重疊；其他程序必須等這批寫入完成才可接管。
    async fn own_writer(&self, path: &Path) -> Result<Arc<std::fs::File>, KernelError> {
        let mut slot = self.writer.lock().await;
        if let Some(owner) = slot.upgrade() {
            return Ok(owner);
        }
        let parent = path.parent().filter(|dir| !dir.as_os_str().is_empty());
        let io = |err| KernelError::InvalidCredential(format!("credential writer lock: {err}"));
        if let Some(dir) = parent {
            tokio::fs::create_dir_all(dir).await.map_err(io)?;
        }
        let mut name = path.as_os_str().to_owned();
        name.push(".writer.lock");
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(name)
            .await
            .map_err(io)?
            .into_std()
            .await;
        file.try_lock().map_err(|err| match err {
            std::fs::TryLockError::WouldBlock => KernelError::KernelBusy,
            std::fs::TryLockError::Error(err) => io(err),
        })?;
        let owner = Arc::new(file);
        *slot = Arc::downgrade(&owner);
        Ok(owner)
    }

    fn state(&self) -> MutexGuard<'_, AuthState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn failing(&self) -> bool {
        self.state().reauth.is_some()
    }

    /// 健康回報的憑據狀態。
    pub(crate) fn status(&self, stored: Option<&Stored>) -> CredentialStatus {
        if self.failing() {
            CredentialStatus::ReauthRequired
        } else if self.refresh.try_lock().is_err() {
            CredentialStatus::Refreshing
        } else if stored.is_some_and(|stored| stored.access_token.is_some()) {
            CredentialStatus::Ok
        } else {
            CredentialStatus::Missing
        }
    }

    /// 記下這次推理的身分輸入。帳號環境只在請求有帶時更新（沒帶的請求不抹掉已知的環境）；
    /// 自訂 UA 跟著最近一次推理（沒有自訂就回到依帳號環境產生）。
    fn note_use(&self, profile: &Profile) {
        let mut state = self.state();
        state.last_used = Some(Instant::now());
        if let Some(environment) = &profile.environment {
            state.profile.environment = Some(environment.clone());
        }
        state.profile.user_agent.clone_from(&profile.user_agent);
    }

    /// 最近一次推理的身分輸入；沒有用過的帳號是空的（換票時用退回身分）。
    pub(crate) fn profile(&self) -> Profile {
        self.state().profile.clone()
    }

    pub(crate) fn needs_writeback(&self) -> bool {
        self.state().unwritten.is_some()
    }

    fn in_backoff(&self) -> bool {
        self.state()
            .retry_after
            .is_some_and(|until| Instant::now() < until)
    }

    fn record_failure(&self) {
        let now = Instant::now();
        let mut state = self.state();
        state.failing_since.get_or_insert(now);
        state
            .failures
            .retain(|at| now.duration_since(*at) < FAILURE_WINDOW);
        state.failures.push(now);
        if state.failures.len() >= MAX_FAILURES {
            state.retry_after = Some(now + BACKOFF);
            state.failures.clear();
        }
    }

    fn record_success(&self) {
        let mut state = self.state();
        state.failures.clear();
        state.retry_after = None;
        state.failing_since = None;
    }

    /// 從第一次暫時失敗到現在多久；沒有失敗為零。
    fn failing_for(&self) -> Duration {
        self.state()
            .failing_since
            .map(|since| since.elapsed())
            .unwrap_or_default()
    }

    fn take_unwritten(&self, current: &Stored) -> Option<Refreshed> {
        self.state()
            .unwritten
            .take()
            .filter(|pending| pending.key == current.key && pending.consumed == current.refresh_token)
            .map(|pending| pending.tokens)
    }

    fn mark_reauth(&self, code: &str) {
        self.state().reauth = Some(code.to_owned());
    }

    /// 匯入新憑據後重新開始：清掉需要重新授權的標記與退避。
    fn reset(&self) {
        let mut state = self.state();
        state.reauth = None;
        state.failures.clear();
        state.retry_after = None;
        state.failing_since = None;
        // 相同憑據的重新匯入也會遞增代次；補寫時依身分與已消耗的票核對。
    }
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

/// 官方 `proactive_refresh_reason`，到期窗口可放寬（背景刷新用）。
fn needs_refresh(stored: &Stored, window_secs: i64) -> Option<RefreshReason> {
    let now = now_secs();
    if let Some(expires_at) = stored.access_expires_at {
        return (expires_at <= now.saturating_add(window_secs)).then_some(RefreshReason::ProactiveExpiry);
    }
    let last = stored.last_refresh?;
    (last < now.saturating_mul(1000).saturating_sub(SCHEDULED_AFTER_MS)).then_some(RefreshReason::Scheduled)
}

/// 依帳號固定的偏移，讓同時接近到期的帳號分散換票。
fn spread(account_ref: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    account_ref.hash(&mut hasher);
    i64::try_from(hasher.finish() % (BACKGROUND_SPREAD_SECS + 1)).unwrap_or_default()
}

/// 換票並寫回。`seen` 是呼叫端讀到的憑據代次：等鎖期間檔案已被換掉（別人剛刷新或匯入）就直接採用，不再換票。
///
/// 換票在獨立任務裡做完（持有單飛鎖）；呼叫端被取消時任務照樣把結果寫回。
pub(crate) async fn refresh(
    env: &AccountEnv,
    runtime: &Arc<HttpUpstream>,
    auth: &Arc<AccountAuth>,
    seen: Option<u64>,
    reason: RefreshReason,
    profile: &Profile,
) -> Result<Stored, KernelError> {
    let task = tokio::spawn(refresh_detached(
        env.clone(),
        Arc::clone(runtime),
        Arc::clone(auth),
        seen,
        reason,
        profile.clone(),
        None,
        RefreshInFlight::start(),
    ));
    task.await.unwrap_or_else(|err| {
        Err(KernelError::RefreshFailed {
            account_ref: env.account_ref.clone(),
            message: err.to_string(),
            sent: UpstreamSent::Unknown,
        })
    })
}

/// `held`：呼叫端已經拿到的單飛鎖（背景刷新先拿鎖、再確認帳號仍啟用）。
#[allow(clippy::too_many_arguments)]
async fn refresh_detached(
    env: AccountEnv,
    runtime: Arc<HttpUpstream>,
    auth: Arc<AccountAuth>,
    seen: Option<u64>,
    reason: RefreshReason,
    profile: Profile,
    held: Option<tokio::sync::OwnedMutexGuard<()>>,
    _in_flight: RefreshInFlight,
) -> Result<Stored, KernelError> {
    let account_ref = env.account_ref.as_str();
    let _single = match held {
        Some(guard) => guard,
        None => Arc::clone(&auth.refresh).lock_owned().await,
    };
    let owner = auth.own_writer(&env.credential_path).await?;
    let current = stored::read(&env.credential_path)
        .await?
        .ok_or(KernelError::NoCredential)?;
    if seen.is_some_and(|seen| seen != current.generation) {
        return Ok(current);
    }
    if auth.failing() {
        return Err(KernelError::ReauthRequired(account_ref.to_owned()));
    }
    // 上次換到卻沒寫進去的票：檔案裡的 refresh token 已被消耗，先補寫，不再換票。
    if let Some(tokens) = auth.take_unwritten(&current) {
        return commit(&env, &auth, &current, tokens, reason, owner).await;
    }
    // 控制面明確要求的換票（ops/refresh）不受退避限制；其餘（含 401 復原）都要守退避。
    if reason != RefreshReason::Forced && auth.in_backoff() {
        return Err(KernelError::RefreshFailed {
            account_ref: account_ref.to_owned(),
            message: "backing off after repeated refresh failures".to_owned(),
            sent: UpstreamSent::No,
        });
    }
    let result = match current.refresh_token.as_deref() {
        Some(token) => runtime.refresh(token, reason, &profile).await,
        None => Err(RefreshError::Permanent {
            code: "no_refresh_token".to_owned(),
        }),
    };
    match result {
        Ok(tokens) => commit(&env, &auth, &current, tokens, reason, owner).await,
        Err(err) => commit_failure(&env, &auth, &current, err).await,
    }
}

/// 與匯入共用寫入鎖：核對失敗所屬的憑據並更新狀態，兩步之間不能插入匯入。
async fn commit_failure(
    env: &AccountEnv,
    auth: &AccountAuth,
    current: &Stored,
    failure: RefreshError,
) -> Result<Stored, KernelError> {
    let _write = auth.write.lock().await;
    let latest = stored::read(&env.credential_path)
        .await?
        .ok_or(KernelError::NoCredential)?;
    if latest.generation != current.generation || latest.refresh_token != current.refresh_token {
        return Ok(latest);
    }
    let account_ref = env.account_ref.as_str();
    match failure {
        RefreshError::Permanent { code } => {
            tracing::warn!(account = account_ref, code = %code, "credential requires reauthorization");
            auth.mark_reauth(&code);
            Err(KernelError::ReauthRequired(account_ref.to_owned()))
        }
        RefreshError::Transient { message, sent } => {
            // 連不到 token 端點是出口的問題（熔斷處理），不算 refresh token 換不到票。
            if sent != UpstreamSent::No {
                auth.record_failure();
            }
            let expired = current
                .access_expires_at
                .is_some_and(|expires_at| expires_at < now_secs());
            if expired && auth.failing_for() > RECOVERY_WINDOW {
                tracing::warn!(
                    account = account_ref,
                    "refresh unavailable past the recovery window"
                );
                auth.mark_reauth("refresh_unavailable");
                return Err(KernelError::ReauthRequired(account_ref.to_owned()));
            }
            tracing::warn!(account = account_ref, error = %message, "token refresh failed");
            Err(KernelError::RefreshFailed {
                account_ref: account_ref.to_owned(),
                message,
                sent,
            })
        }
    }
}

/// 把換到的票寫回。寫不進去就當場重試；仍失敗時把票留在記憶體，下次換票前補寫。
async fn commit(
    env: &AccountEnv,
    auth: &AccountAuth,
    current: &Stored,
    tokens: Refreshed,
    reason: RefreshReason,
    owner: Arc<std::fs::File>,
) -> Result<Stored, KernelError> {
    let account_ref = env.account_ref.as_str();
    let _write = auth.write.lock().await;
    let mut attempt = 0;
    loop {
        attempt += 1;
        match stored::commit_refresh(&env.credential_path, current, &tokens).await {
            Ok(next) => {
                auth.record_success();
                tracing::info!(
                    account = account_ref,
                    reason = reason.as_str(),
                    credential_generation = next.generation,
                    "credential refreshed"
                );
                return Ok(next);
            }
            // 刷新途中憑據被換成另一份（帳號或 refresh token 不同）：這次結果作廢，用新的。
            Err(CommitError::Conflict) => {
                tracing::info!(
                    account = account_ref,
                    "credential replaced during refresh; result discarded"
                );
                return stored::read(&env.credential_path)
                    .await?
                    .ok_or(KernelError::NoCredential);
            }
            Err(CommitError::Io(err)) if attempt < COMMIT_ATTEMPTS => {
                tracing::warn!(account = account_ref, error = %err, attempt, "writing the refreshed credential failed; retrying");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(CommitError::Io(err)) => {
                // token 端點已輪換：檔案裡的 refresh token 已被消耗。新票留在記憶體，下次換票前補寫。
                tracing::error!(event = "credential_writeback_pending", account = account_ref, generation = current.generation, error = %err, "refreshed credential could not be written; kept in memory");
                auth.state().unwritten = Some(Unwritten {
                    key: current.key.clone(),
                    consumed: current.refresh_token.clone(),
                    tokens,
                    _owner: owner,
                });
                return Err(KernelError::RefreshFailed {
                    account_ref: account_ref.to_owned(),
                    message: err.to_string(),
                    sent: UpstreamSent::Unknown,
                });
            }
        }
    }
}

/// 這次推理請求要用的憑據。共享模式在這裡做請求前的刷新。
pub(crate) async fn for_request(
    target: &Target,
    envelope: &ResponsesEnvelope,
) -> Result<Credential, KernelError> {
    let Some(lease) = target.lease() else {
        // 單帳號模式：每次重讀，不自行刷新（設計第 4.6 節）。
        return kin_credentials::load(&target.credential_path).await;
    };
    // 自訂 UA 不合法時在這裡就確定不送（也不記下它）。
    let profile = lease.runtime.profile(envelope)?;
    // 只有推理算「使用過」：背景刷新只照顧真的在用的帳號。
    lease.ext().note_use(&profile);
    prepare(&lease.env, &lease.runtime, lease.ext(), &profile).await
}

/// 送上游前的憑據：需要重新授權就確定不送；接近到期就先換票（推理與帳號操作共用）。
pub(crate) async fn prepare(
    env: &AccountEnv,
    runtime: &Arc<HttpUpstream>,
    auth: &Arc<AccountAuth>,
    profile: &Profile,
) -> Result<Credential, KernelError> {
    if auth.failing() {
        return Err(KernelError::ReauthRequired(env.account_ref.clone()));
    }
    let current = stored::read(&env.credential_path)
        .await?
        .ok_or(KernelError::NoCredential)?;
    if let Some(reason) = needs_refresh(&current, REFRESH_WINDOW_SECS) {
        match refresh(env, runtime, auth, Some(current.generation), reason, profile).await {
            Ok(next) => return next.credential(),
            // 與官方相同：主動刷新暫時失敗時照用現有的 token，交給上游判斷。
            Err(KernelError::RefreshFailed { .. }) => {}
            Err(err) => return Err(err),
        }
    }
    current.credential()
}

fn unauthorized(err: &KernelError) -> bool {
    match err {
        KernelError::Upstream(failure) => failure.status == 401,
        KernelError::UpstreamEvent { status, .. } => *status == 401,
        _ => false,
    }
}

/// 先前的業務請求已由 401 確認收到；之後的握手不能把整輪改成未送出。
/// 若重連前的 create 仍可能已執行，保留 unknown 以免控制面再次補送。
fn after_unauthorized(mut err: KernelError) -> KernelError {
    if let KernelError::WebsocketHandshakeRateLimited {
        sent: sent @ UpstreamSent::No,
        ..
    }
    | KernelError::ProxyRetryBudget {
        sent: sent @ UpstreamSent::No,
    } = &mut err
    {
        *sent = UpstreamSent::Yes;
    }
    err
}

/// 送出推理請求。共享模式在上游回 401 時照官方的步驟復原。
pub(crate) async fn send(
    target: &Target,
    envelope: &ResponsesEnvelope,
    credential: Credential,
    cancel: &CancellationToken,
    timing: &kin_upstream::timing::TurnTiming,
) -> Result<Accepted, KernelError> {
    let first = target.upstream.send(envelope, &credential, cancel, timing).await;
    let Some(lease) = target.lease() else {
        return first;
    };
    let err = match first {
        Err(err) if unauthorized(&err) => err,
        other => return other,
    };
    let path = &lease.env.credential_path;
    let account = lease.env.account_ref.as_str();
    // 第 1 步：重讀憑據檔再試（官方即使檔案沒變也會重試一次）。
    let Ok(Some(reloaded)) = stored::read(path).await else {
        return Err(err);
    };
    let Ok(reloaded_credential) = reloaded.credential() else {
        return Err(err);
    };
    tracing::info!(
        account,
        "upstream answered 401; retrying with the reloaded credential"
    );
    let err = match target
        .upstream
        .send(envelope, &reloaded_credential, cancel, timing)
        .await
        .map_err(after_unauthorized)
    {
        Err(err) if unauthorized(&err) => err,
        other => return other,
    };
    // 第 2 步：換票後再試（身分與這次推理相同）。
    let profile = lease.runtime.profile(envelope)?;
    match refresh(
        &lease.env,
        &lease.runtime,
        lease.ext(),
        Some(reloaded.generation),
        RefreshReason::UnauthorizedRecovery,
        &profile,
    )
    .await
    {
        Ok(next) => {
            let credential = next.credential()?;
            target
                .upstream
                .send(envelope, &credential, cancel, timing)
                .await
                .map_err(after_unauthorized)
        }
        Err(reauth @ KernelError::ReauthRequired(_)) => Err(reauth),
        Err(_) => Err(err),
    }
}

/// 背景刷新：每 30 秒看一次近期用過的帳號，接近到期就提早換票。每個帳號各自一個工作，互不阻塞。
pub(crate) async fn refresh_in_background(state: AppState, shutdown: CancellationToken) {
    let mut tick = tokio::time::interval(BACKGROUND_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            () = shutdown.cancelled() => return,
        }
        let Mode::Shared { registry, .. } = state.mode() else {
            return;
        };
        for view in registry.snapshot() {
            view.runtime.reap_idle();
            let auth = view.ext();
            let idle = auth
                .state()
                .last_used
                .is_none_or(|used| used.elapsed() > RECENT_USE);
            let pending = auth.needs_writeback();
            if !view.env.enabled || auth.failing() || (!pending && (idle || auth.in_backoff())) {
                continue;
            }
            // 先拿單飛鎖再確認帳號仍啟用：控制面停用帳號後，以帳號健康的 `refreshing`（鎖被持有）
            // 判斷核心是否已放手憑據檔。若在快照之後、拿鎖之前被停用，這裡就不再換票。
            let Ok(single) = Arc::clone(&auth.refresh).try_lock_owned() else {
                continue;
            };
            if !registry
                .view(&view.env.account_ref)
                .is_ok_and(|now| now.env.enabled && now.env.credential_path == view.env.credential_path)
            {
                continue;
            }
            let Ok(Some(current)) = stored::read(&view.env.credential_path).await else {
                continue;
            };
            let window = REFRESH_WINDOW_SECS + spread(&view.env.account_ref);
            if pending {
                let env = view.env.clone();
                let auth = Arc::clone(auth);
                let active = RefreshInFlight::start();
                tokio::spawn(async move {
                    let (_active, _single) = (active, single);
                    let Ok(owner) = auth.own_writer(&env.credential_path).await else {
                        return;
                    };
                    if let Some(tokens) = auth.take_unwritten(&current)
                        && let Err(err) =
                            commit(&env, &auth, &current, tokens, RefreshReason::Scheduled, owner).await
                    {
                        view.note_error(&err);
                    }
                });
                continue;
            }
            let Some(reason) = needs_refresh(&current, window) else {
                continue;
            };
            let profile = auth.profile();
            let task = tokio::spawn(refresh_detached(
                view.env.clone(),
                Arc::clone(&view.runtime),
                Arc::clone(auth),
                Some(current.generation),
                reason,
                profile,
                Some(single),
                RefreshInFlight::start(),
            ));
            tokio::spawn(async move {
                if let Ok(Err(err)) = task.await {
                    view.note_error(&err);
                }
            });
        }
    }
}

/// `PUT .../credential`：匯入或替換憑據。只拿寫入鎖，不等進行中的刷新。
pub(crate) async fn import(
    env: &AccountEnv,
    auth: &AccountAuth,
    credential: serde_json::Map<String, serde_json::Value>,
    expected: u64,
) -> Result<u64, KernelError> {
    let _owner = auth.own_writer(&env.credential_path).await?;
    let _write = auth.write.lock().await;
    let generation = stored::import(&env.credential_path, credential, expected).await?;
    auth.reset();
    tracing::info!(account = %env.account_ref, credential_generation = generation, "credential imported");
    Ok(generation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn refresh_failures_and_imports_commit_in_write_lock_order() {
        use futures_util::FutureExt;
        use serde_json::json;

        for failure in [
            RefreshError::Permanent {
                code: "refresh_token_reused".into(),
            },
            RefreshError::Transient {
                message: "unavailable".into(),
                sent: UpstreamSent::Yes,
            },
        ] {
            for import_first in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let env: AccountEnv = serde_json::from_value(json!({
                    "ref": "a", "env_generation": 1, "device_id": "device-a",
                    "credential_path": dir.path().join("credential.json"), "max_concurrency": 1
                }))
                .unwrap();
                let auth = AccountAuth::default();
                let tokens = |access: &str| {
                    serde_json::from_value(json!({
                        "access_token": access, "refresh_token": access
                    }))
                    .unwrap()
                };
                import(&env, &auth, tokens("old"), 0).await.unwrap();
                let old = stored::read(&env.credential_path).await.unwrap().unwrap();

                // 先持有寫入鎖，明確排定兩個 future 的 FIFO 順序，再放行。
                let held = auth.write.lock().await;
                let mut replace = std::pin::pin!(import(&env, &auth, tokens("new"), old.generation));
                let mut reject = std::pin::pin!(commit_failure(&env, &auth, &old, failure.clone()));
                if import_first {
                    assert!(replace.as_mut().now_or_never().is_none());
                    assert!(reject.as_mut().now_or_never().is_none());
                } else {
                    assert!(reject.as_mut().now_or_never().is_none());
                    assert!(replace.as_mut().now_or_never().is_none());
                }
                drop(held);
                let (generation, result) = tokio::join!(replace, reject);
                assert_eq!(generation.unwrap(), 2);
                if import_first {
                    assert_eq!(result.unwrap().generation, 2);
                } else {
                    assert!(result.is_err());
                }
                let latest = stored::read(&env.credential_path).await.unwrap().unwrap();
                assert_eq!(latest.access_token.as_deref(), Some("new"));
                assert_eq!(auth.status(Some(&latest)), CredentialStatus::Ok);
                assert!(auth.state().failures.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn overlapping_writers_share_ownership_only_inside_one_auth() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credential.json");
        let first = AccountAuth::default();
        let second = AccountAuth::default();
        let refresh = first.own_writer(&path).await.unwrap();
        let import = first.own_writer(&path).await.unwrap();
        drop(refresh);
        assert!(matches!(
            second.own_writer(&path).await,
            Err(KernelError::KernelBusy)
        ));
        drop(import);
        assert!(second.own_writer(&path).await.is_ok());
    }

    #[test]
    fn the_background_spread_is_stable_and_bounded() {
        for account in ["vm-25", "vm-39", "gw-01", "a"] {
            let first = spread(account);
            assert_eq!(first, spread(account));
            assert!((0..=150).contains(&first), "{account}: {first}");
        }
    }

    #[test]
    fn backoff_starts_after_five_failures() {
        let auth = AccountAuth::default();
        for _ in 0..4 {
            auth.record_failure();
        }
        assert!(!auth.in_backoff());
        auth.record_failure();
        assert!(auth.in_backoff());
        auth.record_success();
        assert!(!auth.in_backoff());
        auth.mark_reauth("refresh_token_reused");
        assert!(auth.failing());
        assert_eq!(auth.status(None), CredentialStatus::ReauthRequired);
        auth.reset();
        assert_eq!(auth.status(None), CredentialStatus::Missing);
    }

    #[test]
    fn the_recorded_profile_follows_the_latest_inference() {
        let auth = AccountAuth::default();
        let environment = kin_contract::envelope::ClientEnvironment {
            machine: Some("x86_64".to_owned()),
            ..Default::default()
        };
        auth.note_use(&Profile {
            environment: Some(environment.clone()),
            user_agent: Some("project/1".to_owned()),
        });
        assert_eq!(auth.profile().user_agent.as_deref(), Some("project/1"));
        // 沒帶帳號環境的請求不抹掉已知的環境；自訂 UA 跟著最近一次推理。
        auth.note_use(&Profile::default());
        assert_eq!(
            auth.profile(),
            Profile {
                environment: Some(environment),
                user_agent: None
            }
        );
    }
}
