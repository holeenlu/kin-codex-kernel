//! 共享模式（ABI 2）的帳號登錄（vm2api `docs/CODEX_SHARED_KERNEL_DESIGN.md` 第 4.3 節、本倉庫 `docs/ABI2.md`）。
//!
//! - 每個帳號一份執行環境 `R`（正式程式是 `tap_upstream::HttpUpstream`），以 `Arc` 交給請求：
//!   環境更新時新請求拿到新的，在途請求用完舊的才回收，不會被中斷。
//! - 每個帳號一個 [`Gate`]，跨環境更新共用：並發上限、在途數、強制取消與最近錯誤都記在這裡。
//! - 刪除留下帶代次的墓碑，晚到的舊命令不會刪掉或蓋掉新登錄。
//! - 登錄不持久化：從 [`RegistryState::Empty`] 開始，對帳之後才接單。
//!
//! 執行環境用泛型，CAS 與排空的規則可以不碰網路就測試；建立執行環境的函式由呼叫端傳入。

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, RwLockWriteGuard, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use tap_contract::config::DEFAULT_MAX_REQUEST_BYTES;
use tap_contract::envelope::AccountBinding;
use tap_contract::error::KernelError;
use tap_contract::registry::{
    AccountCounts, AccountEnv, AccountList, AccountState, AccountSummary, DeleteAck, LastError, ReconcileAck,
    RegistryState, valid_account_ref,
};
use tokio_util::sync::CancellationToken;

/// 每個帳號的閘門。帳號被刪除後重新建立時換一個新的。
#[derive(Debug, Default)]
pub struct Gate<A = ()> {
    inflight: AtomicU32,
    /// `DELETE ?force=1` 取消這個帳號所有在途請求；請求的取消訊號是它的子訊號。
    cancel: CancellationToken,
    last_error: Mutex<Option<LastError>>,
    /// 呼叫端的每帳號狀態（共享模式放刷新的單飛鎖與退避），與閘門同生同滅。
    ext: A,
}

impl<A> Gate<A> {
    fn with_ext(ext: A) -> Self {
        Self {
            inflight: AtomicU32::new(0),
            cancel: CancellationToken::new(),
            last_error: Mutex::new(None),
            ext,
        }
    }

    /// 呼叫端的每帳號狀態。
    pub fn ext(&self) -> &A {
        &self.ext
    }

    fn try_acquire(self: &Arc<Self>, limit: u32) -> Option<Permit<A>> {
        let mut current = self.inflight.load(Ordering::Acquire);
        loop {
            if current >= limit {
                return None;
            }
            match self.inflight.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(Permit {
                        gate: Arc::clone(self),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    pub fn inflight(&self) -> u32 {
        self.inflight.load(Ordering::Acquire)
    }

    pub fn last_error(&self) -> Option<LastError> {
        self.last_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn note_error(&self, code: &'static str) {
        let at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or_default();
        *self.last_error.lock().unwrap_or_else(PoisonError::into_inner) = Some(LastError { code, at });
    }
}

/// 一個在途請求占用的並發名額；丟棄時歸還。
#[derive(Debug)]
pub struct Permit<A = ()> {
    gate: Arc<Gate<A>>,
}

impl<A> Drop for Permit<A> {
    fn drop(&mut self) {
        self.gate.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

/// 一次推理請求取得的帳號：執行環境、當下的帳號環境與並發名額。名額隨它一起歸還。
#[derive(Debug)]
pub struct Lease<R, A = ()> {
    pub env: AccountEnv,
    pub runtime: Arc<R>,
    permit: Permit<A>,
}

impl<R, A> Lease<R, A> {
    /// 這個帳號的呼叫端狀態。
    pub fn ext(&self) -> &A {
        &self.permit.gate.ext
    }

    /// 這次請求的取消訊號：`POST /internal/v1/cancel` 取消它，`DELETE ?force=1` 取消整個帳號。
    pub fn cancel_token(&self) -> CancellationToken {
        self.permit.gate.cancel.child_token()
    }

    /// 記下這個帳號最近一次錯誤，供帳號健康回報。
    pub fn note_error(&self, err: &KernelError) {
        self.permit.gate.note_error(err.code());
    }
}

/// `PUT` 的結果：建立回 201，其餘回 200。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Created,
    Updated,
    Unchanged,
}

/// 健康回報需要的單一帳號快照。
#[derive(Debug)]
pub struct AccountView<R, A = ()> {
    pub env: AccountEnv,
    pub runtime: Arc<R>,
    pub state: AccountState,
    pub inflight: u32,
    pub last_error: Option<LastError>,
    gate: Arc<Gate<A>>,
}

impl<R, A> AccountView<R, A> {
    /// 這個帳號的呼叫端狀態。
    pub fn ext(&self) -> &A {
        &self.gate.ext
    }

    /// 記下這個帳號最近一次錯誤（背景刷新等不經推理請求的失敗）。
    pub fn note_error(&self, err: &KernelError) {
        self.gate.note_error(err.code());
    }

    /// 帳號操作的取消訊號：`POST /internal/v1/cancel` 取消它，`DELETE ?force=1` 取消整個帳號。
    pub fn cancel_token(&self) -> CancellationToken {
        self.gate.cancel.child_token()
    }
}

fn view_of<R, A>(live: &Live<R, A>) -> AccountView<R, A> {
    AccountView {
        env: live.env.clone(),
        runtime: Arc::clone(&live.runtime),
        state: state_of_live(live),
        inflight: live.gate.inflight(),
        last_error: live.gate.last_error(),
        gate: Arc::clone(&live.gate),
    }
}

#[derive(Debug)]
struct Live<R, A> {
    env: AccountEnv,
    runtime: Arc<R>,
    gate: Arc<Gate<A>>,
}

#[derive(Debug)]
enum Slot<R, A> {
    Live(Live<R, A>),
    /// 已刪除。`gate` 只用來回報排空進度，帳號的在途請求結束後自然失效。
    Tombstone {
        generation: u64,
        gate: Weak<Gate<A>>,
    },
}

#[derive(Debug)]
struct Inner<R, A> {
    state: RegistryState,
    slots: HashMap<String, Slot<R, A>>,
}

/// 對帳時現有帳號的快照：環境、執行環境與閘門。
type Existing<R, A> = HashMap<String, (AccountEnv, Arc<R>, Arc<Gate<A>>)>;

/// 依帳號環境產生閘門的每帳號狀態（共享模式依憑據路徑取同一份換票狀態）。
type ExtFor<A> = Box<dyn Fn(&AccountEnv) -> A + Send + Sync>;

pub struct Registry<R, A = ()> {
    inner: RwLock<Inner<R, A>>,
    max_accounts: usize,
    ext_for: ExtFor<A>,
}

impl<R, A> std::fmt::Debug for Registry<R, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("max_accounts", &self.max_accounts)
            .finish_non_exhaustive()
    }
}

/// 只有這些欄位會影響執行環境本身；其他欄位（上限、啟用、憑據路徑）改變時沿用原執行環境，
/// WebSocket 連線池不必重建。`telemetry_runtime` 是執行環境送分析事件時的預設值，改了就要重建，
/// 否則沒帶 `telemetry_runtime` 的請求會送出舊的執行環境。
fn same_runtime(a: &AccountEnv, b: &AccountEnv) -> bool {
    a.device_id == b.device_id
        && a.proxy_url.trim() == b.proxy_url.trim()
        && a.telemetry_runtime == b.telemetry_runtime
}

fn invalid(message: impl Into<String>) -> KernelError {
    KernelError::InvalidBody(message.into())
}

fn validate(env: &AccountEnv) -> Result<(), KernelError> {
    if !valid_account_ref(&env.account_ref) {
        return Err(invalid(format!("invalid account ref {:?}", env.account_ref)));
    }
    if env.env_generation == 0 {
        return Err(invalid("env_generation must be at least 1"));
    }
    if env.device_id.trim().is_empty() {
        return Err(invalid("device_id must not be empty"));
    }
    if env.credential_path.as_os_str().is_empty() {
        return Err(invalid("credential_path must not be empty"));
    }
    if env.max_concurrency == 0 {
        return Err(invalid("max_concurrency must be at least 1"));
    }
    if env.proxy_required && env.proxy_url.trim().is_empty() {
        return Err(invalid("proxy required"));
    }
    Ok(())
}

fn state_of_live<R, A>(live: &Live<R, A>) -> AccountState {
    if live.env.enabled {
        AccountState::Ready
    } else {
        AccountState::Disabled
    }
}

/// 墓碑上的帳號若還有在途請求，就是排空中。
fn draining<A>(gate: &Weak<Gate<A>>) -> Option<Arc<Gate<A>>> {
    gate.upgrade().filter(|gate| gate.inflight() > 0)
}

impl<R, A: Default> Registry<R, A> {
    pub fn new(max_accounts: usize) -> Self {
        Self::with_ext(max_accounts, |_| A::default())
    }
}

impl<R, A> Registry<R, A> {
    /// `ext_for` 產生新閘門的每帳號狀態。同一個憑據檔必須只有一份換票狀態（單飛鎖與寫入鎖），
    /// 所以共享模式依憑據路徑取用，而不是每個閘門各建一份。
    pub fn with_ext(max_accounts: usize, ext_for: impl Fn(&AccountEnv) -> A + Send + Sync + 'static) -> Self {
        Self {
            inner: RwLock::new(Inner {
                state: RegistryState::Empty,
                slots: HashMap::new(),
            }),
            max_accounts,
            ext_for: Box::new(ext_for),
        }
    }

    fn new_gate(&self, env: &AccountEnv) -> Arc<Gate<A>> {
        Arc::new(Gate::with_ext((self.ext_for)(env)))
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Inner<R, A>> {
        self.inner.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, Inner<R, A>> {
        self.inner.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn ready_for_writes(&self) -> Result<RwLockWriteGuard<'_, Inner<R, A>>, KernelError> {
        let inner = self.write();
        if inner.state == RegistryState::Ready {
            Ok(inner)
        } else {
            Err(KernelError::RegistryNotReady)
        }
    }

    pub fn state(&self) -> RegistryState {
        self.read().state
    }

    /// `PUT /internal/v1/accounts/{ref}`：照 `docs/ABI2.md` 的表決定建立、更新、冪等或拒絕。
    pub fn put<F>(&self, path_ref: &str, env: AccountEnv, build: F) -> Result<PutOutcome, KernelError>
    where
        F: FnOnce(&AccountEnv) -> Result<R, String>,
    {
        if env.account_ref != path_ref {
            return Err(invalid("ref in the body differs from the path"));
        }
        validate(&env)?;
        let mut inner = self.ready_for_writes()?;
        // 兩個帳號共用一個憑據檔，就有兩個換票者輪流消耗同一個 refresh token。
        if inner.slots.iter().any(|(key, slot)| {
            key != path_ref
                && matches!(slot, Slot::Live(live) if live.env.credential_path == env.credential_path)
        }) {
            return Err(invalid("credential_path is used by another account"));
        }
        let live_count = inner
            .slots
            .values()
            .filter(|slot| matches!(slot, Slot::Live(_)))
            .count();
        match inner.slots.get(path_ref) {
            Some(Slot::Live(live)) => {
                let current = live.env.env_generation;
                if env.env_generation < current {
                    return Err(KernelError::StaleGeneration);
                }
                if env.env_generation == current {
                    return if env.same_content(&live.env) {
                        Ok(PutOutcome::Unchanged)
                    } else {
                        Err(KernelError::GenerationConflict)
                    };
                }
                if env.expected_env_generation != current {
                    return Err(KernelError::GenerationConflict);
                }
                let runtime = if same_runtime(&live.env, &env) {
                    Arc::clone(&live.runtime)
                } else {
                    Arc::new(build(&env).map_err(invalid)?)
                };
                // 換了憑據檔就換一份換票狀態（舊檔的需要重新授權、退避不帶過來）；在途請求留在舊閘門。
                let gate = if live.env.credential_path == env.credential_path {
                    Arc::clone(&live.gate)
                } else {
                    self.new_gate(&env)
                };
                inner
                    .slots
                    .insert(path_ref.to_owned(), Slot::Live(Live { env, runtime, gate }));
                Ok(PutOutcome::Updated)
            }
            Some(Slot::Tombstone { generation, .. }) if env.env_generation <= *generation => {
                Err(KernelError::StaleGeneration)
            }
            _ => {
                if live_count >= self.max_accounts {
                    return Err(invalid(format!("more than {} accounts", self.max_accounts)));
                }
                let runtime = Arc::new(build(&env).map_err(invalid)?);
                let gate = self.new_gate(&env);
                inner
                    .slots
                    .insert(path_ref.to_owned(), Slot::Live(Live { env, runtime, gate }));
                Ok(PutOutcome::Created)
            }
        }
    }

    /// `DELETE /internal/v1/accounts/{ref}`：立即停止接新請求並留下墓碑；在途請求結束後執行環境自然回收。
    pub fn delete(&self, path_ref: &str, generation: u64, force: bool) -> Result<DeleteAck, KernelError> {
        if !valid_account_ref(path_ref) {
            return Err(invalid(format!("invalid account ref {path_ref:?}")));
        }
        let mut inner = self.ready_for_writes()?;
        let (tombstone, gate) = match inner.slots.get(path_ref) {
            Some(Slot::Live(live)) => {
                if generation < live.env.env_generation {
                    return Err(KernelError::StaleGeneration);
                }
                (generation, Arc::downgrade(&live.gate))
            }
            // 同代次的重送是冪等的（ACK 遺失）；更舊的代次是晚到的舊命令。
            Some(Slot::Tombstone {
                generation: existing,
                gate,
            }) => {
                if generation < *existing {
                    return Err(KernelError::StaleGeneration);
                }
                ((*existing).max(generation), gate.clone())
            }
            None => (generation, Weak::new()),
        };
        let live_gate = gate.upgrade();
        if force && let Some(gate) = &live_gate {
            gate.cancel.cancel();
        }
        inner.slots.insert(
            path_ref.to_owned(),
            Slot::Tombstone {
                generation: tombstone,
                gate,
            },
        );
        Ok(DeleteAck {
            ok: true,
            account_ref: path_ref.to_owned(),
            tombstone_generation: tombstone,
            inflight: live_gate.map(|gate| gate.inflight()).unwrap_or_default(),
        })
    }

    /// `POST /internal/v1/accounts/reconcile`：以完整清單取代整個登錄。
    ///
    /// 期間狀態為 `reconciling`，所有推理與 PUT／DELETE 回 `registry_not_ready`。
    /// 代次與內容都沒變的帳號保留原執行環境；不在清單內的帳號照 DELETE 處理。
    /// 任何一筆不合法或建立失敗，整份不套用，登錄回到原狀態。
    pub fn reconcile<F>(&self, envs: Vec<AccountEnv>, mut build: F) -> Result<ReconcileAck, KernelError>
    where
        F: FnMut(&AccountEnv) -> Result<R, String>,
    {
        let mut seen = HashSet::new();
        let mut paths = HashSet::new();
        for env in &envs {
            validate(env)?;
            if !seen.insert(env.account_ref.as_str()) {
                return Err(invalid(format!("duplicate account ref {:?}", env.account_ref)));
            }
            if !paths.insert(env.credential_path.as_path()) {
                return Err(invalid(format!(
                    "credential_path of {:?} is used by another account",
                    env.account_ref
                )));
            }
        }
        if envs.len() > self.max_accounts {
            return Err(invalid(format!("more than {} accounts", self.max_accounts)));
        }

        // 第一段：標記對帳中，取出現有帳號的快照；之後不持鎖建立執行環境。
        let (previous, existing) = {
            let mut inner = self.write();
            if inner.state == RegistryState::Reconciling {
                return Err(KernelError::RegistryNotReady);
            }
            let previous = inner.state;
            inner.state = RegistryState::Reconciling;
            let existing: Existing<R, A> = inner
                .slots
                .iter()
                .filter_map(|(key, slot)| match slot {
                    Slot::Live(live) => Some((
                        key.clone(),
                        (
                            live.env.clone(),
                            Arc::clone(&live.runtime),
                            Arc::clone(&live.gate),
                        ),
                    )),
                    Slot::Tombstone { .. } => None,
                })
                .collect();
            (previous, existing)
        };

        let mut built = Vec::with_capacity(envs.len());
        for env in envs {
            let reuse = existing
                .get(&env.account_ref)
                .filter(|(old, _, _)| same_runtime(old, &env));
            let runtime = match reuse {
                Some((_, runtime, _)) => Arc::clone(runtime),
                None => match build(&env) {
                    Ok(runtime) => Arc::new(runtime),
                    Err(message) => {
                        self.write().state = previous;
                        return Err(invalid(message));
                    }
                },
            };
            let gate = existing
                .get(&env.account_ref)
                .filter(|(old, _, _)| old.credential_path == env.credential_path)
                .map_or_else(|| self.new_gate(&env), |(_, _, gate)| Arc::clone(gate));
            built.push(Live { env, runtime, gate });
        }

        // 第二段：一次換上新表。
        let mut inner = self.write();
        let listed: HashSet<String> = built.iter().map(|live| live.env.account_ref.clone()).collect();
        let mut slots: HashMap<String, Slot<R, A>> = HashMap::with_capacity(inner.slots.len());
        for (key, slot) in inner.slots.drain() {
            if listed.contains(&key) {
                continue;
            }
            let tombstone = match slot {
                Slot::Live(live) => Slot::Tombstone {
                    generation: live.env.env_generation,
                    gate: Arc::downgrade(&live.gate),
                },
                tombstone @ Slot::Tombstone { .. } => tombstone,
            };
            slots.insert(key, tombstone);
        }
        let accounts = built.len();
        for live in built {
            slots.insert(live.env.account_ref.clone(), Slot::Live(live));
        }
        inner.slots = slots;
        inner.state = RegistryState::Ready;
        Ok(ReconcileAck {
            ok: true,
            registry_state: RegistryState::Ready,
            accounts,
        })
    }

    /// 推理請求取得帳號。全部在送上游之前判定，失敗的送出狀態一律是「確定沒送出」。
    pub fn lease(&self, binding: &AccountBinding) -> Result<Lease<R, A>, KernelError> {
        let inner = self.read();
        if inner.state != RegistryState::Ready {
            return Err(KernelError::RegistryNotReady);
        }
        let account_ref = binding.account_ref.as_str();
        match inner.slots.get(account_ref) {
            None => Err(KernelError::AccountUnknown(account_ref.to_owned())),
            Some(Slot::Tombstone { gate, .. }) => Err(if draining(gate).is_some() {
                KernelError::AccountDisabled(account_ref.to_owned())
            } else {
                KernelError::AccountUnknown(account_ref.to_owned())
            }),
            Some(Slot::Live(live)) => {
                if !live.env.enabled {
                    return Err(KernelError::AccountDisabled(account_ref.to_owned()));
                }
                if binding.env_generation != live.env.env_generation {
                    return Err(KernelError::AccountGenerationMismatch {
                        account_ref: account_ref.to_owned(),
                        expected: live.env.env_generation,
                        actual: binding.env_generation,
                    });
                }
                let permit = live
                    .gate
                    .try_acquire(live.env.max_concurrency)
                    .ok_or_else(|| KernelError::AccountBusy(account_ref.to_owned()))?;
                Ok(Lease {
                    env: live.env.clone(),
                    runtime: Arc::clone(&live.runtime),
                    permit,
                })
            }
        }
    }

    /// `GET /internal/v1/accounts`：登錄中的帳號，加上仍在排空的已刪除帳號。
    pub fn list(&self) -> AccountList {
        let inner = self.read();
        let mut accounts: Vec<AccountSummary> = inner
            .slots
            .iter()
            .filter_map(|(key, slot)| match slot {
                Slot::Live(live) => Some(AccountSummary {
                    account_ref: key.clone(),
                    env_generation: live.env.env_generation,
                    credential_generation: None,
                    state: state_of_live(live),
                }),
                Slot::Tombstone { generation, gate } => draining(gate).map(|_| AccountSummary {
                    account_ref: key.clone(),
                    env_generation: *generation,
                    credential_generation: None,
                    state: AccountState::Draining,
                }),
            })
            .collect();
        accounts.sort_by(|a, b| a.account_ref.cmp(&b.account_ref));
        AccountList {
            registry_state: inner.state,
            accounts,
        }
    }

    /// 單一帳號的快照，供帳號健康回報。已刪除的帳號回 `account_unknown`。
    pub fn view(&self, account_ref: &str) -> Result<AccountView<R, A>, KernelError> {
        let inner = self.read();
        match inner.slots.get(account_ref) {
            Some(Slot::Live(live)) => Ok(view_of(live)),
            _ => Err(KernelError::AccountUnknown(account_ref.to_owned())),
        }
    }

    /// 控制面對單一帳號下的命令（匯入憑據、強制換票）：與推理相同，登錄要就緒（ABI2.md：`empty` 與
    /// `reconciling` 期間回 `registry_not_ready`）。`require_enabled` 時停用或排空中的帳號回 `account_disabled`：
    /// 控制面停用帳號是為了讓核心放手憑據檔，之後不能再由核心換票。
    pub fn managed(
        &self,
        account_ref: &str,
        require_enabled: bool,
    ) -> Result<AccountView<R, A>, KernelError> {
        let inner = self.read();
        if inner.state != RegistryState::Ready {
            return Err(KernelError::RegistryNotReady);
        }
        match inner.slots.get(account_ref) {
            Some(Slot::Live(live)) if require_enabled && !live.env.enabled => {
                Err(KernelError::AccountDisabled(account_ref.to_owned()))
            }
            Some(Slot::Live(live)) => Ok(view_of(live)),
            Some(Slot::Tombstone { gate, .. }) if require_enabled && draining(gate).is_some() => {
                Err(KernelError::AccountDisabled(account_ref.to_owned()))
            }
            _ => Err(KernelError::AccountUnknown(account_ref.to_owned())),
        }
    }

    /// 健康回報的帳號計數。`failing` 要等憑據與熔斷狀態接入（K2c、K2d）才會有值。
    pub fn counts(&self) -> AccountCounts {
        self.counts_with(|_, _| false)
    }

    /// 帳號操作取得帳號：檢查與推理相同（登錄狀態、啟用、代次），但不占推理的並發名額。
    pub fn account(&self, binding: &AccountBinding) -> Result<AccountView<R, A>, KernelError> {
        let inner = self.read();
        if inner.state != RegistryState::Ready {
            return Err(KernelError::RegistryNotReady);
        }
        let account_ref = binding.account_ref.as_str();
        match inner.slots.get(account_ref) {
            None => Err(KernelError::AccountUnknown(account_ref.to_owned())),
            Some(Slot::Tombstone { gate, .. }) => Err(if draining(gate).is_some() {
                KernelError::AccountDisabled(account_ref.to_owned())
            } else {
                KernelError::AccountUnknown(account_ref.to_owned())
            }),
            Some(Slot::Live(live)) => {
                if !live.env.enabled {
                    return Err(KernelError::AccountDisabled(account_ref.to_owned()));
                }
                if binding.env_generation != live.env.env_generation {
                    return Err(KernelError::AccountGenerationMismatch {
                        account_ref: account_ref.to_owned(),
                        expected: live.env.env_generation,
                        actual: binding.env_generation,
                    });
                }
                Ok(view_of(live))
            }
        }
    }

    /// 健康回報的帳號計數；`failing` 由呼叫端依每帳號狀態判斷（例如需要重新授權）。
    pub fn counts_with(&self, failing: impl Fn(&R, &A) -> bool) -> AccountCounts {
        let inner = self.read();
        let mut counts = AccountCounts {
            total: 0,
            ready: 0,
            failing: 0,
        };
        for slot in inner.slots.values() {
            let Slot::Live(live) = slot else { continue };
            counts.total += 1;
            if failing(&live.runtime, &live.gate.ext) {
                counts.failing += 1;
            } else if live.env.enabled {
                counts.ready += 1;
            }
        }
        counts
    }

    /// 登錄中所有帳號的快照（背景工作用）。
    pub fn snapshot(&self) -> Vec<AccountView<R, A>> {
        self.read()
            .slots
            .values()
            .filter_map(|slot| match slot {
                Slot::Live(live) => Some(view_of(live)),
                Slot::Tombstone { .. } => None,
            })
            .collect()
    }

    /// 讀推理請求本體時的上限：登錄中最大的那個帳號上限。實際比對在取得帳號之後。
    pub fn max_request_bytes(&self) -> u64 {
        self.read()
            .slots
            .values()
            .filter_map(|slot| match slot {
                Slot::Live(live) => Some(live.env.max_request_bytes),
                Slot::Tombstone { .. } => None,
            })
            .max()
            .unwrap_or(DEFAULT_MAX_REQUEST_BYTES)
    }
}

#[cfg(test)]
mod tests;
