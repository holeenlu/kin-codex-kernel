use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use kin_contract::config::{SharedConfig, SingleAccountConfig};
use kin_contract::envelope::ResponsesEnvelope;
use kin_contract::error::KernelError;
use kin_registry::{Lease, Registry};
use kin_upstream::{HttpUpstream, UpstreamSettings};

use crate::credential::AccountAuth;
use tokio_util::sync::CancellationToken;

/// 建置資訊，由執行檔注入；API 層不直接讀編譯期環境變數，方便測試與重用。
#[derive(Debug, Clone)]
pub struct BuildInfo {
    pub name: &'static str,
    pub version: &'static str,
    pub build_sha: &'static str,
}

/// 各處理程式共用的狀態。複製成本是一次 `Arc` 計數。
#[derive(Debug, Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

/// 運作模式。兩種模式共用 HTTP 邊界、推理流程與錯誤對應，只有「這次請求用哪個帳號」不同。
#[derive(Debug)]
pub(crate) enum Mode {
    /// ABI 1：一個程序一個帳號，設定檔就是這個帳號。
    Single {
        config: SingleAccountConfig,
        upstream: Arc<HttpUpstream>,
    },
    /// ABI 2：帳號由控制面登錄，每個帳號一份執行環境。
    Shared {
        config: SharedConfig,
        registry: Registry<HttpUpstream, Arc<AccountAuth>>,
    },
}

#[derive(Debug)]
struct Inner {
    mode: Mode,
    admission: crate::admission::Admission,
    packed: crate::packed::Bases,
    build: BuildInfo,
    /// 在途請求：`request_id` → （登記序號, 取消訊號），供 `POST /internal/v1/cancel` 使用。
    inflight: Mutex<HashMap<String, Entry>>,
    next_seq: AtomicU64,
}

#[derive(Debug)]
struct Entry {
    seq: u64,
    token: CancellationToken,
    interrupt: Option<kin_upstream::interrupt::InterruptHandle>,
}

/// 一次推理請求要用的帳號。共享模式持有並發名額，隨請求結束歸還。
#[derive(Debug)]
pub(crate) struct Target {
    pub(crate) upstream: Arc<HttpUpstream>,
    pub(crate) credential_path: PathBuf,
    lease: Option<Lease<HttpUpstream, Arc<AccountAuth>>>,
}

impl Target {
    /// 共享模式的帳號租約；單帳號模式為 `None`。
    pub(crate) fn lease(&self) -> Option<&Lease<HttpUpstream, Arc<AccountAuth>>> {
        self.lease.as_ref()
    }

    /// 共享模式的請求取消訊號是帳號閘門的子訊號，`DELETE ?force=1` 一次取消整個帳號。
    pub(crate) fn cancel_parent(&self) -> Option<CancellationToken> {
        self.lease.as_ref().map(Lease::cancel_token)
    }

    pub(crate) fn note_error(&self, err: &KernelError) {
        if let Some(lease) = &self.lease {
            lease.note_error(err);
        }
    }
}

impl AppState {
    /// 單帳號模式（ABI 1）。
    pub fn new(config: SingleAccountConfig, build: BuildInfo) -> anyhow::Result<Self> {
        let upstream = Arc::new(HttpUpstream::new(&UpstreamSettings::single(&config))?);
        Ok(Self::with_mode(Mode::Single { config, upstream }, build))
    }

    /// 共享模式（ABI 2）。登錄從空的開始，控制面對帳之後才接單。
    pub fn shared(config: SharedConfig, build: BuildInfo) -> Self {
        // 同一個憑據檔只有一份換票狀態（單飛鎖與寫入鎖），不隨閘門重建而多出第二個換票者。
        let registry = Registry::with_ext(config.max_accounts, |env| {
            crate::credential::auth_for(&env.credential_path)
        });
        Self::with_mode(Mode::Shared { config, registry }, build)
    }

    fn with_mode(mode: Mode, build: BuildInfo) -> Self {
        let limits = match &mode {
            Mode::Single { config, .. } => config.ingress_limits,
            Mode::Shared { config, .. } => config.ingress_limits,
        };
        Self {
            inner: Arc::new(Inner {
                mode,
                admission: crate::admission::Admission::new(limits),
                packed: crate::packed::Bases::default(),
                build,
                inflight: Mutex::new(HashMap::new()),
                next_seq: AtomicU64::new(1),
            }),
        }
    }

    pub(crate) fn admission(&self) -> &crate::admission::Admission {
        &self.inner.admission
    }

    pub(crate) fn packed(&self) -> &crate::packed::Bases {
        &self.inner.packed
    }

    pub(crate) fn mode(&self) -> &Mode {
        &self.inner.mode
    }

    pub fn build(&self) -> &BuildInfo {
        &self.inner.build
    }

    pub fn socket_path(&self) -> &Path {
        match &self.inner.mode {
            Mode::Single { config, .. } => &config.socket_path,
            Mode::Shared { config, .. } => &config.socket_path,
        }
    }

    pub(crate) fn internal_token(&self) -> &str {
        match &self.inner.mode {
            Mode::Single { config, .. } => &config.internal_token,
            Mode::Shared { config, .. } => &config.internal_token,
        }
    }

    /// 日誌用：單帳號模式是槽 ID，共享模式固定為 `shared`。
    pub(crate) fn label(&self) -> &str {
        match &self.inner.mode {
            Mode::Single { config, .. } => &config.vm_id,
            Mode::Shared { .. } => "shared",
        }
    }

    /// 讀推理請求本體時的上限。共享模式先用登錄中最大的帳號上限讀，取得帳號後再依該帳號比對。
    pub(crate) fn body_limit(&self) -> usize {
        let limit = match &self.inner.mode {
            Mode::Single { config, .. } => return config.request_limit(),
            Mode::Shared { registry, .. } => registry.max_request_bytes(),
        };
        usize::try_from(limit).unwrap_or(usize::MAX)
    }

    /// 這次推理請求要用的帳號。共享模式的檢查全部在送上游之前，失敗都是確定沒送出。
    pub(crate) fn target(
        &self,
        envelope: &ResponsesEnvelope,
        body_len: usize,
    ) -> Result<Target, KernelError> {
        match &self.inner.mode {
            Mode::Single { config, upstream } => Ok(Target {
                upstream: Arc::clone(upstream),
                credential_path: config.credential_path.clone(),
                lease: None,
            }),
            Mode::Shared { registry, .. } => {
                let binding = envelope.account.as_ref().ok_or_else(|| {
                    KernelError::InvalidBody("account is required in shared mode".to_owned())
                })?;
                let lease = registry.lease(binding)?;
                let env = &lease.env;
                if u64::try_from(body_len).unwrap_or(u64::MAX) > env.max_request_bytes {
                    return Err(KernelError::BodyTooLarge);
                }
                let mismatch = |field| KernelError::AccountIdentityMismatch {
                    account_ref: env.account_ref.clone(),
                    field,
                };
                let options = &envelope.options;
                if options
                    .installation_id
                    .as_deref()
                    .is_some_and(|id| id.trim() != env.device_id)
                {
                    return Err(mismatch("installation_id"));
                }
                if options.telemetry_runtime.is_some() && options.telemetry_runtime != env.telemetry_runtime {
                    return Err(mismatch("telemetry_runtime"));
                }
                Ok(Target {
                    upstream: Arc::clone(&lease.runtime),
                    credential_path: env.credential_path.clone(),
                    lease: Some(lease),
                })
            }
        }
    }

    /// 登記一個在途請求。回傳的守衛在請求結束（含串流被丟棄）時自動移除登記。
    pub(crate) fn register(
        &self,
        request_id: Option<&str>,
        parent: Option<CancellationToken>,
    ) -> Result<Inflight, KernelError> {
        let token = parent.unwrap_or_default();
        let seq = self.inner.next_seq.fetch_add(1, Ordering::Relaxed);
        let id = request_id
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_owned);
        if let Some(id) = &id {
            let mut inflight = self
                .inner
                .inflight
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let std::collections::hash_map::Entry::Vacant(slot) = inflight.entry(id.clone()) else {
                return Err(KernelError::RequestIdInUse);
            };
            slot.insert(Entry {
                seq,
                token: token.clone(),
                interrupt: None,
            });
        }
        Ok(Inflight {
            state: self.clone(),
            id,
            seq,
            token,
        })
    }

    pub(crate) fn interrupt(&self, id: &str) -> Option<kin_upstream::interrupt::InterruptHandle> {
        self.inner.inflight.lock().ok()?.get(id)?.interrupt.clone()
    }

    /// 取消一個在途請求；找不到時回 `false`。
    pub(crate) fn cancel(&self, request_id: &str) -> bool {
        let token = self
            .inner
            .inflight
            .lock()
            .ok()
            .and_then(|inflight| inflight.get(request_id).map(|entry| entry.token.clone()));
        match token {
            Some(token) if !token.is_cancelled() => {
                token.cancel();
                true
            }
            _ => false,
        }
    }
}

/// 在途請求的登記守衛。
#[derive(Debug)]
pub(crate) struct Inflight {
    state: AppState,
    id: Option<String>,
    seq: u64,
    token: CancellationToken,
}

impl Inflight {
    pub(crate) fn bind(&self, handle: kin_upstream::interrupt::InterruptHandle) -> bool {
        if let Some(id) = &self.id
            && let Ok(mut map) = self.state.inner.inflight.lock()
            && let Some(entry) = map.get_mut(id)
            && entry.seq == self.seq
        {
            entry.interrupt = Some(handle);
            return true;
        }
        false
    }

    pub(crate) fn token(&self) -> &CancellationToken {
        &self.token
    }
}

impl Drop for Inflight {
    fn drop(&mut self) {
        if let Some(id) = &self.id
            && let Ok(mut inflight) = self.state.inner.inflight.lock()
            // 只移除自己的登記：同一個 id 若已被新的請求取代，不能誤刪。
            && inflight.get(id).is_some_and(|entry| entry.seq == self.seq)
        {
            inflight.remove(id);
        }
    }
}
