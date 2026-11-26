//! 多個帳號只共用代理的實體名額；socket、cookie 與續接內容永不共用。
use kin_contract::config::WsLimits;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
pub(super) struct Capacity {
    retry_limit: u16,
    retries: Mutex<VecDeque<Instant>>,
    connecting: Option<Arc<Semaphore>>,
    live: Option<Arc<Semaphore>>,
    pools: Mutex<Vec<Weak<crate::websocket::WebsocketTransport>>>,
}
type Registry = HashMap<([u8; 32], u32, u32, u16), Weak<Capacity>>;
static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(Mutex::default);

impl Capacity {
    pub(super) fn for_proxy(proxy: &str, limits: WsLimits) -> Option<Arc<Self>> {
        if proxy.is_empty()
            || limits.proxy_connections == 0
                && limits.proxy_connecting == 0
                && limits.proxy_retries_per_second == 0
        {
            return None;
        }
        let key = (
            Sha256::digest(proxy.as_bytes()).into(),
            limits.proxy_connecting,
            limits.proxy_connections,
            limits.proxy_retries_per_second,
        );
        let mut registry = REGISTRY.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.retain(|_, c| c.strong_count() > 0);
        if let Some(current) = registry.get(&key).and_then(Weak::upgrade) {
            return Some(current);
        }
        let sem =
            |n: u32| (n != 0).then(|| Arc::new(Semaphore::new((n as usize).min(Semaphore::MAX_PERMITS))));
        let capacity = Arc::new(Self {
            retry_limit: limits.proxy_retries_per_second,
            retries: Mutex::default(),
            connecting: sem(limits.proxy_connecting),
            live: sem(limits.proxy_connections),
            pools: Mutex::default(),
        });
        registry.insert(key, Arc::downgrade(&capacity));
        Some(capacity)
    }

    pub(super) fn admit_retry(&self, now: Instant) -> bool {
        if self.retry_limit == 0 {
            return true;
        }
        let mut recent = self
            .retries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while recent
            .front()
            .is_some_and(|at| now.saturating_duration_since(*at) >= Duration::from_secs(1))
        {
            recent.pop_front();
        }
        if recent.len() >= usize::from(self.retry_limit) {
            return false;
        }
        recent.push_back(now);
        true
    }

    pub(super) fn attach(&self, pool: &Arc<crate::websocket::WebsocketTransport>) {
        let mut pools = self
            .pools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pools.retain(|w| w.strong_count() != 0);
        pools.push(Arc::downgrade(pool));
    }

    fn evict_idle(&self) -> bool {
        // 持有者只移除仍在池中的同一條閒置線，絕不回收借出或進行握手的 socket。
        let pools = self
            .pools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pools
            .iter()
            .filter_map(Weak::upgrade)
            .filter_map(|p| p.oldest_idle().map(|(key, at)| (at, key, p)))
            .min_by_key(|(at, _, _)| *at)
            .is_some_and(|(at, key, p)| p.remove_idle(&key, at))
    }

    /// 不把等待容量的請求當成代理故障；可走 HTTP 的請求不必在此排第二條隊。
    pub(super) fn reserve(&self) -> Result<(Option<OwnedSemaphorePermit>, Option<OwnedSemaphorePermit>), ()> {
        let take = |s: &Option<Arc<Semaphore>>| {
            s.as_ref()
                .map(|s| Arc::clone(s).try_acquire_owned())
                .transpose()
                .map_err(|_| ())
        };
        let connecting = take(&self.connecting)?;
        let live = take(&self.live).or_else(|()| {
            if self.evict_idle() {
                take(&self.live)
            } else {
                Err(())
            }
        })?;
        Ok((connecting, live))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_window_is_shared_by_accounts_on_one_proxy() {
        let limits = WsLimits {
            proxy_retries_per_second: 1,
            ..Default::default()
        };
        let a = Capacity::for_proxy("http://retry-proxy", limits).unwrap();
        let b = Capacity::for_proxy("http://retry-proxy", limits).unwrap();
        let now = Instant::now();
        assert!(a.admit_retry(now));
        assert!(!b.admit_retry(now));
        assert!(b.admit_retry(now + Duration::from_secs(1)));
    }

    #[test]
    fn accounts_share_only_proxy_resources_until_the_socket_drops() {
        let limits = WsLimits {
            proxy_connecting: 1,
            proxy_connections: 2,
            ..Default::default()
        };
        let a = Capacity::for_proxy("http://proxy-a", limits).unwrap();
        let b = Capacity::for_proxy("http://proxy-a", limits).unwrap();
        let different = Capacity::for_proxy("http://proxy-b", limits).unwrap();
        let (dial, first) = a.reserve().unwrap();
        assert!(b.reserve().is_err());
        assert!(different.reserve().is_ok());
        drop(dial);
        let (dial, second) = b.reserve().unwrap();
        drop(dial);
        assert!(a.reserve().is_err());
        drop(first);
        assert!(a.reserve().is_ok());
        drop(second);
    }
}
