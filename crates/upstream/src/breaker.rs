//! 「帳號 × 出口」的連線熔斷（vm2api 設計第 5.3 節）。只在共享模式啟用。
//!
//! 一個上游執行環境就是一個帳號的一個出口（出口改變時重建），所以熔斷器跟著執行環境走，HTTP 與 WebSocket 各一個：
//! - 連線還沒建立就失敗（連不上代理、代理拒絕、TLS 失敗）才算失敗；上游回了任何 HTTP 狀態都表示連得上。
//! - 連續 [`THRESHOLD`] 次失敗就斷開；斷開期間不撥號，HTTP 直接回 `proxy_unavailable`（確定沒送出），
//!   WebSocket 改走 HTTP。
//! - 冷卻時間到了放一個探測請求（半開）；成功就恢復，失敗就重新斷開。
//! - 核心只負責快速失敗，不自行換出口或換帳號；狀態寫進帳號健康，由控制面決定是否避開這個槽。
//!
//! 每次撥號先取一張 [`Ticket`]，連線結果經它回報。通行證被丟棄而沒有回報時（請求被取消、整個
//! future 被丟棄），探測名額自動釋放給下一個請求，熔斷器不會卡在半開。每次斷開或恢復都換一個
//! 代次，舊代次發出的通行證晚到的結果一律不算，不會改變半開探測的判定。

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use kin_contract::registry::BreakerState;

/// 連續幾次失敗就斷開。
pub const THRESHOLD: u32 = 3;

#[derive(Debug, Default)]
struct Inner {
    failures: u32,
    opened_at: Option<Instant>,
    probing: bool,
    /// 每次斷開或恢復時遞增；通行證記下發出時的代次。
    epoch: u64,
}

#[derive(Debug)]
pub struct Breaker {
    cooldown: Duration,
    inner: Mutex<Inner>,
}

/// 一次撥號的准入。用 [`Ticket::success`] 或 [`Ticket::failure`] 回報結果。
#[derive(Debug)]
pub struct Ticket<'a> {
    breaker: &'a Breaker,
    epoch: u64,
    probe: bool,
    reported: bool,
}

impl Ticket<'_> {
    /// 這張是半開狀態下的探測。
    pub const fn is_probe(&self) -> bool {
        self.probe
    }

    /// 連得上（不論上游回什麼狀態）。
    pub fn success(self) {
        self.report(true, Instant::now());
    }

    /// 連線還沒建立就失敗。
    pub fn failure(self) {
        self.report(false, Instant::now());
    }

    fn report(mut self, ok: bool, now: Instant) {
        self.reported = true;
        self.breaker.record(self.epoch, self.probe, ok, now);
    }
}

impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        if !self.reported && self.probe {
            self.breaker.release_probe(self.epoch);
        }
    }
}

impl Breaker {
    pub fn new(cooldown: Duration) -> Self {
        Self {
            cooldown,
            inner: Mutex::new(Inner::default()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// 這次能不能撥號；斷開中（或另一個探測正在進行）回 `None`。
    pub fn admit(&self) -> Option<Ticket<'_>> {
        self.admit_at(Instant::now())
    }

    fn admit_at(&self, now: Instant) -> Option<Ticket<'_>> {
        let mut inner = self.lock();
        let probe = match inner.opened_at {
            None => false,
            Some(opened_at) => {
                if now.duration_since(opened_at) < self.cooldown || inner.probing {
                    return None;
                }
                inner.probing = true;
                true
            }
        };
        Some(Ticket {
            breaker: self,
            epoch: inner.epoch,
            probe,
            reported: false,
        })
    }

    /// 探測沒有結果（被取消或丟棄）：讓下一個請求重新探測。
    fn release_probe(&self, epoch: u64) {
        let mut inner = self.lock();
        if inner.epoch == epoch {
            inner.probing = false;
        }
    }

    fn record(&self, epoch: u64, probe: bool, ok: bool, now: Instant) {
        let mut inner = self.lock();
        // 斷開或恢復之前發出的通行證：結果屬於上一個狀態，不算。
        if inner.epoch != epoch {
            return;
        }
        if ok {
            let transition = inner.opened_at.is_some();
            inner.failures = 0;
            inner.opened_at = None;
            inner.probing = false;
            if transition {
                inner.epoch = inner.epoch.wrapping_add(1);
            }
            return;
        }
        inner.failures = inner.failures.saturating_add(1);
        if probe || (inner.opened_at.is_none() && inner.failures >= THRESHOLD) {
            inner.opened_at = Some(now);
            inner.probing = false;
            inner.epoch = inner.epoch.wrapping_add(1);
        }
    }

    pub fn state(&self) -> BreakerState {
        self.state_at(Instant::now())
    }

    fn state_at(&self, now: Instant) -> BreakerState {
        let inner = self.lock();
        match inner.opened_at {
            None => BreakerState::Closed,
            Some(_) if inner.probing => BreakerState::HalfOpen,
            Some(opened_at) if now.duration_since(opened_at) >= self.cooldown => BreakerState::HalfOpen,
            Some(_) => BreakerState::Open,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fail(breaker: &Breaker, now: Instant) {
        breaker.admit_at(now).expect("admitted").report(false, now);
    }

    #[test]
    fn opens_after_consecutive_failures_and_probes_after_the_cooldown() {
        let breaker = Breaker::new(Duration::from_secs(30));
        let start = Instant::now();
        fail(&breaker, start);
        fail(&breaker, start);
        assert!(breaker.admit_at(start).is_some(), "two failures are not enough");
        breaker.admit_at(start).expect("admitted").success();
        fail(&breaker, start);
        fail(&breaker, start);
        assert!(breaker.admit_at(start).is_some(), "a success resets the count");
        fail(&breaker, start);
        assert_eq!(breaker.state_at(start), BreakerState::Open);
        assert!(breaker.admit_at(start + Duration::from_secs(29)).is_none());

        let later = start + Duration::from_secs(30);
        assert_eq!(breaker.state_at(later), BreakerState::HalfOpen);
        let probe = breaker.admit_at(later).expect("a probe");
        assert!(probe.is_probe());
        assert!(breaker.admit_at(later).is_none(), "only one probe at a time");
        probe.report(false, later);
        assert_eq!(
            breaker.state_at(later),
            BreakerState::Open,
            "a failed probe opens again"
        );

        let again = later + Duration::from_secs(30);
        breaker.admit_at(again).expect("a probe").report(true, again);
        assert_eq!(breaker.state_at(again), BreakerState::Closed);
        assert!(breaker.admit_at(again).is_some_and(|ticket| !ticket.is_probe()));
    }

    #[test]
    fn a_probe_dropped_without_a_result_lets_the_next_request_probe() {
        let breaker = Breaker::new(Duration::from_secs(30));
        let start = Instant::now();
        for _ in 0..THRESHOLD {
            fail(&breaker, start);
        }
        let later = start + Duration::from_secs(30);
        let probe = breaker.admit_at(later).expect("a probe");
        assert!(breaker.admit_at(later).is_none());
        // 控制面斷線，探測的 future 被丟棄
        drop(probe);
        let next = breaker.admit_at(later).expect("probing again");
        assert!(next.is_probe());
        next.report(true, later);
        assert_eq!(breaker.state_at(later), BreakerState::Closed);
    }

    #[test]
    fn a_late_result_from_before_the_opening_does_not_decide_the_probe() {
        let breaker = Breaker::new(Duration::from_secs(30));
        let start = Instant::now();
        // 斷開前就放行、結果晚到的請求
        let early = breaker.admit_at(start).expect("admitted");
        let late_success = breaker.admit_at(start).expect("admitted");
        for _ in 0..THRESHOLD {
            fail(&breaker, start);
        }
        let later = start + Duration::from_secs(30);
        let probe = breaker.admit_at(later).expect("a probe");
        early.report(false, later);
        late_success.report(true, later);
        assert_eq!(breaker.state_at(later), BreakerState::HalfOpen, "still probing");
        assert!(breaker.admit_at(later).is_none(), "no second probe");
        probe.report(true, later);
        assert_eq!(breaker.state_at(later), BreakerState::Closed);
    }
}
