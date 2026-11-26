#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

/// 測試用的執行環境：記下用哪份環境建立，方便驗證是否沿用。
#[derive(Debug)]
struct Fake {
    proxy: String,
}

fn env(account_ref: &str, generation: u64, expected: u64) -> AccountEnv {
    serde_json::from_value(serde_json::json!({
        "ref": account_ref,
        "env_generation": generation,
        "expected_env_generation": expected,
        "credential_path": format!("/data/vms/{account_ref}/codex-credentials.json"),
        "device_id": format!("device-{account_ref}"),
        "max_concurrency": 2,
    }))
    .unwrap()
}

fn with(mut env: AccountEnv, change: impl FnOnce(&mut AccountEnv)) -> AccountEnv {
    change(&mut env);
    env
}

fn build(env: &AccountEnv) -> Result<Fake, String> {
    Ok(Fake {
        proxy: env.proxy_url.clone(),
    })
}

fn ready(accounts: Vec<AccountEnv>) -> Registry<Fake> {
    let registry = Registry::new(16);
    registry.reconcile(accounts, build).unwrap();
    registry
}

fn binding(account_ref: &str, generation: u64) -> AccountBinding {
    AccountBinding {
        account_ref: account_ref.to_owned(),
        env_generation: generation,
    }
}

fn code<T: std::fmt::Debug>(result: Result<T, KernelError>) -> &'static str {
    result.unwrap_err().code()
}

#[test]
fn nothing_is_served_or_changed_before_the_first_reconcile() {
    let registry: Registry<Fake> = Registry::new(16);
    assert_eq!(registry.state(), RegistryState::Empty);
    assert_eq!(code(registry.lease(&binding("a", 1))), "registry_not_ready");
    assert_eq!(
        code(registry.put("a", env("a", 1, 0), build)),
        "registry_not_ready"
    );
    assert_eq!(code(registry.delete("a", 1, false)), "registry_not_ready");
    assert_eq!(registry.list().registry_state, RegistryState::Empty);

    let ack = registry.reconcile(vec![env("a", 1, 0)], build).unwrap();
    assert_eq!((ack.accounts, ack.registry_state), (1, RegistryState::Ready));
    assert!(registry.lease(&binding("a", 1)).is_ok());
}

#[test]
fn put_follows_the_generation_table() {
    let registry = ready(vec![]);
    assert_eq!(
        registry.put("a", env("a", 3, 0), build).unwrap(),
        PutOutcome::Created
    );
    // 同代同內容：冪等（ACK 遺失時重送），CAS 前提不同也一樣
    assert_eq!(
        registry.put("a", env("a", 3, 0), build).unwrap(),
        PutOutcome::Unchanged
    );
    assert_eq!(
        registry.put("a", env("a", 3, 2), build).unwrap(),
        PutOutcome::Unchanged
    );
    // 同代不同內容
    let changed = with(env("a", 3, 0), |e| e.max_concurrency = 9);
    assert_eq!(code(registry.put("a", changed, build)), "generation_conflict");
    // 新代次但 CAS 前提不對
    assert_eq!(
        code(registry.put("a", env("a", 4, 2), build)),
        "generation_conflict"
    );
    // 新代次且前提對
    assert_eq!(
        registry.put("a", env("a", 4, 3), build).unwrap(),
        PutOutcome::Updated
    );
    // 舊代次
    assert_eq!(code(registry.put("a", env("a", 3, 2), build)), "stale_generation");
    assert_eq!(registry.list().accounts[0].env_generation, 4);
}

#[test]
fn put_validates_the_request() {
    let registry = ready(vec![]);
    assert_eq!(code(registry.put("b", env("a", 1, 0), build)), "invalid_request");
    assert_eq!(
        code(registry.put("../x", env("../x", 1, 0), build)),
        "invalid_request"
    );
    assert_eq!(code(registry.put("a", env("a", 0, 0), build)), "invalid_request");
    let no_limit = with(env("a", 1, 0), |e| e.max_concurrency = 0);
    assert_eq!(code(registry.put("a", no_limit, build)), "invalid_request");
    let proxy_missing = with(env("a", 1, 0), |e| e.proxy_required = true);
    assert_eq!(code(registry.put("a", proxy_missing, build)), "invalid_request");
    let failing = |_: &AccountEnv| -> Result<Fake, String> { Err("bad proxy url".to_owned()) };
    let err = registry.put("a", env("a", 1, 0), failing).unwrap_err();
    assert_eq!(
        (err.code(), err.to_string().contains("bad proxy url")),
        ("invalid_request", true)
    );
    assert!(
        registry.list().accounts.is_empty(),
        "nothing registered after a rejected PUT"
    );

    let small: Registry<Fake> = Registry::new(1);
    small.reconcile(vec![env("a", 1, 0)], build).unwrap();
    assert_eq!(code(small.put("b", env("b", 1, 0), build)), "invalid_request");
}

#[test]
fn put_rechecks_the_tombstone_after_building_without_the_registry_lock() {
    let registry = ready(vec![env("a", 1, 0), env("b", 1, 0)]);
    let update = with(env("a", 2, 1), |e| e.device_id = "new-device".into());
    let result = registry.put("a", update, |requested| {
        assert!(
            registry.inner.try_read().is_ok(),
            "builder holds the registry write lock"
        );
        let _other = registry.lease(&binding("b", 1)).unwrap();
        registry.delete("a", 2, false).unwrap();
        build(requested)
    });
    assert_eq!(code(result), "stale_generation");
    assert_eq!(code(registry.view("a")), "account_unknown");
}

#[test]
fn concurrent_creates_recheck_idempotency_capacity_and_credential_ownership() {
    let registry: Registry<Fake> = Registry::new(1);
    registry.reconcile(vec![], build).unwrap();
    let result = registry.put("a", env("a", 1, 0), |requested| {
        assert!(registry.inner.try_write().is_ok());
        registry.put("a", requested.clone(), build).unwrap();
        build(requested)
    });
    assert_eq!(result.unwrap(), PutOutcome::Unchanged);

    for same_path in [false, true] {
        let registry: Registry<Fake> = Registry::new(if same_path { 2 } else { 1 });
        registry.reconcile(vec![], build).unwrap();
        let result = registry.put("a", env("a", 1, 0), |requested| {
            assert!(registry.inner.try_write().is_ok());
            let mut other = env("b", 1, 0);
            if same_path {
                other.credential_path.clone_from(&requested.credential_path);
            }
            registry.put("b", other, build).unwrap();
            build(requested)
        });
        assert_eq!(code(result), "invalid_request");
        assert_eq!(code(registry.view("a")), "account_unknown");
        assert!(registry.view("b").is_ok());
    }
}

#[test]
fn delete_leaves_a_tombstone_that_late_commands_cannot_cross() {
    let registry = ready(vec![env("a", 5, 0)]);
    assert_eq!(code(registry.delete("a", 4, false)), "stale_generation");
    let ack = registry.delete("a", 5, false).unwrap();
    assert_eq!((ack.tombstone_generation, ack.inflight), (5, 0));
    // 同一個 DELETE 重送：冪等
    assert_eq!(registry.delete("a", 5, false).unwrap().tombstone_generation, 5);
    assert_eq!(code(registry.delete("a", 4, false)), "stale_generation");
    assert_eq!(code(registry.lease(&binding("a", 5))), "account_unknown");
    // 晚到的舊 PUT 不會把帳號建回來
    assert_eq!(code(registry.put("a", env("a", 5, 0), build)), "stale_generation");
    assert_eq!(code(registry.put("a", env("a", 3, 0), build)), "stale_generation");
    assert_eq!(
        registry.put("a", env("a", 6, 0), build).unwrap(),
        PutOutcome::Created
    );

    // 刪除不存在的帳號也留下墓碑
    assert_eq!(registry.delete("b", 2, false).unwrap().tombstone_generation, 2);
    assert_eq!(code(registry.put("b", env("b", 2, 0), build)), "stale_generation");
}

#[test]
fn deleting_an_account_with_requests_in_flight_drains_it() {
    let registry = ready(vec![env("a", 1, 0)]);
    let lease = registry.lease(&binding("a", 1)).unwrap();
    let token = lease.cancel_token();

    let ack = registry.delete("a", 1, false).unwrap();
    assert_eq!(ack.inflight, 1);
    assert_eq!(code(registry.lease(&binding("a", 1))), "account_disabled");
    assert_eq!(registry.list().accounts[0].state, AccountState::Draining);
    assert!(!token.is_cancelled(), "a plain DELETE does not cut the request");

    drop(lease);
    assert_eq!(code(registry.lease(&binding("a", 1))), "account_unknown");
    assert!(registry.list().accounts.is_empty());
}

#[test]
fn a_forced_delete_cancels_the_requests_in_flight() {
    let registry = ready(vec![env("a", 1, 0)]);
    let lease = registry.lease(&binding("a", 1)).unwrap();
    let token = lease.cancel_token();
    registry.delete("a", 1, true).unwrap();
    assert!(token.is_cancelled());
}

#[test]
fn the_concurrency_limit_is_per_account_and_follows_updates() {
    let one = |r: &str, g, x| with(env(r, g, x), |e| e.max_concurrency = 1);
    let registry = ready(vec![one("a", 1, 0), one("b", 1, 0)]);
    let first = registry.lease(&binding("a", 1)).unwrap();
    assert_eq!(code(registry.lease(&binding("a", 1))), "account_busy");
    assert!(
        registry.lease(&binding("b", 1)).is_ok(),
        "another account is not affected"
    );
    drop(first);
    let again = registry.lease(&binding("a", 1)).unwrap();

    // 上限提高後立即生效，在途的名額照算
    let two = with(env("a", 2, 1), |e| e.max_concurrency = 2);
    registry.put("a", two, build).unwrap();
    let _second = registry.lease(&binding("a", 2)).unwrap();
    assert_eq!(code(registry.lease(&binding("a", 2))), "account_busy");
    drop(again);
    assert!(registry.lease(&binding("a", 2)).is_ok());
}

#[test]
fn requests_must_carry_the_registered_generation_and_an_enabled_account() {
    let registry = ready(vec![env("a", 3, 0), with(env("b", 1, 0), |e| e.enabled = false)]);
    let err = registry.lease(&binding("a", 2)).unwrap_err();
    assert_eq!(err.code(), "account_generation_mismatch");
    assert_eq!(err.upstream_sent(), kin_contract::send::UpstreamSent::No);
    assert_eq!(code(registry.lease(&binding("b", 1))), "account_disabled");
    assert_eq!(code(registry.lease(&binding("c", 1))), "account_unknown");
    assert_eq!(
        registry.counts(),
        AccountCounts {
            total: 2,
            ready: 1,
            failing: 0
        }
    );
}

#[test]
fn updates_keep_the_runtime_unless_the_exit_or_identity_changes() {
    let registry = ready(vec![env("a", 1, 0)]);
    let before = registry.lease(&binding("a", 1)).unwrap();

    registry
        .put("a", with(env("a", 2, 1), |e| e.max_concurrency = 5), build)
        .unwrap();
    let same = registry.lease(&binding("a", 2)).unwrap();
    assert!(
        Arc::ptr_eq(&before.runtime, &same.runtime),
        "a limit change keeps the connection pool"
    );

    // 兩個在途請求仍占著名額（跨更新照算），所以上限維持 5
    let moved = with(env("a", 3, 2), |e| {
        e.max_concurrency = 5;
        e.proxy_url = "socks5h://p2:1080".to_owned();
    });
    registry.put("a", moved, build).unwrap();
    let after = registry.lease(&binding("a", 3)).unwrap();
    assert!(!Arc::ptr_eq(&before.runtime, &after.runtime));
    assert_eq!(after.runtime.proxy, "socks5h://p2:1080");
    // 在途請求仍持有舊的執行環境，用完才回收
    assert_eq!(before.runtime.proxy, "");

    // 執行環境帶著分析事件的預設 telemetry_runtime：改了就重建，不沿用舊值。
    let runtime = kin_contract::config::TelemetryRuntime {
        runtime_os: "linux".to_owned(),
        runtime_os_version: "24.04".to_owned(),
        runtime_arch: "x86_64".to_owned(),
    };
    let upgraded = with(env("a", 4, 3), |e| {
        e.max_concurrency = 5;
        e.proxy_url = "socks5h://p2:1080".to_owned();
        e.telemetry_runtime = Some(runtime);
    });
    registry.put("a", upgraded, build).unwrap();
    let rebuilt = registry.lease(&binding("a", 4)).unwrap();
    assert!(!Arc::ptr_eq(&after.runtime, &rebuilt.runtime));
}

#[test]
fn reconcile_replaces_the_registry_and_reuses_unchanged_runtimes() {
    let builds = AtomicUsize::new(0);
    let counting = |env: &AccountEnv| {
        builds.fetch_add(1, Ordering::Relaxed);
        build(env)
    };
    let registry: Registry<Fake> = Registry::new(16);
    registry
        .reconcile(vec![env("a", 1, 0), env("b", 1, 0)], counting)
        .unwrap();
    assert_eq!(builds.load(Ordering::Relaxed), 2);
    let a_before = registry.lease(&binding("a", 1)).unwrap();
    registry.delete("c", 7, false).unwrap();

    // a 不變、b 移除、c 以控制面清單為準重新建立（清掉自己的墓碑）
    registry
        .reconcile(vec![env("a", 1, 0), env("c", 1, 0)], counting)
        .unwrap();
    assert_eq!(builds.load(Ordering::Relaxed), 3, "only c was built");
    let a_after = registry.lease(&binding("a", 1)).unwrap();
    assert!(Arc::ptr_eq(&a_before.runtime, &a_after.runtime));
    assert_eq!(code(registry.lease(&binding("b", 1))), "account_unknown");
    assert!(registry.lease(&binding("c", 1)).is_ok());
    assert_eq!(code(registry.put("b", env("b", 1, 0), build)), "stale_generation");
    let refs: Vec<_> = registry
        .list()
        .accounts
        .into_iter()
        .map(|a| a.account_ref)
        .collect();
    assert_eq!(refs, ["a", "c"]);
}

#[test]
fn a_rejected_reconcile_changes_nothing() {
    let registry = ready(vec![env("a", 1, 0)]);
    assert_eq!(
        code(registry.reconcile(vec![env("x", 1, 0), env("x", 2, 0)], build)),
        "invalid_request"
    );
    let failing = |env: &AccountEnv| -> Result<Fake, String> {
        if env.account_ref == "y" {
            Err("bad".to_owned())
        } else {
            build(env)
        }
    };
    assert_eq!(
        code(registry.reconcile(vec![env("x", 1, 0), env("y", 1, 0)], failing)),
        "invalid_request"
    );
    assert_eq!(registry.state(), RegistryState::Ready);
    let refs: Vec<_> = registry
        .list()
        .accounts
        .into_iter()
        .map(|a| a.account_ref)
        .collect();
    assert_eq!(refs, ["a"]);
    assert!(registry.lease(&binding("a", 1)).is_ok());

    let empty: Registry<Fake> = Registry::new(16);
    assert!(empty.reconcile(vec![env("y", 1, 0)], failing).is_err());
    assert_eq!(
        empty.state(),
        RegistryState::Empty,
        "a failed first reconcile stays empty"
    );
}

#[test]
fn errors_are_kept_for_the_account_health() {
    let registry = ready(vec![env("a", 1, 0)]);
    let lease = registry.lease(&binding("a", 1)).unwrap();
    lease.note_error(&KernelError::NoCredential);
    let view = registry.view("a").unwrap();
    assert_eq!((view.inflight, view.state), (1, AccountState::Ready));
    assert_eq!(view.last_error.map(|e| e.code), Some("no_credential"));
    assert_eq!(code(registry.view("zzz")), "account_unknown");
    assert_eq!(registry.max_request_bytes(), DEFAULT_MAX_REQUEST_BYTES);
}

#[test]
fn one_credential_file_belongs_to_one_account() {
    let registry = ready(vec![env("a", 1, 0)]);
    let shared_path = |account: AccountEnv| {
        with(account, |e| {
            e.credential_path = "/data/vms/a/codex-credentials.json".into();
        })
    };
    // 兩個帳號共用一個憑據檔就有兩個換票者
    assert_eq!(
        code(registry.put("b", shared_path(env("b", 1, 0)), build)),
        "invalid_request"
    );
    assert_eq!(
        code(registry.reconcile(vec![env("a", 1, 0), shared_path(env("b", 1, 0))], build)),
        "invalid_request"
    );
    assert!(
        registry.lease(&binding("a", 1)).is_ok(),
        "a rejected reconcile keeps the registry"
    );
    // 刪掉原帳號後，別的帳號才能接手這個檔案
    registry.delete("a", 2, false).unwrap();
    assert!(registry.put("b", shared_path(env("b", 1, 0)), build).is_ok());
}

#[test]
fn a_new_credential_file_gets_new_per_account_state() {
    let made = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&made);
    let registry: Registry<Fake, usize> =
        Registry::with_ext(16, move |_| counter.fetch_add(1, Ordering::Relaxed));
    registry.reconcile(vec![env("a", 1, 0)], build).unwrap();
    let first = *registry.view("a").unwrap().ext();

    registry
        .put("a", with(env("a", 2, 1), |e| e.max_concurrency = 5), build)
        .unwrap();
    assert_eq!(
        *registry.view("a").unwrap().ext(),
        first,
        "same file, same refresh state"
    );
    registry
        .reconcile(vec![with(env("a", 2, 1), |e| e.max_concurrency = 5)], build)
        .unwrap();
    assert_eq!(*registry.view("a").unwrap().ext(), first);

    let moved = |generation, expected| {
        with(env("a", generation, expected), |e| {
            e.credential_path = "/data/vms/a/codex-credentials.shared.json".into();
        })
    };
    registry.put("a", moved(3, 2), build).unwrap();
    let second = *registry.view("a").unwrap().ext();
    assert_ne!(second, first, "a reauth mark on the old file does not carry over");
    registry
        .reconcile(vec![with(env("a", 4, 0), |e| e.max_concurrency = 5)], build)
        .unwrap();
    assert_ne!(
        *registry.view("a").unwrap().ext(),
        second,
        "reconcile follows the file too"
    );
    assert_eq!(made.load(Ordering::Relaxed), 3);
}

#[test]
fn control_plane_commands_wait_for_the_registry_and_respect_disabling() {
    let empty: Registry<Fake> = Registry::new(16);
    assert_eq!(code(empty.managed("a", false)), "registry_not_ready");

    let registry = ready(vec![env("a", 1, 0), with(env("b", 1, 0), |e| e.enabled = false)]);
    assert!(registry.managed("a", true).is_ok());
    // 停用的帳號可以匯入憑據，但核心不再自己換票
    assert!(registry.managed("b", false).is_ok());
    assert_eq!(code(registry.managed("b", true)), "account_disabled");
    assert_eq!(code(registry.managed("c", false)), "account_unknown");

    let lease = registry.lease(&binding("a", 1)).unwrap();
    let account_token = registry.view("a").unwrap().cancel_token();
    registry.delete("a", 2, false).unwrap();
    assert_eq!(code(registry.managed("a", true)), "account_disabled", "draining");
    assert_eq!(code(registry.managed("a", false)), "account_unknown");
    drop(lease);
    assert_eq!(code(registry.managed("a", true)), "account_unknown");
    assert!(!account_token.is_cancelled());
}
