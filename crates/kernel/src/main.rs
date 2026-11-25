//! kin-codex-kernel 執行檔。
//!
//! 這裡只做組裝：解析命令列、讀設定檔、初始化日誌與執行環境、把訊號轉成關閉訊號，
//! 再把設定與建置資訊注入 API 層。業務邏輯一律不放在這裡。

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::Context;
use kin_api::{AppState, BuildInfo};
use kin_contract::config::{SharedConfig, SingleAccountConfig};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

const BUILD: BuildInfo = BuildInfo {
    name: env!("CARGO_PKG_NAME"),
    version: env!("CARGO_PKG_VERSION"),
    build_sha: match option_env!("KIN_BUILD_SHA") {
        Some(sha) => sha,
        None => "dev",
    },
};

enum Command {
    /// 單帳號相容模式（ABI 1）：`<config.json>`。
    Run(PathBuf),
    /// 共享模式（ABI 2）：`--shared <shared.json>`。
    Shared(PathBuf),
    Version,
}

fn parse_args() -> Option<Command> {
    let mut args = std::env::args_os().skip(1);
    let Some(first) = args.next() else {
        let path = std::env::var_os("CODEX_KERNEL_CONFIG")
            .filter(|p| !p.is_empty())
            .map_or_else(|| PathBuf::from("/run/guest/codex-kernel.json"), PathBuf::from);
        return Some(Command::Run(path));
    };
    let second = args.next();
    if args.next().is_some() {
        return None;
    }
    match (first.to_str(), second) {
        (Some("--version"), None) => Some(Command::Version),
        (Some("--shared"), Some(path)) => Some(Command::Shared(PathBuf::from(path))),
        (Some(flag), None) if flag.starts_with('-') => None,
        (_, None) => Some(Command::Run(PathBuf::from(first))),
        _ => None,
    }
}

fn main() -> ExitCode {
    let Some(command) = parse_args() else {
        eprintln!(
            "usage: {} <config.json> | --shared <shared.json> | --version",
            BUILD.name
        );
        return ExitCode::from(2);
    };
    match command {
        Command::Version => {
            use std::io::Write;
            let line = format!("{} {} ({})", BUILD.name, BUILD.version, BUILD.build_sha);
            match writeln!(std::io::stdout().lock(), "{line}") {
                Ok(()) => ExitCode::SUCCESS,
                Err(_) => ExitCode::from(1),
            }
        }
        Command::Run(path) => exit_code(run(&path, single)),
        Command::Shared(path) => exit_code(run(&path, shared)),
    }
}

fn exit_code(result: anyhow::Result<()>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(error = format!("{err:#}"), "kernel stopped");
            ExitCode::from(1)
        }
    }
}

fn single(raw: &[u8]) -> anyhow::Result<AppState> {
    let config = SingleAccountConfig::from_slice(raw)?;
    AppState::new(config, BUILD).context("build upstream client")
}

fn shared(raw: &[u8]) -> anyhow::Result<AppState> {
    Ok(AppState::shared(SharedConfig::from_slice(raw)?, BUILD))
}

/// 日誌只寫 stderr（控制面把它導到每槽日誌檔），不記 token、cookie 或請求內容。
/// `KIN_LOG` 控制層級，`KIN_LOG_FORMAT=json` 輸出結構化日誌。
fn init_tracing() {
    let filter = EnvFilter::try_from_env("KIN_LOG")
        .or_else(|_| EnvFilter::try_from_env("TAP_LOG"))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false);
    if std::env::var("KIN_LOG_FORMAT")
        .or_else(|_| std::env::var("TAP_LOG_FORMAT"))
        .is_ok_and(|format| format == "json")
    {
        builder.json().init();
    } else {
        builder.init();
    }
}

fn run(path: &Path, state: fn(&[u8]) -> anyhow::Result<AppState>) -> anyhow::Result<()> {
    init_tracing();
    let raw = std::fs::read(path).with_context(|| format!("read config {}", path.display()))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("start tokio runtime")?;
    runtime.block_on(async {
        // 執行環境（reqwest、WebSocket）要在 tokio 裡建立。
        let state = state(&raw).with_context(|| format!("load config {}", path.display()))?;
        let shutdown = CancellationToken::new();
        tokio::spawn(forward_signals(shutdown.clone()));
        kin_api::serve(state, shutdown).await
    })
}

/// SIGTERM（控制面回收）與 SIGINT 都觸發優雅結束。
async fn forward_signals(shutdown: CancellationToken) {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(term) => term,
        Err(err) => {
            tracing::error!(error = %err, "cannot install SIGTERM handler; shutting down");
            shutdown.cancel();
            return;
        }
    };
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    tracing::info!("shutdown requested");
    shutdown.cancel();
}
