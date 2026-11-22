//! Unix socket 的綁定與服務生命週期。

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use tokio::net::UnixListener;
use tokio_util::sync::CancellationToken;

use crate::routes::router;
use crate::state::AppState;

/// 綁定 Unix socket：先移除殘留的 socket 檔，綁定後把權限設為 0600。
/// 路徑是目錄時拒絕，不刪除任何目錄。
pub fn bind(socket_path: &Path) -> std::io::Result<UnixListener> {
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match std::fs::symlink_metadata(socket_path) {
        Ok(meta) if meta.file_type().is_dir() => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "socket_path is a directory",
            ));
        }
        Ok(_) => std::fs::remove_file(socket_path)?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    let listener = UnixListener::bind(socket_path)?;
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// 在設定的 socket 上提供服務，直到 `shutdown` 被取消；結束時移除 socket 檔。
pub async fn serve(state: AppState, shutdown: CancellationToken) -> anyhow::Result<()> {
    let socket_path = state.socket_path().to_path_buf();
    let listener = bind(&socket_path)?;
    tracing::info!(kernel = %state.label(), socket = %socket_path.display(), "listening");
    let maintenance = state.clone();
    let stop = shutdown.clone();
    let janitor = tokio::spawn(async move {
        match maintenance.mode() {
            crate::state::Mode::Shared { .. } => {
                crate::credential::refresh_in_background(maintenance, stop).await
            }
            crate::state::Mode::Single { upstream, .. } => loop {
                tokio::select! {
                    () = stop.cancelled() => break,
                    () = tokio::time::sleep(std::time::Duration::from_secs(30)) => upstream.reap_idle(),
                }
            },
        }
    });
    let result = axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await;
    janitor.abort();
    let _ = janitor.await;
    // token 端點可能已輪換 refresh token：等換票寫回再結束。
    crate::credential::wait_for_refreshes(std::time::Duration::from_secs(30)).await;
    // 不留下沒有人監聽的 socket 檔，否則控制面會連到一個死掉的端點。
    if let Err(err) = std::fs::remove_file(&socket_path)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(error = %err, "failed to remove socket");
    }
    result.map_err(Into::into)
}
