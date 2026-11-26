//! Unix socket 的綁定與服務生命週期。

use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind};
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::time::Duration;

use axum::Router;
use hyper::server::conn::http1;
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use socket2::{Domain, SockAddr, Socket, Type};
use tokio::net::UnixListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::routes::router;
use crate::state::AppState;

/// 鎖檔保留在目錄中，所有實例競爭同一個 inode；程序退出時核心會釋放檔案鎖。
fn locked_bind(path: &Path) -> std::io::Result<(UnixListener, File)> {
    if path.is_dir() {
        return Err(Error::new(ErrorKind::AlreadyExists, "socket_path is a directory"));
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".lock");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(lock_path)?;
    if !lock.metadata()?.is_file() {
        return Err(Error::new(ErrorKind::AlreadyExists, "invalid socket lock file"));
    }
    lock.try_lock().map_err(|err| match err {
        std::fs::TryLockError::WouldBlock => {
            Error::new(ErrorKind::AddrInUse, "kernel socket is owned by another process")
        }
        std::fs::TryLockError::Error(err) => err,
    })?;
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => {
            // 升級時可能遇到未使用鎖檔的舊版本，只清除明確拒絕連線的殘留 socket。
            let probe = Socket::new(Domain::UNIX, Type::STREAM, None)?;
            probe.set_nonblocking(true)?;
            match probe.connect(&SockAddr::unix(path)?) {
                Err(err) if err.kind() == ErrorKind::ConnectionRefused => std::fs::remove_file(path)?,
                Err(err) => return Err(err),
                Ok(_) => return Err(Error::new(ErrorKind::AddrInUse, "kernel socket is listening")),
            }
        }
        Ok(_) => {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                "socket_path is not a socket",
            ));
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    let listener = UnixListener::bind(path)?;
    if let Err(err) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
        std::fs::remove_file(path)?;
        return Err(err);
    }
    Ok((listener, lock))
}

/// 綁定獨立的 listener；完整服務使用相同的綁定流程並持鎖到排空結束。
pub fn bind(path: &Path) -> std::io::Result<UnixListener> {
    Ok(locked_bind(path)?.0)
}

/// 每條連線都由服務任務持有；排空期限涵蓋讀取中、處理中與被下游背壓卡住的回應。
async fn serve_connections(listener: UnixListener, app: Router, stop: CancellationToken, drain: Duration) {
    let mut connections = JoinSet::<()>::new();
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => break,
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if result.is_err() {
                    tracing::warn!("HTTP connection task stopped unexpectedly");
                }
            },
            accepted = listener.accept() => match accepted {
                Ok((io, _)) => {
                    let service = TowerToHyperService::new(app.clone());
                    let shutdown = stop.clone();
                    connections.spawn(async move {
                        let conn = http1::Builder::new().serve_connection(TokioIo::new(io), service);
                        tokio::pin!(conn);
                        tokio::select! {
                            _ = &mut conn => {}
                            () = shutdown.cancelled() => {
                                conn.as_mut().graceful_shutdown();
                                let _ = conn.await;
                            }
                        }
                    });
                }
                Err(err) => {
                    tracing::warn!(error = %err, "socket accept failed");
                    tokio::select! {
                        () = stop.cancelled() => break,
                        () = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
        }
    }
    drop(listener);
    if tokio::time::timeout(drain, async { while connections.join_next().await.is_some() {} })
        .await
        .is_err()
    {
        tracing::warn!(connections = connections.len(), "socket drain deadline reached");
        connections.shutdown().await;
    }
}

async fn maintain(state: AppState) {
    match state.mode() {
        crate::state::Mode::Shared { .. } => crate::credential::refresh_in_background(state).await,
        crate::state::Mode::Single { upstream, .. } => loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            upstream.reap_idle();
        },
    }
}

/// 在設定的 socket 上提供服務，直到 `shutdown` 被取消；結束時移除 socket 檔。
pub async fn serve(state: AppState, shutdown: CancellationToken) -> anyhow::Result<()> {
    let socket_path = state.socket_path().to_path_buf();
    let (listener, _ownership) = locked_bind(&socket_path)?;
    let identity = std::fs::symlink_metadata(&socket_path)?;
    tracing::info!(kernel = %state.label(), socket = %socket_path.display(), "listening");
    let connections = serve_connections(
        listener,
        router(state.clone()),
        shutdown.clone(),
        Duration::from_secs(30),
    );
    // 掃描與服務共用生命週期；已開始的換票由 RefreshInFlight 追蹤到寫回。
    tokio::join!(connections, shutdown.run_until_cancelled(maintain(state)));
    // token 端點可能已輪換 refresh token：等換票寫回再結束。
    crate::credential::wait_for_refreshes(std::time::Duration::from_secs(30)).await;
    // 不留下沒有人監聽的 socket 檔，否則控制面會連到一個死掉的端點。
    let cleanup = || -> std::io::Result<()> {
        let current = std::fs::symlink_metadata(&socket_path)?;
        if (identity.dev(), identity.ino()) == (current.dev(), current.ino())
            && current.file_type().is_socket()
        {
            std::fs::remove_file(&socket_path)?;
        }
        Ok(())
    };
    if let Err(err) = cleanup()
        && err.kind() != ErrorKind::NotFound
    {
        tracing::warn!(error = %err, "failed to remove socket");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{StreamExt, stream};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn dropping_serve_releases_the_maintenance_runtime_without_a_shutdown_signal() {
        use futures_util::FutureExt;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("test.sock");
        let config = serde_json::from_value(serde_json::json!({
            "socket_path": socket, "vm_id": "test", "device_id": "test",
            "credential_path": dir.path().join("credential.json"), "internal_token": "test"
        }))
        .unwrap();
        let state = AppState::new(
            config,
            crate::BuildInfo {
                name: "test",
                version: "test",
                build_sha: "test",
            },
        )
        .unwrap();
        let crate::state::Mode::Single { upstream, .. } = state.mode() else {
            unreachable!()
        };
        let runtime = Arc::downgrade(upstream);
        let stop = CancellationToken::new();
        let mut service = Box::pin(serve(state, stop.clone()));
        assert!(service.as_mut().now_or_never().is_none());
        assert!(socket.exists());
        drop(service);
        assert!(!stop.is_cancelled());
        assert!(runtime.upgrade().is_none(), "maintenance still owns the runtime");
    }

    #[tokio::test]
    async fn drain_ends_a_response_that_never_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sock");
        let listener = bind(&path).unwrap();
        let stop = CancellationToken::new();
        let app = Router::new().route(
            "/",
            axum::routing::get(|| async {
                let first = stream::once(async {
                    Ok::<_, Error>(axum::body::Bytes::from_static(b"data: hello\n\n"))
                });
                let stream = first.chain(stream::pending());
                axum::body::Body::from_stream(stream)
            }),
        );
        let grace = Duration::from_millis(25);
        let server = tokio::spawn(serve_connections(listener, app, stop.clone(), grace));
        let mut peer = tokio::net::UnixStream::connect(path).await.unwrap();
        peer.write_all(b"GET / HTTP/1.1\r\nhost: local\r\n\r\n")
            .await
            .unwrap();
        let mut prefix = [0; 12];
        tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut prefix))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&prefix, b"HTTP/1.1 200");
        let started = tokio::time::Instant::now();
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        assert!(started.elapsed() >= grace);
        let mut rest = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), peer.read_to_end(&mut rest))
            .await
            .unwrap()
            .unwrap();
    }
}
