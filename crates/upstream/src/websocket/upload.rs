//! response.create 上傳期間的雙向 I/O。

use std::collections::VecDeque;
use std::io;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{FutureExt, SinkExt, StreamExt};
use tungstenite::Message;

use super::{Conn, Next, event_type, next_frame, read_with_idle};
use crate::sse::TERMINAL;

/// 一個 create 只有一次送出回執；大文字的切點是位元組，UTF-8 字元可跨幀。
fn create_frames(text: String) -> (Message, impl Iterator<Item = Message>) {
    use tungstenite::protocol::frame::{Fragment, Frame, coding::Data};
    const LIMIT: usize = 1024 * 1024;
    let (first, mut rest) = if text.len() <= LIMIT {
        (Message::Text(text.into()), Bytes::new())
    } else {
        let mut bytes = Bytes::from(text);
        let first = Frame::message_fragment(bytes.split_to(LIMIT), Data::Text, Fragment::More);
        (Message::Frame(first), bytes)
    };
    let continuations = std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        let part = rest.split_to(rest.len().min(LIMIT));
        let fragment = if rest.is_empty() {
            Fragment::Final
        } else {
            Fragment::More
        };
        Some(Message::Frame(Frame::message_fragment(
            part,
            Data::Continue,
            fragment,
        )))
    });
    (first, continuations)
}

/// 保存上傳時已到達的事件。首個文字之後的 Close 永遠排在該文字之後。
fn buffer_upload_frame(frame: Next, buffered: &mut VecDeque<Next>, budget: &mut usize) -> bool {
    match frame {
        Next::Text(text) => {
            *budget = budget.saturating_sub(text.len());
            let terminal = event_type(&text).is_some_and(|kind| TERMINAL.contains(&kind.as_str()));
            buffered.push_back(Next::Text(text));
            terminal
        }
        Next::Oversized => {
            buffered.push_back(Next::Oversized);
            true
        }
        Next::Closed | Next::Broken => {
            buffered.push_back(Next::Broken);
            true
        }
    }
}

/// 寫出首幀後同時讀取上游。未完整寫出的連線不再入池，已讀文字優先交給此輪。
pub(super) async fn send_create(
    conn: &mut Conn,
    text: String,
    buffered: &mut VecDeque<Next>,
    idle: Option<Duration>,
) -> Result<bool, tungstenite::Error> {
    let (first, frames) = create_frames(text);
    // 在 start_send 前發生的 Close 不能當成本次上傳收到的 1009。
    futures_util::future::poll_fn(|cx| conn.poll_ready_unpin(cx)).await?;
    let started = conn.start_send_unpin(first);
    let (mut writer, mut reader) = conn.split();
    let mut budget = 1024 * 1024usize;
    let error = 'upload: {
        match started {
            Ok(()) => {}
            Err(err @ tungstenite::Error::Io(_)) => break 'upload err,
            Err(err) => return Err(err),
        }
        // 首幀已交給 sink。每次只驅動一個分片，等待可寫時同時處理對端的控制幀。
        for frame in std::iter::once(None).chain(frames.map(Some)) {
            // 即使每個分片都可立即寫出，也要在送下一幀前讀取已到達的回覆。
            if frame.is_some()
                && budget > 0
                && buffered.len() < 32
                && let Some(frame) = next_frame(&mut reader).now_or_never()
                && buffer_upload_frame(frame, buffered, &mut budget)
            {
                return Ok(false);
            }
            let write = async {
                match frame {
                    Some(frame) => writer.send(frame).await,
                    None => writer.flush().await,
                }
            };
            tokio::pin!(write);
            loop {
                let step = async {
                    tokio::select! {
                        biased;
                        result = &mut write => result.map(|()| None),
                        frame = next_frame(&mut reader), if budget > 0 && buffered.len() < 32 => Ok(Some(frame)),
                    }
                };
                match read_with_idle(idle, step).await {
                    Ok(Ok(None)) => break,
                    Ok(Ok(Some(frame))) => {
                        if buffer_upload_frame(frame, buffered, &mut budget) {
                            return Ok(false);
                        }
                    }
                    Ok(Err(err)) => break 'upload err,
                    Err(()) => {
                        break 'upload tungstenite::Error::Io(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "websocket upload timed out",
                        ));
                    }
                }
            }
        }
        return Ok(true);
    };
    // 寫入錯誤可能與 peer 的回覆同時就緒。只取已到達的訊息，不等待推測的 Close。
    while budget > 0 && buffered.len() < 32 {
        let Some(frame) = next_frame(&mut reader).now_or_never() else {
            break;
        };
        if matches!(frame, Next::Broken | Next::Closed) {
            break;
        }
        if buffer_upload_frame(frame, buffered, &mut budget) {
            return Ok(false);
        }
    }
    if buffered.is_empty() {
        Err(error)
    } else {
        buffered.push_back(Next::Broken);
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::websocket::Io;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    #[tokio::test]
    async fn partial_upload_reads_early_replies_in_order() {
        use tokio::io::AsyncReadExt;
        use tungstenite::protocol::{CloseFrame, Role, frame::coding::CloseCode};

        for (metadata, close) in [(false, true), (true, true), (false, false)] {
            // 小容量的雙向管道保證上游送出 Close 或斷線時，首幀仍未寫完。
            let (client, mut peer) = tokio::io::duplex(512);
            let io: Box<dyn Io> = Box::new(client);
            let mut client =
                WebSocketStream::from_raw_socket(MaybeTlsStream::Plain(io), Role::Client, None).await;
            let server = tokio::spawn(async move {
                assert_eq!(peer.read_u8().await.unwrap(), 0x01);
                if close {
                    let mut peer = WebSocketStream::from_raw_socket(peer, Role::Server, None).await;
                    if metadata {
                        peer.send(Message::text(r#"{"type":"response.created"}"#))
                            .await
                            .unwrap();
                    }
                    peer.close(Some(CloseFrame {
                        code: CloseCode::Size,
                        reason: "limit".into(),
                    }))
                    .await
                    .unwrap();
                }
            });
            let mut received = VecDeque::new();
            let result = send_create(
                &mut client,
                "a".repeat(1024 * 1024 + 1),
                &mut received,
                Some(Duration::from_secs(2)),
            )
            .await;
            server.await.unwrap();
            if metadata {
                assert!(
                    matches!(received.pop_front(), Some(Next::Text(text)) if text.contains("response.created"))
                );
            }
            if close {
                assert!(!result.unwrap());
                assert!(matches!(received.pop_front(), Some(Next::Oversized)));
            } else {
                assert!(result.is_err() || matches!(received.pop_front(), Some(Next::Broken)));
            }
            assert!(received.is_empty());
        }
    }

    #[tokio::test]
    async fn upload_answers_ping_before_the_last_continuation() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tungstenite::protocol::Role;

        let (client, mut peer) = tokio::io::duplex(2048);
        let io: Box<dyn Io> = Box::new(client);
        let mut client =
            WebSocketStream::from_raw_socket(MaybeTlsStream::Plain(io), Role::Client, None).await;
        let server = tokio::spawn(async move {
            let mut pong = false;
            loop {
                let opcode = peer.read_u8().await.unwrap();
                let size = peer.read_u8().await.unwrap();
                assert_ne!(size & 0x80, 0);
                let len = match size & 0x7f {
                    126 => u64::from(peer.read_u16().await.unwrap()),
                    127 => peer.read_u64().await.unwrap(),
                    len => u64::from(len),
                };
                if opcode == 0x01 {
                    peer.write_all(&[0x89, 0x01, b'p']).await.unwrap();
                }
                let mut mask = [0u8; 4];
                peer.read_exact(&mut mask).await.unwrap();
                if opcode == 0x8a {
                    assert_eq!(len, 1);
                    assert_eq!(peer.read_u8().await.unwrap() ^ mask[0], b'p');
                    pong = true;
                } else {
                    assert_eq!(
                        tokio::io::copy(&mut (&mut peer).take(len), &mut tokio::io::sink())
                            .await
                            .unwrap(),
                        len
                    );
                }
                if opcode == 0x80 {
                    assert!(pong);
                    return;
                }
            }
        });
        let mut buffered = VecDeque::new();
        assert!(
            send_create(
                &mut client,
                "a".repeat(3 * 1024 * 1024 + 1),
                &mut buffered,
                Some(Duration::from_secs(4))
            )
            .await
            .unwrap()
        );
        server.await.unwrap();
        assert!(buffered.is_empty());
    }
}
