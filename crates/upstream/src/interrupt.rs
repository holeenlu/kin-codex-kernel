//! 每輪一條有界控制通道。寫入結果只表示已轉送，確認幀仍經原始推理串流交付。
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Debug)]
pub struct InterruptHandle(mpsc::Sender<Command>);

#[derive(Debug)]
pub(super) struct Command {
    pub(super) response_id: String,
    pub(super) written: oneshot::Sender<bool>,
}

pub(super) fn channel() -> (InterruptHandle, mpsc::Receiver<Command>) {
    let (tx, rx) = mpsc::channel(1);
    (InterruptHandle(tx), rx)
}

impl InterruptHandle {
    pub async fn forward(&self, response_id: String) -> bool {
        let (tx, rx) = oneshot::channel();
        if self
            .0
            .try_send(Command {
                response_id,
                written: tx,
            })
            .is_err()
        {
            return false;
        }
        rx.await.unwrap_or(false)
    }
}
