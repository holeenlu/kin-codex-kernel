//! 在讀入本體前限制業務請求數，按實際收到的 KiB 共用一份本體預算。
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::response::Response;
use futures_util::StreamExt;
use http_body_util::BodyExt;
use kin_contract::config::IngressLimits;
use kin_contract::error::KernelError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
pub(crate) struct Admission {
    requests: Arc<Semaphore>,
    kib: Arc<Semaphore>,
    images: Arc<Semaphore>,
}

impl Admission {
    pub(crate) fn new(limits: IngressLimits) -> Self {
        Self {
            requests: Arc::new(Semaphore::new(usize::from(limits.requests.get()))),
            kib: Arc::new(Semaphore::new(usize::from(limits.body_mib.get()) * 1024)),
            images: Arc::new(Semaphore::new(usize::from(limits.images.get()))),
        }
    }

    pub(crate) fn enter(&self, image: bool) -> Result<RequestPermit, KernelError> {
        let acquire = |s: &Arc<Semaphore>| s.clone().try_acquire_owned().map_err(|_| KernelError::KernelBusy);
        Ok(RequestPermit {
            _image: image.then(|| acquire(&self.images)).transpose()?,
            _request: acquire(&self.requests)?,
            pool: self.kib.clone(),
            bytes: None,
        })
    }
}

pub(crate) struct RequestPermit {
    _image: Option<OwnedSemaphorePermit>,
    _request: OwnedSemaphorePermit,
    pool: Arc<Semaphore>,
    bytes: Option<OwnedSemaphorePermit>,
}

impl RequestPermit {
    pub(crate) fn reserve(&mut self, total: usize) -> Result<(), KernelError> {
        let held = self.bytes.as_ref().map_or(0, OwnedSemaphorePermit::num_permits);
        let extra = total.div_ceil(1024).saturating_sub(held);
        if extra != 0 {
            let count = u32::try_from(extra).map_err(|_| KernelError::KernelBusy)?;
            let permit = self
                .pool
                .clone()
                .try_acquire_many_owned(count)
                .map_err(|_| KernelError::KernelBusy)?;
            match &mut self.bytes {
                Some(held) => held.merge(permit),
                None => self.bytes = Some(permit),
            }
        }
        Ok(())
    }

    pub(crate) async fn read(&mut self, body: Body, limit: usize) -> Result<Bytes, KernelError> {
        let mut body = body.into_data_stream();
        let mut first: Option<Bytes> = None;
        let mut joined = Vec::new();
        let mut size = 0usize;
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|_| KernelError::BodyTooLarge)?;
            size = size
                .checked_add(chunk.len())
                .filter(|n| *n <= limit)
                .ok_or(KernelError::BodyTooLarge)?;
            self.reserve(size)?;
            if let Some(previous) = first.take() {
                joined.extend_from_slice(&previous[..]);
            }
            if joined.is_empty() {
                first = Some(chunk);
            } else {
                joined.extend_from_slice(&chunk);
            }
        }
        Ok(first.unwrap_or_else(|| Bytes::from(joined)))
    }

    /// 大型回應可能等待慢客戶端讀取；程序與圖片名額隨整個回應生命週期歸還。
    pub(crate) fn finish(mut self, response: Response) -> Response {
        self.bytes.take();
        response.map(|body| {
            Body::new(body.map_frame(move |frame| {
                let _keep = &self;
                frame
            }))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bytes_and_image_slots_follow_their_resource_lifetimes() {
        let limits = serde_json::from_str(r#"{"images":1,"requests":3,"body_mib":1}"#).unwrap();
        let admission = Admission::new(limits);
        let mut image = admission.enter(true).unwrap();
        image.reserve(1024 * 1024).unwrap();
        let mut text = admission.enter(false).unwrap();
        assert_eq!(text.reserve(1), Err(KernelError::KernelBusy));
        let response = image.finish(Response::new(Body::from("image")));
        text.reserve(1).unwrap();
        assert!(matches!(admission.enter(true), Err(KernelError::KernelBusy)));
        drop(response);
        assert!(admission.enter(true).is_ok());
        let chunks = [b"first".as_slice(), b"", b"second", b"third"];
        let stream =
            futures_util::stream::iter(chunks.map(|c| Ok::<_, std::io::Error>(Bytes::from_static(c))));
        assert_eq!(
            text.read(Body::from_stream(stream), 16).await.unwrap(),
            "firstsecondthird"
        );
    }
}
