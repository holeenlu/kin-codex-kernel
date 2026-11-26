//! ABI 1 的本機傳輸。完整解碼並套用原大小上限後才可進入推理路徑。
use std::collections::VecDeque;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use flate2::bufread::GzDecoder;
use kin_contract::error::KernelError;
use kin_contract::headers::{UDS_BASE, UDS_COPY, UDS_MISS, UDS_RECEIVED, UDS_STORE, UPSTREAM_SENT};
use kin_contract::send::UpstreamSent;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

use crate::{AppState, error::ApiError, responses::respond};

const BUDGET: usize = 32 << 20;
const TTL: Duration = Duration::from_secs(300);
// 同時緩衝與還原的上限按槽套用。推理與串流不占用解碼名額。
const DECODE_SLOTS: usize = 4;

#[derive(Debug)]
pub(crate) struct Bases {
    entries: Mutex<VecDeque<(Instant, String, Bytes)>>,
    slots: Arc<Semaphore>,
}

impl Default for Bases {
    fn default() -> Self {
        Self {
            entries: Mutex::default(),
            slots: Arc::new(Semaphore::new(DECODE_SLOTS)),
        }
    }
}

impl Bases {
    fn get(&self, key: &str) -> Option<Bytes> {
        let mut entries = self.entries.lock().ok()?;
        entries.retain(|(at, _, _)| at.elapsed() < TTL);
        entries
            .iter()
            .find(|(_, id, _)| id == key)
            .map(|(_, _, b)| b.clone())
    }

    fn store(&self, raw: Bytes) -> Option<HeaderValue> {
        if raw.len() > BUDGET {
            return None;
        }
        let digest = Sha256::digest(&raw);
        let key = format!("{:x}", Bytes::copy_from_slice(&digest));
        let mut entries = self.entries.lock().ok()?;
        entries.retain(|(at, id, _)| at.elapsed() < TTL && id != &key);
        let mut size = raw.len() + entries.iter().map(|(_, _, v)| v.len()).sum::<usize>();
        while size > BUDGET || entries.len() >= 64 {
            size -= entries.pop_front()?.2.len();
        }
        let header = HeaderValue::from_str(&key).ok()?;
        entries.push_back((Instant::now(), key, raw));
        Some(header)
    }
}

fn invalid() -> KernelError {
    KernelError::InvalidBody("invalid packed request".to_owned())
}

fn expand(
    data: Bytes,
    gzip: bool,
    copy: Option<(Bytes, usize, usize, usize)>,
    limit: usize,
    mut reserve: impl FnMut(usize) -> Result<(), KernelError>,
) -> Result<Bytes, KernelError> {
    let copied = copy.as_ref().map_or(0, |(_, _, _, n)| *n);
    let remaining = limit.checked_sub(copied).ok_or(KernelError::BodyTooLarge)?;
    let literal = if gzip {
        let mut decoder = GzDecoder::new(&data[..]);
        let mut buf = Vec::new();
        let mut block = [0_u8; 16 * 1024];
        loop {
            let n = decoder.read(&mut block).map_err(|_| invalid())?;
            if n == 0 {
                break;
            }
            let size = buf
                .len()
                .checked_add(n)
                .filter(|n| *n <= remaining)
                .ok_or(KernelError::BodyTooLarge)?;
            reserve(data.len().saturating_add(size))?;
            buf.extend_from_slice(&block[..n]);
        }
        if !decoder.into_inner().is_empty() {
            return Err(invalid());
        }
        Bytes::from(buf)
    } else {
        data
    };
    if literal.len() > remaining {
        return Err(KernelError::BodyTooLarge);
    }
    let Some((base, offset, insert, count)) = copy else {
        return Ok(literal);
    };
    let source = base
        .get(offset..offset.checked_add(count).ok_or_else(invalid)?)
        .ok_or_else(invalid)?;
    if insert > literal.len() {
        return Err(invalid());
    }
    let output_size = literal.len() + count;
    reserve(literal.len().saturating_add(output_size))?;
    let mut raw = Vec::with_capacity(output_size);
    raw.extend_from_slice(&literal[..insert]);
    raw.extend_from_slice(source);
    raw.extend_from_slice(&literal[insert..]);
    Ok(raw.into())
}

pub(crate) async fn responses(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    let permit = Arc::clone(&state.packed().slots)
        .try_acquire_owned()
        .map_err(|_| KernelError::AccountBusy(state.label().to_owned()))?;
    let limit = state.body_limit();
    let mut admission = state.admission().enter(false)?;
    let data = admission.read(body, limit).await?;
    let received = data.len();
    let bases = state.packed();
    let mut response = match decode(bases, &headers, data, limit, permit, admission).await {
        Ok(Some((raw, admission))) => {
            let cache = (raw.len() <= BUDGET && headers.get(UDS_STORE).is_some_and(|v| v == "1"))
                .then(|| raw.clone());
            let response = respond(&state, raw).await.into_response();
            admission.finish(finish(bases, response, cache).await)
        }
        Ok(None) => (
            StatusCode::CONFLICT,
            [(UDS_MISS, "1"), (UPSTREAM_SENT, UpstreamSent::No.header_value())],
            axum::Json(serde_json::json!({"error": {"code": "local_base_unavailable", "message": "local base unavailable"}})),
        )
            .into_response(),
        Err(error) => ApiError(error).into_response(),
    };
    response
        .headers_mut()
        .insert(UDS_RECEIVED, HeaderValue::from(received as u64));
    Ok(response)
}

/// 只有合法的增量請求找不到基底時才回傳 None。
async fn decode(
    bases: &Bases,
    headers: &HeaderMap,
    data: Bytes,
    limit: usize,
    permit: tokio::sync::OwnedSemaphorePermit,
    mut admission: crate::admission::RequestPermit,
) -> Result<Option<(Bytes, crate::admission::RequestPermit)>, KernelError> {
    let gzip = match headers.get("content-encoding").map(HeaderValue::as_bytes) {
        None | Some(b"identity") => false,
        Some(b"gzip") => true,
        _ => return Err(invalid()),
    };
    let copy = match (headers.get(UDS_BASE), headers.get(UDS_COPY)) {
        (None, None) => None,
        (Some(key), Some(range)) => {
            let key = key.to_str().map_err(|_| invalid())?;
            if key.len() != 64
                || !key
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(invalid());
            }
            let mut parts = range.to_str().map_err(|_| invalid())?.split(',');
            let mut next = || {
                parts
                    .next()
                    .and_then(|v| v.parse::<usize>().ok())
                    .ok_or_else(invalid)
            };
            let (offset, insert, count) = (next()?, next()?, next()?);
            if parts.next().is_some() || count == 0 {
                return Err(invalid());
            }
            let Some(base) = bases.get(key) else {
                return Ok(None);
            };
            Some((base, offset, insert, count))
        }
        _ => return Err(invalid()),
    };
    let (raw, admission) = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        expand(data, gzip, copy, limit, |size| admission.reserve(size)).map(|raw| (raw, admission))
    })
    .await
    .map_err(|_| invalid())??;
    Ok(Some((raw, admission)))
}

async fn finish(bases: &Arc<Bases>, mut response: Response, cache: Option<Bytes>) -> Response {
    response.headers_mut().remove(UDS_MISS);
    response.headers_mut().remove(UDS_BASE);
    if response.status().is_success()
        && let Some(bytes) = cache
    {
        let bases = Arc::clone(bases);
        if let Ok(Some(key)) = tokio::task::spawn_blocking(move || bases.store(bytes)).await {
            response.headers_mut().insert(UDS_BASE, key);
        }
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compression, write::GzEncoder};
    use std::io::Write;

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut out = GzEncoder::new(Vec::new(), Compression::fast());
        out.write_all(data).unwrap();
        out.finish().unwrap()
    }

    #[test]
    fn gzip_reserves_actual_bytes_with_a_large_configured_limit() {
        let data = Bytes::from(gzip(b"small"));
        let needed = data.len() + 5;
        for budget in [needed - 1, needed] {
            let result = expand(data.clone(), true, None, usize::MAX, |bytes| {
                if bytes <= budget {
                    Ok(())
                } else {
                    Err(KernelError::KernelBusy)
                }
            });
            if budget == needed {
                assert_eq!(result.unwrap(), "small");
            } else {
                assert_eq!(result.unwrap_err(), KernelError::KernelBusy);
            }
        }
    }

    #[test]
    fn delta_restores_exact_bytes_and_bounds_the_full_envelope() {
        let base = Bytes::from_static(b"old[history]old");
        let copy = Some((base.clone(), 3, 4, 9));
        let literal = Bytes::from(gzip(b"headtail"));
        assert_eq!(
            expand(literal.clone(), true, copy.clone(), 17, |_| Ok(())).unwrap(),
            b"head[history]tail"[..]
        );
        assert!(matches!(
            expand(literal, true, copy, 16, |_| Ok(())),
            Err(KernelError::BodyTooLarge)
        ));
        assert!(
            expand(
                Bytes::new(),
                false,
                Some((base, usize::MAX, 0, 1)),
                99,
                |_| Ok(())
            )
            .is_err()
        );
    }

    #[test]
    fn corrupt_or_trailing_gzip_bytes_are_rejected_before_dispatch() {
        let mut data = gzip(b"{}");
        data.push(0);
        assert!(expand(data.into(), true, None, 16, |_| Ok(())).is_err());
        let mut data = gzip(b"{}");
        data.pop();
        assert!(expand(data.into(), true, None, 16, |_| Ok(())).is_err());
    }

    #[tokio::test]
    async fn only_successful_requested_stores_publish_a_local_base() {
        let bases = Arc::new(Bases::default());
        let bytes = Bytes::from_static(b"requested base");
        for (status, cache) in [
            (StatusCode::BAD_GATEWAY, Some(bytes.clone())),
            (StatusCode::OK, None),
            (StatusCode::OK, Some(bytes.clone())),
        ] {
            let upstream = (status, [(UDS_MISS, "1"), (UDS_BASE, "upstream")]).into_response();
            let store = status.is_success() && cache.is_some();
            let response = finish(&bases, upstream, cache).await;
            assert_eq!(response.status(), status);
            assert!(!response.headers().contains_key(UDS_MISS));
            if store {
                let key = response.headers()[UDS_BASE].to_str().unwrap();
                assert_eq!(bases.get(key), Some(bytes.clone()));
            } else {
                assert!(!response.headers().contains_key(UDS_BASE));
                assert!(bases.entries.lock().unwrap().is_empty());
            }
        }
    }

    #[test]
    fn eviction_cannot_resolve_an_old_key_to_different_bytes() {
        let bases = Bases::default();
        let first = bases.store(Bytes::from_static(b"one")).unwrap();
        assert_eq!(bases.get(first.to_str().unwrap()).unwrap(), b"one"[..]);
        for n in 0..64 {
            bases.store(Bytes::from(vec![n]));
        }
        assert!(bases.get(first.to_str().unwrap()).is_none());
    }
}
