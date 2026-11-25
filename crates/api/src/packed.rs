//! ABI 1 的本機傳輸。完整解碼並套用原大小上限後才可進入推理路徑。
use std::collections::VecDeque;
use std::io::Read;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes, to_bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use flate2::bufread::GzDecoder;
use kin_contract::error::KernelError;
use sha2::{Digest, Sha256};

use crate::{AppState, error::ApiError, routes};

const BUDGET: usize = 32 << 20;
const TTL: Duration = Duration::from_secs(300);
const BASE: &str = "x-kin-uds-base";
const MISS: &str = "x-kin-uds-miss";

#[derive(Debug, Default)]
pub(crate) struct Bases(Mutex<VecDeque<(Instant, String, Bytes)>>);

impl Bases {
    fn get(&self, key: &str) -> Option<Bytes> {
        let mut entries = self.0.lock().ok()?;
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
        let mut entries = self.0.lock().ok()?;
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
) -> Result<Bytes, KernelError> {
    let copied = copy.as_ref().map_or(0, |(_, _, _, n)| *n);
    let remaining = limit.checked_sub(copied).ok_or(KernelError::BodyTooLarge)?;
    let literal = if gzip {
        let mut decoder = GzDecoder::new(&data[..]);
        let mut buf = Vec::new();
        decoder
            .by_ref()
            .take(remaining as u64 + 1)
            .read_to_end(&mut buf)
            .map_err(|_| invalid())?;
        if buf.len() > remaining {
            return Err(KernelError::BodyTooLarge);
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
    let mut raw = Vec::with_capacity(literal.len() + count);
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
    let limit = state.body_limit();
    let data = to_bytes(body, limit)
        .await
        .map_err(|_| KernelError::BodyTooLarge)?;
    let gzip = match headers.get("content-encoding").map(HeaderValue::as_bytes) {
        None | Some(b"identity") => false,
        Some(b"gzip") => true,
        _ => return Err(invalid().into()),
    };
    let copy = match (headers.get(BASE), headers.get("x-kin-uds-copy")) {
        (None, None) => None,
        (Some(key), Some(range)) => {
            let key = key.to_str().map_err(|_| invalid())?;
            let mut parts = range.to_str().map_err(|_| invalid())?.split(',');
            let mut next = || {
                parts
                    .next()
                    .and_then(|v| v.parse::<usize>().ok())
                    .ok_or_else(invalid)
            };
            let (offset, insert, count) = (next()?, next()?, next()?);
            if parts.next().is_some() || count == 0 {
                return Err(invalid().into());
            }
            let Some(base) = state.packed().get(key) else {
                return Ok((
                    StatusCode::CONFLICT,
                    [(MISS, "1"), ("x-kin-upstream-sent", "0")],
                    "local base unavailable",
                )
                    .into_response());
            };
            Some((base, offset, insert, count))
        }
        _ => return Err(invalid().into()),
    };
    let received = data.len();
    let raw = tokio::task::spawn_blocking(move || expand(data, gzip, copy, limit))
        .await
        .map_err(|_| invalid())??;
    let cache = (raw.len() <= BUDGET && headers.get("x-kin-uds-store").is_some_and(|v| v == "1"))
        .then(|| raw.clone());
    let mut response = match routes::respond(state.clone(), raw).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    };
    response
        .headers_mut()
        .insert("x-kin-uds-received", HeaderValue::from(received as u64));
    response.headers_mut().remove(MISS);
    response.headers_mut().remove(BASE);
    if response.status().is_success()
        && let Some(bytes) = cache
        && let Ok(Some(key)) = tokio::task::spawn_blocking(move || state.packed().store(bytes)).await
    {
        response.headers_mut().insert(BASE, key);
    }
    Ok(response)
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
    fn delta_restores_exact_bytes_and_bounds_the_full_envelope() {
        let base = Bytes::from_static(b"old[history]old");
        let copy = Some((base.clone(), 3, 4, 9));
        let literal = Bytes::from(gzip(b"headtail"));
        assert_eq!(
            expand(literal.clone(), true, copy.clone(), 17).unwrap(),
            b"head[history]tail"[..]
        );
        assert!(matches!(
            expand(literal, true, copy, 16),
            Err(KernelError::BodyTooLarge)
        ));
        assert!(expand(Bytes::new(), false, Some((base, usize::MAX, 0, 1)), 99).is_err());
    }

    #[test]
    fn corrupt_or_trailing_gzip_bytes_are_rejected_before_dispatch() {
        let mut data = gzip(b"{}");
        data.push(0);
        assert!(expand(data.into(), true, None, 16).is_err());
        let mut data = gzip(b"{}");
        data.pop();
        assert!(expand(data.into(), true, None, 16).is_err());
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
