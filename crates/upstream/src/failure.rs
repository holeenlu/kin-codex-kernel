//! 控制面的上游錯誤事實；只保留明示、可安全放入內部標頭的短欄位。
use serde_json::Value;
use tap_contract::error::UpstreamFacts;

pub(super) fn facts(
    stage: &'static str,
    status: Option<u16>,
    event: &Value,
    id: Option<&str>,
    retry: Option<&str>,
) -> Box<UpstreamFacts> {
    let field = |key| event.pointer(key).and_then(Value::as_str);
    let safe = |s: &str| {
        (!s.is_empty() && s.len() <= 256 && s.bytes().all(|c| c.is_ascii_graphic() || c == b' '))
            .then(|| s.to_owned())
    };
    Box::new(UpstreamFacts {
        stage,
        status,
        code: field("/error/code").and_then(safe),
        kind: field("/error/type").and_then(safe),
        request_id: id
            .or(field("/headers/x-request-id"))
            .or(field("/headers/x-oai-request-id"))
            .or(field("/request_id"))
            .and_then(safe),
        retry_after: retry
            .or(field("/headers/retry-after"))
            .or(field("/error/retry_after"))
            .and_then(safe)
            .or_else(|| {
                event
                    .pointer("/error/retry_after")
                    .and_then(Value::as_u64)
                    .map(|n| n.to_string())
            }),
    })
}
