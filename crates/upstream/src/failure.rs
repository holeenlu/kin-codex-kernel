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
    let header = |path, name: &str| {
        event
            .pointer(path)?
            .as_object()?
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))?
            .1
            .as_str()
    };
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
            .or(header("/error/headers", "retry-after"))
            .or(header("/headers", "retry-after"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn retry_after_preserves_nested_headers_and_rejects_unsafe_values() {
        let event = json!({"error": {"headers": {"Retry-After": "7"}, "retry_after": 9}, "headers": {"retry-after": "11"}});
        assert_eq!(
            facts("event", Some(429), &event, None, None)
                .retry_after
                .as_deref(),
            Some("7")
        );
        assert_eq!(
            facts("event", Some(429), &event, None, Some("3"))
                .retry_after
                .as_deref(),
            Some("3")
        );
        let event = json!({"headers": {"RETRY-AFTER": "13"}});
        assert_eq!(
            facts("event", None, &event, None, None).retry_after.as_deref(),
            Some("13")
        );
        let event = json!({"error": {"headers": {"Retry-After": "7\r\nx-other: value"}}});
        assert!(facts("event", None, &event, None, None).retry_after.is_none());
        assert_eq!(
            facts("event", None, &json!({"error": {"retry_after": 17}}), None, None)
                .retry_after
                .as_deref(),
            Some("17")
        );
    }
}
