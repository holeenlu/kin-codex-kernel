//! 下游的安裝身分改寫成這個帳號的安裝身分（`device_id`）。
//!
//! 官方客戶端在 `x-codex-turn-metadata` 標頭與本體的 `client_metadata`（`x-codex-installation-id`、
//! 以字串存放的 `x-codex-turn-metadata`）帶自己機器的安裝 id。同一個帳號被多個下游使用時，
//! 原樣轉發會讓上游看到一個帳號出現多個安裝 id。核心將本體與標頭中出現的下游安裝 id 換成本帳號的 `device_id`，其餘位元組保持原樣。
//!
//! 作法是位元組層級的字串替換：先從已知位置找出下游 id（必須像官方的 UUID），
//! 再把本體與標頭中所有該 id 換掉。UUID 等長，所以替換後長度不變。

use kin_contract::config::looks_like_uuid;
use kin_contract::envelope::ResponsesEnvelope;
use serde_json::{Map, Value};

use crate::json_object::Object;

/// 改寫後要用的本體與 `x-codex-turn-metadata` 標頭。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scrubbed {
    pub body: String,
    pub turn_metadata: Option<String>,
    /// 換掉了幾個不同的下游 id（記錄用）。
    pub replaced: usize,
    /// 本體頂層有 `client_metadata`（含跳脫寫法的鍵）。之後處理 metadata 時據此決定要不要再解析本體。
    pub client_metadata: bool,
}

pub fn installation_ids(envelope: &ResponsesEnvelope, device_id: &str) -> Scrubbed {
    let object = Object::parse(envelope.body.get());
    installation_ids_from_object(envelope, device_id, object.as_ref())
}

pub(super) fn installation_ids_from_object(
    envelope: &ResponsesEnvelope,
    device_id: &str,
    object: Option<&Object<'_>>,
) -> Scrubbed {
    let body = envelope.body.get();
    let header = envelope.header("x-codex-turn-metadata");
    let mut found: Vec<String> = Vec::new();
    let mut note = |candidate: Option<&str>| {
        if let Some(id) = candidate.map(str::trim)
            && looks_like_uuid(id)
            && !id.eq_ignore_ascii_case(device_id)
            && !found.iter().any(|known| known == id)
        {
            found.push(id.to_owned());
        }
    };
    note(header.and_then(turn_metadata_installation).as_deref());
    note(envelope.header("x-codex-installation-id"));
    // 依原順序讀頂層成員：跳脫寫法的鍵、重複的鍵都看得到（整份本體只掃一次）。
    let client_metadata = object.is_some_and(|object| object.contains("client_metadata"));
    for metadata in object
        .into_iter()
        .flat_map(|object| object.values("client_metadata"))
        .filter_map(|raw| serde_json::from_str::<Map<String, Value>>(raw.get()).ok())
    {
        note(metadata.get("x-codex-installation-id").and_then(Value::as_str));
        note(metadata.get("installation_id").and_then(Value::as_str));
        note(
            metadata
                .get("x-codex-turn-metadata")
                .and_then(Value::as_str)
                .and_then(turn_metadata_installation)
                .as_deref(),
        );
    }
    let mut body = body.to_owned();
    let mut turn_metadata = header.map(str::to_owned);
    for id in &found {
        body = body.replace(id.as_str(), device_id);
        turn_metadata = turn_metadata.map(|value| value.replace(id.as_str(), device_id));
    }
    Scrubbed {
        body,
        turn_metadata,
        replaced: found.len(),
        client_metadata,
    }
}

fn turn_metadata_installation(raw: &str) -> Option<String> {
    let value: Value = serde_json::from_str(raw).ok()?;
    value.get("installation_id")?.as_str().map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOWN: &str = "11111111-2222-4333-8444-555555555555";
    const SLOT: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";

    fn envelope(raw: &str) -> ResponsesEnvelope {
        serde_json::from_str(raw).unwrap()
    }

    #[test]
    fn replaces_the_downstream_installation_id_everywhere_and_nothing_else() {
        let meta = format!(r#"{{\"installation_id\":\"{DOWN}\",\"session_id\":\"s1\"}}"#);
        let raw = format!(
            r#"{{"body":{{"model":"m","input":[],"client_metadata":{{"x-codex-installation-id":"{DOWN}","x-codex-turn-metadata":"{meta}","session_id":"s1"}}}},
            "headers":{{"x-codex-turn-metadata":"{{\"installation_id\":\"{DOWN}\",\"turn_id\":\"t\"}}"}}}}"#
        );
        let scrubbed = installation_ids(&envelope(&raw), SLOT);
        assert_eq!(scrubbed.replaced, 1);
        assert!(!scrubbed.body.contains(DOWN));
        assert_eq!(scrubbed.body.matches(SLOT).count(), 2);
        assert_eq!(scrubbed.body.len(), envelope(&raw).body.get().len());
        assert_eq!(
            scrubbed.turn_metadata.as_deref(),
            Some(format!(r#"{{"installation_id":"{SLOT}","turn_id":"t"}}"#).as_str())
        );
    }

    #[test]
    fn leaves_requests_without_installation_ids_untouched() {
        let raw = r#"{"body":{"model":"m","input":[{"text":"11111111-2222-4333-8444-555555555555"}]},"headers":{}}"#;
        let scrubbed = installation_ids(&envelope(raw), SLOT);
        assert_eq!(scrubbed.replaced, 0);
        assert_eq!(scrubbed.body, envelope(raw).body.get());
        assert_eq!(scrubbed.turn_metadata, None);
    }

    #[test]
    fn only_uuid_shaped_ids_are_replaced() {
        assert!(looks_like_uuid(DOWN));
        assert!(!looks_like_uuid("not-a-uuid"));
        assert!(!looks_like_uuid("1111111-2222-4333-8444-555555555555"));
        let raw = r#"{"body":{"client_metadata":{"x-codex-installation-id":"x"}},"headers":{}}"#;
        assert_eq!(installation_ids(&envelope(raw), SLOT).replaced, 0);
    }
}
