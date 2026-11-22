//! 完整快照一直在控制面的 envelope。閒置 WS 只記成功回應的 ID、項目數及摘要，
//! 記憶體不隨大歷史膨脹，也不把續接狀態搬到別的連線。採嚴格位元組比對；不確定就送原文。
//! 比對的是對話設定：`input` 與每輪不同的 `client_metadata` 不計入。

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use tap_contract::envelope::WsReplay;

use crate::websocket::create_message;

const MAX_OUTPUT_ITEMS: usize = 4096;

type Hash = [u8; 32];

pub(super) struct Snapshot {
    settings: Hash,
    count: usize,
    prefix: Hash,
    id: String,
}

impl Snapshot {
    /// 這條連線上一輪完成的回應 ID（續接提示要對得上它才送增量）。
    pub(super) fn id(&self) -> &str {
        &self.id
    }

    #[cfg(test)]
    pub(super) fn for_test(id: &str) -> Self {
        Self {
            settings: [0; 32],
            count: 0,
            prefix: [0; 32],
            id: id.to_owned(),
        }
    }
}

pub(super) struct Pending {
    settings: Hash,
    count: usize,
    input: Sha256,
    finished: BTreeMap<usize, Hash>,
    max_index: Option<usize>,
    incomplete: bool,
    saw_output: bool,
}

fn digest(raw: &RawValue) -> Hash {
    Sha256::digest(raw.get().as_bytes()).into()
}

/// 控制面會把解析出的 output 以緊湊 JSON 放回 input。忽略上游字串以外的空白，
/// 保留鍵順序、數值字面值、字串內空白與跳脫，不將不同的工具參數或大數值視為相同。
fn output_digest(raw: &RawValue) -> Hash {
    let bytes = raw.get().as_bytes();
    let mut h = Sha256::new();
    let mut start = 0;
    let (mut string, mut escaped) = (false, false);
    for (i, &b) in bytes.iter().enumerate() {
        if string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                string = false;
            }
        } else if b == b'"' {
            string = true;
        } else if b.is_ascii_whitespace() {
            if start < i {
                h.update(&bytes[start..i]);
            }
            start = i + 1;
        }
    }
    h.update(&bytes[start..]);
    h.finalize().into()
}

/// 即使收到不合資格的歷史也照送完整原文；只有命中時才改寫 input。
pub(super) fn prepare(
    body: &str,
    hint: Option<&WsReplay>,
    previous: Option<&Snapshot>,
) -> (String, Option<Pending>, &'static str) {
    let Some(hint) = hint else {
        return (create_message(body), None, "as_received");
    };
    let parse = || {
        let fields: BTreeMap<&str, &RawValue> = serde_json::from_str(body).ok()?;
        if fields.contains_key("previous_response_id")
            || fields.get("generate").is_some_and(|v| v.get() == "false")
        {
            return None;
        }
        let raw = *fields.get("input")?;
        let items: Vec<&RawValue> = serde_json::from_str(raw.get()).ok()?;
        let mut settings = Sha256::new();
        for (key, value) in fields {
            // `client_metadata` 是這一輪請求的描述（輪次 id、安裝身分），不是對話設定：每輪本來就不同，
            // 不影響上游以 `previous_response_id` 接續。增量訊息沿用本輪的 client_metadata。
            if !matches!(key, "input" | "client_metadata") {
                settings.update((key.len() as u64).to_be_bytes());
                settings.update(key.as_bytes());
                settings.update(digest(value));
            }
        }
        let settings: Hash = settings.finalize().into();
        let candidate = previous.filter(|p| {
            hint.previous_response_id.as_deref() == Some(&p.id)
                && settings == p.settings
                && p.count < items.len()
        });
        let mut input = Sha256::new();
        let mut offset = None;
        for (i, item) in items.iter().enumerate() {
            if candidate.is_some_and(|p| p.count == i && Hash::from(input.clone().finalize()) == p.prefix) {
                offset = Some(i);
            }
            input.update(digest(item));
        }
        let pending = Pending {
            settings,
            count: items.len(),
            input,
            finished: BTreeMap::new(),
            max_index: None,
            incomplete: false,
            saw_output: false,
        };
        let Some((previous, offset)) = candidate.zip(offset) else {
            return Some((
                create_message(body),
                Some(pending),
                if hint.previous_response_id.is_some() {
                    "full_replay"
                } else {
                    "full"
                },
            ));
        };
        // RawValue 借自目前 body；只替換該 input 陣列，連未知欄位及數值字面值都原樣保留。
        let start = (raw.get().as_ptr() as usize).checked_sub(body.as_ptr() as usize)?;
        let suffix = serde_json::to_string(&items[offset..]).ok()?;
        let id = serde_json::to_string(&previous.id).ok()?;
        let delta = format!(
            "{}{},\"previous_response_id\":{}{}",
            body.get(..start)?,
            suffix,
            id,
            body.get(start + raw.get().len()..)?
        );
        Some((create_message(&delta), Some(pending), "incremental"))
    };
    parse().unwrap_or_else(|| {
        (
            create_message(body),
            None,
            if hint.previous_response_id.is_some() {
                "full_replay"
            } else {
                "full"
            },
        )
    })
}

#[derive(Deserialize)]
struct OutputItem<'a> {
    output_index: usize,
    #[serde(borrow)]
    item: &'a RawValue,
}

#[derive(Deserialize)]
struct Completed<'a> {
    #[serde(borrow)]
    response: Response<'a>,
}
#[derive(Deserialize)]
struct Response<'a> {
    id: String,
    status: String,
    incomplete_details: Option<IncompleteDetails>,
    #[serde(borrow)]
    output: Option<Vec<&'a RawValue>>,
}

#[derive(Deserialize)]
struct IncompleteDetails {
    reason: Option<String>,
}

impl Pending {
    pub(super) fn observe(&mut self, kind: &str, text: &str) {
        if !kind.starts_with("response.")
            || matches!(
                kind,
                "response.created" | "response.in_progress" | "response.completed"
            )
        {
            return;
        }
        self.saw_output = true;
        if !matches!(kind, "response.output_item.added" | "response.output_item.done") {
            return;
        }
        let Ok(item) = serde_json::from_str::<OutputItem>(text) else {
            self.incomplete = true;
            return;
        };
        self.max_index = Some(
            self.max_index
                .map_or(item.output_index, |n| n.max(item.output_index)),
        );
        if item.output_index >= MAX_OUTPUT_ITEMS {
            self.incomplete = true;
            return;
        }
        if kind == "response.output_item.done"
            && self
                .finished
                .insert(item.output_index, output_digest(item.item))
                .is_some()
        {
            self.incomplete = true;
        }
    }

    pub(super) fn complete(mut self, text: &str) -> Option<Snapshot> {
        let r = serde_json::from_str::<Completed>(text).ok()?.response;
        // 中斷可能丟棄先前已串出的項目；只能依上游終態的明示完整清單（包含空清單）續接。
        let interrupted = r.status == "incomplete"
            && r.incomplete_details.as_ref().and_then(|d| d.reason.as_deref()) == Some("interrupted")
            && r.output.is_some();
        if (!interrupted && r.status != "completed") || r.id.is_empty() || r.id.len() > 256 {
            return None;
        }
        if let Some(items) = r.output.as_ref().filter(|o| interrupted || !o.is_empty()) {
            for item in items {
                self.input.update(output_digest(item));
            }
            self.count += items.len();
        } else {
            // 稀疏終態：只採信依 index 連續且已完成的 output_item，不猜補推理或工具項目。
            if self.incomplete
                || (self.saw_output || r.output.is_none()) && self.finished.is_empty()
                || self
                    .max_index
                    .is_some_and(|n| n.checked_add(1) != Some(self.finished.len()))
            {
                return None;
            }
            self.count += self.finished.len();
            for item in self.finished.into_values() {
                self.input.update(item);
            }
        }
        Some(Snapshot {
            id: r.id,
            settings: self.settings,
            count: self.count,
            prefix: self.input.finalize().into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[test]
    fn only_the_last_exact_completed_prefix_and_all_settings_qualify() {
        let hint = WsReplay {
            previous_response_id: Some("r".into()),
        };
        let previous = json!({"model":"m","tools":[{"name":"fn"}],"reasoning":{"effort":"high"},"input":[{"role":"user","content":"hello"}]});
        let (_, pending, _) = prepare(&previous.to_string(), Some(&WsReplay::default()), None);
        let mut p = pending.unwrap();
        p.observe(
            "response.output_item.done",
            r#"{"output_index":0, "item":{ "encrypted_content": "a", "type":"reasoning" }}"#,
        );
        let saved = p
            .complete(r#"{"response":{"id":"r","status":"completed"}}"#)
            .unwrap();
        let mut next = previous.clone();
        next["input"] = json!([previous["input"][0],{"encrypted_content":"a","type":"reasoning"},{"type":"message","content":"next"}]);
        let check = |body: &Value, h: &WsReplay, cache: Option<&Snapshot>| {
            prepare(&body.to_string(), Some(h), cache).2
        };
        assert_eq!(check(&next, &hint, Some(&saved)), "incremental");
        // 每輪不同的 client_metadata（合成的 turn_id）不妨礙增量；增量訊息帶的是本輪的值。
        let mut with_metadata = next.clone();
        with_metadata["client_metadata"] = json!({"session_id":"s","turn_id":"turn-2"});
        let (message, _, mode) = prepare(&with_metadata.to_string(), Some(&hint), Some(&saved));
        assert_eq!(mode, "incremental");
        assert!(message.contains(r#""turn_id":"turn-2""#));
        for (key, value) in [
            ("model", json!("other")),
            ("tools", json!([])),
            ("reasoning", json!({"effort":"low"})),
            ("future_setting", json!({"n":1})),
        ] {
            let mut body = next.clone();
            body[key] = value;
            assert_eq!(check(&body, &hint, Some(&saved)), "full_replay");
        }
        next["input"][1]["encrypted_content"] = json!("b");
        assert_eq!(check(&next, &hint, Some(&saved)), "full_replay");
        assert_eq!(check(&next, &hint, None), "full_replay");
        assert_eq!(
            check(
                &next,
                &WsReplay {
                    previous_response_id: Some("other".into())
                },
                Some(&saved)
            ),
            "full_replay"
        );
        for terminal in [
            r#"{"response":{"id":"r","status":"incomplete","output":[]}}"#,
            r#"{"response":{"status":"completed","output":[]}}"#,
        ] {
            let (_, p, _) = prepare(&previous.to_string(), Some(&hint), None);
            assert!(p.unwrap().complete(terminal).is_none());
        }
    }
}
