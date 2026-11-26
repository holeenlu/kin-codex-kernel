//! 每輪內部收據。時刻是核心本輪開始後的單調毫秒，連線各階段則是耗時。
//! 只保存固定名稱、計數與時間；不保留上游事件原文。

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{Value, value::RawValue};

#[derive(Debug, Default, Serialize)]
pub(crate) struct Facts {
    pub transport: Option<&'static str>,
    pub ws_reused: Option<bool>,
    pub ws_deflate: Option<bool>,
    pub ws_frame_compressed: Option<bool>,
    pub fallback: Option<&'static str>,
    pub handshake_status: Option<u16>,
    pub ws_handshakes: usize,
    pub upstream_attempts: usize,
    pub ws_send_attempts: usize,
    pub dial_ms: Option<u64>,
    pub tls_ms: Option<u64>,
    pub upgrade_ms: Option<u64>,
    pub send_complete_ms: Option<u64>,
    pub http_headers_ms: Option<u64>,
    pub request_mode: Option<&'static str>,
    pub full_body_bytes: usize,
    pub request_bytes: Option<usize>,
    pub http_zstd_bytes: Option<usize>,
    pub cache_miss: bool,
    pub first_event_ms: Option<u64>,
    pub first_event_type: Option<&'static str>,
    pub first_generated_ms: Option<u64>,
    pub first_generated_type: Option<&'static str>,
    pub ws_first_yield_ms: Option<u64>,
    pub ws_generated_yield_ms: Option<u64>,
    pub ws_pre_generated_read_ms: Option<u64>,
    pub ws_pre_generated_unpolled_ms: Option<u64>,
    pub first_text_ms: Option<u64>,
    pub first_text_type: Option<&'static str>,
    pub outcome: Option<&'static str>,
    pub sent: Option<&'static str>,
}

#[derive(Debug, Clone)]
pub struct TurnTiming {
    start: Instant,
    facts: Arc<Mutex<Facts>>,
}

impl Default for TurnTiming {
    fn default() -> Self {
        Self {
            start: Instant::now(),
            facts: Arc::default(),
        }
    }
}

pub(super) fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

impl TurnTiming {
    pub(super) fn begin_attempt(&self) {
        self.update(|f| {
            *f = Facts {
                upstream_attempts: f.upstream_attempts + 1,
                ws_send_attempts: f.ws_send_attempts,
                ..Facts::default()
            }
        });
    }
    pub(super) fn ms(&self) -> u64 {
        millis(self.start.elapsed())
    }
    pub(super) fn update(&self, f: impl FnOnce(&mut Facts)) {
        if let Ok(mut facts) = self.facts.lock() {
            f(&mut facts);
        }
    }
    pub fn json(&self) -> Option<String> {
        serde_json::to_string(&*self.facts.lock().ok()?).ok()
    }
    pub fn finish_if_open(&self, outcome: &'static str) {
        self.update(|f| {
            f.outcome.get_or_insert(outcome);
        });
    }
    pub fn end(&self, outcome: &'static str, sent: Option<&'static str>) {
        self.update(|f| {
            f.outcome = Some(outcome);
            if sent.is_some() {
                f.sent = sent;
            }
        });
    }
    pub(crate) fn event(&self, e: &Event<'_>) {
        let t = e.kind;
        if t != "error" && !t.starts_with("response.") {
            return;
        }
        self.update(|f| {
            let at = self.ms();
            let kind = event_label(t);
            if f.first_event_ms.is_none() {
                f.first_event_ms = Some(at);
                f.first_event_type = Some(kind);
            }
            if f.first_text_ms.is_none() || f.first_generated_ms.is_none() {
                let (generated, text) = e.content();
                if generated && f.first_generated_ms.is_none() {
                    f.first_generated_ms = Some(at);
                    f.first_generated_type = Some(kind);
                }
                if text && f.first_text_ms.is_none() {
                    f.first_text_ms = Some(at);
                    f.first_text_type = Some(kind);
                }
            }
            if crate::sse::TERMINAL.contains(&t) {
                f.outcome = Some(kind);
            }
        });
    }
}

// 未知事件只記固定代號，避免日誌被上游任意字串污染。
macro_rules! labels { ($t:expr, $($label:literal),*) => {match $t { $($label=>$label,)* _=>"response.other" }}; }
fn event_label(t: &str) -> &'static str {
    labels!(
        t,
        "error",
        "response.created",
        "response.in_progress",
        "response.queued",
        "response.completed",
        "response.done",
        "response.incomplete",
        "response.failed",
        "response.output_item.added",
        "response.output_item.done",
        "response.content_part.added",
        "response.content_part.done",
        "response.output_text.delta",
        "response.output_text.done",
        "response.reasoning_text.delta",
        "response.reasoning_summary_text.delta",
        "response.reasoning_summary_part.added",
        "response.reasoning_summary_text.done",
        "response.function_call_arguments.delta",
        "response.custom_tool_call_input.delta",
        "response.refusal.delta",
        "response.image_generation_call.partial_image"
    )
}

#[derive(Deserialize)]
pub(crate) struct Event<'a> {
    #[serde(rename = "type")]
    pub kind: &'a str,
    #[serde(borrow)]
    delta: Option<&'a RawValue>,
    #[serde(borrow)]
    text: Option<&'a RawValue>,
    #[serde(borrow)]
    item: Option<&'a RawValue>,
    #[serde(borrow)]
    part: Option<&'a RawValue>,
    #[serde(borrow)]
    response: Option<&'a RawValue>,
}

impl Event<'_> {
    fn content(&self) -> (bool, bool) {
        let nonempty =
            |s: Option<&RawValue>| s.is_some_and(|v| v.get().starts_with('"') && v.get().len() > 2);
        if self.kind.ends_with(".delta") {
            let generated = nonempty(self.delta);
            return (generated, generated && self.kind == "response.output_text.delta");
        }
        if self.kind == "response.output_text.done" {
            let text = nonempty(self.text);
            return (text, text);
        }
        if matches!(self.kind, "response.completed" | "response.done") {
            let text = self
                .response
                .and_then(|raw| serde_json::from_str::<Value>(raw.get()).ok())
                .is_some_and(|r| {
                    r["output"].as_array().into_iter().flatten().any(|i| {
                        i["content"].as_array().into_iter().flatten().any(|p| {
                            p["type"] == "output_text" && p["text"].as_str().is_some_and(|s| !s.is_empty())
                        })
                    })
                });
            return (text, text);
        }
        let block = match self.kind {
            "response.content_part.added" | "response.content_part.done" => self.part,
            "response.output_item.added" | "response.output_item.done" => self.item,
            _ => None,
        }
        .and_then(|r| serde_json::from_str::<Value>(r.get()).ok());
        let Some(block) = block else {
            return (false, false);
        };
        let flags = |v: &Value| {
            let has = |k: &str| v[k].as_str().is_some_and(|s| !s.is_empty());
            let text = v["type"] == "output_text" && has("text");
            let generated = has("text")
                || has("thinking")
                || has("refusal")
                || (v["type"] == "redacted_thinking" && has("data"))
                || (matches!(v["type"].as_str(), Some("function_call" | "custom_tool_call"))
                    && (has("name") || has("arguments") || has("input")));
            (generated, text)
        };
        block["content"]
            .as_array()
            .into_iter()
            .flatten()
            .map(flags)
            .fold(flags(&block), |(a, b), (c, d)| (a || c, b || d))
    }
}

/// 旁聽 HTTP SSE，既不改動 chunk，也不等待事件完整才向下游放行。過大的診斷事件直接略過。
#[derive(Default)]
pub(super) struct SseObserver {
    line: Vec<u8>,
    data: Vec<u8>,
    skip: bool,
    line_has_content: bool,
}
impl SseObserver {
    pub(super) fn push(&mut self, chunk: &[u8], timing: &TurnTiming) {
        use crate::sse::TIMING_EVENT_LIMIT;
        for segment in chunk.split_inclusive(|&b| b == b'\n') {
            let end = segment.ends_with(b"\n");
            let piece = if end {
                &segment[..segment.len() - 1]
            } else {
                segment
            };
            if piece.iter().any(|&b| b != b'\r') {
                self.line_has_content = true;
            }
            if self
                .data
                .len()
                .saturating_add(self.line.len())
                .saturating_add(piece.len())
                > TIMING_EVENT_LIMIT
            {
                self.skip = true;
            }
            if !self.skip {
                self.line.extend_from_slice(piece);
            }
            if !end {
                continue;
            }
            if !self.line_has_content {
                if !self.skip
                    && let Ok(e) = serde_json::from_slice::<Event>(&self.data)
                {
                    timing.event(&e);
                }
                self.data.clear();
                self.skip = false;
            } else if !self.skip
                && let Some(data) = self
                    .line
                    .strip_suffix(b"\r")
                    .unwrap_or(&self.line)
                    .strip_prefix(b"data:")
            {
                if !self.data.is_empty() {
                    self.data.push(b'\n');
                }
                self.data
                    .extend_from_slice(data.strip_prefix(b" ").unwrap_or(data));
            }
            self.line.clear();
            self.line_has_content = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fragmented_crlf_multiline_and_oversize_events_do_not_fabricate_text() {
        let t = TurnTiming::default();
        let mut s = SseObserver::default();
        s.push(b"data: ", &t);
        s.push(&vec![b'x'; 260 * 1024], &t);
        s.push(b"\n\n", &t);
        let raw=b"data: {\"type\":\"response.created\"}\r\n\r\ndata: {\"type\":\"response.reasoning_text.delta\",\r\ndata: \"delta\":\"private\"}\r\n\r\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"\"}\n\n";
        for b in raw.chunks(3) {
            s.push(b, &t);
        }
        let mut f: Value = serde_json::from_str(&t.json().unwrap()).unwrap();
        assert_eq!(f["first_event_type"], "response.created");
        assert_eq!(f["first_generated_type"], "response.reasoning_text.delta");
        assert!(f["first_text_ms"].is_null());
        s.push(
            b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"secret\"}\n\n",
            &t,
        );
        let raw = t.json().unwrap();
        assert!(!raw.contains("secret") && !raw.contains("private"));
        f = serde_json::from_str(&raw).unwrap();
        assert_eq!(f["first_text_type"], "response.output_text.delta");
    }
}
