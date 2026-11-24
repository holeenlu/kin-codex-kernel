//! 非串流請求的 SSE 彙整：完整終態事件到達後即可回傳，不等待上游關閉串流。

use serde_json::Value;

use crate::timing::{Event, TurnTiming};

pub(super) const TIMING_EVENT_LIMIT: usize = 256 * 1024;
const MAX_EVENT: usize = 64 * 1024 * 1024;

/// 終態事件：讀到其中之一，這一輪就結束。
pub const TERMINAL: &[&str] = &[
    "response.completed",
    "response.failed",
    "response.incomplete",
    "response.done",
    "error",
];

/// 只保留尚未完成的一行／事件與最後終態，永不緩衝整輪歷史。
#[derive(Default)]
pub(crate) struct TerminalDecoder {
    line: Vec<u8>,
    data: Vec<u8>,
    last: Option<Value>,
    skip_timing: bool,
    // 舊計時旁聽也把全是 CR 的行當成空行。終態仍依單一 LF 或 CRLF 分隔。
    timing_start: usize,
}

impl TerminalDecoder {
    pub(crate) fn push(&mut self, bytes: &[u8], timing: &TurnTiming) -> std::io::Result<()> {
        for part in bytes.split_inclusive(|c| *c == b'\n') {
            if self.data.len() + self.line.len() + part.len() > MAX_EVENT {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "SSE event exceeds 64 MiB",
                ));
            }
            self.line.extend_from_slice(part);
            if part.ends_with(b"\n") {
                self.consume_line(Some(timing));
            }
        }
        Ok(())
    }

    fn consume_line(&mut self, timing: Option<&TurnTiming>) {
        let line = self.line.strip_suffix(b"\n").unwrap_or(&self.line);
        // 計時上限沿用旁聽的計數：已組成的 data 加上目前整行，包含欄位前綴和 CR。
        self.skip_timing |= self.data.len() - self.timing_start + line.len() > TIMING_EVENT_LIMIT;
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            self.flush(timing);
        } else if let Some(piece) = line.strip_prefix(b"data:") {
            let piece = piece.strip_prefix(b" ").unwrap_or(piece);
            if piece == b"[DONE]" {
                // 終態解碼忽略 [DONE]。計時旁聽原本不會從含它的事件解析出 JSON。
                self.skip_timing = true;
            } else {
                if !self.data.is_empty() {
                    if self.timing_start == self.data.len() {
                        self.timing_start += 1;
                    }
                    self.data.push(b'\n');
                }
                self.data.extend_from_slice(piece);
            }
        } else if let Some(timing) = timing
            && line.starts_with(b"\r")
            && line.iter().all(|&b| b == b'\r')
        {
            self.observe(timing);
            self.timing_start = self.data.len();
            self.skip_timing = false;
        }
        self.line.clear();
    }

    fn observe(&self, timing: &TurnTiming) -> Option<bool> {
        if self.skip_timing {
            return None;
        }
        let event: Event<'_> = serde_json::from_slice(&self.data[self.timing_start..]).ok()?;
        timing.event(&event);
        Some(TERMINAL.contains(&event.kind))
    }

    fn flush(&mut self, timing: Option<&TurnTiming>) {
        #[derive(serde::Deserialize)]
        struct Kind<'a> {
            #[serde(rename = "type")]
            kind: &'a str,
        }
        let observed = timing.and_then(|timing| self.observe(timing));
        // 只解析一次正常事件。略過計時或計時欄位不合法時，終態仍只靠 type 判斷。
        let terminal = observed.filter(|_| self.timing_start == 0).unwrap_or_else(|| {
            serde_json::from_slice::<Kind>(&self.data).is_ok_and(|event| TERMINAL.contains(&event.kind))
        });
        if terminal && let Ok(value) = serde_json::from_slice(&self.data) {
            self.last = Some(value);
        }
        self.data.clear();
        self.timing_start = 0;
        self.skip_timing = false;
    }

    pub(crate) fn finish(mut self) -> Option<Value> {
        // 沒有空行的最後一個事件仍可在 EOF 解出終態，與先前旁聽一樣不補記時間。
        self.consume_line(None);
        self.flush(None);
        self.last
    }

    pub(crate) fn take_terminal(&mut self) -> Option<Value> {
        self.last.take()
    }
}

#[cfg(test)]
fn terminal_event(bytes: &[u8]) -> Option<Value> {
    let mut decoder = TerminalDecoder::default();
    let timing = TurnTiming::default();
    for chunk in bytes.chunks(3) {
        decoder.push(chunk, &timing).unwrap();
    }
    decoder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timing::SseObserver;

    fn timing_types(timing: &TurnTiming) -> Value {
        let facts: Value = serde_json::from_str(&timing.json().unwrap()).unwrap();
        serde_json::json!([
            facts["first_event_type"],
            facts["first_generated_type"],
            facts["first_text_type"],
            facts["outcome"],
        ])
    }

    #[test]
    fn picks_the_last_terminal_event_across_crlf_and_multiline_data() {
        let raw = b"event: response.created\r\ndata: {\"type\":\"response.created\"}\r\n\r\n\
event: response.failed\ndata: {\"type\":\"response.failed\",\ndata: \"response\":{\"status\":\"failed\"}}\n\n";
        let event = terminal_event(raw).unwrap();
        assert_eq!(event["type"], "response.failed");
        assert_eq!(event["response"]["status"], "failed");
    }

    #[test]
    fn a_stream_without_a_terminal_event_has_none() {
        assert!(terminal_event(b"data: {\"type\":\"response.created\"}\n\n").is_none());
        // 內容裡出現同名字串不算終態。
        assert!(
            terminal_event(
                b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"response.completed\"}\n\n"
            )
            .is_none()
        );
    }

    #[test]
    fn non_stream_timing_matches_the_observer_across_sse_boundaries() {
        let raw = concat!(
            ": comment\r\n",
            "event: message\r\n",
            "data: {\"type\":\"response.created\"}\r\n\r\n",
            "data: {\"type\":\"response.reasoning_text.delta\",\r\n",
            "data:\"delta\":\"推理\"}\r\n\r\n",
            "data: [DONE]\r\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ignored\"}\r\n\r\n",
            "data: {\"type\":\"response.in_progress\"}\r\n\r\r\n",
            ": comment after a nonstandard blank line\r\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"本文\"}\r\n\r\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\"}}\r\n\r\n"
        )
        .as_bytes();
        for chunk_size in [1, 3, 7, 64, raw.len()] {
            let timing = TurnTiming::default();
            let reference = TurnTiming::default();
            let mut decoder = TerminalDecoder::default();
            let mut observer = SseObserver::default();
            for chunk in raw.chunks(chunk_size) {
                decoder.push(chunk, &timing).unwrap();
                observer.push(chunk, &reference);
            }
            assert_eq!(decoder.take_terminal().unwrap()["response"]["id"], "r1");
            let expected = serde_json::json!([
                "response.created",
                "response.reasoning_text.delta",
                "response.output_text.delta",
                "response.completed",
            ]);
            assert_eq!(timing_types(&timing), expected);
            assert_eq!(timing_types(&reference), expected);
        }
    }

    #[test]
    fn diagnostic_limit_counts_the_wire_line_but_does_not_reject_a_terminal() {
        let prefix = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"padding\":\"";
        let suffix = "\"}}\r";
        for extra in [0, 1] {
            let padding = "x".repeat(TIMING_EVENT_LIMIT + extra - prefix.len() - suffix.len());
            let raw = format!("{prefix}{padding}{suffix}\n\n");
            let mut decoder = TerminalDecoder::default();
            let mut observer = SseObserver::default();
            let timing = TurnTiming::default();
            let reference = TurnTiming::default();
            for chunk in raw.as_bytes().chunks(4093) {
                decoder.push(chunk, &timing).unwrap();
                observer.push(chunk, &reference);
            }
            assert_eq!(decoder.take_terminal().unwrap()["response"]["id"], "r1");
            let expected = if extra == 0 {
                Value::String("response.completed".to_owned())
            } else {
                Value::Null
            };
            assert_eq!(timing_types(&timing)[3], expected);
            assert_eq!(timing_types(&timing), timing_types(&reference));
        }
    }

    #[test]
    fn terminal_fallback_preserves_json_with_bad_timing_fields_and_at_eof() {
        for raw in [
            b"data: {\"type\":\"response.failed\",\"delta\":null,\"delta\":\"x\"}\n\n".as_slice(),
            b"data: {\"type\":\"response.failed\"}",
            b"data: {\"type\":\"response.failed\"}\r\n",
        ] {
            let mut decoder = TerminalDecoder::default();
            let timing = TurnTiming::default();
            for chunk in raw.chunks(3) {
                decoder.push(chunk, &timing).unwrap();
            }
            assert_eq!(decoder.finish().unwrap()["type"], "response.failed");
            assert_eq!(timing_types(&timing), serde_json::json!([null, null, null, null]));
        }
    }

    #[test]
    fn non_stream_limit_includes_the_newline_and_resets_for_the_next_event() {
        let timing = TurnTiming::default();
        let block = [b':'; 8192];
        let mut decoder = TerminalDecoder::default();
        for _ in 1..MAX_EVENT / block.len() {
            decoder.push(&block, &timing).unwrap();
        }
        decoder.push(&block[..block.len() - 1], &timing).unwrap();
        assert_eq!(
            decoder.push(b"x\n", &timing).unwrap_err().to_string(),
            "SSE event exceeds 64 MiB"
        );
        decoder
            .push(b"\n\ndata: {\"type\":\"response.completed\"}\n\n", &timing)
            .unwrap();
        assert_eq!(decoder.take_terminal().unwrap()["type"], "response.completed");
        assert_eq!(timing_types(&timing)[3], "response.completed");
    }
}
