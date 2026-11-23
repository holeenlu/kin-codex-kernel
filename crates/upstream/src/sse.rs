//! 非串流請求的 SSE 彙整：完整終態事件到達後即可回傳，不等待上游關閉串流。

use serde_json::Value;

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
}

impl TerminalDecoder {
    pub(crate) fn push(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        const MAX_EVENT: usize = 64 * 1024 * 1024;
        for part in bytes.split_inclusive(|c| *c == b'\n') {
            if self.data.len() + self.line.len() + part.len() > MAX_EVENT {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "SSE event exceeds 64 MiB",
                ));
            }
            self.line.extend_from_slice(part);
            if part.ends_with(b"\n") {
                self.consume_line();
            }
        }
        Ok(())
    }

    fn consume_line(&mut self) {
        let line = self.line.strip_suffix(b"\n").unwrap_or(&self.line);
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            self.flush();
        } else if let Some(piece) = line.strip_prefix(b"data:") {
            let piece = piece.strip_prefix(b" ").unwrap_or(piece);
            if piece != b"[DONE]" {
                if !self.data.is_empty() {
                    self.data.push(b'\n');
                }
                self.data.extend_from_slice(piece);
            }
        }
        self.line.clear();
    }

    fn flush(&mut self) {
        #[derive(serde::Deserialize)]
        struct Kind<'a> {
            #[serde(rename = "type")]
            kind: &'a str,
        }
        if serde_json::from_slice::<Kind>(&self.data).is_ok_and(|event| TERMINAL.contains(&event.kind))
            && let Ok(value) = serde_json::from_slice(&self.data)
        {
            self.last = Some(value);
        }
        self.data.clear();
    }

    pub(crate) fn finish(mut self) -> Option<Value> {
        self.consume_line();
        self.flush();
        self.last
    }

    pub(crate) fn take_terminal(&mut self) -> Option<Value> {
        self.last.take()
    }
}

#[cfg(test)]
fn terminal_event(bytes: &[u8]) -> Option<Value> {
    let mut decoder = TerminalDecoder::default();
    for chunk in bytes.chunks(3) {
        decoder.push(chunk).unwrap();
    }
    decoder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
