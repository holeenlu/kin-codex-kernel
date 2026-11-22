//! 只有能獨立送往 HTTP 的完整請求可在首次寫入前降級。
use serde::Deserialize;
use tap_contract::{error::KernelError, send::UpstreamSent};

pub(super) struct Policy {
    pub(super) model: Option<String>,
    generate: bool,
    pub(super) previous_response_id: Option<String>,
    portable: bool,
}

impl Policy {
    pub(super) fn parse(raw: &str) -> Self {
        use serde_json::value::RawValue;
        #[derive(Default, Deserialize)]
        struct Fields<'a> {
            #[serde(borrow)]
            model: Option<&'a RawValue>,
            #[serde(borrow)]
            generate: Option<&'a RawValue>,
            #[serde(borrow)]
            previous_response_id: Option<&'a RawValue>,
        }
        let fields = serde_json::from_str::<Fields>(raw).ok();
        let portable = fields.as_ref().is_some_and(|f| {
            f.previous_response_id.is_none() && f.generate.is_none_or(|g| g.get() == "true")
        });
        let f = fields.unwrap_or_default();
        Self {
            model: f.model.and_then(|v| serde_json::from_str(v.get()).ok()),
            generate: f.generate.is_none_or(|v| v.get() != "false"),
            previous_response_id: f
                .previous_response_id
                .and_then(|v| serde_json::from_str(v.get()).ok()),
            portable,
        }
    }
    pub(super) fn full(&self) -> bool {
        self.portable
    }
    pub(super) fn warmup(&self) -> bool {
        !self.generate
    }
    pub(super) fn rejection(&self, oversized: bool, sent: UpstreamSent) -> KernelError {
        let (code, message) = if self.previous_response_id.is_some() {
            (
                "previous_response_not_found",
                "Original WebSocket continuation unavailable; resend the full input.",
            )
        } else if oversized {
            (
                "websocket_message_too_large",
                "Request exceeds the negotiated WebSocket message size.",
            )
        } else {
            (
                "websocket_required",
                "This request requires a WebSocket connection.",
            )
        };
        KernelError::WebsocketPolicy { code, message, sent }
    }
}
