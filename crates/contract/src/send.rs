//! 送出狀態：這次請求是否已經送到上游。
//!
//! 控制面依此決定能否重送（設計第 4.5 節）：
//! `0` → `not_sent`、`1` → `sent`、`unknown` 或缺少、不合法 → `ambiguous`。
//! 核心只有在**確定**沒有送出時才可以回 `0`。

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamSent {
    /// 確定沒有送出，例如驗證失敗、大小超限、帳號未知、代理在連線前就失敗。
    No,
    /// 已送上游，且收到上游的明確回應。
    Yes,
    /// 可能已送出，但結果不明。
    Unknown,
}

impl UpstreamSent {
    pub const fn header_value(self) -> &'static str {
        match self {
            Self::No => "0",
            Self::Yes => "1",
            Self::Unknown => "unknown",
        }
    }

    /// 解析標頭值。缺少或不合法一律視為 [`UpstreamSent::Unknown`]，保守處理。
    pub fn parse(value: Option<&str>) -> Self {
        match value.map(str::trim) {
            Some("0") => Self::No,
            Some("1") => Self::Yes,
            _ => Self::Unknown,
        }
    }
}

impl fmt::Display for UpstreamSent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.header_value())
    }
}

#[cfg(test)]
mod tests {
    use super::UpstreamSent;

    #[test]
    fn missing_or_invalid_values_are_ambiguous() {
        assert_eq!(UpstreamSent::parse(None), UpstreamSent::Unknown);
        assert_eq!(UpstreamSent::parse(Some("yes")), UpstreamSent::Unknown);
        assert_eq!(UpstreamSent::parse(Some(" 0 ")), UpstreamSent::No);
        assert_eq!(UpstreamSent::parse(Some("1")), UpstreamSent::Yes);
    }

    #[test]
    fn round_trips_through_the_header_value() {
        for state in [UpstreamSent::No, UpstreamSent::Yes, UpstreamSent::Unknown] {
            assert_eq!(UpstreamSent::parse(Some(state.header_value())), state);
        }
    }
}
