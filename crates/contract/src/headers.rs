//! 內部 API 使用的標頭名稱。全部小寫，與 HTTP/1.1 的大小寫不敏感比對一致。

/// 控制面每個請求都帶的內部 token。
pub const INTERNAL_TOKEN: &str = "x-kin-internal-token";
/// 舊名稱，上游核心同時接受；保留到控制面不再送出為止。
pub const LEGACY_INTERNAL_TOKEN: &str = "x-internal-token";
/// 錯誤回應上的送出狀態（設計第 4.5 節），值見 [`crate::send::UpstreamSent`]。
pub const UPSTREAM_SENT: &str = "x-kin-upstream-sent";
/// 僅限核心到控制面。起始標頭是當時已知欄位，正常讀完時 trailer 補齊本輪計時。
pub const TURN_TIMING: &str = "x-tap-turn-timing";
