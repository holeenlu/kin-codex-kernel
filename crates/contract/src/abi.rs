//! 核心對控制面宣告的 ABI 版本（`/internal/v1/describe` 的 `abi`）。

/// ABI 版本。控制面以它判斷能否啟用某種模式，而不是看二進位名稱或版本字串。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(into = "u32")]
pub enum Abi {
    /// 單帳號相容模式：與上游 `kin-codex-kernel` 相同的設定格式與端點，一槽一程序。
    SingleAccount,
    /// 多帳號共享模式：帳號登錄、代次與帳號操作（設計第 4.3–4.5 節）。
    Shared,
}

impl From<Abi> for u32 {
    fn from(abi: Abi) -> Self {
        match abi {
            Abi::SingleAccount => 1,
            Abi::Shared => 2,
        }
    }
}

/// 能力名稱。每完成一項功能就加進 `describe` 的 `capabilities`，控制面依此決定可用的路徑。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    Responses,
    ResponsesWebsocket,
    ResponsesWsReplay,
    UdsPackedV1,
    ResponsesInterrupt,
    ResponsesTurnTiming,
    Cancel,
    Analytics,
    AccountOps,
    MultiAccount,
}
