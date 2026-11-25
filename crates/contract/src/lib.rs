//! kin-codex-kernel 與 vm2api 控制面之間的線路契約。
//!
//! 這個 crate 只放型別與常數，不做任何 IO，也不相依 HTTP 框架；
//! 內部 API、測試與日後的共享模式都以它為唯一的契約來源。
//! 對應 vm2api `docs/CODEX_SHARED_KERNEL_DESIGN.md` 第 4 節與第 4.6 節完整性清單。

pub mod abi;
pub mod config;
pub mod envelope;
pub mod error;
pub mod headers;
pub mod registry;
pub mod send;
pub mod status;
