//! kin-codex-kernel 的內部 HTTP API。
//!
//! 分層規則：這一層只負責 HTTP 的邊界（驗證、路由、請求大小、錯誤對應、socket），
//! 不放業務邏輯；推理、帳號、憑據等功能日後由各自的 crate 實作，透過 [`AppState`] 注入。

mod accounts;
mod admission;
mod auth;
mod credential;
mod error;
mod listener;
mod packed;
mod responses;
mod routes;
mod state;

pub use listener::{bind, serve};
pub use routes::router;
pub use state::{AppState, BuildInfo};
