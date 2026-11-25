//! `GET /internal/health` 與 `GET /internal/v1/describe` 的回應格式。

use serde::Serialize;

use crate::abi::{Abi, Capability};

/// 健康回應。欄位對應 vm2api `workerHealth` 讀取的值。
///
/// 刻意**沒有** `status` 欄位：vm2api 會把回應本體展開蓋在 HTTP 狀態之上，
/// 本體裡若有非數字的 `status`，健康判斷就會失敗。
#[derive(Debug, Clone, Serialize)]
pub struct Health {
    pub uds_packed_v1: bool,
    pub ok: bool,
    pub engine: &'static str,
    pub provider: &'static str,
    pub version: &'static str,
    /// 上游核心回報的版本欄位名稱；控制面讀 `version` 或 `worker_version`，兩個都給。
    pub worker_version: &'static str,
    pub build_sha: &'static str,
    pub abi: Abi,
    pub vm_id: String,
    /// 與上游核心一致，回報這個槽的安裝身分（不是秘密）。
    pub device_id: String,
    pub accounts: u32,
    /// 程序在服務中（與上游核心相同，控制面以 HTTP 狀態判斷可達，不讀這個值）。
    pub reachable: bool,
    /// 沒有代理時為 true；有代理時是最近一次連到代理的結果（上游核心固定回 true）。
    pub proxy_ok: Option<bool>,
}

/// 能力宣告。控制面依 `abi` 與 `capabilities` 決定可以使用哪些路徑。
#[derive(Debug, Clone, Serialize)]
pub struct Describe {
    pub abi: Abi,
    pub kernel: &'static str,
    pub kernel_version: &'static str,
    pub build_sha: &'static str,
    pub capabilities: Vec<Capability>,
    /// 出站時模擬的官方客戶端身分；上游傳輸完成前為 `null`。
    pub impersonate: Option<Impersonate>,
}

/// 預設的出站身分。單帳號模式設定了自訂 User-Agent 時，是由它的產品標記帶出的 originator 與版本；
/// 逐請求的自訂 UA 不在此列。
#[derive(Debug, Clone, Serialize)]
pub struct Impersonate {
    pub originator: String,
    pub version: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_never_serializes_a_status_field() {
        let health = Health {
            uds_packed_v1: true,
            ok: true,
            engine: "codex",
            provider: "kin-codex-kernel",
            version: "0.0.1",
            worker_version: "0.0.1",
            build_sha: "dev",
            abi: Abi::SingleAccount,
            vm_id: "vm-1".to_owned(),
            device_id: "device-1".to_owned(),
            accounts: 1,
            reachable: true,
            proxy_ok: None,
        };
        let value = serde_json::to_value(health).unwrap();
        assert!(value.get("status").is_none());
        assert_eq!(value["abi"], 1);
        assert!(value["proxy_ok"].is_null());
    }
}
