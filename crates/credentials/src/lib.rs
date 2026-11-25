//! 單帳號相容模式的憑據（設計第 4.6 節「憑據檔讀取」與「刷新」）。
//!
//! - 每次請求重讀 `credential_path`：Node 換票後寫回的檔案，下一個請求就生效。
//! - **不自行刷新**：換票只由 Node 的 `refreshCodexSlotAccess` 負責；核心自己刷新會與它競寫憑據檔，
//!   也可能燒掉輪換中的 refresh token。過期的 token 照用，上游回 401 就原樣回給控制面。
//! - 檔案格式 `{accounts: [{access_token, chatgpt_account_id, expires_at, ...}]}`，使用第一筆有 access token 的帳號。
//!   `expires_at` 必須是整數（與上游核心相同；字串會得到 `invalid_json`）。

pub mod stored;

use std::fmt;
use std::path::Path;

use kin_contract::error::KernelError;
use serde::Deserialize;

/// 送上游需要的憑據。`Debug` 不輸出 token。
#[derive(Clone)]
pub struct Credential {
    pub access_token: String,
    pub chatgpt_account_id: Option<String>,
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credential")
            .field("access_token", &"<redacted>")
            .field("chatgpt_account_id", &self.chatgpt_account_id)
            .finish()
    }
}

#[derive(Deserialize)]
struct CredentialFile {
    #[serde(default)]
    accounts: Vec<AccountEntry>,
}

#[derive(Deserialize)]
struct AccountEntry {
    #[serde(default)]
    access_token: Option<String>,
    #[serde(default)]
    chatgpt_account_id: Option<String>,
    /// 只驗證型別，不據此刷新（見模組說明）。
    #[serde(default)]
    #[allow(dead_code)]
    expires_at: Option<i64>,
}

/// 讀取並解析憑據檔。
pub async fn load(path: &Path) -> Result<Credential, KernelError> {
    let raw = tokio::fs::read(path)
        .await
        .map_err(|err| KernelError::InvalidCredential(format!("read credential file: {err}")))?;
    parse(&raw)
}

impl AccountEntry {
    /// 有非空的 access token：推理只會選這樣的列。
    fn usable(&self) -> bool {
        self.access_token
            .as_deref()
            .is_some_and(|token| !token.trim().is_empty())
    }
}

/// 健康回應的帳號數：可解析的憑據檔裡有可用 access token 的列數。檔案不在、無法解析或沒有可用列時為 0，
/// 不將程序可達當作憑據有效。
pub async fn account_count(path: &Path) -> u32 {
    let Ok(raw) = tokio::fs::read(path).await else {
        return 0;
    };
    serde_json::from_slice::<CredentialFile>(&raw).map_or(0, |file| {
        u32::try_from(file.accounts.iter().filter(|row| row.usable()).count()).unwrap_or(u32::MAX)
    })
}

pub fn parse(raw: &[u8]) -> Result<Credential, KernelError> {
    let file: CredentialFile =
        serde_json::from_slice(raw).map_err(|err| KernelError::InvalidCredential(err.to_string()))?;
    let account = file
        .accounts
        .into_iter()
        .find(AccountEntry::usable)
        .ok_or(KernelError::NoCredential)?;
    let access_token = account
        .access_token
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
        .ok_or(KernelError::NoCredential)?;
    let chatgpt_account_id = account
        .chatgpt_account_id
        .map(|id| id.trim().to_owned())
        .filter(|id| !id.is_empty());
    Ok(Credential {
        access_token,
        chatgpt_account_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_first_account() {
        let credential = parse(
            br#"{"accounts":[{"id":"a","access_token":" at-1 ","chatgpt_account_id":"acct","expires_at":1794800000000},
                {"access_token":"at-2"}]}"#,
        )
        .unwrap();
        assert_eq!(credential.access_token, "at-1");
        assert_eq!(credential.chatgpt_account_id.as_deref(), Some("acct"));
        assert!(!format!("{credential:?}").contains("at-1"));
    }

    #[test]
    fn skips_empty_rows_and_uses_the_first_usable_account() {
        let credential = parse(br#"{"accounts":[{"access_token":"  "},{"refresh_token":"rt"},{"access_token":" selected ","chatgpt_account_id":"ws"},{"access_token":"later"}]}"#).unwrap();
        assert_eq!(credential.access_token, "selected");
        assert_eq!(credential.chatgpt_account_id.as_deref(), Some("ws"));
    }

    #[test]
    fn missing_or_empty_tokens_are_no_credential() {
        for raw in [
            r#"{"accounts":[]}"#,
            r#"{}"#,
            r#"{"accounts":[{"access_token":""}]}"#,
            r#"{"accounts":[{"refresh_token":"rt"}]}"#,
        ] {
            assert_eq!(
                parse(raw.as_bytes()).unwrap_err(),
                KernelError::NoCredential,
                "{raw}"
            );
        }
    }

    #[test]
    fn a_string_expiry_is_invalid_json_like_the_upstream_kernel() {
        let err = parse(br#"{"accounts":[{"access_token":"at","expires_at":"2026-11-16T00:00:00Z"}]}"#)
            .unwrap_err();
        assert_eq!(err.code(), "invalid_json");
        assert_eq!(err.status(), 502);
    }

    #[tokio::test]
    async fn the_health_count_only_includes_usable_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("codex-credentials.json");
        assert_eq!(account_count(&path).await, 0);
        for (raw, expected) in [
            (r#"{"accounts":[{"access_token":""},{"refresh_token":"rt"}]}"#, 0),
            (
                r#"{"accounts":[{"access_token":" "},{"access_token":"a"},{"access_token":"b"}]}"#,
                2,
            ),
            (r#"{"accounts":[{"access_token":"a","expires_at":"soon"}]}"#, 0),
            ("not json", 0),
        ] {
            std::fs::write(&path, raw).unwrap();
            assert_eq!(account_count(&path).await, expected, "{raw}");
        }
    }

    #[tokio::test]
    async fn rereads_the_file_on_every_call() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("codex-credentials.json");
        std::fs::write(&path, r#"{"accounts":[{"access_token":"old"}]}"#).unwrap();
        assert_eq!(load(&path).await.unwrap().access_token, "old");
        std::fs::write(&path, r#"{"accounts":[{"access_token":"new"}]}"#).unwrap();
        assert_eq!(load(&path).await.unwrap().access_token, "new");
    }
}
