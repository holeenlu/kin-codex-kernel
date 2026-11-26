//! 共享模式的憑據檔：核心是唯一寫入者（vm2api 設計第 5.4 節）。
//!
//! - 檔案格式沿用 `{accounts: [{access_token, refresh_token, id_token, expires_at, chatgpt_account_id, ...}]}`，
//!   使用第一筆有 access token 的帳號（都沒有時用第一筆，以便換票）；頂層另加 `kin_credential_generation`
//!   （單調遞增）。寫回時只改那一列，保留其他列與欄位。
//! - **提交時比對：** 刷新結果寫回前重讀檔案，帳號唯一鍵或 refresh token 與刷新開始時不同，
//!   就放棄寫入。若只有代次改變，仍提交輪換後的 token。
//! - 寫入：同目錄暫存檔、fsync、rename、fsync 目錄，權限 0600。`expires_at` 與 vm2api 相同用 Unix 毫秒。
//!
//! JWT 只解碼不驗簽：用來判斷到期時間與帳號身分，不是授權依據。

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use kin_contract::error::KernelError;
use serde_json::{Map, Value};
use tokio::io::AsyncWriteExt;

use crate::Credential;

/// 帳號唯一鍵：登入身分 × 工作區（設計第 4.0 節）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountKey {
    pub user: String,
    pub workspace: String,
}

/// 讀到的憑據檔。`Debug` 不輸出 token。
#[derive(Clone)]
pub struct Stored {
    doc: Value,
    pub generation: u64,
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub chatgpt_account_id: Option<String>,
    /// access token 的 JWT `exp`（Unix 秒）。
    pub access_expires_at: Option<i64>,
    /// 上次刷新成功的時間（Unix 毫秒）。
    pub last_refresh: Option<i64>,
    pub key: Option<AccountKey>,
}

impl std::fmt::Debug for Stored {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stored")
            .field("generation", &self.generation)
            .field("has_access_token", &self.access_token.is_some())
            .field("has_refresh_token", &self.refresh_token.is_some())
            .field("access_expires_at", &self.access_expires_at)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl Stored {
    /// 推理要用的憑據；沒有 access token 時與單帳號模式相同回 `no_credential`。
    pub fn credential(&self) -> Result<Credential, KernelError> {
        Ok(Credential {
            access_token: self.access_token.clone().ok_or(KernelError::NoCredential)?,
            chatgpt_account_id: self.chatgpt_account_id.clone(),
        })
    }

    /// 新 access token 的到期時間（Unix 毫秒），供回應與健康回報使用。
    pub fn expires_at_ms(&self) -> Option<u64> {
        self.access_expires_at
            .and_then(|seconds| u64::try_from(seconds).ok())
            .map(|seconds| seconds.saturating_mul(1000))
    }
}

/// 換票成功時 token 端點回的內容（官方 `RefreshResponse`：三個欄位都可能省略）。
#[derive(Clone, Default)]
pub struct Refreshed {
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub id_token: Option<String>,
    pub expires_in: Option<u64>,
}

impl std::fmt::Debug for Refreshed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Refreshed")
            .field("has_access_token", &self.access_token.is_some())
            .field("has_refresh_token", &self.refresh_token.is_some())
            .field("expires_in", &self.expires_in)
            .finish_non_exhaustive()
    }
}

/// 刷新結果寫回失敗的原因。
#[derive(Debug)]
pub enum CommitError {
    /// 檔案在刷新途中被換掉（匯入或另一次刷新）；這次結果作廢。
    Conflict,
    Io(KernelError),
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

fn text(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

/// base64url（無補位）解碼。
fn base64url(input: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut buffer = 0u32;
    let mut bits = 0u32;
    for byte in input.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' | b'+' => 62,
            b'_' | b'/' => 63,
            b'=' => break,
            _ => return None,
        };
        buffer = (buffer << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(u8::try_from((buffer >> bits) & 0xff).ok()?);
        }
    }
    Some(out)
}

/// JWT 的 payload（不驗簽）。
pub fn jwt_claims(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    serde_json::from_slice(&base64url(payload)?).ok()
}

fn auth_claims(claims: Option<&Value>) -> Option<&Map<String, Value>> {
    claims?.get("https://api.openai.com/auth")?.as_object()
}

/// 與 vm2api `codexAccountKey` 相同的取法：使用者先取 auth 宣告的 `chatgpt_user_id`／`user_id`，再退回 `sub`；
/// 工作區先取憑據檔的 `chatgpt_account_id`，再取宣告。
fn account_key(
    id_token: Option<&str>,
    access_token: Option<&str>,
    workspace: Option<&str>,
) -> Option<AccountKey> {
    let id = id_token.and_then(jwt_claims);
    let access = access_token.and_then(jwt_claims);
    let (id_auth, access_auth) = (auth_claims(id.as_ref()), auth_claims(access.as_ref()));
    let claim = |auth: Option<&Map<String, Value>>, name: &str| text(auth.and_then(|auth| auth.get(name)));
    let user = claim(id_auth, "chatgpt_user_id")
        .or_else(|| claim(id_auth, "user_id"))
        .or_else(|| claim(access_auth, "chatgpt_user_id"))
        .or_else(|| claim(access_auth, "user_id"))
        .or_else(|| text(id.as_ref().and_then(|claims| claims.get("sub"))))
        .or_else(|| text(access.as_ref().and_then(|claims| claims.get("sub"))))?;
    let workspace = workspace
        .map(str::to_owned)
        .or_else(|| claim(id_auth, "chatgpt_account_id"))
        .or_else(|| claim(access_auth, "chatgpt_account_id"))?;
    Some(AccountKey { user, workspace })
}

// 沿用每槽的有效列；只有刷新憑據時保留第一筆，以便換票復原。
fn selected_index(accounts: &[Value]) -> usize {
    accounts
        .iter()
        .position(|a| text(a.get("access_token")).is_some())
        .unwrap_or(0)
}

fn parse(raw: &[u8]) -> Result<Stored, KernelError> {
    let doc: Value =
        serde_json::from_slice(raw).map_err(|err| KernelError::InvalidCredential(err.to_string()))?;
    let generation = doc
        .get("kin_credential_generation")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let account = doc
        .get("accounts")
        .and_then(Value::as_array)
        .and_then(|accounts| accounts.get(selected_index(accounts)))
        .and_then(Value::as_object);
    let field = |name: &str| text(account.and_then(|account| account.get(name)));
    // 與單帳號模式相同：`expires_at` 存在就必須是整數。
    if let Some(expires_at) = account.and_then(|account| account.get("expires_at"))
        && !expires_at.is_null()
        && !expires_at.is_i64()
        && !expires_at.is_u64()
    {
        return Err(KernelError::InvalidCredential(
            "expires_at must be an integer".to_owned(),
        ));
    }
    let access_token = field("access_token");
    let id_token = field("id_token");
    let chatgpt_account_id = field("chatgpt_account_id");
    let access_expires_at = access_token
        .as_deref()
        .and_then(jwt_claims)
        .and_then(|claims| claims.get("exp").and_then(Value::as_i64));
    let last_refresh = account
        .and_then(|account| account.get("last_refresh"))
        .and_then(Value::as_i64);
    let key = account_key(
        id_token.as_deref(),
        access_token.as_deref(),
        chatgpt_account_id.as_deref(),
    );
    Ok(Stored {
        refresh_token: field("refresh_token"),
        doc,
        generation,
        access_token,
        chatgpt_account_id,
        access_expires_at,
        last_refresh,
        key,
    })
}

/// 帳號清單只需頂層代次。略過帳號陣列的反序列化與 JWT 解碼。
pub async fn read_generation(path: &Path) -> Result<Option<u64>, KernelError> {
    #[derive(serde::Deserialize)]
    struct Generation {
        #[serde(default)]
        kin_credential_generation: Value,
    }
    let Some(bytes) = read_bytes(path).await? else {
        return Ok(None);
    };
    let generation: Generation =
        serde_json::from_slice(&bytes).map_err(|err| KernelError::InvalidCredential(err.to_string()))?;
    Ok(Some(
        generation.kin_credential_generation.as_u64().unwrap_or_default(),
    ))
}

/// 讀憑據檔；檔案不存在時回 `None`。
pub async fn read(path: &Path) -> Result<Option<Stored>, KernelError> {
    read_bytes(path).await?.map(|raw| parse(&raw)).transpose()
}

async fn read_bytes(path: &Path) -> Result<Option<Vec<u8>>, KernelError> {
    match tokio::fs::read(path).await {
        Ok(raw) => Ok(Some(raw)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(KernelError::InvalidCredential(format!(
            "read credential file: {err}"
        ))),
    }
}

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// 原子寫入：同目錄暫存檔（0600）、fsync、rename，再 fsync 目錄。
async fn write_atomic(path: &Path, doc: &Value) -> Result<(), KernelError> {
    let io = |err: std::io::Error| KernelError::InvalidCredential(format!("write credential file: {err}"));
    let dir = path
        .parent()
        .ok_or_else(|| KernelError::InvalidCredential("credential path has no directory".to_owned()))?;
    tokio::fs::create_dir_all(dir).await.map_err(io)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("credentials");
    let temp = dir.join(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let bytes =
        serde_json::to_vec_pretty(doc).map_err(|err| KernelError::InvalidCredential(err.to_string()))?;
    let written = async {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)
            .await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&temp, path).await
    }
    .await;
    if let Err(err) = written {
        let _ = tokio::fs::remove_file(&temp).await;
        return Err(io(err));
    }
    // 目錄的 fsync 讓 rename 本身在斷電後也留下來；失敗不影響檔案內容的完整性，不視為寫入失敗。
    if let Ok(dir) = tokio::fs::File::open(dir).await {
        let _ = dir.sync_all().await;
    }
    Ok(())
}

fn account_entry(doc: &mut Value) -> Option<&mut Map<String, Value>> {
    let accounts = doc.get_mut("accounts")?.as_array_mut()?;
    let index = selected_index(accounts);
    accounts.get_mut(index)?.as_object_mut()
}

/// 把刷新結果寫回。呼叫端必須持有該帳號的寫入鎖；檔案在刷新途中被換掉時回 [`CommitError::Conflict`]。
pub async fn commit_refresh(
    path: &Path,
    seen: &Stored,
    refreshed: &Refreshed,
) -> Result<Stored, CommitError> {
    let current = read(path)
        .await
        .map_err(CommitError::Io)?
        .ok_or(CommitError::Conflict)?;
    // 代次變了但帳號與 refresh token 沒變（例如同一份憑據被重新匯入）：換到的票仍然屬於這份憑據，
    // 照樣寫回；丟掉的話，檔案裡留下的就是剛被消耗的 refresh token。
    if current.key != seen.key || current.refresh_token != seen.refresh_token {
        return Err(CommitError::Conflict);
    }
    let mut doc = current.doc;
    let now = now_ms();
    let access_token = refreshed.access_token.clone().or(current.access_token);
    let expires_at = access_token
        .as_deref()
        .and_then(jwt_claims)
        .and_then(|claims| claims.get("exp").and_then(Value::as_i64))
        .map(|seconds| seconds.saturating_mul(1000))
        .or_else(|| {
            refreshed
                .expires_in
                .and_then(|seconds| i64::try_from(seconds).ok())
                .map(|seconds| now.saturating_add(seconds.saturating_mul(1000)))
        });
    let Some(account) = account_entry(&mut doc) else {
        return Err(CommitError::Conflict);
    };
    if let Some(token) = access_token {
        account.insert("access_token".to_owned(), Value::String(token));
    }
    if let Some(token) = &refreshed.refresh_token {
        account.insert("refresh_token".to_owned(), Value::String(token.clone()));
    }
    if let Some(token) = &refreshed.id_token {
        account.insert("id_token".to_owned(), Value::String(token.clone()));
    }
    if let Some(expires_at) = expires_at {
        account.insert("expires_at".to_owned(), Value::from(expires_at));
    }
    account.insert("last_refresh".to_owned(), Value::from(now));
    if let Some(object) = doc.as_object_mut() {
        object.insert(
            "kin_credential_generation".to_owned(),
            Value::from(current.generation + 1),
        );
    }
    write_atomic(path, &doc).await.map_err(CommitError::Io)?;
    let raw = serde_json::to_vec(&doc)
        .map_err(|err| CommitError::Io(KernelError::InvalidCredential(err.to_string())))?;
    parse(&raw).map_err(CommitError::Io)
}

/// 匯入或替換憑據（`PUT .../credential`）：代次 CAS，回傳新的代次。呼叫端必須持有該帳號的寫入鎖。
pub async fn import(
    path: &Path,
    mut credential: Map<String, Value>,
    expected_generation: u64,
) -> Result<u64, KernelError> {
    let current = read(path).await?;
    let generation = current.as_ref().map_or(0, |stored| stored.generation);
    if generation != expected_generation {
        return Err(KernelError::GenerationConflict);
    }
    if text(credential.get("access_token")).is_none() {
        return Err(KernelError::InvalidBody(
            "credential needs an access_token".to_owned(),
        ));
    }
    match credential.get("expires_at") {
        None | Some(Value::Null) => {
            let exp = text(credential.get("access_token"))
                .as_deref()
                .and_then(jwt_claims)
                .and_then(|claims| claims.get("exp").and_then(Value::as_i64));
            if let Some(seconds) = exp {
                credential.insert("expires_at".to_owned(), Value::from(seconds.saturating_mul(1000)));
            }
        }
        Some(value) if value.is_i64() || value.is_u64() => {}
        Some(_) => {
            return Err(KernelError::InvalidBody(
                "expires_at must be an integer".to_owned(),
            ));
        }
    }
    let mut doc = current.map_or_else(|| Value::Object(Map::new()), |stored| stored.doc);
    let Some(object) = doc.as_object_mut() else {
        return Err(KernelError::InvalidCredential(
            "credential file is not an object".to_owned(),
        ));
    };
    object.insert(
        "accounts".to_owned(),
        Value::Array(vec![Value::Object(credential)]),
    );
    object.insert(
        "kin_credential_generation".to_owned(),
        Value::from(generation + 1),
    );
    write_atomic(path, &doc).await?;
    Ok(generation + 1)
}

#[cfg(test)]
mod tests;
