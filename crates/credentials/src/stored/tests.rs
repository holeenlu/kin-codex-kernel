#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::os::unix::fs::PermissionsExt;

use serde_json::json;

use super::*;

fn b64(value: &Value) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let bytes = serde_json::to_vec(value).unwrap();
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = u32::from(b[0]) << 16 | u32::from(b[1]) << 8 | u32::from(b[2]);
        for i in 0..=chunk.len() {
            out.push(char::from(TABLE[(n >> (18 - 6 * i)) as usize & 63]));
        }
    }
    out
}

fn jwt(claims: &Value) -> String {
    format!("{}.{}.sig", b64(&json!({"alg": "RS256"})), b64(claims))
}

fn token(user: &str, workspace: &str, exp: i64) -> String {
    jwt(&json!({
        "sub": format!("sub-{user}"),
        "exp": exp,
        "https://api.openai.com/auth": {"chatgpt_user_id": user, "chatgpt_account_id": workspace},
    }))
}

async fn write(path: &Path, doc: Value) {
    tokio::fs::write(path, serde_json::to_vec(&doc).unwrap())
        .await
        .unwrap();
}

#[test]
fn decodes_jwt_claims_and_the_account_key() {
    let claims = jwt_claims(&token("user-1", "ws-1", 1_800_000_000)).unwrap();
    assert_eq!(claims["exp"], 1_800_000_000);
    let parsed = parse(
        serde_json::to_vec(&json!({"accounts": [{
            "access_token": token("user-1", "ws-1", 1_800_000_000),
            "id_token": token("user-1", "ws-1", 1_800_000_000),
            "refresh_token": "rt",
        }]}))
        .unwrap()
        .as_slice(),
    )
    .unwrap();
    assert_eq!(parsed.access_expires_at, Some(1_800_000_000));
    assert_eq!(
        parsed.key,
        Some(AccountKey {
            user: "user-1".to_owned(),
            workspace: "ws-1".to_owned()
        })
    );
    assert_eq!(parsed.expires_at_ms(), Some(1_800_000_000_000));
    assert_eq!(parsed.generation, 0);
    assert!(!format!("{parsed:?}").contains("rt"), "Debug never prints tokens");
    assert!(jwt_claims("not-a-jwt").is_none());
}

#[tokio::test]
async fn a_refresh_is_written_atomically_with_the_next_generation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("codex-credentials.shared.json");
    write(
        &path,
        json!({"accounts": [{"id": "a", "email": "a@example.invalid", "access_token": token("u", "w", 100),
            "refresh_token": "rt-1", "id_token": token("u", "w", 100), "chatgpt_account_id": "w", "expires_at": 100_000}],
            "kin_credential_generation": 4}),
    )
    .await;
    let seen = read(&path).await.unwrap().unwrap();
    let refreshed = Refreshed {
        access_token: Some(token("u", "w", 2_000_000_000)),
        refresh_token: Some("rt-2".to_owned()),
        id_token: None,
        expires_in: Some(3600),
    };
    let next = commit_refresh(&path, &seen, &refreshed).await.unwrap();
    assert_eq!(next.generation, 5);
    assert_eq!(next.refresh_token.as_deref(), Some("rt-2"));
    assert_eq!(next.expires_at_ms(), Some(2_000_000_000_000));
    let on_disk: Value = serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
    let account = &on_disk["accounts"][0];
    assert_eq!(
        account["expires_at"], 2_000_000_000_000_i64,
        "expires_at is in milliseconds, like vm2api"
    );
    assert_eq!(account["email"], "a@example.invalid", "other fields are kept");
    assert_eq!(
        account["id_token"],
        json!(token("u", "w", 100)),
        "a missing id_token keeps the old one"
    );
    assert!(account["last_refresh"].as_i64().unwrap() > 0);
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert_eq!(leftovers.len(), 1, "no temporary file is left behind");
}

#[tokio::test]
async fn a_late_refresh_never_overwrites_a_newer_credential() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.json");
    write(
        &path,
        json!({"accounts": [{"access_token": token("u", "w", 1), "refresh_token": "rt-1"}]}),
    )
    .await;
    let seen = read(&path).await.unwrap().unwrap();
    // 刷新途中，控制面匯入了另一個帳號
    let generation = import(
        &path,
        json!({"access_token": token("other", "w2", 9_999_999_999_i64), "refresh_token": "rt-other"})
            .as_object()
            .unwrap()
            .clone(),
        0,
    )
    .await
    .unwrap();
    assert_eq!(generation, 1);
    let late = Refreshed {
        access_token: Some(token("u", "w", 2_000_000_000)),
        ..Refreshed::default()
    };
    assert!(matches!(
        commit_refresh(&path, &seen, &late).await,
        Err(CommitError::Conflict)
    ));
    let kept = read(&path).await.unwrap().unwrap();
    assert_eq!(kept.refresh_token.as_deref(), Some("rt-other"));
    assert_eq!(kept.generation, 1);
}

#[tokio::test]
async fn import_is_a_compare_and_set_on_the_credential_generation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("vms").join("a").join("c.json");
    let credential = |rt: &str| {
        json!({"access_token": token("u", "w", 2_000_000_000), "refresh_token": rt})
            .as_object()
            .unwrap()
            .clone()
    };
    assert_eq!(
        import(&path, credential("rt-1"), 0).await.unwrap(),
        1,
        "creates the directory and the file"
    );
    let stored = read(&path).await.unwrap().unwrap();
    assert_eq!(stored.expires_at_ms(), Some(2_000_000_000_000));
    let on_disk: Value = serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
    assert_eq!(on_disk["accounts"][0]["expires_at"], 2_000_000_000_000_i64);
    assert_eq!(
        import(&path, credential("rt-2"), 0).await.unwrap_err().code(),
        "generation_conflict"
    );
    assert_eq!(import(&path, credential("rt-2"), 1).await.unwrap(), 2);
    let no_access = json!({"refresh_token": "rt"}).as_object().unwrap().clone();
    assert_eq!(
        import(&path, no_access, 2).await.unwrap_err().code(),
        "invalid_request"
    );
    let bad_expiry = json!({"access_token": "at", "expires_at": "soon"})
        .as_object()
        .unwrap()
        .clone();
    assert_eq!(
        import(&path, bad_expiry, 2).await.unwrap_err().code(),
        "invalid_request"
    );
    assert_eq!(
        read(&dir.path().join("missing.json"))
            .await
            .unwrap()
            .map(|s| s.generation),
        None
    );
}

#[tokio::test]
async fn refresh_updates_the_selected_row_and_preserves_empty_predecessors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("credential.json");
    let empty = json!({"access_token":" ","refresh_token":"untouched"});
    write(
        &path,
        json!({"accounts":[empty.clone(), {"access_token":"selected", "refresh_token":"rt-1"}]}),
    )
    .await;
    let seen = read(&path).await.unwrap().unwrap();
    assert_eq!(seen.access_token.as_deref(), Some("selected"));
    let next = commit_refresh(
        &path,
        &seen,
        &Refreshed {
            access_token: Some("rotated".into()),
            refresh_token: Some("rt-2".into()),
            id_token: None,
            expires_in: Some(3600),
        },
    )
    .await
    .unwrap();
    assert_eq!(next.access_token.as_deref(), Some("rotated"));
    let doc: Value = serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
    assert_eq!(doc["accounts"][0], empty);
    assert_eq!(doc["accounts"][1]["refresh_token"], "rt-2");
    assert_eq!(crate::load(&path).await.unwrap().access_token, "rotated");
}
