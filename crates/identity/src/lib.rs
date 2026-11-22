//! 出站身分（設計第 5.3 節；擬真基準見 `fidelity/README.md`）。
//!
//! 對齊官方互動式 TUI：originator `codex-tui`，User-Agent 照官方規則由帳號環境組成：
//! `codex-tui/<版本> (<作業系統> <版本>; <架構>) <終端> (codex-tui; <版本>)`。
//!
//! - 作業系統段重現 os_info 3.14：os-release 的 `ID` 決定名稱，`VERSION_ID` 補成三段數字並去掉前導零。
//! - 終端段重現官方 `terminal-detection`：`TERM_PROGRAM[/TERM_PROGRAM_VERSION]`（tmux 除外），否則 `TERM`。
//! - 只接受實測過的發行版與 `x86_64`（TLS 基準是官方 linux-x64 發行版）；其他情況改用退回身分，
//!   依 `device_id` 從內建的 Linux 設定表穩定挑一組，同一帳號每次都相同。

use sha2::{Digest, Sha256};
use tap_contract::envelope::{ClientEnvironment, OsRelease};

/// 對齊的官方版本。換版時與 TLS 基準、`version` 標頭一起更新。
pub const TARGET_VERSION: &str = "0.171.0";
/// 官方互動式 TUI 的 originator。
pub const ORIGINATOR: &str = "codex-tui";

/// 實測過的發行版：os-release `ID` → os_info 的名稱。
const OS_TYPES: &[(&str, &str)] = &[("debian", "Debian"), ("ubuntu", "Ubuntu"), ("fedora", "Fedora")];
/// TLS 基準只涵蓋 linux-x64。
const MACHINES: &[&str] = &["x86_64"];

/// 退回身分的設定表：每一組都是實測過的環境。
const FALLBACK_PROFILES: &[(&str, &str, &str)] = &[
    ("ubuntu", "24.04", "xterm-256color"),
    ("ubuntu", "22.04", "xterm-256color"),
    ("debian", "12", "xterm-256color"),
    ("fedora", "41", "xterm-256color"),
    ("ubuntu", "24.04", "tmux-256color"),
    ("debian", "12", "tmux-256color"),
];

/// 身分從哪裡來：帳號環境、自訂 User-Agent，或因為環境無法如實表達而改用退回身分。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentitySource {
    Environment,
    Configured,
    Fallback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub originator: String,
    pub version: String,
    pub user_agent: String,
    pub source: IdentitySource,
}

/// 決定出站身分的輸入：帳號環境與自訂 User-Agent。推理、帳號操作與換票都由它組身分，
/// 同一個帳號對上游才只呈現一種身分。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Profile {
    pub environment: Option<ClientEnvironment>,
    /// 自訂 UA（已去頭尾空白並驗證可放進標頭）；`None` 表示依帳號環境產生。
    pub user_agent: Option<String>,
}

impl Profile {
    pub fn identity(&self, device_id: &str) -> Identity {
        resolve(self.environment.as_ref(), self.user_agent.as_deref(), device_id)
    }
}

/// 有自訂 UA 時以它為準：originator 與 version 取自它的第一個產品標記（官方 UA 是
/// `{originator}/{版本} (...)`）；沒有可用的產品標記時沿用官方的 originator 與版本。
/// 沒有自訂 UA 時同 [`compose`]。
pub fn resolve(
    environment: Option<&ClientEnvironment>,
    user_agent: Option<&str>,
    device_id: &str,
) -> Identity {
    let Some(user_agent) = user_agent else {
        return compose(environment, device_id);
    };
    let (originator, version) = advertised(Some(user_agent));
    Identity {
        originator: originator.to_owned(),
        version: version.to_owned(),
        user_agent: user_agent.to_owned(),
        source: IdentitySource::Configured,
    }
}

/// 這個 User-Agent 會帶出的 originator 與 version（`describe` 也據此回報模擬的身分）。
pub fn advertised(user_agent: Option<&str>) -> (&str, &str) {
    user_agent
        .and_then(product_token)
        .unwrap_or((ORIGINATOR, TARGET_VERSION))
}

/// User-Agent 的第一個產品標記 `name/version`，兩段都必須是 HTTP token；格式不符時回 `None`。
pub fn product_token(user_agent: &str) -> Option<(&str, &str)> {
    let (name, version) = user_agent.split_ascii_whitespace().next()?.split_once('/')?;
    let token = |part: &str| {
        !part.is_empty()
            && part
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
    };
    (token(name) && token(version)).then_some((name, version))
}

/// 依帳號環境組出身分；`device_id` 只用來穩定挑選退回身分。
pub fn compose(environment: Option<&ClientEnvironment>, device_id: &str) -> Identity {
    match environment.and_then(user_agent_for) {
        Some(user_agent) => Identity {
            originator: ORIGINATOR.into(),
            version: TARGET_VERSION.into(),
            user_agent,
            source: IdentitySource::Environment,
        },
        None => Identity {
            originator: ORIGINATOR.into(),
            version: TARGET_VERSION.into(),
            user_agent: fallback_user_agent(device_id),
            source: IdentitySource::Fallback,
        },
    }
}

/// 帳號環境能如實表達時，回傳官方在那台機器上會送出的 User-Agent。
fn user_agent_for(environment: &ClientEnvironment) -> Option<String> {
    let machine = environment.machine.as_deref().map(str::trim)?;
    if !MACHINES.contains(&machine) {
        return None;
    }
    let (os_type, os_version) = os_segment(environment.os_release.as_ref()?)?;
    let terminal = terminal_token(environment);
    Some(format_user_agent(os_type, &os_version, machine, &terminal))
}

fn format_user_agent(os_type: &str, os_version: &str, machine: &str, terminal: &str) -> String {
    format!(
        "{ORIGINATOR}/{TARGET_VERSION} ({os_type} {os_version}; {machine}) {terminal} ({ORIGINATOR}; {TARGET_VERSION})"
    )
}

/// os_info 3.14 的名稱與版本格式。
fn os_segment(release: &OsRelease) -> Option<(&'static str, String)> {
    let id = release.id.as_deref()?.trim().to_ascii_lowercase();
    let os_type = OS_TYPES
        .iter()
        .find_map(|(known, name)| (*known == id).then_some(*name))?;
    let version = semantic_version(release.version_id.as_deref()?)?;
    Some((os_type, version))
}

/// `24.04` → `24.4.0`、`12` → `12.0.0`；不是一到三段數字就不接受。
fn semantic_version(raw: &str) -> Option<String> {
    let raw = raw.trim().trim_matches('"');
    let parts: Vec<u64> = raw
        .split('.')
        .map(|part| part.parse::<u64>().ok())
        .collect::<Option<_>>()?;
    if parts.is_empty() || parts.len() > 3 {
        return None;
    }
    let get = |index: usize| parts.get(index).copied().unwrap_or(0);
    Some(format!("{}.{}.{}", get(0), get(1), get(2)))
}

/// 官方 `terminal-detection` 的 `user_agent_token`（限於帳號環境能提供的變數）。
fn terminal_token(environment: &ClientEnvironment) -> String {
    let non_empty = |value: Option<&String>| value.map(|v| v.trim().to_owned()).filter(|v| !v.is_empty());
    let raw = match non_empty(environment.term_program.as_ref()) {
        Some(program) if !program.eq_ignore_ascii_case("tmux") => {
            match non_empty(environment.term_program_version.as_ref()) {
                Some(version) => format!("{program}/{version}"),
                None => program,
            }
        }
        _ => non_empty(environment.term.as_ref()).unwrap_or_else(|| "unknown".to_owned()),
    };
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn fallback_user_agent(device_id: &str) -> String {
    let digest = Sha256::digest(device_id.as_bytes());
    let index = usize::from(digest[0]) % FALLBACK_PROFILES.len();
    let (id, version_id, term) = FALLBACK_PROFILES[index];
    let environment = ClientEnvironment {
        os_release: Some(OsRelease {
            id: Some(id.to_owned()),
            version_id: Some(version_id.to_owned()),
        }),
        machine: Some("x86_64".to_owned()),
        term: Some(term.to_owned()),
        term_program: None,
        term_program_version: None,
    };
    // 設定表裡每一組都可表達；萬一日後被改壞，退到最常見的一組，不讓請求失敗。
    user_agent_for(&environment)
        .unwrap_or_else(|| format_user_agent("Ubuntu", "24.4.0", "x86_64", "xterm-256color"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(id: &str, version_id: &str, machine: &str, term: &str) -> ClientEnvironment {
        ClientEnvironment {
            os_release: Some(OsRelease {
                id: Some(id.to_owned()),
                version_id: Some(version_id.to_owned()),
            }),
            machine: Some(machine.to_owned()),
            term: Some(term.to_owned()),
            term_program: None,
            term_program_version: None,
        }
    }

    fn ua(os: &str, terminal: &str) -> String {
        format!("codex-tui/0.171.0 ({os}; x86_64) {terminal} (codex-tui; 0.171.0)")
    }

    #[test]
    fn reproduces_the_recorded_official_user_agents() {
        // 這四組都由官方 CLI 0.167.1 實測（fidelity/baseline/.../expected-identity.json）。
        let ubuntu = env("ubuntu", "24.04", "x86_64", "xterm-256color");
        assert_eq!(
            compose(Some(&ubuntu), "d").user_agent,
            ua("Ubuntu 24.4.0", "xterm-256color")
        );

        let mut tmux = env("ubuntu", "22.04", "x86_64", "tmux-256color");
        tmux.term_program = Some("tmux".to_owned());
        tmux.term_program_version = Some("3.4".to_owned());
        assert_eq!(
            compose(Some(&tmux), "d").user_agent,
            ua("Ubuntu 22.4.0", "tmux-256color")
        );

        let mut vscode = env("fedora", "41", "x86_64", "xterm-256color");
        vscode.term_program = Some("vscode".to_owned());
        vscode.term_program_version = Some("1.95.3".to_owned());
        assert_eq!(
            compose(Some(&vscode), "d").user_agent,
            ua("Fedora 41.0.0", "vscode/1.95.3")
        );

        let debian = env("debian", "12", "x86_64", "xterm-256color");
        let identity = compose(Some(&debian), "d");
        assert_eq!(identity.user_agent, ua("Debian 12.0.0", "xterm-256color"));
        assert_eq!(identity.source, IdentitySource::Environment);
        assert_eq!(
            (identity.originator.as_str(), identity.version.as_str()),
            ("codex-tui", "0.171.0")
        );
    }

    #[test]
    fn terminal_tokens_are_sanitised_like_the_official_client() {
        let mut odd = env("ubuntu", "24.04", "x86_64", "xterm 256(color)");
        assert!(
            compose(Some(&odd), "d")
                .user_agent
                .contains(") xterm_256_color_ (")
        );
        odd.term = None;
        assert!(compose(Some(&odd), "d").user_agent.contains(") unknown ("));
    }

    #[test]
    fn unusable_environments_fall_back_to_a_stable_linux_identity() {
        let mac = ClientEnvironment {
            machine: Some("arm64".to_owned()),
            term_program: Some("iTerm.app".to_owned()),
            ..ClientEnvironment::default()
        };
        let arm = env("ubuntu", "24.04", "aarch64", "xterm-256color");
        let unknown_distro = env("gentoo", "2.15", "x86_64", "xterm-256color");
        let bad_version = env("ubuntu", "noble", "x86_64", "xterm-256color");
        for environment in [
            None,
            Some(&mac),
            Some(&arm),
            Some(&unknown_distro),
            Some(&bad_version),
        ] {
            let first = compose(environment, "device-1");
            assert_eq!(first.source, IdentitySource::Fallback);
            assert_eq!(compose(environment, "device-1").user_agent, first.user_agent);
            assert!(first.user_agent.starts_with("codex-tui/0.171.0 ("));
            assert!(first.user_agent.contains("; x86_64) "));
        }
    }

    #[test]
    fn every_fallback_profile_is_representable() {
        for (id, version_id, term) in FALLBACK_PROFILES {
            assert!(
                user_agent_for(&env(id, version_id, "x86_64", term)).is_some(),
                "{id} {version_id}"
            );
        }
    }

    #[test]
    fn a_custom_user_agent_carries_its_own_product_token() {
        let debian = env("debian", "12", "x86_64", "xterm-256color");
        let profile = Profile {
            environment: Some(debian.clone()),
            user_agent: Some("codex_exec/0.170.0 (Debian 12.0.0; x86_64) unknown".to_owned()),
        };
        let identity = profile.identity("d");
        assert_eq!(identity.source, IdentitySource::Configured);
        assert_eq!(
            (identity.originator.as_str(), identity.version.as_str()),
            ("codex_exec", "0.170.0")
        );
        // 沒有產品標記（或標記不是 token）時沿用官方的 originator 與版本。
        for odd in ["curl", "a/b/c", "/1", "x/ 1", "名/1"] {
            assert_eq!(advertised(Some(odd)), (ORIGINATOR, TARGET_VERSION), "{odd}");
            assert_eq!(resolve(None, Some(odd), "d").user_agent, odd);
        }
        // 沒有自訂 UA：與 compose 相同。
        let auto = Profile {
            environment: Some(debian.clone()),
            user_agent: None,
        };
        assert_eq!(auto.identity("d"), compose(Some(&debian), "d"));
    }

    #[test]
    fn semantic_versions_follow_os_info() {
        assert_eq!(semantic_version("24.04").as_deref(), Some("24.4.0"));
        assert_eq!(semantic_version("12").as_deref(), Some("12.0.0"));
        assert_eq!(semantic_version("9.4.1").as_deref(), Some("9.4.1"));
        assert_eq!(semantic_version("1.2.3.4"), None);
        assert_eq!(semantic_version("rolling"), None);
        assert_eq!(semantic_version(""), None);
    }
}
