//! 每個帳號的 cookie：只保存官方允許的 Cloudflare 基礎設施 cookie 與 `__oailb` 路由 cookie
//! （官方 CLI 0.167.1 的 `codex-rs/http-client/src/chatgpt_cloudflare_cookies.rs`），其他 cookie 一律不存、不送。
//!
//! 擬真基準（`fidelity/baseline/.../ops-and-cookies.json`，2026-11-16 錄得）：官方收到 Set-Cookie 後，
//! 之後對 chatgpt.com 的 HTTPS 請求與 WebSocket 握手都帶回允許的 cookie；工作階段 cookie 與其他 cookie 被丟掉。
//! HTTP 的 `cookie` 標頭由 reqwest 的 cookie provider 加在所有明確標頭之後；WebSocket 由這裡在插入順序的最後加上。
//!
//! 官方的 cookie 罐是整個程序共用一份。共享模式一個程序服務多個帳號，路由 cookie 不能在帳號之間沿用，
//! 所以每個帳號執行環境各一份；出口改變時執行環境重建，cookie 也跟著重來（Cloudflare 的 cookie 本來就跟出口綁定）。

use reqwest::Url;
use reqwest::cookie::{CookieStore, Jar};
use reqwest::header::HeaderValue;

/// 官方的允許清單（`is_allowed_cloudflare_cookie_name`）。
fn allowed(name: &str) -> bool {
    matches!(
        name,
        "__cf_bm"
            | "__cflb"
            | "__cfruid"
            | "__cfseq"
            | "__cfwaitingroom"
            | "__oailb"
            | "_cfuvid"
            | "cf_clearance"
            | "cf_ob_info"
            | "cf_use_ob"
    ) || name.starts_with("cf_chl_")
}

/// 官方的主機清單（`chatgpt_hosts.rs` 的 `is_allowed_chatgpt_host`）。
fn chatgpt_host(host: &str) -> bool {
    matches!(host, "chatgpt.com" | "chat.openai.com" | "chatgpt-staging.com")
        || host.ends_with(".chatgpt.com")
        || host.ends_with(".chatgpt-staging.com")
}

fn set_cookie_name(header: &HeaderValue) -> Option<&str> {
    let (name, _) = header.to_str().ok()?.split_once('=')?;
    let name = name.trim();
    (!name.is_empty()).then_some(name)
}

fn only_allowed(header: &HeaderValue) -> Option<HeaderValue> {
    let kept: Vec<&str> = header
        .to_str()
        .ok()?
        .split(';')
        .map(str::trim)
        .filter(|cookie| {
            cookie
                .split_once('=')
                .is_some_and(|(name, _)| allowed(name.trim()))
        })
        .collect();
    if kept.is_empty() {
        None
    } else {
        HeaderValue::from_str(&kept.join("; ")).ok()
    }
}

/// 一個帳號執行環境的 cookie 罐。
#[derive(Debug, Default)]
pub struct AccountCookies {
    jar: Jar,
    /// 測試端點（`test_endpoints`）：受控的假上游走 http 與本機位址，照樣適用。正式設定一律為 false。
    test_hosts: bool,
}

impl AccountCookies {
    pub fn new(test_hosts: bool) -> Self {
        Self {
            jar: Jar::default(),
            test_hosts,
        }
    }

    fn applies(&self, url: &Url) -> bool {
        if self.test_hosts {
            return matches!(url.scheme(), "http" | "https");
        }
        url.scheme() == "https" && url.host_str().is_some_and(chatgpt_host)
    }

    /// WebSocket 握手的 cookie 範圍與 HTTPS 相同（官方 `chatgpt_cookie_url`）。
    fn http_url(url: &str) -> Option<Url> {
        let mut url = Url::parse(url).ok()?;
        let scheme = match url.scheme() {
            "wss" => "https",
            "ws" => "http",
            _ => return Some(url),
        };
        url.set_scheme(scheme).ok()?;
        Some(url)
    }

    /// WebSocket 握手要帶的 `cookie` 標頭。
    pub fn header_for(&self, url: &str) -> Option<HeaderValue> {
        self.cookies(&Self::http_url(url)?)
    }

    /// 保存 WebSocket 握手回應的 Set-Cookie。
    pub fn store_from<'a>(&self, url: &str, headers: impl Iterator<Item = &'a HeaderValue>) {
        if let Some(url) = Self::http_url(url) {
            let mut headers = headers;
            self.set_cookies(&mut headers, &url);
        }
    }
}

impl CookieStore for AccountCookies {
    fn set_cookies(&self, cookie_headers: &mut dyn Iterator<Item = &HeaderValue>, url: &Url) {
        if !self.applies(url) {
            return;
        }
        let mut kept = cookie_headers.filter(|header| set_cookie_name(header).is_some_and(allowed));
        self.jar.set_cookies(&mut kept, url);
    }

    fn cookies(&self, url: &Url) -> Option<HeaderValue> {
        if !self.applies(url) {
            return None;
        }
        self.jar.cookies(url).as_ref().and_then(only_allowed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(cookies: &AccountCookies, url: &str, headers: &[&'static str]) {
        let values: Vec<HeaderValue> = headers.iter().map(|h| HeaderValue::from_static(h)).collect();
        cookies.store_from(url, values.iter());
    }

    #[test]
    fn keeps_only_the_official_allowlist_for_chatgpt_hosts() {
        let cookies = AccountCookies::new(false);
        store(
            &cookies,
            "https://chatgpt.com/backend-api/wham/usage",
            &[
                "__cf_bm=a; Path=/; Domain=.chatgpt.com; Secure; HttpOnly",
                "__oailb=b; Path=/; Secure",
                "__Secure-next-auth.session-token=secret; Path=/; Secure",
                "oai-did=device; Path=/; Secure",
                "cf_chl_rc_ni=c; Path=/; Secure",
            ],
        );
        let header = cookies
            .header_for("wss://chatgpt.com/backend-api/codex/responses")
            .unwrap_or_else(|| HeaderValue::from_static(""));
        let mut names: Vec<&str> = header
            .to_str()
            .unwrap_or_default()
            .split("; ")
            .filter_map(|cookie| cookie.split_once('=').map(|(name, _)| name))
            .collect();
        names.sort_unstable();
        assert_eq!(names, ["__cf_bm", "__oailb", "cf_chl_rc_ni"]);
        // 其他主機不送、不存
        assert!(
            cookies
                .header_for("https://auth.openai.com/oauth/token")
                .is_none()
        );
        store(&cookies, "https://example.com/", &["__cf_bm=x; Path=/"]);
        assert!(cookies.header_for("https://example.com/").is_none());
        assert!(
            cookies.header_for("http://chatgpt.com/").is_none(),
            "only https in production"
        );
    }

    #[test]
    fn test_endpoints_accept_the_controlled_upstream() {
        let cookies = AccountCookies::new(true);
        store(
            &cookies,
            "http://127.0.0.1:9/backend-api/codex/responses",
            &["__oailb=lb; Path=/"],
        );
        assert_eq!(
            cookies.header_for("ws://127.0.0.1:9/backend-api/codex/responses"),
            Some(HeaderValue::from_static("__oailb=lb"))
        );
    }
}
