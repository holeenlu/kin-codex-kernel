//! 出口代理：解析設定中的 `proxy_url`，替 WebSocket 撥號，並檢查代理是否可連。
//!
//! HTTP 傳輸交給 reqwest（`Proxy::all`）；WebSocket 由核心自己撥號，不讀環境變數裡的代理：
//! - `socks5h://`：主機名交給代理解析；`socks5://`：本機解析後送位址（與 reqwest 相同）。
//! - `http://`：以 `CONNECT host:port` 建立通道，帳密以 Basic 送出。

use std::io;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// 代理回應 CONNECT 時，標頭最多讀這麼多位元組。
const CONNECT_RESPONSE_LIMIT: usize = 8 * 1024;
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Socks5 { remote_dns: bool },
    HttpConnect,
}

/// 設定中的代理。帳密已做百分比解碼。
#[derive(Clone, PartialEq, Eq)]
pub struct Proxy {
    pub kind: Kind,
    pub host: String,
    pub port: u16,
    pub credentials: Option<(String, String)>,
}

impl std::fmt::Debug for Proxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Proxy")
            .field("kind", &self.kind)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("credentials", &self.credentials.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// 解析的結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parsed {
    None,
    Supported(Proxy),
    /// 其他協定（例如 `https://` 代理）：只給 reqwest 的 HTTP 傳輸使用。
    Unsupported {
        host: String,
        port: u16,
    },
}

pub fn parse(url: &str) -> Parsed {
    let url = url.trim();
    if url.is_empty() {
        return Parsed::None;
    }
    let (scheme, rest) = url.split_once("://").unwrap_or(("http", url));
    let scheme = scheme.to_ascii_lowercase();
    let rest = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    let (credentials, hostport) = match rest.rsplit_once('@') {
        Some((userinfo, hostport)) => {
            let (user, pass) = userinfo.split_once(':').unwrap_or((userinfo, ""));
            (Some((percent_decode(user), percent_decode(pass))), hostport)
        }
        None => (None, rest),
    };
    let default_port = match scheme.as_str() {
        "socks5" | "socks5h" => 1080,
        "https" => 443,
        _ => 80,
    };
    let (host, port) = split_host_port(hostport, default_port);
    let kind = match scheme.as_str() {
        "socks5h" => Kind::Socks5 { remote_dns: true },
        "socks5" => Kind::Socks5 { remote_dns: false },
        "http" => Kind::HttpConnect,
        _ => return Parsed::Unsupported { host, port },
    };
    Parsed::Supported(Proxy {
        kind,
        host,
        port,
        credentials,
    })
}

fn split_host_port(hostport: &str, default_port: u16) -> (String, u16) {
    if let Some(rest) = hostport.strip_prefix('[') {
        // [IPv6]:port
        let (host, after) = rest.split_once(']').unwrap_or((rest, ""));
        let port = after
            .strip_prefix(':')
            .and_then(|p| p.parse().ok())
            .unwrap_or(default_port);
        return (host.to_owned(), port);
    }
    match hostport.rsplit_once(':') {
        Some((host, port)) => (host.to_owned(), port.parse().unwrap_or(default_port)),
        None => (hostport.to_owned(), default_port),
    }
}

fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2]))
        {
            out.push(hi << 4 | lo);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// 代理能否連上（只建立 TCP 連線，不送任何資料）。
pub async fn reachable(host: &str, port: u16) -> bool {
    matches!(
        tokio::time::timeout(PROBE_TIMEOUT, TcpStream::connect((host, port))).await,
        Ok(Ok(_))
    )
}

/// 經 `CONNECT` 建立到 `target_host:target_port` 的通道。
pub async fn http_connect(proxy: &Proxy, target_host: &str, target_port: u16) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect((proxy.host.as_str(), proxy.port)).await?;
    let authority = if target_host.contains(':') {
        format!("[{target_host}]:{target_port}")
    } else {
        format!("{target_host}:{target_port}")
    };
    let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if let Some((user, pass)) = &proxy.credentials {
        request.push_str(&format!(
            "Proxy-Authorization: Basic {}\r\n",
            base64(format!("{user}:{pass}").as_bytes())
        ));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::with_capacity(256);
    let mut byte = [0_u8; 1];
    // 逐位元組讀到空行為止：代理在 200 之後不會先送資料，不能多讀到 TLS 的位元組。
    while !response.ends_with(b"\r\n\r\n") {
        if response.len() >= CONNECT_RESPONSE_LIMIT || stream.read(&mut byte).await? == 0 {
            return Err(io::Error::other("proxy closed the CONNECT tunnel"));
        }
        response.push(byte[0]);
    }
    let status_line = response.split(|&b| b == b'\r').next().unwrap_or_default();
    let status = String::from_utf8_lossy(status_line);
    let code = status.split_whitespace().nth(1).unwrap_or_default();
    if code != "200" {
        return Err(io::Error::other(format!("proxy refused CONNECT: {status}")));
    }
    Ok(stream)
}

fn base64(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = u32::from(b[0]) << 16 | u32::from(b[1]) << 8 | u32::from(b[2]);
        for (i, shift) in [18, 12, 6, 0].into_iter().enumerate() {
            if i <= chunk.len() {
                out.push(char::from(TABLE[(n >> shift) as usize & 63]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn supported(url: &str) -> Proxy {
        match parse(url) {
            Parsed::Supported(proxy) => proxy,
            other => unreachable!("{url}: {other:?}"),
        }
    }

    #[test]
    fn parses_the_proxy_kinds_and_decodes_credentials() {
        let socks = supported("socks5h://u1:p%40ss@10.0.0.1:1080");
        assert_eq!(socks.kind, Kind::Socks5 { remote_dns: true });
        assert_eq!((socks.host.as_str(), socks.port), ("10.0.0.1", 1080));
        assert_eq!(socks.credentials, Some(("u1".to_owned(), "p@ss".to_owned())));
        assert!(!format!("{socks:?}").contains("p@ss"));

        let local = supported("socks5://127.0.0.1:9050");
        assert_eq!(local.kind, Kind::Socks5 { remote_dns: false });

        let http = supported("http://proxy.local:3128/");
        assert_eq!(
            (http.kind, http.host.as_str(), http.port),
            (Kind::HttpConnect, "proxy.local", 3128)
        );

        assert_eq!(
            parse("https://proxy.local"),
            Parsed::Unsupported {
                host: "proxy.local".to_owned(),
                port: 443
            }
        );
        assert_eq!(parse("  "), Parsed::None);
        assert_eq!(percent_decode("a%2"), "a%2");
        assert_eq!(percent_decode("%7e%7E"), "~~");
    }

    #[test]
    fn base64_matches_the_standard_alphabet() {
        assert_eq!(base64(b"u1:p@ss"), "dTE6cEBzcw==");
        assert_eq!(base64(b"ab"), "YWI=");
        assert_eq!(base64(b"abc"), "YWJj");
        assert_eq!(base64(b""), "");
    }
}
