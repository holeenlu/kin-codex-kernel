# 擬真基準

這裡保存「線上實際長什麼樣子」的錄製結果，作為 tap-codex-kernel 逐項比對的依據。
這些比對資料放在本倉庫，不隨 vm2api 發布。

## 錄製方式

所有錄製都在 `--network none` 的 linux/amd64 容器裡進行，不會連到外部網路。
容器內以 `--add-host` 把 `chatgpt.com`、`auth.openai.com`（官方 CLI 還有 `ab.chatgpt.com`）指向 127.0.0.1：
受測程式送出的 SNI 是正式網域，連線卻落在本機的擷取器上。有兩種擷取方式：

- **只擷取握手：** 讀完第一筆 TLS 紀錄就斷線，握手不會完成，也不需要憑證。
- **終結 TLS（只用於官方 CLI）：** 先解析 ClientHello，再把原始位元組原封轉給本機的假後端。
  假後端回應官方 CLI 走到 WebSocket 前需要的請求（`wham/accounts/check`），並完成 WebSocket 升級，讓一輪對話跑完。
  臨時 CA 只寫進容器內的系統信任路徑，不設 `SSL_CERT_FILE` 或 `CODEX_CA_CERTIFICATE`：
  官方設了自訂 CA 會強制改用 rustls，握手就不再是正式路徑。實錄中 HTTP 的握手與只擷取時完全相同，證明信任方式沒有改變 TLS 路徑。

| 工具 | 位置 | 用途 |
| --- | --- | --- |
| 契約測試與執行器 | vm2api `test/kernel-contract/`、`scripts/kernel-contract.mjs` | 用正式的設定寫入器與 client 驅動核心，`--record <dir>` 輸出觀測結果 |
| ClientHello 解析 | vm2api `test/kernel-contract/tls-capture.mjs` | 解析擴充、群組、簽章演算法，計算 JA3 與 JA4（FoxIO 規格，已用規格範例驗證） |
| 官方 CLI 錄製（只擷取握手） | 本倉庫 `fidelity/tools/capture-official-cli.mjs` | 在 443 埠擷取，讓官方 `codex exec` 以假 token 發出請求 |
| 官方 CLI 錄製（終結 TLS） | `fidelity/tools/record-official-session.mjs`、`make-capture-ca.sh` | 假後端加臨時 CA，錄下每條連線的握手與傳輸方式、HTTP 請求與 WebSocket 訊息 |
| 彙整與基準 | `fidelity/tools/summarize-sessions.mjs`、`derive-expected-tls.mjs` | 多輪實錄彙整成 `tls-sessions-exec.json` 與 `session-exec.json`，再與 `tls.json` 合成 `expected-tls.json` |

錄製上游核心（在 vm2api 主工作區）：

```bash
node scripts/kernel-contract.mjs --upstream pinned --record /tmp/kc-record
```

錄製官方 CLI（`<codex-vendor>` 是 npm 套件 `@openai/codex@<版本>-linux-x64` 解開後的 `vendor/x86_64-unknown-linux-musl`；
下載後先用 `npm view ... dist.integrity --registry https://registry.npmjs.org` 核對完整性）：

```bash
docker run --rm --platform linux/amd64 --network none \
  --add-host chatgpt.com:127.0.0.1 --add-host auth.openai.com:127.0.0.1 \
  -v <vm2api>:/repo:ro -v <codex-vendor>:/codex:ro \
  -v <tap>/fidelity/tools:/script:ro -v /tmp/kc-official:/out \
  node:22-bookworm-slim node /script/capture-official-cli.mjs /codex/bin/codex /out/official-fresh.json fresh
```

第三個參數可以是 `fresh`（有效 token）、`expired`（access token 的 `exp` 已過）或 `stale`（`last_refresh` 在 40 天前）。

終結 TLS 的實錄（臨時 CA 只放在暫存目錄，**不可提交**；每輪約 1 分鐘，至少錄 3 輪才看得出擴充順序是否隨機）。
`tui` 模式以 util-linux `script` 配置偽終端，回應 TUI 送出的終端查詢（游標位置、裝置屬性、預設顏色），回合完成 8 秒後送 Ctrl-C 退出。
要用完整的 `node:22-bookworm` 映像：TUI 會啟動背景 app-server 並呼叫 `ps`，slim 映像沒有 `ps` 會直接失敗；不要改用 `--no-daemon`，那不是使用者的預設路徑。

```bash
fidelity/tools/make-capture-ca.sh /tmp/capture-ca
docker run --rm --platform linux/amd64 --network none \
  --add-host chatgpt.com:127.0.0.1 --add-host auth.openai.com:127.0.0.1 --add-host ab.chatgpt.com:127.0.0.1 \
  -v <vm2api>:/repo:ro -v <codex-vendor>:/codex:ro -v <tap>/fidelity/tools:/rec:ro \
  -v /tmp/capture-ca:/certs:ro -v /tmp/kc-session:/out \
  node:22-bookworm node /rec/record-official-session.mjs /codex/bin/codex /certs /out/run1.json [exec|tui]
node fidelity/tools/summarize-sessions.mjs <基準目錄>/tls-sessions-<exec 或 tui>.json <基準目錄>/session-<exec 或 tui>.json /tmp/kc-session/run*.json
node fidelity/tools/derive-expected-tls.mjs <基準目錄>/tls.json <基準目錄>/tls-sessions-<exec 或 tui>.json <基準目錄>/expected-tls.json
```

**帳號操作與 cookie：** `REC_COOKIES=1` 讓假後端在每個 chatgpt.com 回應（含 WebSocket 握手）發 Set-Cookie：允許清單內的、工作階段 cookie 與一個不在清單的 cookie，用來錄官方保留哪些 cookie、`cookie` 標頭的位置，以及各帳號操作在有無 cookie 時的標頭順序。2026-11-16 錄得的結果整理在 `baseline/codex-cli-0.167.1-linux-x64/ops-and-cookies.json`。

**搜尋與圖片：** `REC_TOOL` 讓假模型在第一輪回一個工具呼叫，官方 CLI 自己去打對應端點：`search`（`web.run` → `/codex/alpha/search`）、`imagegen`（`image_gen.imagegen` → `/codex/images/generations`）、`imageedit`（`imagegen` 帶 `referenced_image_paths` 指向工作目錄裡的一張圖 → `/codex/images/edits`）。假後端回最小的成功本體（圖片是 1×1 PNG），第二輪結束對話。2026-11-16 錄得的端點、標頭順序與本體欄位併入 `ops-and-cookies.json` 的 `ops.search`、`ops.images_generate`、`ops.images_edit`。

**換票請求：** `record-official-session.mjs` 以環境變數 `REC_REFRESH` 錄官方的換票：`expiring`（access token 60 秒後到期，下一次要用 token 時主動換票）、`unauthorized`（第一次推理回 401；官方只重讀 auth.json 再試，不換票）、`unauthorized2`（前兩次回 401，第二次後以 `unauthorized_recovery` 換票）。後兩種要加 `REC_WEBSOCKET=reject`。2026-11-16 錄得的結果整理在 `baseline/codex-cli-0.167.1-linux-x64/refresh.json`。

## 目前相容目標

官方穩定版為 `rust-v0.171.0`（`ccf9be8cc458bd6b8336fe84d68d8893f2691a5e`）。2026-11-22 在斷網 Debian 容器使用 npm 發行的 Linux x64 二進位，完成 TUI WebSocket、拒絕握手轉 HTTP、兩次 401 換票、搜尋、圖片生成／編輯及 exec 回合。當版基準僅保存契約比對欄位，歷史錄製保留原版本。HTTP 回應可使用 gzip 或 zstd，協商值為 `accept-encoding: zstd,gzip`。以本機新建置的 Linux amd64 Tap 執行：

```bash
baseline=/path/to/tap-codex-kernel/fidelity/baseline/codex-cli-0.171.0-linux-x64
node scripts/kernel-contract.mjs --tap /path/to/tap-codex-kernel-linux-amd64 \
  --tls-baseline "$baseline/expected-tls.json" --identity-baseline "$baseline/expected-identity.json" \
  --refresh-baseline "$baseline/refresh.json" --ops-baseline "$baseline/ops-and-cookies.json"
```

在 vm2api `38d53a640` 上以這四份基準執行契約測試，86 項通過，11 個沒有設定測試目標時使用的占位案例略過。唯一失敗在 `node-shared.contract.test.mjs`：代理熔斷回報 `proxy_unavailable`、送出狀態為 `not_sent` 時，控制面的 `isCodexFailoverError` 回傳 false。同一斷言在 vm2api 釘選的 Tap v0.4.1 也失敗。 2026-11-23 在 vm2api 共用換槽判斷補上 `proxy_unavailable` 的送出狀態檢查後，以同一組基準重跑，87 項通過、11 個占位案例略過，全部實際案例通過。

## 基準清單

| 目錄 | 對象 | 錄製日期 |
| --- | --- | --- |
| `baseline/codex-cli-0.171.0-linux-x64/` | 官方 CLI 0.171.0 Linux x64，TUI 與 exec，二進位 SHA256 `b76a027d6b210f9a8da7aaf321dd235c700f0ef7a794d4a5314b9af6c134f0f0` | 2026-11-22 |
| `baseline/kin-codex-kernel-0.1.2/` | 生產環境現用的上游核心（payload 釘選版本，sha256 `ec9abb01…fb61376d`） | 2026-11-16 |
| `baseline/codex-cli-0.167.1-linux-x64/` | 官方 Codex CLI 0.167.1 的 linux-x64 npm 發行版（`codex` sha256 `dd5bfdf1…d461e8a02`） | 2026-11-16 |

上游核心的每個檔案對應一個契約案例（檔名去掉 `upstream.` 前綴），出站請求已遮去 token。
官方 CLI 的檔案：

| 檔案 | 內容 |
| --- | --- |
| `tls.json` | 只擷取握手的兩輪（有效 token、過期 token）：不同指紋、各主機連線數、錯誤日誌摘錄 |
| `tls-sessions-exec.json`、`tls-sessions-tui.json` | 終結 TLS 的三輪（`codex exec`、互動式 TUI 各三輪）：依傳輸方式（HTTP、WebSocket）分開的指紋與擴充順序變化 |
| `session-exec.json`、`session-tui.json` | 各取一輪的完整請求（方法、主機、路徑、標頭順序、本體，token 已遮去）與 WebSocket 訊息，供 K1 比對標頭與本體 |
| `expected-tls.json` | 契約測試用的基準：`{client, extension_order_varies: {傳輸方式: 布林}, fingerprints: {傳輸方式: {主機: [指紋]}}}` |

## 2026-11-16 錄得的事實

### TLS 握手

| 項目 | 上游核心 0.1.2 | 官方 CLI 0.167.1（linux-x64） |
| --- | --- | --- |
| JA4（HTTP） | `t13d1011h1_61a7ad8aa9b6_3fcd1a44f3e3` | `t13d301100_1d37bd780c83_8e6e362c5eac` |
| JA4（WebSocket） | `t13d101000_61a7ad8aa9b6_3fcd1a44f3e3` | `t13d101000_61a7ad8aa9b6_0d308c48d2a3`（三輪實錄一致） |
| HTTP 密碼套件 | 10 個（rustls 預設組，含 `00ff`） | 30 個（OpenSSL 預設組，含 DHE 與 CBC） |
| HTTP 金鑰交換群組 | x25519、P-256、P-384；key share 只有 x25519 | X25519MLKEM768、x25519、P-256、x448、P-384、P-521、ffdhe2048、ffdhe3072；key share 是 X25519MLKEM768 與 x25519 |
| HTTP 簽章演算法 | 9 個 | 26 個，含 ML-DSA |
| WebSocket 密碼套件 | 與 HTTP 相同的 10 個 | 同樣 10 個、同樣順序（兩邊都是 rustls） |
| WebSocket 金鑰交換群組 | x25519、P-256、P-384；key share 只有 x25519 | X25519MLKEM768、x25519、P-256、P-384；key share 是 X25519MLKEM768 與 x25519 |
| WebSocket 簽章演算法 | 9 個 | 13 個：多了 `0603`（ECDSA P-521）與 ML-DSA 三個（`0904`–`0906`） |
| ALPN | HTTP 送 `http/1.1`；WebSocket 不送 | 一律不送 |
| 擴充順序 | 每條連線隨機 | HTTP 固定；WebSocket 每條連線隨機 |
| 推定的 TLS 實作 | HTTP 與 WebSocket 都是 rustls 加 ring provider | HTTP 是 OpenSSL 3.6.4（經 native-tls）；WebSocket 是 rustls 加 aws-lc-rs provider |

**結論：** 上游核心在 HTTP 上與官方完全不同（rustls 對 OpenSSL）；在 WebSocket 上兩邊都是 rustls，只差在 provider（ring 對 aws-lc-rs），
所以密碼套件相同、群組與簽章演算法不同。上游核心的 User-Agent 宣稱 `codex_cli_rs/0.167.1 (linux x86_64)`，握手卻不是官方 linux 版本的握手。

### 連線行為

- 上游核心在 `prefer_websocket` 下，先嘗試兩次 WebSocket，再退回 HTTP 一次；全部失敗才回 `502 upstream_transport`。
- 官方 CLI 一次 `exec` 會打出大量連線（69 條），包括重試、analytics、MCP 與 workspace 路由探索。
- 官方 CLI 在 workspace 路由探索（`GET /backend-api/wham/accounts/check`）成功之前，不會送出推理請求；成功後只開一條 WebSocket，先送一個 `generate: false` 的預熱 `response.create`，再送真正的請求。
- 官方 WebSocket 升級帶 `sec-websocket-extensions: permessage-deflate; client_max_window_bits`，以及 `x-client-request-id`、`thread-id`、`x-codex-window-id`、`x-codex-turn-metadata`、`x-codex-beta-features` 等標頭；`codex exec` 的 originator 是 `codex_exec`，User-Agent 的作業系統段是 `Debian 12.0.0; x86_64`。上游核心的 `(linux x86_64)` 與官方格式不同，K1 比對標頭時要處理（詳見 `session-exec.json`）。
- 官方的 OTLP 指標送往 `ab.chatgpt.com`，握手與 chatgpt.com 的 HTTP 相同。

### 互動式 TUI 與 `codex exec` 的差異（2026-11-16，各三輪）

| 項目 | `codex exec` | 互動式 TUI（`codex <prompt>`） |
| --- | --- | --- |
| TLS 握手 | HTTP 是 OpenSSL、WebSocket 是 rustls 加 aws-lc | **完全相同**（指紋與擴充順序規則一致，`expected-tls.json` 不變） |
| originator | `codex_exec` | `codex-tui` |
| User-Agent | `codex_exec/0.167.1 (Debian 12.0.0; x86_64) unknown (codex_exec; 0.167.1)` | `codex-tui/0.167.1 (Debian 12.0.0; x86_64) xterm-256color (codex-tui; 0.167.1)` |
| WebSocket 連線 | 1 條：主對話 | 2 條：主對話，以及產生對話標題的子對話（`thread_source: thread_title`，用較小的模型，`text.format` 是 JSON schema） |
| 每條 WebSocket 的訊息 | 先送 `generate: false` 的預熱，再送真正的請求 | 同左 |
| WebSocket 升級標頭 | 名稱與順序 | 與 exec 完全相同，只有值不同 |
| 推理請求本體 | 頂層欄位、`input` 的項目種類與順序 | 與 exec 相同；`client_metadata` 欄位集合相同，順序不同 |
| 額外的 HTTP | — | `wham/rate-limit-reset-credits`、`wham/usage`、`wham/security-setup` |
| 連線數 | 約 20 條 | 約 37 條 |

官方 User-Agent 的格式是 `{originator}/{版本} ({作業系統} {版本}; {架構}) {終端}`，後面再接 `({client 名稱}; {版本})`（`login/src/auth/default_client.rs`）。
`codex_cli_rs` 是官方的**預設** originator，只在呼叫端沒有設定時才會用；0.167.1 的 TUI 和 exec 都會改成自己的名稱。

**與上游核心對照：** 上游核心送出的 originator 是 `codex_cli_rs`，User-Agent 來自 vm2api 路由設定 `codex.user_agent`，預設是 `codex_cli_rs/0.167.1 (linux x86_64)`（上游 `codex-route.mjs` 的 `DEFAULT_CODEX_USER_AGENT`）。
它的名稱與格式都對不上官方任何一種用法：0.167.1 的 TUI 是 `codex-tui`，exec 是 `codex_exec`，而且官方的作業系統段帶版本號與分號，後面還接著終端名稱與 client 後綴。
tap 要對齊哪一種官方身分（TUI 或 exec）、User-Agent 由誰組（Node 的路由設定，或核心依帳號環境資料組），在 K1 處理標頭時決定。

### Token 刷新

- 上游核心**從不自己刷新**：access token 已過期（毫秒、秒、JWT `exp` 三種寫法都試過）、閒置 75 秒、上游回 401，都沒有連到 `auth.openai.com`。
  上游回 401 時原樣回 `401 upstream_auth`，訊息保留上游原文，不重試。換票完全由 vm2api 的 `refreshCodexSlotAccess` 負責。
- 官方 CLI 在 access token 的 JWT `exp` 已過時，會先主動刷新（60 次連到 `auth.openai.com`，握手與 chatgpt.com 相同）。

### 憑據檔

- 缺 access token 或為空字串：`503 no_credential`（"no schedulable credential"），不送出。
- `expires_at` 必須是整數；ISO 字串會在**請求時**得到 `502 invalid_json`，表示核心每次請求都重讀憑據檔。

### 內部端點

- 只接受舊標頭 `x-internal-token`；只帶 `x-kin-internal-token` 會得到 401。vm2api 的 client 兩個都送，所以正式流量不受影響。
- 未知路徑在驗證之前就回 404。

## 決定：TLS 對齊官方 CLI（使用者 2026-11-16）

tap-codex-kernel 送出的每個 ClientHello，都必須是目標官方版本在同一主機、同一傳輸方式下會送出的握手。
上游核心的握手只作對照錄製，不是目標。單帳號相容模式是逐槽替換，所以握手的改變也是逐槽發生，不會一次全部改變。

### 官方原始碼核對（openai/codex `rust-v0.167.1`）

| 路徑 | 官方的做法 | 出處 |
| --- | --- | --- |
| HTTP（Responses、模型目錄、analytics、OAuth 刷新） | reqwest 的預設 TLS 後端（`TlsBackend::TransportDefault`），也就是 native-tls；Linux musl 發行版以 `openssl-sys` 的 `vendored` 靜態連結 OpenSSL。沒有開 `native-tls-alpn`，所以不送 ALPN | `http-client/src/client_builder.rs`、`core/Cargo.toml`、workspace `reqwest` 預設 feature |
| WebSocket（Responses WebSocket） | tokio-tungstenite 加 rustls，provider 明確安裝 aws-lc-rs，並檢查支援 ECDSA P-521 | `websocket-client`、`utils/rustls-provider/src/lib.rs`、workspace `tokio-tungstenite` feature `rustls-tls-native-roots` |
| TLS 退回 | 部分 client（明確 TLS 設定、MCP）在 TLS 協商失敗時改用 rustls 重試一次；Responses 主路徑是否啟用尚未核對 | `http-client/src/route_aware_client_pool.rs` |
| OpenSSL 版本 | `Cargo.lock` 鎖 `openssl-src 300.6.1+3.6.3`，發行的二進位字串卻是 `OpenSSL 3.6.4`（發行建置另有來源，可能是 Bazel） | 以二進位實測為準 |

終結 TLS 的實錄證實了原始碼的推斷：WebSocket 的握手是 rustls 加 aws-lc-rs（含 X25519MLKEM768 與 ML-DSA），HTTP 的是 OpenSSL。

### 對 tap-upstream 的要求

- **HTTP：** 走 native-tls，靜態連結與目標版本相同的 OpenSSL（以發行二進位的版本字串為準），不送 ALPN。不得自動選用 rustls。
- **WebSocket：** 走 rustls，明確安裝 aws-lc-rs provider，用 rustls 的預設參數（不自訂密碼套件、群組與簽章演算法），不送 ALPN；擴充順序沿用 rustls 每條連線隨機的行為。依賴圖裡若同時有 ring，provider 必須明確安裝，否則會出現上游核心那種 ring 握手。
  rustls 版本以目標官方版本的 `Cargo.lock` 為準（0.167.1 是 0.23.46）。
- **版本綁定：** User-Agent 的官方版本、HTTP 的 OpenSSL 版本與 WebSocket 的 rustls 版本，都對齊同一個官方發行版。換目標版本時，先重錄基準、更新 `expected-tls.json`，再改程式。
- **驗收：** vm2api 契約測試帶 `--tls-baseline fidelity/baseline/<目標版本>/expected-tls.json` 執行，TLS 案例全部通過；基準缺的傳輸方式會標為略過，不算通過。

### 契約測試的基準檔

`expected-tls.json` 由 `fidelity/tools/derive-expected-tls.mjs` 從 `tls.json` 與 `tls-sessions-exec.json` 產生，格式是
`{client, extension_order_varies: {<傳輸方式>: 布林}, fingerprints: {<傳輸方式>: {<主機>: [指紋]}}}`。
0.167.1 的基準：HTTP 涵蓋 chatgpt.com、auth.openai.com、ab.chatgpt.com，擴充順序固定；WebSocket 涵蓋 chatgpt.com，擴充順序隨機。
指紋欄位與 vm2api `tls-capture.mjs` 的 `FINGERPRINT_FIELDS` 相同，兩邊改動時要一起改。

```bash
node scripts/kernel-contract.mjs --upstream pinned --tap <tap 二進位> \
  --tls-baseline <tap>/fidelity/baseline/codex-cli-0.167.1-linux-x64/expected-tls.json
```

## 決定：出站身分對齊互動式 TUI（使用者 2026-11-16）

推理請求（Responses 的 HTTP 與 WebSocket）一律使用官方互動式 TUI 的身分：

| 標頭 | 值 |
| --- | --- |
| `originator` | `codex-tui` |
| `version` | 目標官方版本（目前為 0.171.0） |
| `User-Agent` | `codex-tui/<版本> (<Linux 發行版> <版本>; <架構>) <終端> (codex-tui; <版本>)`，例：`codex-tui/0.167.1 (Debian 12.0.0; x86_64) xterm-256color (codex-tui; 0.167.1)` |

- **作業系統段必須是 Linux 發行版：** TLS 基準是 linux-x64 發行版，宣稱 macOS 或 Windows 會與握手矛盾。
- **終端段**只能含英數字與 `- _ . /`（官方 `terminal-detection` 的清理規則）。
- **啟動階段的請求另有身分：** TUI 的 app-server client 起來之前，官方的帳號檢查、第一次模型目錄、外掛清單用的是預設 originator `codex_cli_rs`，User-Agent 沒有 client 後綴（`codex_cli_rs/0.167.1 (Debian 12.0.0; x86_64) xterm-256color`）；MCP 用 `codex-mcp-client/0.167.1`，OTLP 用 `OTel-OTLP-Exporter-Rust/0.31.0`。K2 代發帳號操作時照這張對照表選身分（`session-tui.json`）。
- **驗收：** `expected-identity.json` 由 vm2api 契約測試的 `--identity-baseline` 讀取，非參考核心的推理請求必須符合 originator、version 與 User-Agent 格式。
- **標頭順序：** `expected-identity.json` 的 `header_order` 是官方的線上順序（HTTP 由「拒絕 WebSocket 讓官方退回 HTTP」錄得，WebSocket 由 TUI 實錄錄得；錄製中沒出現的選用標頭依原始碼擺放）。契約測試要求核心送出的標頭都在清單內、相對順序一致。WebSocket 的線上順序來自官方 tungstenite 分支的 `generate_request`：它以 `swap_remove` 取出 `host`、`connection`、`upgrade`、`sec-websocket-version`、`sec-websocket-key`，於是最後插入的五個標頭會倒序移到前面；照官方的插入順序送進同一個分支，線上順序就相同。

```bash
node scripts/kernel-contract.mjs --upstream pinned --tap <tap 二進位> \
  --tls-baseline <tap>/fidelity/baseline/codex-cli-0.167.1-linux-x64/expected-tls.json \
  --identity-baseline <tap>/fidelity/baseline/codex-cli-0.167.1-linux-x64/expected-identity.json
```

### 決定：User-Agent 由核心依帳號環境組成（使用者 2026-11-16）

核心照官方用戶在那台機器上會得到的結果，自己組出 User-Agent；Node 路由設定 `codex.user_agent` 的單一全域字串對 tap 不再生效。

- **輸入（帳號環境，請求選項 `client_environment`；共享模式改由帳號登錄提供）：**
  `{os_release: {id, version_id}, machine, term, term_program?, term_program_version?}`，都是那台機器的原始事實，不先格式化。
- **作業系統段（重現 os_info 3.14）：** 由 os-release 的 `ID` 決定名稱（`debian` → `Debian`、`ubuntu` → `Ubuntu`、`fedora` → `Fedora`），
  `VERSION_ID` 補成三段數字並去掉前導零：`24.04` → `24.4.0`、`22.04` → `22.4.0`、`12` → `12.0.0`、`41` → `41.0.0`。
- **終端段（重現 terminal-detection）：** 有 `TERM_PROGRAM` 且不是 tmux 時用 `TERM_PROGRAM[/TERM_PROGRAM_VERSION]`，否則用 `TERM`；`[A-Za-z0-9._/-]` 以外的字元換成 `_`。
  實測：`vscode` 1.95.3 → `vscode/1.95.3`；tmux（`TERM=tmux-256color`）→ `tmux-256color`；`TERM=xterm-256color` → `xterm-256color`。
- **只接受驗證過的環境：** os-release `ID` 限 debian、ubuntu、fedora（其他發行版先用 os-release 替換法實測後再加入），架構限 `x86_64`（TLS 基準是 linux-x64）。
- **退回身分：** 不符合上述條件（macOS、Windows、aarch64、缺資料）時，從內建的 Linux 設定表依帳號穩定挑一組（例如以 `device_id` 雜湊），同一帳號每次都相同；健康回報要標出用了退回身分，讓 Node 補正帳號環境。
- **驗收：** `expected-identity.json` 的 `composition.cases` 是逐字比對的案例（作業系統段與終端段都經實測），`fallback_environments` 要求退回身分符合格式且同一帳號兩次請求相同；vm2api `identity.contract.test.mjs` 讀取這兩組資料。
- **Node 端的後續工作（第 1 階段 A2）：** `codex_env` 存放 `client_environment`（新槽依系統目錄產生，舊槽由採集資料轉入；非 Linux 的舊槽改配一組 Linux 環境並記錄），`codexKernelRequestOptions` 每次請求帶上。

**實測方法：** 作業系統段用 `-v <某發行版的 os-release>:/usr/lib/os-release:ro` 掛進 `node:22-bookworm` 容器再跑官方 CLI（容器沒有 `lsb_release`，os_info 直接讀 os-release）；
終端段用 `docker run -e TERM=... -e TERM_PROGRAM=... -e TERM_PROGRAM_VERSION=...`，錄製工具會把這幾個變數傳給官方 CLI。

## 尚未錄製

- 假後端對模型目錄、外掛、analytics、用量等端點一律回 404；這些請求本身已錄在 session 檔，但端點成功時的後續行為（例如依模型目錄改選模型）沒有錄到。
- WebSocket 上的錯誤與重連（假後端一律成功）。
- macOS 發行版的握手（目前 Codex 槽宣稱 linux x86_64，暫不需要）。
