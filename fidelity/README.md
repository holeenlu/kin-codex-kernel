# 擬真基準

這裡保存「線上實際長什麼樣子」的錄製結果，作為 kin-codex-kernel 逐項比對的依據。
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

錄製官方 CLI（`<codex-vendor>` 是 npm 套件 `@openai/codex@<版本>-linux-x64` 解開後的 `vendor/x86_64-unknown-linux-musl`；
下載後先用 `npm view ... dist.integrity --registry https://registry.npmjs.org` 核對完整性）：

```bash
docker run --rm --platform linux/amd64 --network none \
  --add-host chatgpt.com:127.0.0.1 --add-host auth.openai.com:127.0.0.1 \
  -v <vm2api>:/repo:ro -v <codex-vendor>:/codex:ro \
  -v <kin>/fidelity/tools:/script:ro -v /tmp/kc-official:/out \
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
  -v <vm2api>:/repo:ro -v <codex-vendor>:/codex:ro -v <kin>/fidelity/tools:/rec:ro \
  -v /tmp/capture-ca:/certs:ro -v /tmp/kc-session:/out \
  node:22-bookworm node /rec/record-official-session.mjs /codex/bin/codex /certs /out/run1.json [exec|tui]
node fidelity/tools/summarize-sessions.mjs /tmp/kc-session/tls-sessions-<exec 或 tui>.json /tmp/kc-session/session-<exec 或 tui>.json /tmp/kc-session/run*.json
node fidelity/tools/derive-expected-tls.mjs /tmp/kc-session/tls.json /tmp/kc-session/tls-sessions-<exec 或 tui>.json /tmp/kc-session/expected-tls.json
```

**帳號操作與 cookie：** `REC_COOKIES=1` 讓假後端在每個 chatgpt.com 回應（含 WebSocket 握手）發 Set-Cookie：允許清單內的、工作階段 cookie 與一個不在清單的 cookie，用來錄官方保留哪些 cookie、`cookie` 標頭的位置，以及各帳號操作在有無 cookie 時的標頭順序。

**搜尋與圖片：** `REC_TOOL` 讓假模型在第一輪回一個工具呼叫，官方 CLI 自己去打對應端點：`search`（`web.run` → `/codex/alpha/search`）、`imagegen`（`image_gen.imagegen` → `/codex/images/generations`）、`imageedit`（`imagegen` 帶 `referenced_image_paths` 指向工作目錄裡的一張圖 → `/codex/images/edits`）。假後端回最小的成功本體（圖片是 1×1 PNG），第二輪結束對話。

**換票請求：** `record-official-session.mjs` 以環境變數 `REC_REFRESH` 錄官方的換票：`expiring`（access token 60 秒後到期，下一次要用 token 時主動換票）、`unauthorized`（第一次推理回 401；官方只重讀 auth.json 再試，不換票）、`unauthorized2`（前兩次回 401，第二次後以 `unauthorized_recovery` 換票）。後兩種要加 `REC_WEBSOCKET=reject`。

## 目前相容目標

**官方原始碼**已於 2026-11-26 14:19（UTC+8）確認，在 Kin 適用範圍對齊 `openai/codex` main `af71942309704997f4bdc86c020717646423c080`。逐提交處置見[本輪同步紀錄](../docs/sync/2026-11-26-af719423-main.md)。

2026-11-24 使用下述官方 0.171.0 發行套件補錄服務等級。在既有 TUI 終結 TLS 錄製器寫入的臨時 `config.toml` 頂層設定 `model = "gpt-5.5"` 與 `service_tier = "priority"`，遇到模型升級提示時選用現有模型。先跑正常 WS，再以 `REC_WEBSOCKET=reject` 錄製拒絕握手後的 HTTP。兩輪都完成生成，握手與 HTTP POST 都帶 `model=gpt-5.5;tier=priority`，預熱與生成幀、本體也帶 `priority`。這次補錄的觀察及原始碼依據見[服務等級](../docs/sync/2026-11-24-main.md#服務等級)。

**出站實錄相容目標：** 官方正式版 `rust-v0.171.0`（`ccf9be8cc458bd6b8336fe84d68d8893f2691a5e`）。2026-11-22 在斷網 Debian 容器使用 npm 發行的 Linux x64 二進位，完成 TUI WebSocket、拒絕握手轉 HTTP、兩次 401 換票、搜尋、圖片生成／編輯及 exec 回合。當版基準保存契約測試使用的四份 JSON 摘要。HTTP 回應可使用 gzip 或 zstd，協商值為 `accept-encoding: zstd,gzip`。以本機新建置的 Linux amd64 核心執行：

```bash
baseline=/path/to/kin-codex-kernel/fidelity/baseline/codex-cli-0.171.0-linux-x64
node scripts/kernel-contract.mjs --tap /path/to/kin-codex-kernel-linux-amd64 \
  --tls-baseline "$baseline/expected-tls.json" --identity-baseline "$baseline/expected-identity.json" \
  --refresh-baseline "$baseline/refresh.json" --ops-baseline "$baseline/ops-and-cookies.json"
```

在 vm2api `38d53a640` 上以這四份基準執行契約測試，86 項通過，11 個沒有設定測試目標時使用的占位案例略過。唯一失敗在 `node-shared.contract.test.mjs`：代理熔斷回報 `proxy_unavailable`、送出狀態為 `not_sent` 時，控制面的 `isCodexFailoverError` 回傳 false。同一斷言在 vm2api 釘選的 Tap v0.4.1 也失敗。 2026-11-23 在 vm2api 共用換槽判斷補上 `proxy_unavailable` 的送出狀態檢查後，以同一組基準重跑，87 項通過、11 個占位案例略過，全部實際案例通過。

2026-11-23 同步 CPR v3.54.4 時，重新核對官方 CLI 二進位雜湊，並以候選 Tap 重跑上述四份基準。驗收結果見[同步紀錄](../ARCHITECTURE.md#2026-11-23-同步)。

同日 main 雙來源同步沒有變更 User-Agent 版本、TLS provider 或基準 JSON。採用的 WS fork 差異只修正出站 payload 日誌；cookie 升級及傳輸修正以候選 amd64 二進位搭配四份基準重新驗收，87 項通過、0 失敗、11 個占位案例略過。與官方 main 的 exclude 預設及 Orca 別名正規化仍有明示差異；這份正式版實錄不代表 main 全部出站分支。完整驗收輸入及四份 JSON 的 SHA256 見 [main 同步紀錄](../docs/sync/2026-11-23-main.md#驗證)。

## 基準清單

2026-11-22 錄製使用的 Linux x64 官方 CLI 二進位 SHA256 為 `b76a027d6b210f9a8da7aaf321dd235c700f0ef7a794d4a5314b9af6c134f0f0`。

| 檔案 | 契約比對內容 |
| --- | --- |
| [expected-tls.json](baseline/codex-cli-0.171.0-linux-x64/expected-tls.json) | HTTP 與 WebSocket 握手指紋、擴充順序是否變動 |
| [expected-identity.json](baseline/codex-cli-0.171.0-linux-x64/expected-identity.json) | Responses 出站身分、User-Agent 組成與標頭順序 |
| [ops-and-cookies.json](baseline/codex-cli-0.171.0-linux-x64/ops-and-cookies.json) | 帳號、搜尋與圖片操作，以及帶 cookie 的標頭順序 |
| [refresh.json](baseline/codex-cli-0.171.0-linux-x64/refresh.json) | 到期前換票與收到 401 的處理案例 |

## 決定：TLS 對齊官方 CLI（使用者 2026-11-16）

kin-codex-kernel 送出的每個 ClientHello，都必須是目標官方版本在同一主機、同一傳輸方式下會送出的握手。

### 對 kin-upstream 的要求

- **HTTP：** 走 native-tls，靜態連結與目標版本相同的 OpenSSL（以發行二進位的版本字串為準），不送 ALPN。不得自動選用 rustls。
- **WebSocket：** 走 rustls，明確安裝 aws-lc-rs provider，用 rustls 的預設參數（不自訂密碼套件、群組與簽章演算法），不送 ALPN；擴充順序沿用 rustls 每條連線隨機的行為。依賴圖裡若同時有 ring，provider 必須明確安裝，否則會出現上游核心那種 ring 握手。
  rustls 版本以目標官方版本的 `Cargo.lock` 為準。
- **版本綁定：** User-Agent 的官方版本、HTTP 的 OpenSSL 版本與 WebSocket 的 rustls 版本，都對齊同一個官方發行版。換目標版本時，先重錄基準、更新 `expected-tls.json`，再改程式。
- **驗收：** vm2api 契約測試帶 `--tls-baseline fidelity/baseline/<目標版本>/expected-tls.json` 執行，TLS 案例全部通過；基準缺的傳輸方式會標為略過，不算通過。

## 決定：出站身分對齊互動式 TUI（使用者 2026-11-16）

推理請求（Responses 的 HTTP 與 WebSocket）一律使用官方互動式 TUI 的身分：

| 標頭 | 值 |
| --- | --- |
| `originator` | `codex-tui` |
| `version` | 目標官方版本（目前為 0.171.0） |
| `User-Agent` | `codex-tui/<版本> (<Linux 發行版> <版本>; <架構>) <終端> (codex-tui; <版本>)`，例：`codex-tui/0.171.0 (Debian 12.0.0; x86_64) xterm-256color (codex-tui; 0.171.0)` |

- **作業系統段必須是 Linux 發行版：** TLS 基準是 linux-x64 發行版，宣稱 macOS 或 Windows 會與握手矛盾。
- **終端段**只能含英數字與 `- _ . /`（官方 `terminal-detection` 的清理規則）。
- **驗收：** `expected-identity.json` 由 vm2api 契約測試的 `--identity-baseline` 讀取，非參考核心的推理請求必須符合 originator、version 與 User-Agent 格式。
- **標頭順序：** `expected-identity.json` 的 `header_order` 是官方的線上順序（HTTP 由「拒絕 WebSocket 讓官方退回 HTTP」錄得，WebSocket 由 TUI 實錄錄得；錄製中沒出現的選用標頭依原始碼擺放）。契約測試要求核心送出的標頭都在清單內、相對順序一致。WebSocket 的線上順序來自官方 tungstenite 分支的 `generate_request`：它以 `swap_remove` 取出 `host`、`connection`、`upgrade`、`sec-websocket-version`、`sec-websocket-key`，於是最後插入的五個標頭會倒序移到前面；照官方的插入順序送進同一個分支，線上順序就相同。

### 決定：User-Agent 由核心依帳號環境組成（使用者 2026-11-16）

核心照官方用戶在那台機器上會得到的結果，自己組出 User-Agent；Node 路由設定 `codex.user_agent` 的單一全域字串對核心不再生效。

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

- 測試後端尚未覆蓋模型目錄、外掛、analytics 與用量等端點成功回應後的行為，例如依模型目錄改選模型。
- WebSocket 錯誤與重連。
- macOS 發行版的握手。目前契約基準使用 Linux x64。
