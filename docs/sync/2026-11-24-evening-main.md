# 2026-11-24 晚間 CPR 與官方 Codex main 同步

本輪於 2026-11-24 21:59（UTC+8）查詢來源，Tap 從 v0.4.5 的 `6f97d673b5ec0d6a7a36f0087b30a00d493e9394` 開始。

| 來源 | 上次已對齊 | 本次已審查及對齊 | 增量 |
| --- | --- | --- | --- |
| `zyycn/codex-proxy-rs/main` | `037cbcd6052fdb5741daf7ee8ef0f2c487319958` | `21622eeacdab919013408a4b77f5536ed8888f32` | 6 筆，38 個變更檔案 |
| `openai/codex/main` | `6aaf2a86b198bb0ee8f9d995e79155ad5c7f6357` | `75df5098a6d92b40dc61e7a9964233ce9c359677` | 33 筆，301 個變更檔案 |

兩個起點皆為目標的祖先，增量沒有 merge 提交。逐提交用 `git rev-list --reverse --topo-order` 盤點，共 2 筆部分採用、2 筆已具備、35 筆不適用。表中的完整來源目標即下次增量起點。

## 429 額度事實

CPR `98724da9` 讓 WS 升級遭拒時的額度標頭進入失敗分類，並保留重置期限與視窗供配額恢復使用。Tap 既有 `crates/upstream/src/websocket.rs` 回 429 時只帶錯誤事實以及 `Retry-After`，`lib.rs` 的 HTTP 429 也未交付額度標頭。Tap 現透過兩條路徑將觀察到的 `x-codex-active-limit`、通用與具名視窗的用量、時長和重置時間及點數欄位交給控制面。憑據、cookie 及無關回應標頭仍由回應邊界過濾。私有收據欄位、429 狀態與初握手 `0`、重連 `unknown`、已送出 HTTP `1` 的語義保留。

在 vm2api `c04de132d2196ab596e405bc763c47525d7651c6`，`go-worker-client.mjs` 讀取核心錯誤標頭，`handle-codex.mjs` 交給 `codexHeaderObservation`。定向案例驗證 `x-codex-luna-reserve-primary-*` 只更新具名 `codex_luna_reserve` 視窗，通用 `x-codex-primary-*` 另讀。CPR 按 `limit_window_minutes` 與 `resets_at` 做視窗匹配、持久化及復查，這些都屬於控制面的額度策略。本輪保留 Tap 既有的 HTTP 原始錯誤文本與 WS 錯誤收據形狀。

CPR `8075487f` 所需的 WebSocket 日誌修正，Tap 已釘在相同的 `7a920470`、`e19950ef`。上游 API Key、專案和組織的 spend-limit 錯誤碼由既有錯誤路徑保留，原樣的 SSE 終態已在 `non_stream_returns_terminal_events_before_the_upstream_closes` 涵蓋。CPR 的憑據停用與配額重查屬控制面。

## 模型目錄 ETag

官方 `19859256` 在 `codex-rs/codex-api/src/endpoint/responses_websocket.rs` 暫存握手的 `x-models-etag`，讓首個生成回合在預熱後仍可讀取，並避免重播同連線的舊值。HTTP SSE 早已讀取這個標頭。Tap 以既有內部 HTTP 回應標頭交付有效 ETag，且在 WS 首次接受生成時取出。換線會使用新握手值。既有 `codex.response.metadata` 事件完整轉送。官方 CLI 透過型別化事件驅動自己的目錄刷新，Tap 的呼叫端自行消費標頭與事件。vm2api 此 SHA 的目錄沒有依 Tap 的 ETag 自動刷新。

本輪只變更入站的上游回應中繼資料。出站請求、TLS、身份、cookie 及換票相容輸入與 `rust-v0.171.0` Linux amd64 四份基準一致。官方 main 的握手 ETag 行為依凍結原始碼與上游測試判定，Tap 的行為另以實際本機 HTTP 與 WS fixture 驗證。相容版本與操作方式見[實錄基準](../../fidelity/README.md#目前相容目標)和[Responses 契約](../RESPONSES_TIMING.md)。

## 驗證

受測程式提交 `da2b3ab834b17de7c1b1a0a1f2f1d56b50d6704e`，Git 樹 `49299fcbb5d73982bc1d1aa1d3c85d0932fab8bc`。候選的鎖檔、工具鏈及四份實錄基準與 v0.4.5 相同。Tap Cargo.lock SHA256 為 `70f12d28879bc70179b44dca3bab48e18403e105293c6db77518a26fad06f347`。

- 使用上版程式與新增案例，重現 HTTP 429／WS 握手 429 遺漏額度標頭、HTTP／WS 遺漏 ETag、預熱後首輪缺少 ETag。候選在原 API 入口驗證這些案例，並覆蓋連線重用、新握手與流內 ETag。
- 隔離 Linux arm64 使用 Rust 1.99.1，fmt、嚴格 Clippy（`cargo clippy --workspace --all-targets --locked -- -D warnings`）及 164 項鎖版 workspace 測試均通過。測試與開發 profile 停用除錯資訊，鎖檔與 crate 功能不變。cargo-deny 的 advisories、bans、licenses、sources 均通過。
- 候選用 `cargo zigbuild --release --locked -p tap-codex-kernel --target <target>.2.34` 建置，以 `cargo xtask check-glibc --binary <binary> --max 2.34` 核對。
- 以候選 amd64 二進位及 vm2api `c04de132d2196ab596e405bc763c47525d7651c6` 的唯讀副本執行 `node scripts/kernel-contract.mjs --tap <候選二進位> --tls-baseline <expected-tls.json> --identity-baseline <expected-identity.json> --refresh-baseline <refresh.json> --ops-baseline <ops-and-cookies.json>`。Linux amd64 容器使用 `--network none`，ABI 1／2 合計 87 項通過、0 失敗，11 個未指定核心時使用的占位案例略過。vm2api package-lock.json SHA256 為 `8de7334eb47168795c90bd7a4832595b6907bd692dce38344cd6adc9fcdae0d7`。

| Linux 目標 | 大小（bytes） | SHA256 | 最高 GLIBC |
| --- | ---: | --- | --- |
| amd64 | 15,913,128 | `c0495d6a6ec4d7ec211b2227c727a306d5c53443f20f367da08941a1e92775d1` | 2.34 |
| arm64 | 13,610,424 | `647c63c0bc696b8a07002712b4da0f4021793eebe8b70847d19c046e73c8d770` | 2.34 |

| 官方 `rust-v0.171.0` 基準檔案 | SHA256 |
| --- | --- |
| `expected-identity.json` | `4e16302c36b7da0ccc5ff7ec9ebf91f1f08efa2650f4c99871c6735d904790d0` |
| `expected-tls.json` | `e58f6685bafcb15ca2c517ecc39f090ba654aab584a1997c28702c5adb8fe925` |
| `ops-and-cookies.json` | `e4b9364b4a385f6fd53ccb80e1107bdfb34c6639e0626033fb827178e523d839` |
| `refresh.json` | `c2ef50b11e36d5f43f94d0906f3dddf5f8f4749971866bcf981781d07ec9de7c` |

## CPR 逐提交處置

| 提交 | 變更與來源路徑 | Tap 的處置與依據 |
| --- | --- | --- |
| `8075487f573c38758224a7072247d62866ab5d2b` | 區分 API Key、project、organization 支出拒絕，更新官方 WS fork<br>`backend/crates/providers/openai/src/transport/diagnostics/mod.rs` | **已具備**。Tap 已鎖定同一對 fork，且保留原始支出錯誤碼。Tap 以原始結果交給控制面，CPR 的停槽、配額刷新與跨憑據判定由控制面負責。 |
| `45a4d580c7e1ac2b2c0308d72ae5018148f2516a` | CPR 升版至 3.54.5<br>`release/notes.md` | **不適用**。CPR 版本說明記錄來源。Tap 由自己的版本標籤發布。 |
| `f5d040efa19ca6cae399fae07e198b2cfdb67ad7` | 更新 CPR 發布頻道到 3.54.5<br>`release/version.yaml` | **不適用**。Tap 的 GitHub release 工作使用自身標籤。 |
| `a515d5413657305f005d0b4c579d2df935ba6fe1` | 額度拒絕的期限與已知視窗不符時，依額外刷新證據恢復<br>`backend/crates/providers/openai/src/credential/quota/recovery.rs` | **不適用**。Tap 回報原始傳輸與帳號操作結果，選號與解除冷卻不在核心。 |
| `98724da9680e08c32f11a76029653d3a08dfae99` | 保留拒絕回應的具名桶證據及視窗時長，供恢復與排程<br>`backend/crates/providers/openai/src/transport/websocket/handshake.rs` | **部分採用**。HTTP 429 與 WS 握手 429 保留現有控制面可消費的額度標頭。CPR 的持久化、窗口匹配及恢復規則由控制面處置。 |
| `21622eeacdab919013408a4b77f5536ed8888f32` | 後續明確放行且兩次用量未觸頂時解除漂移視窗<br>`backend/crates/providers/openai/src/credential/quota/recovery.rs` | **不適用**。修正屬 CPR 恢復策略及帳號 UI。Tap 不維護此狀態。 |

## 官方 Codex 逐提交處置

| 提交 | 變更與來源路徑 | Tap 的處置與依據 |
| --- | --- | --- |
| `6d0d839f1a729908487121ab046057f7d6db552a` | 讓 Guardian 在首次判定前取得使用者指示<br>`codex-rs/ext/guardian-v3/src/lib.rs` | **不適用**。Guardian 審查上下文由代理端準備，Tap 收取完整推理請求。 |
| `5716c2e22898b263d638099995b18f7902bdb671` | 預熱 Guardian v3 的根審查者<br>`codex-rs/ext/guardian-v3/src/conversation.rs` | **不適用**。預熱審查代理的生命週期與 Tap 的 Responses 連線預熱分開。 |
| `6b776372fde0f7d7af5a2dfa0d475c28e999113f` | codex exec 套用設定及已保存的核准<br>`codex-rs/exec/src/lib.rs` | **不適用**。Tap 不啟動 codex exec 或解讀本機核准設定。 |
| `5b249d57b2321e37ac855d09b44a203cf5182939` | 寫入終端復原診斷失敗時避免 panic<br>`codex-rs/tui/src/custom_terminal.rs` | **不適用**。Tap 是背景 HTTP 服務，無終端復原路徑。 |
| `9ba0ceac1ae9a72b1198f492a252b12c061ae399` | 恢復 TUI 前等待前景終端所有權<br>`codex-rs/tui/src/tui/job_control.rs` | **不適用**。程序的終端前景信號由 TUI 管理。 |
| `98835f5ebafcb4c29dd13610a17627777c02b8ee` | 回合準備中斷時保留已接收的提示<br>`codex-rs/core/src/session/turn.rs` | **不適用**。這是代理輸入佇列的保留規則。Tap 依已收請求及送出事實處理取消。 |
| `94772d9fb6cd2d49bb7a96685a84c95345f419b1` | 要求 thread store 提供工作階段模型 ID 儲存<br>`codex-rs/thread-store/src/store.rs` | **不適用**。Tap 不存工作階段模型選擇。 |
| `4e5cd2f07dfff86fe9962c253a8782acce9bf5ef` | 加入遠端外掛封存驗簽的端對端案例<br>`codex-rs/plugin-archive-verification/tests/app_server.rs` | **不適用**。Tap 不安裝或下載外掛封存。 |
| `dc9e1478fbde08617c1484d2b15e6a95b60bd943` | 限制 eval 外掛簽章金鑰的適用範圍<br>`codex-rs/plugin-archive-verification/src/archive_signing_keys.rs` | **不適用**。Plugin Service 簽章信任範圍與 Tap 的上游 TLS 無交集。 |
| `7c5b5098fa4118fee29aa516031665487ad6ec43` | 使 MCP 擴充可提供逐請求認證<br>`codex-rs/codex-api/src/auth.rs` | **不適用**。`codex-api` 共用認證錯誤只新增 Clone。呼叫路徑在 MCP，Tap 保留指定帳號的認證。 |
| `1985925638538b75d5c6058ed3c18afcf1fe21cc` | 讀取連線握手的模型目錄 ETag 並在首輪生成交付<br>`codex-rs/codex-api/src/endpoint/responses_websocket.rs` | **部分採用**。復用 Tap 的回應標頭契約交付 ETag，含預熱後首輪。模型目錄刷新由呼叫方決定。 |
| `714bc44f0dd9787632ed9d294fa0d6cb1fb844c8` | 5 秒後回收閒置的本機 Code Mode 主機<br>`codex-rs/core/src/code_mode_host.rs` | **不適用**。Tap 沒有本機工具宿主。Responses socket 池閒置由自己的 TTL 和取消契約管理。 |
| `96681d86f08f880ed12ba29044e855002809e0d1` | 去除 Code Mode 工具輸出的重複純文字<br>`codex-rs/code-mode-runtime/src/runtime/mcp_result.rs` | **不適用**。工具結果組裝在代理端。Tap 原樣傳送既有輸入與上游事件。 |
| `a90526e8a751628092aa766a7477e64db3ed2117` | 避免 headless Linux 讀取金鑰環時跳出解鎖<br>`codex-rs/keyring-store/src/secret_service_read.rs` | **不適用**。Tap 從配置好的帳號憑據來源讀取，不使用 Secret Service。 |
| `baf9b39b16dc73e3b826cb0edde199a25a3e15f3` | TUI 輸入推導 workspace_kind 並附入逐輪 metadata<br>`codex-rs/core/src/turn_metadata.rs` | **已具備**。Tap 的 `options::Defaults::metadata`、`scrub::installation_ids` 保留呼叫方既有的逐輪 metadata 欄位。從工作區推導種類屬於 TUI。 |
| `9038698ffa548b7b7b382a248166b9191efd8a69` | 模型選擇器使用當前執行緒的 provider<br>`codex-rs/tui/src/app/model_defaults.rs` | **不適用**。模型選擇與 catalog UI 由 TUI 提供。 |
| `f64f003d985ae59e82b4c63b37fe8c494d203ce9` | 不需轉換的提示圖片改由呼叫端持有緩衝區<br>`codex-rs/utils/image/src/lib.rs` | **不適用**。Tap 的圖片操作轉送 API 原始本體，沒有提示圖片裁切管線。 |
| `74f208237b19ef4460121b60e4b9d940564707f4` | 避免部分 exec-server 測試的埠競爭<br>`codex-rs/app-server/tests/suite/v2/selected_capability_stack.rs` | **不適用**。這些測試需要 app-server 啟動 exec-server，Tap 的測試自有本機 listener。 |
| `7ce4437488c69950fe87f1d8ab4b00bdb4bbd193` | 將 resume 的自動核准限制在起始對話<br>`codex-rs/tui/src/resume_permissions.rs` | **不適用**。Tap 不管理 CLI resume 的核准狀態。 |
| `d4b84f9bb0d2a418fbf97e5acf6b992edecc0c38` | 移除舊 app-server 執行緒摘要測試 helper<br>`codex-rs/app-server/src/request_processors/thread_summary.rs` | **不適用**。Tap 不建立 app-server 執行緒摘要。 |
| `37aeac06b3f49ab4b96dea01c974971d9391c2ef` | 為 Code Mode 提供選用的延後生成圖片<br>`codex-rs/ext/image-generation/src/tool.rs` | **不適用**。延後工具工作屬於代理執行。Tap 只執行被指定的圖片 HTTP 操作。 |
| `8e757686c1ba2d4dc0864edfb8aa286f7029d90d` | TUI 斷線後可用 Ctrl+R 重連<br>`codex-rs/tui/src/app/reconnect.rs` | **不適用**。按鍵與 TUI 的 app-server 連線屬用戶端。 |
| `c3186b5d954e1f0c0fdfa3d67c11f060df2ab033` | 改經 app-server rollout 路徑測試 thread preview<br>`codex-rs/app-server/src/request_processors/thread_summary_tests.rs` | **不適用**。預覽讀取 app-server 對話記錄，Tap 不提供該介面。 |
| `92a34573a0fa979b349dc1cee24f460047001215` | 移除 MCP 自訂工具核准優先序的重複測試<br>`codex-rs/core/src/mcp_tool_call_tests.rs` | **不適用**。MCP 工具核准由 Codex 代理端執行。 |
| `32119bac269b881d512b82df74af285e8c60adc2` | 移除僅供測試的外掛指引 renderer<br>`codex-rs/core/src/plugins/render.rs` | **不適用**。Tap 沒有以外掛指引組裝推理提示的端點。 |
| `abc92e6bd4ee43b9fd2a33f63610355a538bd320` | 透過 app-server 回報每個 MCP 伺服器啟動耗時<br>`codex-rs/codex-mcp/src/connection_manager.rs` | **不適用**。Tap 的傳輸計時由回合及連線事件產生，沒有 MCP 啟動事件。 |
| `1f43c02f3f3489edd61c6426fc5d392bea236a41` | 簡化外掛的 MCP 核准策略測試<br>`codex-rs/core-plugins/src/manager_tests.rs` | **不適用**。Tap 沒有 plugin manager 或 MCP 的核准策略。 |
| `cd97e717de79de8b17b29ffb716bb89f993a31a5` | Linux sandbox 升級 etherparse 至 0.21.1<br>`codex-rs/linux-sandbox/Cargo.toml` | **不適用**。Tap 依賴鎖檔沒有 etherparse。此依賴屬本機沙箱網路路由。 |
| `8b096dce07b94aa4428ca310b871e5a873c8d2a2` | 移除 /mcp 顯示遮蔽敏感值的 TUI snapshot<br>`codex-rs/tui/src/history_cell/tests.rs` | **不適用**。上游只清理 TUI 測試，Tap 無 /mcp 畫面。 |
| `a0d5b0a661db3b34cd87bdf1c93736b90fd3694b` | 留言板連線尚在時允許閒置子執行緒卸載<br>`codex-rs/message-board-mcp/src/app_server.rs` | **不適用**。該 WS 為 app-server 控制面。Tap 各帳號管理自己的 Responses 連線。 |
| `50aeac27d825007a0ac6bd846087ed4e7095bdb3` | 在外掛快取案例驗證 MCP 工具核准覆寫<br>`codex-rs/core-plugins/src/manager_tests.rs` | **不適用**。外掛快取讀取與工具核准不在 Tap 呼叫鏈。 |
| `e351ffa4b625325575e2a49e8b554e45c1c73b88` | 移除舊的 /mcp 渲染器及測試依賴<br>`codex-rs/tui/src/history_cell/mcp.rs` | **不適用**。TUI 渲染與 Tap 出站傳輸相依無交集。 |
| `75df5098a6d92b40dc61e7a9964233ce9c359677` | 用 app-server 遞送記憶體留言板通知<br>`codex-rs/message-board-mcp/src/app_server.rs` | **不適用**。Tap 不提供代理留言板、其持久化或通知路由。 |

## 下游接入

Tap 的兩個 ABI 沿用原有端點與狀態。下一次 Tap 正式版提供雙架構附件後，vm2api 維護者可在其倉庫執行 `python3 scripts/release/sync_tap_payload.py --apply` 核對 SHA256 與建置記錄、準備鏡像及更新 `deploy/payload.json`。下游部署後逐槽核對核心版本、能力與健康狀態。
