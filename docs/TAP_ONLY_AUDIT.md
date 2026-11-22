# Tap 專用 Codex 核查與修正（2026-11-18）

## 發行與交付結果

2026-11-18 已完成使用者授權的發布與平台釘選交付：

- [Tap v0.4.0](https://github.com/holeenlu/tap-codex-kernel/releases/tag/v0.4.0) 已發布，正式來源為 `707a7e68e6350f19bae27539896a4cce8487dfdb`。[GitHub 發行流程](https://github.com/holeenlu/tap-codex-kernel/actions/runs/44164340241) 通過品質關卡與 Linux amd64／arm64 正式建置。
- 已核對兩份二進位、兩份建置記錄、SHA256SUMS 與 NOTICE 的來源提交、檔案大小、SHA256、ELF 架構與 glibc ≤ 2.34。兩個正式產物各自通過 87 項平台隔離契約，零失敗（各 98 項中的 11 個略過僅為缺少二進位時的提示案例）。
- 核對附件後立即恢復 `holeenlu/tap-codex-kernel` 私有；管理 API 確認 private，匿名倉庫與 v0.4.0 Release API 皆回 404，授權帳號下載 SHA256SUMS 仍成功。
- vm2api 的元件鏡像 `payload-tap-codex-v0.4.0` 已發布，`deploy/payload.json` 釘選上述正式來源與雙架構成品。[平台提交 `96ab150e`](https://github.com/holeenlu/vm2api/commit/96ab150e530d4d8d62510a5e3da010a53d19ccbf) 已推送 main；唯一父提交仍為 `91c9be8f3fbc17b89426048375cd1be941ea037f`，沒有同步上游或增加應用版本。
- 推送前發現遠端獨立提交 `5d67640b`，其 WebSocket 取消結算／升級排空修復已完整保留；合併後 55 項定向測試及 Tap 續接契約通過，再以精確 lease 推送。舊歷史完整 bundle 已驗證保留，本地／遠端 main 一致。
- 此次只發布 Tap 元件與交付平台程式碼，未發布 vm2api 應用版本、未部署生產。平台私有倉庫 CI 另有帳戶執行限制，不能視為已通過；下方四個全平台環境／元件驗收項亦沒有隱藏。

| 正式產物 | 大小（bytes） | SHA256 |
| --- | ---: | --- |
| linux/amd64 | 15657664 | `d57c966cbba2cdc58311f0c1b097534f8a61b17bc13f84c502f068839dd316de` |
| linux/arm64 | 13429496 | `48132d50572606411018ad2a51947700f5674701c5bcddf6da1e1e30e2340285` |

## v0.4.1 審查修正發行（2026-11-18）

v0.4.0 程式審查的 15 項發現中，14 項修正，1 項（自訂 UA 的產品標記帶出 originator 與版本）確認為設計並寫入 [ABI2 的逐請求設定](ABI2.md#逐請求設定)。重點：

- metadata 與 turn metadata 改為位元組級就地編修，不再重排欄位。
- WebSocket 續接不計每輪不同的 `client_metadata`。
- 推理、帳號操作與換票共用同一身分，設定錯誤在啟動時拒絕。
- 分析事件對齊官方錄製並遵守熔斷。
- `telemetry_runtime` 改變時重建執行環境；健康回應只計有可用 token 的列。

發行與核對：

- [Tap v0.4.1](https://github.com/holeenlu/tap-codex-kernel/releases/tag/v0.4.1) 已發布（prerelease），正式來源為 `93af9c6429f542e8e8879e0ed527153409807f8b`，附註標籤 `v0.4.1`。
- **本機驗證：** macOS ARM64 `cargo fmt --check`、clippy（全 workspace／all-targets／`-D warnings`）與 `cargo test --workspace --locked --features openssl/vendored` 通過，145 項測試（v0.4.0 為 130 項）。
- **發行流程：** [GitHub 發行流程](https://github.com/holeenlu/tap-codex-kernel/actions/runs/44197471694)（2026-11-17 23:13–23:21 UTC）各步驟皆成功：版本與標籤核對、品質關卡、Linux amd64／arm64（glibc ≤ 2.34）建置與發布。
- **附件核對：** 6 個附件齊全，`SHA256SUMS` 全數相符；兩份 ELF 分別為 x86-64 與 aarch64。兩份建置記錄的來源提交為 `93af9c6`、tree `0871b286`，未含工作樹改動，`Cargo.lock` 雜湊與本機一致；建置時可見性記為 `public`。
- **公開窗口：** 推送前公開，核對附件後立即恢復私有，約 10 分鐘。管理 API 回 `private: true`，匿名查詢倉庫與 v0.4.1 Release 皆回 404，授權帳號仍可下載。
- **尚未進行：**
  - 未對 v0.4.1 正式二進位重跑 vm2api 隔離契約。
  - vm2api 未釘選 v0.4.1。
  - 未部署生產。
  - v0.4.0 起 `options.user_agent` 生效；若 vm2api 仍帶舊的預設 UA，出站身分會改用它，需在平台端確認。

| 正式產物 | 大小（bytes） | SHA256 |
| --- | ---: | --- |
| linux/amd64 | 15651712 | `ef06020181a6a83220fc67f2208384c83824199530226b41c29e6819f3084d58` |
| linux/arm64 | 13431328 | `e28309646062db94430506d32aeea72a5b9cd6f15c075ebe7242711c702e03d5` |

## 核查範圍與修正

| 路徑 | 修正及驗證重點 |
| --- | --- |
| 每槽啟動、升級後接管 | 唯一候選是 Tap；舊核心只供排空退役識別。檢查 `health.provider`，不放行非 Tap。無效顯式二進位路徑不回退其他版本。未正常回答的存活 listener 不會被刪除 socket。 |
| 共享核心與帳號操作 | 同時核對 health provider、describe kernel、ABI 與 multi_account；不符即停止帳號登錄和派發。補足 account_ops、analytics 能力宣告。 |
| 設定與請求參數 | UA、auto、client_features、analytics、telemetry_runtime 與 client_environment 接通；JSON 跳脫鍵也能正確處理 metadata，安裝身分取帳號設定，保留其他本體資料。分析事件有獨立併發限制及會話去重，失敗不重送推理。 |
| 憑據及刷新 | Node、Tap 推理、共享換票、額度查詢、vault 和匯出對齊第一筆有效 access token；空列不遮蔽有效列，刷新寫回同一列並保留其他列。共享與每槽仍各有唯一憑據寫入者。 |
| 匯入、重建、面板 | 移除核心選擇；保留每槽／共享模式。修正匯入的未定義變數與暫停記錄格式。共享匯入成功需登錄及健康通過，失敗保留憑據和可恢復 hold。 |
| Docker、payload、發行附件、節點 | 不再安裝或打包舊 Codex 核心；Tap 雙架構及 NOTICE 沿用正式 payload 路徑。修正 CI 將 ARM64 成品當成 x86-64 驗證的錯誤。Codex 在控制面執行，遠端槽不再攜帶未使用的 Codex 二進位。 |
| 契約測試與文件 | 代理、回應格式測試直接驗證明確的 Tap 契約，不再因沒有 Kin 對照程式而跳過。修正建置、部署、API 與設計文件的現行指引；歷史測量和發行記錄保留並標明用途。 |

保留 `kin-kernel` 等仍供 Claude 使用的元件，以及既有內部 ABI 的 `x-kin-*` 標頭、共用控制面環境變數；它們不構成對 `kin-codex-kernel` 的執行依賴。舊 `codex_kernel_impl` 已不參與核心選擇；面板 API 的 `impl: tap` 僅供既有客戶端冪等確認，其他值拒絕。

## 實際驗證

| 檢查 | 結果 |
| --- | --- |
| Rust 1.95.0 macOS ARM64 `cargo test --workspace --locked --features openssl/vendored` | 130 項通過；macOS 額外啟用 vendored OpenSSL 供直接 OpenSSL 依賴建置。 |
| Linux ARM64 `cargo fmt --check`、clippy 全 workspace／all-targets／warnings-as-errors、`cargo test --workspace --locked` | 全部通過，130 項測試。 |
| Linux ARM64 Tap 真實二進位、完整平台 kernel-contract suite | 98 項：87 通過、0 失敗；11 個略過是「未提供二進位」提示案例，實際已提供 Tap。涵蓋 HTTP、WS、TLS 基準、代理、隔離、取消、續接、刷新、帳號操作及共享套件匯入。全程 `--network none`，沒有真實供應商請求。 |
| 商業授權、計費、模型目錄及健康探測限額回歸 | Linux 隔離容器 18 項通過。使用本次源碼建置的 API helper；可寫設定放在 tmpfs，原工作樹唯讀。 |
| 發行工具與部署測試 | 發行工具 86 項通過；部署 97 項在 Linux 通過。 |
| 前端 | 受影響的 63 項測試、i18n 檢查、TypeScript 與正式 build 通過；macOS 內建舊 Python 的 tomllib 缺失，改用已安裝 Python 3.12 後相關測試通過。 |
| Node 全平台擴大回歸 | macOS 577 個隔離測試檔中 552 個通過。對其餘 25 檔補查：20 檔在 Linux 通過，health-real-quota 在可寫 tmpfs 設定下也通過。剩餘 4 檔見下節，**沒有宣稱全量通過**。本次 Codex 與槽位匯入相關測試均通過。 |
| 語法與格式 | 修改的 Node 檔語法檢查、Biome／Prettier 及兩倉庫 diff whitespace 檢查通過。 |

## 尚未關閉的驗收項

- `oauth-binary.test.mjs`：缺少私有發行品 `kin-oauth-auth`，本輪沒有下載或驗證該 Claude OAuth 二進位。
- `gemini-host.test.mjs`、`kernel-catalog.test.mjs`：案例要求 x86-64；在 ARM64 分別得到 unsupported_host 與 arch_mismatch。相關 Gemini／CRAG 程式未在本次修改。
- `egress.test.mjs`：Linux ARM64 輔助程式的三個程序擁有權／就緒案例仍失敗，需要在正式 CI 環境核對。代理契約中 Tap 的 SOCKS5／SOCKS5h／HTTP CONNECT 與無直連回退檢查已通過；未將兩者混為同一驗收。
- 未做真實供應商推理、線上流量遷移或生產部署驗收。

## 變更統計與起點

工作樹起點：Tap `72a12da39c9cd8ff318010ceb56625ba79e4023e`，vm2api `cec64e118014a7d00d3ae3eeed36034444c4925a`。保留並接續先前模型在同一任務的未提交修改；未追趕上游。

以下為初次核查的任務差異統計，不含後續保留的遠端 WebSocket 獨立修復。統計包含新增的 `crates/upstream/src/options.rs`；以檔案歸類，Rust 原始碼內嵌單元測試列於程式檔：

| 專案／分類 | 新增行 | 刪除行 | 淨變化 |
| --- | ---: | ---: | ---: |
| Tap 程式檔（含內嵌單測） | 393 | 32 | +361 |
| Tap 獨立測試檔 | 114 | 8 | +106 |
| vm2api 程式、前端資源與工具 | 163 | 192 | -29 |
| vm2api 測試與測試說明 | 264 | 130 | +134 |

文件、部署清單及版本欄位另外計入 Git diff；未列為業務程式。新增邏輯處理相容設定與 Tap 身分檢查，刪除邏輯為已退役核心的選擇、打包與啟動路徑。


## 本次資源收口

- 驗證輸出與來源檔雜湊收據保留在本次聊天的 `tap-only-audit` 附件目錄，合計約 366 KiB，便於復核；未保留整份平台臨時副本為正式交付物。
- `cargo clean` 已清除本輪 macOS 建置輸出（工具回報 2.3 GiB），本輪組合的 Docker 測試映像標籤已移除；既有 Rust／Node／Go 映像與 Docker 建置快取保留。
- 原臨時工作目錄 80 MiB 已移至本機垃圾桶，可還原；不是已釋放磁碟容量。
- Rust 工具鏈／cargo 快取約 846 MiB、平台前端相依約 1.1 GiB 保留供後續開發及發行，未修改使用者 shell 的 PATH 設定。這些屬可重用 SDK／相依，不做機器層級清除。
