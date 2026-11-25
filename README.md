# kin-codex-kernel

TapModels 與 MaxModels 使用的 Codex 多帳號共享核心，安裝路徑為 `bin/kin-codex-kernel`。本倉庫平時為私有；Linux 二進位由本倉庫的 GitHub Release 提供。

- **原 Tap 核心的平台核查紀錄：** [修正範圍、驗證與尚未完成的發行關卡](docs/TAP_ONLY_AUDIT.md)
- **設計與契約：** vm2api 的 `docs/CODEX_SHARED_KERNEL_DESIGN.md`、`docs/CODEX_RUST_CLUSTER_PLAN.md`
- **程式結構與工程準則：** [ARCHITECTURE.md](ARCHITECTURE.md)
- **Responses 原生續接、安全重送與首文字觀測：** [讀取每輪的計時](docs/RESPONSES_TIMING.md)

## 目前狀態

單帳號相容模式（ABI 1）與[多帳號共享模式（ABI 2）](docs/ABI2.md) 都支援 Responses HTTP/SSE、WebSocket、續接與取消。核心透過受 internal token 保護的 Unix socket 提供服務；以 `/internal/v1/describe` 核對執行中二進位支援的能力。共享模式還負責帳號登錄、憑據刷新與列舉的帳號操作。

## 開發

工具鏈固定在 `rust-toolchain.toml`（與官方 openai/codex 一致）。本機沒有 Rust 時，可以用同版本的官方容器：

```bash
docker run --rm -v "$PWD":/work -w /work rust:1.99.1-bookworm \
  bash -c 'cargo fmt --all -- --check && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked'
```

執行：

```bash
cargo run -p kin-codex-kernel -- path/to/codex-kernel.json
```

設定檔格式見 `crates/contract/src/config.rs`；日誌層級用 `KIN_LOG`，JSON 格式用 `KIN_LOG_FORMAT=json`。既有設定仍可使用 `TAP_LOG` 與 `TAP_LOG_FORMAT`。

## 發布

以下規則只適用於 `holeenlu/kin-codex-kernel`。可見性由有管理權限的發版操作者切換，不需要把管理權杖交給 Actions。

每版從該版本標籤內的 `docs/releases/vX.Y.Z.md` 發布繁體中文說明。第一行寫 `# vX.Y.Z`，可在版本後加上本版主題。以下五個二級標題各寫至少一個以 `- ` 開頭的項目。撰寫時比較上一個已發版本，將每項行為與提交、同步紀錄或測試核對。

| Markdown 標題 | 讀者需要知道的內容 |
| --- | --- |
| `## 新增功能` | 誰能在什麼情境使用新能力，以及如何啟用。 |
| `## 優化` | 哪個操作或路徑有改善。提到效能數字時附上量測條件。 |
| `## Bug 修正` | 觸發條件、原來可見的問題，以及升級後的行為。 |
| `## 升級影響` | ABI、配置、資料與下游載荷釘選的影響，並說明操作者要做什麼。可直接升級時也請明寫。 |
| `## 驗證結果` | 已執行的檢查、涵蓋的環境與結果。列出影響判讀的略過、失敗或尚未驗證範圍。 |

前三類若當版沒有相應變更，寫「本版無此項」。建置提交、Actions 連結、雙架構驗證與附件校驗指令由發版工作在建置成功後附加。已發布版本的寫法可參考 [v0.4.2](https://github.com/holeenlu/kin-codex-kernel/releases/tag/v0.4.2) 與 [v0.4.3](https://github.com/holeenlu/kin-codex-kernel/releases/tag/v0.4.3)。

1. **在私有狀態準備。** 核對倉庫、待發提交與工作樹，更新 `Cargo.toml` 和 `Cargo.lock` 的版本，完成相關測試並將結果寫入該版說明。將實際版本代入 `python3 .github/scripts/check_release_notes.py vX.Y.Z`，檢查輸出的全文並隨版本提交，再建立本機標籤。
2. **發版前才公開。** 執行 `gh api -X PATCH repos/holeenlu/kin-codex-kernel -F private=false`。確認 API 回傳 `visibility: public`，匿名取得倉庫也回 200，再推送版本提交與標籤。公開期間原始碼及既有 Release 也可被讀取。
3. **等 GitHub 完成編譯與發布。** 標籤推送與手動重跑都先從對應標籤檢查說明。缺檔、缺章、空項或待補文字會中止發布。`release` 通過品質關卡後建置 linux/amd64 與 linux/arm64（glibc ≤ 2.34），以正式版發布並用 `--latest` 明確設為 Latest，讓 [releases/latest](https://github.com/holeenlu/kin-codex-kernel/releases/latest) 指向本次版本。若標籤已存在、Release 尚未建立，以 `gh workflow run release.yml -R holeenlu/kin-codex-kernel --ref vX.Y.Z -f tag=vX.Y.Z` 使用該標籤的建置流程重跑，不移動標籤。不可將只有標籤、工作已排隊或草稿視為發版成功。
4. **驗收後立即恢復私有。** 確認 Actions 成功、Release 已發布、兩份二進位、兩份建置記錄、`SHA256SUMS`、`NOTICE` 齊全，核對來源提交及檔案雜湊，並比對 Release 正文與標籤內的五節說明、附加的建置資訊。執行 `gh api repos/holeenlu/kin-codex-kernel/releases/latest --jq .tag_name`，確認回傳本次版本標籤。隨即執行 `gh api -X PATCH repos/holeenlu/kin-codex-kernel -F private=true`，確認管理 API 回傳 `private: true`、匿名查詢倉庫與新版 Release 都回 404，且具讀取權的帳號仍可下載。不要為等待下游發版或部署延長公開時間。
5. **失敗或中止。** 先確認 Release 與附件的實際狀態；需要在當次窗口立即重試時沿用原標籤。若無法繼續，先恢復並核驗私有，再報告失敗或部分發布；下次重試重新開啟窗口。

Linux 附件為 `kin-codex-kernel-linux-amd64` 與 `kin-codex-kernel-linux-arm64`，各附同名的 `.build.json`。下游依架構與 `SHA256SUMS` 核對下載檔，再安裝為 `bin/kin-codex-kernel`。舊版附件沿用各版本發布時的名稱。
