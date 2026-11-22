# tap-codex-kernel

tapmodels 自建的 Codex 多帳號共享核心，用來取代上游的 `kin-codex-kernel`。本倉庫平時為私有；Linux 二進位由本倉庫的 GitHub Release 提供。

- **Tap 專用平台核查：** [修正範圍、驗證與尚未完成的發行關卡](docs/TAP_ONLY_AUDIT.md)
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
cargo run -p tap-codex-kernel -- path/to/codex-kernel.json
```

設定檔格式見 `crates/contract/src/config.rs`；日誌層級用 `TAP_LOG`，JSON 格式用 `TAP_LOG_FORMAT=json`。

## 發布

以下規則只適用於 `holeenlu/tap-codex-kernel`。可見性由有管理權限的發版操作者切換，不需要把管理權杖交給 Actions。

1. **在私有狀態準備。** 核對倉庫、待發提交與工作樹，更新 `Cargo.toml` 和 `Cargo.lock` 的版本，完成相關測試，在本機建立對應的 `vX.Y.Z` 標籤。
2. **發版前才公開。** 執行 `gh api -X PATCH repos/holeenlu/tap-codex-kernel -F private=false`。確認 API 回傳 `visibility: public`，匿名取得倉庫也回 200，再推送版本提交與標籤。公開期間原始碼及既有 Release 也可被讀取。
3. **等 GitHub 完成編譯與發布。** `release` 通過品質關卡後建置 linux/amd64 與 linux/arm64（glibc ≤ 2.34）。若標籤已存在、Release 尚未建立，以 `gh workflow run release.yml -R holeenlu/tap-codex-kernel --ref main -f tag=vX.Y.Z` 從原標籤重跑，不移動標籤。不可將只有標籤、工作已排隊或草稿視為發版成功。
4. **驗收後立即恢復私有。** 確認 Actions 成功、Release 已發布、兩份二進位、兩份建置記錄、`SHA256SUMS`、`NOTICE` 齊全，核對來源提交及檔案雜湊。隨即執行 `gh api -X PATCH repos/holeenlu/tap-codex-kernel -F private=true`，確認管理 API 回傳 `private: true`、匿名查詢倉庫與新版 Release 都回 404，且具讀取權的帳號仍可下載。不要為等待下游發版或部署延長公開時間。
5. **失敗或中止。** 先確認 Release 與附件的實際狀態；需要在當次窗口立即重試時沿用原標籤。若無法繼續，先恢復並核驗私有，再報告失敗或部分發布；下次重試重新開啟窗口。

vm2api 發版準備工具 `scripts/release/sync_tap_payload.py` 選取已發布的最新 Tap 版本，核對兩個架構的建置記錄、大小及 SHA256，將附件鏡像到 vm2api，再由 `deploy/payload.json` 釘選。vm2api 映像包含這些二進位，容器啟動時將它們裝入掛載的 `bin/`。
