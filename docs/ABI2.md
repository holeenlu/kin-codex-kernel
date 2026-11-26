# ABI 2：多帳號共享模式的內部 API

依據 vm2api `docs/CODEX_SHARED_KERNEL_DESIGN.md` 第 4.0–4.5 節。型別在 `crates/contract`（`registry.rs`、`envelope.rs`、`error.rs`）。設計沒有明寫、由本文件補定的地方，集中列在最後一節。

## 啟動與宣告

- 啟動：`kin-codex-kernel --shared <shared.json>`。程序層設定只有 `socket_path`、`internal_token`、`max_accounts`（預設 1024）與測試端點；internal token 與單帳號模式一樣直接寫在設定檔（0600），不另設 token 檔；帳號由下列 API 登錄，不寫在設定檔。單帳號模式（ABI 1）`kin-codex-kernel <config.json>` 照舊。
- `GET /internal/v1/describe`：`abi: 2`，`kernel: "kin-codex-kernel"`，`capabilities` 含 `multi_account`、`account_ops` 與 `analytics`，另有 Responses／WS／取消與續接能力。控制面須同時核對 health 的 provider 與 describe 的 kernel。
- 認證、socket 權限、`x-kin-upstream-sent` 與錯誤本體格式都與 ABI 1 相同。

## 端點

| 端點 | 方法 | 說明 |
| --- | --- | --- |
| `/internal/health` | GET | `SharedHealth`：`{ok, engine, provider, version, worker_version, build_sha, abi: 2, registry_state, accounts: {total, ready, failing}}`；沒有 `status` 欄位 |
| `/internal/v1/accounts` | GET | `AccountList`：`registry_state` 加每個帳號的 `ref`、兩種代次與 `state` |
| `/internal/v1/accounts/reconcile` | POST | `{accounts: [AccountEnv…]}`，以完整清單取代整個登錄 |
| `/internal/v1/accounts/{ref}` | PUT | 本體 `AccountEnv`，代次 CAS |
| `/internal/v1/accounts/{ref}` | DELETE | 本體 `{env_generation}`；`?force=1` 取消在途請求 |
| `/internal/v1/accounts/{ref}/health` | GET | `AccountHealth`，不含秘密 |
| `/internal/v1/codex/responses` | POST | 與 ABI 1 相同的 envelope，另加必填的 `account: {ref, env_generation}` |
| `/internal/v1/cancel` | POST | `{request_id}`，與 ABI 1 相同 |
| `/internal/v1/accounts/{ref}/credential` | PUT | `{credential, env_generation, expected_credential_generation}`：匯入或替換憑據，由核心寫入；回 `{ok, ref, credential_generation}` |
| `/internal/v1/accounts/{ref}/ops/refresh` | POST | 本體必填 `{env_generation}`。強制換票；只回 `{ok, ref, expires_at, credential_generation}`，不回任何 token |
| `/internal/v1/accounts/{ref}/ops/{op}` | POST | 帳號操作代發：`usage`、`accounts_check`、`models`、`reset_credits`、`consume_credit`（K2d）；本體 `{request_id, env_generation, body}` |

## 登錄狀態

- 核心啟動時 `registry_state` 為 `empty`；登錄不持久化。
- 控制面送 `reconcile`；進行中為 `reconciling`，完成後為 `ready`。
- `empty` 與 `reconciling` 期間，推理與帳號操作都回 `503 registry_not_ready`（`x-kin-upstream-sent: 0`）。

## `PUT /internal/v1/accounts/{ref}` 的語義

| 情況 | 結果 |
| --- | --- |
| 帳號不存在，且沒有墓碑或墓碑代次較舊 | 建立，`201` |
| 目標代次大於現有代次，且 `expected_env_generation` 等於現有代次 | 更新，`200` |
| 目標代次等於現有代次、內容相同（不比 `expected_env_generation`） | 冪等，`200` |
| 目標代次等於現有代次、內容不同 | `409 generation_conflict` |
| 目標代次小於現有代次，或小於等於墓碑代次 | `409 stale_generation`，不套用 |

成功回 `AccountAck {ok, ref, env_generation}`。更新不中斷在途請求：新請求用新環境，舊的在途請求用完舊環境後才回收。

## `DELETE /internal/v1/accounts/{ref}` 的語義

- 代次小於現有代次或小於墓碑代次：`409 stale_generation`。晚到的舊 DELETE 刪不掉新登錄。等於墓碑代次是同一個 DELETE 的重送，冪等回 `200`。
- 否則立刻停止接新請求（新請求回 `409 account_disabled`），留下帶代次的墓碑；在途請求結束後移除。
- 回 `DeleteAck {ok, ref, tombstone_generation, inflight}`；`inflight > 0` 表示仍在排空。

兩種 ABI 的呼叫端每次發送均使用新的 request_id。相同 ID 在原請求結束前回 409；取消只中止原登記，釋放後再取消回 cancelled: false。控制面不得將已完成的 ID 配給另一筆工作。

## 推理信封

```json
{ "body": {}, "headers": {}, "stream": true, "session": {"session_id": "s1"},
  "options": {"upstream_transport": "prefer_websocket", "installation_id": "…", "telemetry_runtime": {…}},
  "request_id": "r1",
  "account": {"ref": "vm-25", "env_generation": 8} }
```

- 缺 `account`：`400 invalid_request`。帳號不在登錄：`404 account_unknown`。停用或排空中：`409 account_disabled`。
- `env_generation` 與登錄不同：`409 account_generation_mismatch`。
- 實際使用的 `installation_id`、`telemetry_runtime` 以登錄為準。

## 憑據與刷新（K2c）

升級順序為先更新控制面，使匯入及強制刷新都傳送派發時的環境代次，再升級核心。缺少代次回 400；與目前登錄不符時回 409 account_generation_mismatch，憑據檔與上游均無變更。匯入另以憑據代次執行 CAS。

- **唯一寫入者：** 共享模式下憑據檔只由核心寫入（匯入走 `PUT .../credential`，換票由核心自己做）。檔案沿用 `{accounts: [{...}]}`，頂層加 `kin_credential_generation`；`expires_at` 與 vm2api 相同用 Unix 毫秒，取自 access token 的 JWT `exp`；另記 `last_refresh`（毫秒）。寫入為暫存檔、fsync、rename、fsync 目錄，權限 0600，保留其他欄位。
- **兩把鎖：** 換票的單飛鎖與寫檔鎖分開。匯入只拿寫檔鎖，不等進行中的換票；換票晚回來時，提交前發現帳號唯一鍵或 refresh token 已變，就丟掉自己的結果、改用新的憑據（只有代次變了、refresh token 相同時照樣寫入，否則檔案會留下已被消耗的 refresh token）。
- **以憑據檔為單位：** 兩把鎖與退避狀態依 `credential_path` 在整個程序只有一份：帳號被刪除後重建、對帳換新閘門時，同一個檔案仍只有一個換票者。帳號換了 `credential_path` 就換一份（舊檔的重新授權標記與退避不帶過來）。
- **不受取消影響：** 換票與寫回在獨立任務裡做完：控制面斷線、請求被取消，token 端點一旦輪換了 refresh token，新的照樣寫進檔案。寫檔失敗當場重試 3 次；仍失敗時新 token 留在記憶體、回 `502 refresh_failed`（unknown），下次換票前先補寫。程序結束前最多等 30 秒讓進行中的換票寫完。
- **請求前刷新：** access token 在 5 分鐘內到期就先換票（`proactive_expiry`）；讀不到到期時間且 `last_refresh` 超過 8 天也換（`scheduled`）。暫時失敗照官方，用舊 token 繼續送。
- **401 復原：** 同一個 token 重讀後重試 → 再 401 就以 `unauthorized_recovery` 換票後重試 → 第三次仍 401 交回控制面（`401 upstream_auth`，`x-kin-upstream-sent: 1`）。
- **背景刷新：** 每 30 秒檢查近 15 分鐘用過的帳號；到期窗口另加依帳號固定的 0–150 秒偏移。閒置帳號不刷新。官方沒有背景刷新，這是設計第 5.4 節的決定。
- **換票請求：** 照官方 CLI 0.171.0（私有基準 `fidelity/baseline/.../refresh.json`）：走帳號自己的 HTTP client 與出口；身分與當下的推理請求相同；本體為 JSON、欄位依字母排序。
- **失敗：** `refresh_token_expired`／`refresh_token_reused`／`refresh_token_invalidated`、401、400 加 `invalid_grant` 為永久失敗，帳號標為需要重新授權，之後的請求回 `401 reauth_required`（確定沒送出），直到匯入新憑據。未知的 401 錯誤碼統一記為 `http_401`。換票錯誤正文最多讀 16 KiB，成功正文上限為 256 KiB；超限回暫時失敗。其他為暫時失敗：30 分鐘內 5 次就停 10 分鐘（`ops/refresh` 不受限，401 復原也要守）；access token 已過期、且從第一次暫時失敗起連續 2 小時換不到票，也標為需要重新授權。連不到 token 端點（確定沒送出）是出口問題，不計入。失敗回來時若檔案已被匯入換掉（代次或 refresh token 不同），失敗屬於舊 token，不標記、不計入，直接採用新憑據。
- **健康：** 帳號健康回 `credential`（`ok`、`refreshing`、`reauth_required`、`missing`）與 `credential_generation`；`/internal/health` 的 `accounts.failing` 計入需要重新授權的帳號；`GET /internal/v1/accounts` 帶各帳號的憑據代次。回應、健康與日誌都不含 token。

## 帳號操作、cookie 與熔斷（K2d）

- **帳號操作：** 照官方 CLI 0.171.0 代發（私有基準 `fidelity/baseline/.../ops-and-cookies.json`）：
  - `usage` → `GET /backend-api/wham/usage`（帶 `x-openai-codex-luna-reserve: 1`）；`accounts_check` → `GET /backend-api/wham/accounts/check`；`reset_credits` → `GET /backend-api/wham/rate-limit-reset-credits`；`consume_credit` → `POST /backend-api/wham/rate-limit-reset-credits/consume`，本體 `{redeem_request_id, credit_id?}`（`redeem_request_id` 必填，由控制面產生並原樣轉送）。這四個由官方的 backend-client 送出：沒有 `originator`，只有 User-Agent。
  - `models` → `GET /backend-api/codex/models?client_version=<版本>`，帶 `version` 與 `originator`。
  - 身分沿用這個帳號最近一次推理的帳號環境（會話中為 `codex-tui`）；帳號操作本身不算「使用過」，不會讓閒置帳號進入背景刷新。
  - 檢查與推理相同（登錄狀態、啟用、代次），但不占推理的並發名額；送出前會照推理的規則先換票。本體先以登錄中最大的上限讀入，取得帳號後再比對該帳號自己的 `max_request_bytes`。
  - 取消與推理相同：掛在帳號閘門下，`POST /internal/v1/cancel`（帶 `request_id`）與 `DELETE ?force=1` 都能中止，包括已收到上游回應、正在讀本體的時候（回 `x-kin-upstream-sent: unknown`）。
  - 回應原樣轉回：狀態碼、本體、`content-type` 與上游的 `x-codex-*`，並帶 `x-kin-upstream-sent: 1`。上游的 401 也原樣轉回，不做推理那套三步復原。
  - `search` → `POST /backend-api/codex/alpha/search`；`images_generate` → `POST /backend-api/codex/images/generations`；`images_edit` → `POST /backend-api/codex/images/edits`（本體是 JSON，圖片以 `images: [{image_url: "data:..."}]` 帶入，不是 multipart）。這三個由 codex-api 送出：`version` 在最前；圖片接著 `x-codex-image-turn-id`；三者都有 `x-codex-turn-metadata`；之後 `authorization`、`chatgpt-account-id`、`content-type`、`originator`、`user-agent`、`accept`。官方是模型呼叫工具（`web.run`、`image_gen.imagegen`）時由 CLI 自己送出，以假模型回工具呼叫錄得。
  - 這三個的請求為 `{request_id, env_generation, body, headers}`。`body` 必須是 JSON 物件，由控制面組好、核心**原樣**轉送（不重新序列化，欄位順序照控制面給的，順序本身就是可辨識的差異）；缺本體或不是物件回 `400 invalid_request`。`headers` 只取小寫鍵 `x-codex-turn-metadata` 與 `x-codex-image-turn-id`（後者只用於圖片），其他一律不轉；turn metadata 裡的 `installation_id` 換成帳號登錄的，與推理相同只認 UUID 形式（其他值不替換，避免誤換相同的子字串），其他內容與順序不變。
  - 請求本體上限同推理；回應本體上限：圖片 64 MiB，其他 8 MiB。已收到上游回應、但本體讀到一半失敗時回 `x-kin-upstream-sent: unknown`：圖片是付費操作，控制面不得因此自動重新生成。
  - `consume_credit` 沒有錄到（TUI 只在使用者操作時消費），標頭順序由同一個 client 的查詢請求與原始碼推出。
- **cookie：** 每個帳號執行環境一份 cookie 罐（reqwest 的 Jar，版本同官方），只保存官方允許的 Cloudflare 與 `__oailb` cookie，只用於 chatgpt.com 系列的 HTTPS 與 WebSocket。HTTP 的 `cookie` 在明確標頭之後；WebSocket 在插入順序的最後（分支搬移後緊接在 `Sec-WebSocket-Key` 之後）。官方是整個程序共用一份；共享模式多帳號同程序，所以改為每帳號一份，出口改變時跟著重來。單帳號模式同樣保存與送出 cookie（擬真）。
- **熔斷（只在共享模式）：** 每個執行環境（帳號 × 出口）HTTP 與 WebSocket 各一個。連續 3 次「連線還沒建立就失敗」就斷開；斷開期間 HTTP 直接回 `502 proxy_unavailable`（`x-kin-upstream-sent: 0`），WebSocket 改走 HTTP。冷卻 `breaker_cooldown_ms`（程序層設定，預設 30000）後放一個探測請求（半開）。探測被取消、沒有結果時讓下一個請求重新探測；每次斷開或恢復換一個代次，斷開前放行的請求晚到的結果不改變半開的判定。WebSocket 撥號與握手 30 秒逾時，逾時算連線失敗。帳號健康回報 `breaker`；HTTP 斷開時帳號 `state: failing`，`/internal/health` 的 `failing` 計入。

## 錯誤碼

| 錯誤碼 | HTTP | `x-kin-upstream-sent` |
| --- | --- | --- |
| `registry_not_ready` | 503 | 0 |
| `account_unknown` | 404 | 0 |
| `account_disabled` | 409 | 0 |
| `account_generation_mismatch`、`generation_conflict`、`stale_generation` | 409 | 0 |
| `proxy_unavailable` | 502 | 0（連線前失敗）或 unknown（連線後失敗） |
| `reauth_required` | 401 | 0 |
| `request_id_in_use`（與任何在途推理或帳號操作重複） | 409 | 0 |
| `account_busy` | 429 | 0 |
| `body_too_large` | 413 | 0 |
| `refresh_failed`（`ops/refresh` 暫時失敗） | 502 | 0（未連上）或 unknown；unknown 時不得再要求換票，改以帳號健康的憑據代次確認 |
| 上游錯誤（`usage_limit_reached` 等） | 原樣 | 1 |

## 設計未明寫、本文件補定

1. **CAS 前提不成立：** 目標代次大於現有代次、但 `expected_env_generation` 不等於現有代次時，回 `409 generation_conflict`。設計第 4.3 節的表沒有這一列。
2. **對帳前的 PUT 與 DELETE：** `empty` 與 `reconciling` 期間一律回 `503 registry_not_ready`，強制先對帳。設計只寫了推理與帳號操作。
3. **身分不一致的錯誤碼：** 信封的 `installation_id` 或 `telemetry_runtime` 與登錄不同時，用 `409 account_generation_mismatch`。設計只寫「回 409」，沒有指定錯誤碼；處置與代次不符相同（重新同步後重派）。
4. **不存在帳號的 DELETE：** 冪等回 `200`，並記一筆該代次的墓碑。這樣 ACK 遺失時可以重送，晚到的舊 PUT 也會被拒。
5. **DELETE 不阻塞：** 立即回應並回報 `inflight`，帳號在在途請求結束後自動移除。設計第 7.4 節切換流程會在 DELETE 之前先在控制面排空，正常情況下 `inflight` 為 0。
6. **reconcile 的權威性：** reconcile 以控制面清單為準，不看 `expected_env_generation` 與墓碑；清單內的帳號會清掉自己的墓碑。不在清單內的帳號照 DELETE 處理（排空後移除、留下墓碑）；代次與內容都沒變的帳號保留原執行環境，在途請求不受影響。清單內重複的 `ref` 回 `400 invalid_request`。
7. **路徑與本體的 `ref`：** 兩者不同、或 `ref` 含檔名不安全的字元、超過 64 位元組時，回 `400 invalid_request`。`reconcile` 保留給對帳端點，不能當帳號參照。
8. **DELETE 的重送：** 設計對 PUT 寫「小於等於墓碑代次就是過時」。DELETE 若照搬，ACK 遺失後重送同一個 DELETE 會被拒，所以 DELETE 只有「小於墓碑代次」才算過時，等於時冪等成功。
9. **帳號健康與計數：** `failing` 計入需要重新授權的帳號（K2c）；熔斷（K2d）接入後再加。單一帳號的健康回報在需要重新授權、沒有 access token 或代理連不上時回 `state: failing`、`ok: false`。
10. **憑據 CAS 不符：** `PUT .../credential` 的 `expected_credential_generation` 不等於檔案代次時回 `409 generation_conflict`。
11. **`refresh_failed`：** 設計第 4.5 節沒有這個錯誤碼；`ops/refresh` 暫時失敗時用它，帶實際送出狀態。
13. **熔斷的參數：** 連續失敗門檻 3 次、冷卻 30 秒；設計只寫「短時間內連續失敗」與「單一探測」，數值待生產量測後調整。
14. **帳號操作不算使用：** 設計只說背景刷新看「近期有使用的帳號」；控制面定期查額度若算使用，會讓所有閒置帳號被背景刷新，所以只有推理算。
15. **控制面對帳號的命令：** `PUT .../credential` 與 `ops/refresh` 和推理一樣，`empty` 與 `reconciling` 期間回 `503 registry_not_ready`。`ops/refresh` 對停用或排空中的帳號回 `409 account_disabled`：控制面停用帳號是為了讓核心放手憑據檔，之後核心不能再換票；匯入仍可。
16. **一個憑據檔一個帳號：** `PUT` 與 `reconcile` 拒絕兩個登錄中的帳號共用同一個 `credential_path`（`400 invalid_request`），否則同一個 refresh token 會有兩個換票者。
17. **非串流的取消：** 非串流推理在讀上游回應途中被取消（`/cancel` 或 `DELETE ?force=1`）時回 `502 upstream_transport`。
18. **WebSocket 連線重用：** 閒置連線的鍵加上憑據指紋（`authorization` 與 `chatgpt-account-id`），換了憑據就不沿用舊連線。重用的連線在送出前就收到 Close 才換新連線重送；送出後斷線（reset、沒有 Close）回 `x-kin-upstream-sent: unknown`，不重送，避免上游重複生成。
12. **背景刷新的參數：** 「近期使用」取 15 分鐘、檢查間隔 30 秒、偏移 0–150 秒；設計只寫「近期有使用」與「加隨機抖動」，這些數值待生產量測後調整。


## 逐請求設定

**User-Agent：** 優先序是請求的 `options.user_agent`、單帳號設定的 `user_agent`、帳號環境。自訂值覆寫設定；`auto`（不分大小寫）改依帳號環境產生；缺欄位、空字串或只有空白都沿用設定。值不能放進標頭（換行等控制字元）或超過 512 位元組時，請求回 `400 invalid_request`，設定檔則在啟動時拒絕。自訂 UA 的第一個產品標記 `originator/版本` 帶出 `originator` 與 `version` 標頭（官方 UA 的格式）；沒有可用的產品標記時沿用 `codex-tui` 與目標版本。共享模式記下每個帳號最近一次推理的帳號環境與自訂 UA，帳號操作與換票用同一個身分。單帳號模式的 `describe.impersonate` 回報設定的 UA 帶出的身分。

**client_metadata：** `client_features: true` 合成本帳號的 installation id 與會話／輪次欄位，保留其他 metadata；顯式 `false` 移除 client_metadata 和標頭 installation_id。本體與 `x-codex-turn-metadata` 只就地改寫目標欄位，其他欄位的位元組與順序不變（新增的欄位加在最後）。未指定時沿用單帳號設定；未啟用合成且未顯式關閉時保留既有下游 metadata 的改寫行為。單帳號設定開啟 `client_features` 時 `device_id` 必須是 UUID，否則啟動時拒絕。WebSocket 續接比對對話設定時不計 `client_metadata`（每輪的輪次 id 不同），增量訊息帶本輪的值。

**analytics：** 預設關閉，逐請求可覆寫。啟用且有完整 telemetry_runtime、session 和 model 才送出 `codex_thread_initialized`；欄位、順序與標頭同官方錄製，client 名稱與版本取這次請求的 originator 與版本。使用該帳號的憑據、代理及 HTTP client，每個帳號／會話去重，失敗不重送推理；出口 HTTP 熔斷斷開時不撥號，半開的探測名額留給推理，連線失敗計入熔斷。共享模式的 installation_id 與 telemetry_runtime 必須符合帳號登錄；登錄的 telemetry_runtime 改變時重建該帳號的執行環境。metadata 身分一律取帳號設定。

**憑據列：** 單帳號憑據檔有多列時，推理與刷新都選第一筆有效 access token 的帳號；空列不遮蔽後面的有效憑據。單帳號健康回應的 `accounts` 是有可用 access token 的列數。
