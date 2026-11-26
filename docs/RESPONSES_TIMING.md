# Responses：續接、安全重送與首文字

兩個 ABI 的 describe 均提供 `responses_ws_replay`、`responses_turn_timing`、`responses_interrupt`。控制面透過原有推理端點使用，不需要切換共享模式。舊控制面不送續接提示時，核心照送原本的請求；舊核心沒有回傳觀測值時，控制面保留缺值。

HTTP 請求與 WebSocket 握手的 `x-codex-routing-hint` 取自本輪請求本體。例如模型 `m` 的 `service_tier: "priority"` 對應 `model=m;tier=priority`。缺省、`auto`、`standard` 或 `default` 只送 `model=m`。同一帳號與會話切換等級時，核心使用握手等級相符的連線。這個提示表示請求的等級，實際服務等級仍以上游回應為準。

**原生續接。** 入口 WS 的每一輪，由控制面在內部 envelope 送 `options.ws_replay: {}`；只有同一條入口 WS 的歷史快取解析成功時，才填入 `previous_response_id`。無論如何，`body` 都保留展開後的完整輸入。核心只有取回同帳號、同會話、同握手設定的原連線，且上輪有已確認的完成終態（或明示 `interrupted` 並附完整最終 output）、回應 ID、完整先前輸入＋輸出前綴及輸入以外的設定均一致、確實有新增項目，才把 `previous_response_id` 和後綴送出。前綴及設定保存在記憶體的是 SHA-256，不額外留存全文；不忽略工具參數、加密推理或未知設定。欄位順序、數字寫法等保守比對可能降低命中率；稀疏終態只使用有完整連續索引的已完成項目。未命中增量時使用保存的完整本體；只有這類可獨立回放的輸入才允許送出前降級。HTTP 客戶端原有的明示續接另依其原契約。

**重送界線。** 尚未送出推理，只做撥號／握手、或已在閒置探測發現斷線時，可以安全換線。一旦開始寫入 create，一般 Close 無法排除上游已接受推理。此時回 502、`x-kin-upstream-sent: unknown`，本輪停止發送。上傳期間同步讀取上游，保留提早抵達的文字與控制幀順序。部分寫入時的一般 Close 視為送出狀態不明。首條文字前若收到 Close 1009（訊息過大），首次嘗試改以 HTTP 送完整輸入；預熱、缺少完整輸入或已經補送過的請求，直接回錯誤。已收到文字後的 1009 結束該輪，避免重複執行。若首個事件明確回報 `previous_response_not_found`，且 ID 是核心此輪產生的增量 ID，可在同一條線補送完整本體。寫入失敗、沒有 Close 的無聲斷線或取消都不是未執行證據，不另開 WS、不降級重播，錯誤標頭為 `x-kin-upstream-sent: unknown`。入口快取不存在時由控制面請客戶端重送全史。

**每筆怎麼查。** 控制面的 `hop_meta.codex_vm_timing.attempts[]` 以 `kernel_request_id` 對照核心的 `responses turn` 日誌。同一筆的 `upstream` 來自內部 `x-tap-turn-timing` 標頭；串流完成的觀測由同名 trailer 補齊，未讀完整時只能看到當時已有的值。該標頭與 trailer 不轉給外部客戶，不插入 SSE 計時事件。讀值時分開兩座時鐘：

- 核心的 `first_event_ms`、`first_generated_ms`、`first_text_ms` 與各自的 `*_type`：本輪進入上游發送流程之後，到讀到對應事件的單調耗時。生成包括有內容的推理／工具；文字要求非空輸出文字。HTTP SSE 逐塊旁聽，單一診斷事件大於 256 KiB 略過，不截斷或延後轉發；`end_without_observed_terminal` 不是對業務終態的裁決。下游背壓可能延後核心讀取網路，不應把時間差稱為模型生成時間。
- `transport` 是本輪實際採用的 `http` 或 `websocket`；`ws_reused`、`ws_deflate` 和 `ws_frame_compressed` 分別描述是否重用、升級是否協商 permessage-deflate、此次已成功寫完的文字訊框有無套用。這個固定版本的 WS 編碼器只要協商成功就壓縮所有資料訊框。HTTP 的 `request_bytes` 與 `http_zstd_bytes` 是送上游的原文和壓縮本體尺寸；WS 的 `request_bytes` 是壓縮前的 UTF-8 訊息尺寸，不是線上傳輸位元組。
- `request_mode` 區分未改寫的請求（as_received）、WS 首次完整（full）、完整回放（full_replay）和原生增量（incremental）。`cache_miss` 是上游明確拒絕增量 ID；`upstream_attempts` 包含明確 401 的憑據復原，`ws_send_attempts` 計算 WS 寫入嘗試。出站撥號含代理交涉（dial_ms）、TLS 與升級各為最近一次有量到的握手階段耗時；重用不虛報握手。WS 的 `send_complete_ms` 是寫完的時刻；HTTP 客戶端只提供 `http_headers_ms`，不以它冒充寫完。降級理由、握手狀態和次數獨立記錄。
- Node 的 `first_generated_event_type`、`first_release_type`、`first_text_ms`、`client_first_write_ms` 與 `client_first_write_source_type` 分別是觀察生成、觸發放行、觀察可見文字、實際開始對客寫入與該次寫入對應的來源事件。首放行會送出先前暫存的協議事件，因此首寫的來源可能是 response.created。客戶端畫面首次顯示文字仍須在客戶端量，兩端的毫秒欄位不能直接相減。

**借線及送出前檢查。** 借用任何 socket 都比對帳號、憑據、會話、thread/window、路由、協議及握手設定；每輪追蹤 ID 與 turn metadata 裡的 turn_id、root_turn_id、turn_started_at_unix_ms 不影響相容性。每條線保存其協商的 `x-codex-websocket-max-message-bytes`；以最終 create 的未壓縮 UTF-8 長度比對，含完整補送。超過 1 MiB 時拆成不大於 1 MiB 的連續幀；整則文字仍是一次 create，計為一次 `ws_send_attempts`。預熱（generate=false）不改走 HTTP。純增量原線失效時回重放錯誤；已補送的請求不因大小再改走 HTTP。強制 HTTP 的請求若標記為原生 WS 續接且只提供增量，核心在送出前回錯誤。非串流 HTTP 分塊讀取，完整終態事件到達後即回傳，不等待上游關閉串流；單一 SSE 事件上限 64 MiB，整輪沒有這個上限。

**主動中斷。** 串流回應的私有頭 `x-tap-interrupt` 為 `websocket` 或 `unavailable`；沒有此頭的舊核心維持舊契約。控制面可發 `POST /internal/v1/interrupt`，以內部 token 帶 `{request_id,response_id}`。僅該輪已建立、ID 相符的活躍 WS 接受 `discard_partial_items`；204 是控制幀已寫入，不是上游承諾。真正的 accepted／failed 與終態來自原推理串流。控制幀寫入失敗時仍交付已到達的終態及原始用量，該線不再重用。無法轉送回 409，不中止生成；客戶端斷線照舊走 cancel。只有上游 incomplete_details.reason 明示 interrupted 且提供最終 output 陣列時，才用該陣列建立可驗證的續接前綴。

**每輪回應標頭。** WS 握手的 `x-codex-*` 回應標頭只屬於新線的首輪。重用同一條線時，不把舊額度或 `x-codex-turn-state` 當成本輪資訊；當輪的流內事件照常轉送。協商的最大訊息長度和壓縮仍適用於整條實體連線。

**模型目錄 ETag。** HTTP 成功回應保留上游的 `x-models-etag`。WS 在實體線首次接受生成時交付握手取得的 ETag，預熱會將它留給首輪生成。同一條線之後的回合只照常轉送當輪事件。新的握手有自己的 ETag，更新模型目錄由呼叫端決定。

**失敗事實。** `x-kin-upstream-error` 是控制面使用的私有收據，stage 區分 HTTP（`http`）、WS 握手的明示 429（`websocket_handshake`）及 WS 首個錯誤事件（`websocket_first_event`）。收據保留明示 status、code、type、request_id、retry_after，缺值為 null。初次握手遇 429 時，即使是完整請求也不再握手或改送 HTTP；預熱同樣回原 429。此時 `x-kin-upstream-sent: 0` 指沒有送出模型請求；共享模式若先前已有業務 401 明確證實送達，換票重試的握手 429 不會把整輪改成未送出。首個 WS 事件若為 `invalid_request_error` 且狀態為 400 或未提供，回應保留上游的 `error` 物件，包括 `param`。缺少狀態時對呼叫端回 400，私有收據仍保留缺值。錯誤摘錄上限為 64 KiB；串流已開始時不改寫 HTTP 狀態。vm2api 不把這份收據轉給客戶端。

HTTP 推理或 WS 握手回 429 時，核心另行保留額度與點數標頭，包括 `x-codex-active-limit`、具名額度桶與各視窗重置時間。控制面使用標頭的原始名稱和值決定額度歸屬。上游拒絕的狀態、送出事實與重送判斷沿用前述契約。

**容量與期限。** 單帳號與共享程序設定都可放 `ws_limits`。同一代理 URL 的帳號可共用實體 WS 名額與握手名額，仍各用自己的 socket、cookie 和憑據。容量滿時先回收同一出口最舊的閒置線；活躍線不被回收。閒置線持續處理 Ping/Pong，包括 Pong 寫入背壓；收到關閉或異常業務訊息便淘汰。借還與背景檢查（30 秒）清除池項目，TTL 為 240 秒，借出的線不受池內背景讀取影響。

| 設定 | 預設 | vm2api 程序環境覆寫 |
| --- | ---: | --- |
| proxy_connecting（並行握手） | 0，不設限 | TAP_WS_PROXY_CONNECTING |
| proxy_connections（含閒置實體線） | 0，不設限 | TAP_WS_PROXY_CONNECTIONS |
| fallback_after_ms（可降級完整請求的握手等待預算） | 0，維持原等待 | TAP_WS_FALLBACK_AFTER_MS |
| read_idle_ms（等上游 headers、WS 上傳分片寫入或回覆、首幀及下次讀取） | 300000；0 關閉 | TAP_WS_READ_IDLE_MS |

沒有生產量測前不強制所有出口共用同一個小上限。設定於程序啟動時讀取。短預算逾時取消未寫入的握手，再送一次 HTTP；不在背景競送推理。成功串流的讀取計時只在要求下一塊資料時啟動，不把下游背壓或整輪執行時間當作逾時。

## WebSocket 首生成前的串流計時

核心將第一個 SSE 幀與第一個含生成內容的 SSE 幀交給 HTTP body 消費者時，分別記錄本輪時刻 `ws_first_yield_ms` 與 `ws_generated_yield_ms`。兩者都早於客戶端顯示。從第一幀交付到首生成幀交付，`ws_pre_generated_unpolled_ms` 累計前一幀已交付、消費者尚未再次取幀的時間；`ws_pre_generated_read_ms` 累計取幀後等待 WebSocket 資料或控制事件的時間，含執行器排程。解析和建幀也會花時間，兩個累計值不必加總為完整區間。首幀已含生成時兩項都是零，未觀察到生成或非 WS 串流時保留缺值。新欄位沿用內部收據與 trailer。

## 單帳號 Unix socket 傳輸

ABI 1 的 health 欄位及 describe 能力 `uds_packed_v1` 允許閘道向 `POST /internal/v1/codex/responses/packed` 傳送位元組。端點沿用 internal token，接受 identity 或 gzip。傳輸大小及完整還原後的 JSON 都受該槽的請求上限約束。

閘道可用 `x-kin-uds-store: 1` 請求暫存本輪完整 JSON。成功回應的 `x-kin-uds-base` 是其 SHA-256。續接時帶回該值及 `x-kin-uds-copy`，後者依次為基底起點、解壓後本輪資料的插入位置、複製長度，三個十進位位元組數以逗號分隔。核心先將該區段插回本輪資料，再進入既有推理端點的處理流程。核心最多保留 32 MiB、64 份基底，可在五分鐘內重用。程序重啟或基底被淘汰時，端點在推理前回傳 409、`x-kin-uds-miss: 1` 及 `x-kin-upstream-sent: 0`，允許閘道完整傳送一次。`x-kin-uds-received` 是核心讀到的本機請求體大小。舊閘道仍可使用原端點。

每個單帳號槽最多同時接收及解碼 4 筆 packed 請求。名額用盡時，核心在讀取本體前回 429 `account_busy` 與 `x-kin-upstream-sent: 0`。完整讀入本體後，成功、基底缺失及解碼失敗的回應都帶 `x-kin-uds-received`。

| 本機封包結果 | HTTP 與識別值 | 閘道處置 |
| --- | --- | --- |
| 已認證、合法的基底鍵與複製描述，查無基底 | 409、`local_base_unavailable`、`x-kin-uds-miss: 1`、`x-kin-upstream-sent: 0` | 只有三個狀態訊號（409、miss=1、sent=0）齊全才完整補送一次，舊核心可能回純文字 |
| 標頭未配對、基底鍵格式錯誤、複製長度為零、已知基底的區段越界、gzip 或 JSON 損壞 | 400、`invalid_request`、sent=0 | 結束本次請求 |
| 傳輸大小或完整還原後超出槽上限 | 413、`body_too_large`、sent=0 | 結束本次請求 |
| 2xx 但無基底鍵（例如還原資料大於 32 MiB 快取預算） | 沿用本輪推理結果 | 本輪已交付；下一輪以完整內容開始 |

鍵是 64 個小寫十六進位字元。核心先檢查傳輸大小與標頭，再查基底，最後解壓並驗證區段及 JSON；因此缺失基底時，本輪尚未驗證增量的解壓內容。
