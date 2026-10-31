# 架構說明

**繁體中文** | [English](ARCHITECTURE.en.md)

## 1. 元件

| 元件 | 角色 |
|---|---|
| **Agent Hub** | 唯一對外的服務。提供前端、保存對話、把問題轉發給 Agent 並串流回答 |
| **Agent** | 獨立服務，各自持有資料與檢索邏輯；透過 agentkit 提供統一的 `/a2a/*` 介面，本身無狀態 |
| **agentkit** | 共用契約：Agent 端的路由（`server.py`）、呼叫其他 Agent 的客戶端（`client.py`）、委派迴圈（`delegate.py`）、LLM 客戶端（`gateway.py`） |
| **LLM Gateway** | Anthropic Messages API 相容的模型服務，所有 Agent 共用同一組設定 |
| **registry** | `agentkit/registry.py`，所有 Agent 的 id、埠與上下游關係的單一事實來源 |

為什麼需要 Hub 這一層，而不是前端直接打各 Agent：

1. **不必改動既有服務**：瀏覽器跨埠呼叫需要每個 Agent 開 CORS；由 Hub 同源轉發就不用。
2. **對話需要一個家**：狀態集中在 Hub，每輪把歷史帶給 Agent，Agent 不必各自實作持久化，也能讓同一段對話換另一個 Agent 接著回答。
3. **統一呈現狀態**：某個 Agent 沒開、忙碌或未就緒，在同一處顯示。
4. **只開一個對外埠**：Agent 不對外，連附件 PDF 都由 Hub 代理（`GET /hub/file/{agent}/{doc_id}`）。

## 2. A2A 契約（protocol `a2a/1`）

每個 Agent 掛上 `build_router(spec)` 後提供：

| 端點 | 用途 |
|---|---|
| `GET /a2a/card` | 名片：身分、能力、狀態、卡在哪、可呼叫誰、範例問句 |
| `GET /a2a/health` | 依賴檢查（有硬期限，保證快速回應） |
| `GET /a2a/version` | 啟動時載入的 agentkit 版本與原始碼雜湊 |
| `GET /a2a/tools` | Anthropic tool schema 與 system 提示（供上游掛工具） |
| `POST /a2a/tool/{name}` | 執行一次工具，固定回 `{"ok": bool, ...}` |
| `POST /a2a/search` | 統一檢索，回 `{hits, raw}` |
| `POST /a2a/message` | 統一對話（SSE） |
| `GET /a2a/peers` | 自己下游目前的狀態 |

### 回答模式

`/a2a/message` 以兩個布林控制：

| `use_tools` | `delegate` | 行為 |
|---|---|---|
| false | false | 檢索 + LLM 摘要，不呼叫任何工具 |
| true | false | 工具迴圈，只掛自己的工具 |
| — | true | 工具迴圈 + 下游 Agent 的工具 |

`no_llm: true` 優先於上述設定：只回檢索結果。Hub 偵測到 LLM 無法使用時也會自動切成此模式。

「用自己的工具」與「問其他 Agent」是兩種不同的授權——後者會把問題送到另一個服務。
未允許委派時有兩道保護：模型的工具清單與 system 中完全不出現下游；即使模型硬湊出下游工具名，執行前也會被拒絕。

### SSE 事件順序

```
meta → results → trace* → delta* → error? → done
```

| 事件 | 內容 |
|---|---|
| `meta` | 由誰回答、使用哪種模式 |
| `results` | 檢索結果卡片——**一定在呼叫 LLM 之前送出** |
| `trace` | 工具／委派軌跡：步驟、呼叫對象、參數、成功與否、耗時 |
| `delta` | 回答逐字送達 |
| `error` | LLM 或下游異常，附建議處置；`busy` / `timeout` / `cancelled` 旗標區分原因 |
| `done` | 本輪結束，附用量統計 |

Hub 在最前面多送一個 `hub` 事件（`conv_id`、`turn_id`、`request_id`）。

### 卡片格式

每筆 `hits[]` 為 `{id, title, subtitle, body, score, why[], badges[], blocks[], links[], raw}`：

- `blocks[]`：`kv`（標籤／值）、`text`、`table` 三種，前端用同一套渲染畫所有 Agent 的卡片
- `links[]`：指向原始檔案的連結，一律經由 Hub 代理
- `raw`：原封不動的原始資料列；正規化只為了渲染，不取代原始資料
- `kv` 中 `null` 表示「未填」、`0` 表示「填了 0」，兩者全程不合併

## 3. Agent 之間的協作

### 兩種呼叫方式

| 介面 | 形狀 | 使用時機 |
|---|---|---|
| `<agent>__<tool>` | 結構化參數 | 知道要哪一筆資料 |
| `ask_<agent>` | 一句自然語言 | 問題屬於對方專業但不確定用哪支工具；或對方需要再往下問 |

`ask_` 讓「只能對話、工具尚未就緒」的 Agent 仍然能被使用——它可以再委派給自己的下游。

### 防環

委派迴圈只把 registry 中的 `downstream` 掛給模型，呼叫圖是單向的有向無環圖。
往上游呼叫會被拒絕並回覆「不是你的下游」。葉節點（`downstream=()`）是最安全的形狀。

### 預先查詢（prefetch）

小型本地模型在「自己的檢索結果碰巧含有相關料號」時，常常不呼叫任何工具就直接作答，這無法靠提示詞修正。
因此改成確定性的做法：

1. 問句命中某個下游的 `tags`（`AgentRef.tags`）時，先直接打對方的 `/a2a/search`（不經 LLM，速度快）。
2. 把結果放進**問句**本身，模型開口前資料已在眼前。
3. 下游的卡片以 `results`（`append: true`）一併送到畫面，並在每張卡標上來源 Agent。

因此 `tags` 是有功能的欄位，應填使用者實際會打出的詞。

### 迴圈的保護機制

- **判斷是否執行工具看 content 中的 `tool_use` block**，不依賴 `stop_reason`（部分 gateway 會回錯）。
- **工具結果過長時折半刪筆數**，不截斷字元——截斷 JSON 會讓模型輸出亂碼。
- **重複呼叫**：同參數重複呼叫同一工具兩次後，將該工具從模型清單移除，剩餘步數留給其他子問題。
- **強制作答**：步數用完仍未作答時，收回工具、以一般問答把已取得的資料整理成答案，確保畫面不會空白。
- **純文字工具呼叫的救援**：模型若把工具呼叫或答案包成 JSON 寫進文字，`gateway.py` 會辨識並還原。

### 降級是常態

- 下游離線 → `{"ok": false, "offline": true}`，不拋例外
- 工具 `ready=False` → 出現在名片上但不掛給模型
- 下游的資料限制寫在 `ToolSpec.description`，會原樣掛給上游模型，讓上游的回答自帶限制說明

## 4. Hub 端點

| 端點 | 用途 |
|---|---|
| `GET /` · `/manual` · `/metrics` | 對話頁、說明書、統計總覽 |
| `GET /hub/agents?fresh=1` | 所有 Agent 的名片（含離線） |
| `GET /hub/graph` | 呼叫關係圖 |
| `GET /hub/health` | Hub、LLM、各 Agent、資料庫與備份狀態 |
| `GET /hub/deployment` | 各服務的 agentkit 版本一致性檢查 |
| `POST /hub/message` | 主入口：轉發給 Agent（SSE）並保存對話 |
| `POST /hub/search/{agent}` | 統一檢索（不經 LLM） |
| `POST /hub/tool/{agent}/{name}` | 直接執行某個工具 |
| `GET /hub/file/{agent}/{doc_id}` | 代理 Agent 的原始附件 |
| `GET /hub/conversations` · `GET/PATCH/DELETE /hub/conversations/{id}` | 對話清單、讀取、改名、刪除 |
| `GET /hub/conversations/{id}/metrics` | 單一對話每輪的耗時與用量 |
| `GET /hub/metrics?days=7` | 統計總覽資料 |

## 5. 對話保存

- **儲存**：SQLite（WAL），存取集中在 `store.py`。每輪有 `turn_id` 與 `status`（`done` / `error` / `timeout` / `cancelled`），失敗或中斷的一輪也會保存已取得的卡片與文字。
- **歸屬**：瀏覽器第一次請求時取得 httponly cookie `hub_session`；程式化呼叫可帶 `X-Session-Id`。清單、讀取、刪除只認自己的 session。這是防止誤看他人對話，**不是登入機制**。
- **韌性**：啟動時檢查資料庫，損壞的檔案會隔離保存並以新庫啟動；自動線上備份並驗證。

## 6. 時間、併發與取消

**整題時間預算**：Hub 收到問題即開始計時（預設 180 秒），以 `X-A2A-Deadline-Ms` header 把剩餘時間往下傳給 Agent、下游與 LLM；每過一層預扣 2 秒，讓最內層先到期並說明原因。到期時停止產生，已取得的資料保留。

**入場控管**（每個 process 各自計算）：

| 位置 | 預設 | 滿載時 |
|---|---|---|
| Hub 對話 | 同時 10 題、排隊 8 題、等 5 秒 | SSE `error`（`busy: true`），不建立空對話 |
| Agent 檢索／工具 | 同時 4 筆 | 同上 |
| LLM 呼叫 | 每 process 同時 2 份 | 模型那一步結束即歸還名額 |
| 下游串流 | 同時 10 條 | 同上 |

忙碌不會被當成服務故障。

**取消**：使用者按「停止」→ 瀏覽器中斷連線 → Hub 取消對 Agent 的串流 → Agent 關閉 LLM 請求與下游呼叫。停止後不再發起新的工具呼叫或 LLM 請求。

**健康檢查**：依賴檢查有 2.5 秒硬期限與快取，逾時回上一次結果並標示為舊；同一時間只派一次檢查（single-flight），避免資料庫卡住時檢查越積越多。

## 7. 可觀測性

- Hub 為每輪建立 `request_id`（`X-A2A-Request-ID`），下游回報的統計依此合併、去重。
- 每輪記錄總耗時、首次卡片／首字時間、排隊時間、工具與下游呼叫次數、LLM 請求數與回報的 token 用量。統計不包含問題、答案或工具參數。
- 未回報的用量記為 `null` 而非 0，並以 `usage_complete` 標示是否完整。
- `/metrics` 頁面提供回答數、失敗率、平均／P95 耗時、Agent 排行、每日量與最慢 10 筆。

## 8. agentkit 版本一致性

各 Agent 的 image 內含一份 agentkit 快照。每個服務啟動時記錄 agentkit 的版本與所有原始碼的 SHA-256（換行先正規化），透過 `/a2a/version` 回報。

```bash
python agentkit/version.py --sha256                          # 本次發版的雜湊
python scripts/package_agentkit.py                           # 打包成可重現的 ZIP
python scripts/check_deployment.py --hub-url http://127.0.0.1:8600   # 檢查全部服務；不一致 exit 1
python scripts/check_deployment.py --agents rfq,partrag --json       # 只檢查這一批
```

結果分為 `ok`、`mismatch`（內容不同）、`unversioned`（無版本端點）、`unavailable`（連不上）、`invalid`（回應格式錯誤）。

## 9. 依賴鎖定

`requirements.txt`（正式）與 `requirements-dev.txt`（開發）涵蓋 Python 3.12 的 Windows 與 Linux，固定所有直接與間接依賴並附 SHA-256（`--require-hashes`）。
修改 `requirements*.in` 後以 `python scripts/lock_dependencies.py` 重新產生。

## 環境變數

| 變數 | 預設 | 說明 |
|---|---|---|
| `LLM_BASE_URL` | `http://127.0.0.1:4000` | LLM gateway 位址 |
| `LLM_API_KEY` | — | 以 `Authorization: Bearer` 送出 |
| `LLM_MODEL` / `LLM_MODEL_FALLBACK` | — | 主模型；主模型不存在時的備援模型 |
| `LLM_TIMEOUT` | 240 | 單次 LLM 請求逾時（秒） |
| `LLM_PROBE_TTL` / `LLM_PROBE_TIMEOUT` | 60 / 10 | LLM 健康探測的快取與逾時（秒） |
| `A2A_MAX_TOKENS` | `LLM_MAX_TOKENS` 或 9000 | 委派迴圈的思考 + 答案總預算 |
| `A2A_MAX_STEPS` | 5 | 一題最多幾輪工具呼叫 |
| `A2A_MAX_STEPS_DELEGATE` | 0（沿用上一項） | 委派模式單獨的步數上限 |
| `A2A_MAX_CONSECUTIVE_FAILS` | 3 | 工具連續失敗幾次就收回工具、直接作答 |
| `A2A_PREFETCH_CHARS` | 3000 | 預先查詢塞進問句的資料上限 |
| `A2A_<ID>_URL` | `http://127.0.0.1:<埠>` | 某個 Agent 的位址 |
| `A2A_HUB_PORT` | 8600 | Hub 埠 |
| `A2A_STATUS_DEADLINE` / `A2A_STATUS_TTL` | 2.5 / 15 | 依賴檢查期限與快取（秒） |
| `A2A_STATUS_MAX_STALE` / `A2A_STATUS_STUCK` | 60 / 30 | 舊結果最長沿用時間；判定卡住的時間（秒） |
| `A2A_SEARCH_CONCURRENCY` / `_MAX_WAITING` / `_WAIT_S` | 4 / 8 / 5 | Agent 檢索與工具的併發、排隊、等待 |
| `A2A_LLM_CONCURRENCY` / `_MAX_WAITING` / `_WAIT_S` | 2 / 8 / 5 | LLM 名額的併發、排隊、等待 |
| `A2A_STREAM_CONCURRENCY` / `_MAX_WAITING` / `_WAIT_S` | 10 / 8 / 5 | 下游串流連線的併發、排隊、等待 |
| `A2A_DEADLINE_HOP_MARGIN_S` | 2 | 剩餘時間每往下一層預扣幾秒 |
| `A2A_DEADLINE_MIN_LLM_S` | 5 | 剩不到幾秒就不再發起 LLM 呼叫 |
| `HUB_MESSAGE_BUDGET_S` | 180 | 一題的總時間上限（秒） |
| `HUB_MESSAGE_CONCURRENCY` / `_MAX_WAITING` / `_WAIT_S` | 10 / 8 / 5 | Hub 對話的併發、排隊、等待 |
| `HUB_DIAG_WAIT_S` | 3 | LLM 狀態未知時，一題最多等探測幾秒 |
| `HUB_DB_PATH` | `data/hub.db` | 對話資料庫 |
| `HUB_LEGACY_JSON` | `data/conversations.json` | 舊格式對話檔（首次啟動時自動遷移） |
| `HUB_BACKUP_DIR` | `data/backups` | 備份目錄 |
| `HUB_BACKUP_EVERY_H` / `HUB_BACKUP_KEEP` | 24 / 14 | 備份間隔（小時，0 = 停用）與保留份數 |
