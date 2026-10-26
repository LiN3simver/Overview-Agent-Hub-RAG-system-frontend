# 部署（Docker Compose + git push）

在本機用 Git Bash 執行，透過 SSH 把指定 commit `git push` 到伺服器，
伺服器端的 `apply.sh` 依「改了哪些檔案」決定要重建 image、只重啟，或什麼都不做。

```bash
bash deploy/deploy.sh bootstrap   # 第一次：在伺服器建 repo、本機加 remote（可重複執行）
bash deploy/deploy.sh plan        # 預覽這次會傳什麼、伺服器會做什麼（唯讀）
bash deploy/deploy.sh deploy      # 部署；健康檢查失敗會自動回滾
bash deploy/deploy.sh status      # 伺服器目前版本與健康狀態
bash deploy/deploy.sh rollback    # 回到上一個健康的版本
bash deploy/deploy.sh check       # 比對 Hub 與各 agent 載入的 agentkit 版本（唯讀）
```

## 設定

部署目標與規則都在 [`target.conf`](target.conf)：

| 變數 | 說明 |
|---|---|
| `T_HOST` / `T_REMOTE` | SSH 目標與伺服器上的目錄（預設是範例值，請改成自己的） |
| `T_IMAGE_RE` | 命中這些路徑就重建 image |
| `T_COMPOSE_RE` | 命中就重新 `docker compose up` |
| `T_HEALTH_CMD` | 部署後的健康檢查（預設打 Hub 首頁要回 200） |
| `T_IMAGE_KEEP` | 保留幾個舊 image 供回滾 |
| `T_DEPENDENT_DIRS` | 改了 `agentkit/` 時一併詢問要不要重建的其他 agent 專案 |

## 設計重點

- **只傳版控裡的東西**：`.env`、`data/`、`.venv/` 都在 `.gitignore`，永遠不會被上傳或覆蓋。
- **部署的是 commit，不是工作目錄**：伺服器上每一版都追得回來；有未 commit 修改時預設中止。
- **對外 port 防呆**：部署前比對「正在跑的 port」與「這次會用的 port」，不同就在動容器之前中止。
- **快速回滾**：每次 build 前把正在跑的 image 打上 commit 標籤，回滾不需重新 build。
- **agentkit 連帶**：其他 agent 的 image 內含一份 agentkit 快照；改了 `agentkit/` 只部署 Hub
  會讓版本不一致，`deploy` 會提示一併重建，`check` 可驗證所有服務是否載入同一版。
