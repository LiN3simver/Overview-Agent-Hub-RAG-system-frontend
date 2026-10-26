# Agent_Hub —— 各 RAG/Agent 的統一前端與轉接站（:8600）。
# 單一服務、單一 port：前端由同一個 FastAPI 提供 static/index.html，fetch() 都打
# 相對路徑 /hub/*，不需要另開 frontend port 或處理跨源。
#
#   docker build -t agent_hub .
#   docker compose up --build
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# store.py 是對話資料庫（SQLite）的存取層，main.py 會 import，漏 COPY 會 ModuleNotFoundError。
# 資料庫檔本身在 data/（由 compose 掛載）。
COPY main.py store.py metrics.py ./
COPY agentkit ./agentkit
COPY scripts ./scripts
COPY static ./static

EXPOSE 8600

# 打 / 而不是 /hub/agents：後者會逐一詢問下游 agent 的 card，下游未上線時累積的逾時
# 會超過健康檢查 timeout，把「下游沒開」誤報成「Hub 壞了」。/ 不碰任何下游。
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8600/ || exit 1

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8600"]
