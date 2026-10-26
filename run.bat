@echo off
REM 開 http://127.0.0.1:8600，不要用 localhost：Windows 的 localhost 會先解析到 IPv6 ::1，
REM 而 uvicorn 只綁 IPv4，每個請求都要等 IPv6 逾時再回退（每筆約多 2 秒，Hub 載入時還會乘上 agent 數）。
cd /d "%~dp0"
.venv\Scripts\python.exe -m uvicorn main:app --host 127.0.0.1 --port 8600 %*
