"""讓測試找得到 Hub 的模組，並且**絕對不碰真的 data/**。

main.py 在 import 時就打開對話資料庫（HUB_DB_PATH，預設 data/hub.db）並嘗試從
data/conversations.json 遷移。測試若照預設路徑跑，等於拿正式對話當測試資料寫。
所以在任何測試 import main 之前，先把兩個路徑指到 pytest 的暫存目錄。
"""
import os
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_TMP = Path(tempfile.mkdtemp(prefix="hub-test-"))
os.environ["HUB_DB_PATH"] = str(_TMP / "hub.db")
os.environ["HUB_LEGACY_JSON"] = str(_TMP / "no-legacy.json")
os.environ["HUB_BACKUP_EVERY_H"] = "0"


@pytest.fixture(autouse=True)
def isolate_request_context():
    from agentkit import cancellation, deadline, telemetry
    cancel_token = cancellation.set_current(None)
    deadline_token = deadline.set_current(None)
    metrics_token = telemetry.set_current(None)
    yield
    cancellation._current.reset(cancel_token)
    deadline._current.reset(deadline_token)
    telemetry._current.reset(metrics_token)
