"""Hub 的對話儲存：SQLite（交易、並行安全、可線上備份）。

    python store.py backup  [目錄]         # 現在做一份備份（線上、一致的快照）
    python store.py verify  <檔案.db>      # 檢查一份備份能不能用：完整性 + 筆數
    python store.py export-json <輸出.json>  # 匯出成舊版 conversations.json 的格式（退版用）

★ 為什麼用 SQLite
  每次寫入是只動相關列的交易，WAL 模式下讀寫不互卡，同機多個 process 也安全
  （檔案鎖 + busy_timeout）；整檔重寫的 JSON 做不到這些，讀檔失敗時還可能把損壞變成全部遺失。
  Hub 是單機服務，SQLite 內建於 Python，不必另開資料庫容器，備份就是一個檔案。
  需要多機部署時只要替換這個模組（main.py 只透過 Store 的方法存取）。

★ 回合（turn_id）
  同一段對話可能同時有兩輪在跑（兩個分頁、連按兩次送出）。每一輪有自己的 turn_id，
  問題與回答靠它配對，順序一律照問題送出的先後。

★ 遷移（第一次啟動時自動做，只做一次）
  conversations.json 存在且資料庫還沒有對話時，在一個交易裡全部匯入，並核對筆數與
  抽樣內容，對不上就整批放棄。JSON 原檔不刪、不改，退版時仍可使用。
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import sqlite3
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS conversation (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT,                 -- NULL = 無主（session 欄位加進來之前的舊對話）
    agent_id    TEXT NOT NULL,
    title       TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS conversation_by_session
    ON conversation(session_id, updated_at);
CREATE TABLE IF NOT EXISTS message (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    conv_id   INTEGER NOT NULL REFERENCES conversation(id) ON DELETE CASCADE,
    turn_id   TEXT NOT NULL,
    role      TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    content   TEXT NOT NULL,
    agent_id  TEXT,
    hits      TEXT,                   -- JSON
    trace     TEXT,                   -- JSON
    status    TEXT,                   -- assistant：done / error / timeout / cancelled；舊資料 NULL
    at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS message_by_conv ON message(conv_id, id);
CREATE INDEX IF NOT EXISTS message_by_metrics_time ON message(conv_id, role, at);
-- 一輪只有一個問題、一個回答。回答重存（例如重試）是覆寫，不是多一筆。
CREATE UNIQUE INDEX IF NOT EXISTS message_one_per_turn ON message(conv_id, turn_id, role);
"""


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _with_metrics(trace: list, metrics: dict | None) -> list:
    if metrics is None:
        return trace
    return [item for item in trace if not (isinstance(item, dict) and item.get("phase") == "turn_metrics")] + [
        {"phase": "turn_metrics", "metrics": metrics}]


def _connect(path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True,
                             check_same_thread=False)
    else:
        # isolation_level=None：自己下 BEGIN/COMMIT，交易邊界看得見（見 Store._tx）
        db = sqlite3.connect(path, timeout=10, check_same_thread=False,
                             isolation_level=None)
    try:
        if not readonly:
            db.execute("PRAGMA journal_mode=WAL")
            # FULL：每個交易 commit 都落盤。對話寫入頻率低，換取斷電也不遺失已存的資料。
            db.execute("PRAGMA synchronous=FULL")
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=10000")
    except BaseException:
        # 壞掉的檔案在第一個 PRAGMA 就會失敗。不關掉的話 Windows 上檔案會被佔著，
        # Store._open 搬去隔離區時會 PermissionError。
        db.close()
        raise
    return db


def _counts(db: sqlite3.Connection) -> dict[str, int]:
    return {
        "conversations": db.execute("SELECT count(*) FROM conversation").fetchone()[0],
        "messages": db.execute("SELECT count(*) FROM message").fetchone()[0],
        "orphans": db.execute(
            "SELECT count(*) FROM conversation WHERE session_id IS NULL").fetchone()[0],
    }


class Store:
    def __init__(self, path: Path, legacy_json: Path | None = None) -> None:
        self.path = path
        self._lock = threading.RLock()
        # 啟動時發生過的事（損壞隔離、遷移結果），由 /hub/health 露出，不只印在 log。
        self.startup: dict[str, Any] = {}
        self.last_backup: dict[str, Any] | None = None
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = self._open()
        if legacy_json is not None:
            self._migrate_json(legacy_json)

    # -- 開檔與損壞隔離 -----------------------------------------------------
    def _open(self) -> sqlite3.Connection:
        if self.path.exists():
            problem = None
            db = None
            try:
                db = _connect(self.path)
                ok = db.execute("PRAGMA quick_check").fetchone()[0]
                if ok != "ok":
                    problem = ok
            except sqlite3.DatabaseError as e:
                problem = f"{type(e).__name__}: {e}"
            if problem is None and db is not None:
                db.executescript(SCHEMA)
                self._set_meta(db, "schema_version", str(SCHEMA_VERSION))
                return db
            # 一定要先關掉：Windows 上開著的檔案搬不走
            if db is not None:
                db.close()
            # ★ 壞掉的檔案搬走、不刪，改用新的空資料庫。不能「讀不到就當空的」再寫回原檔；
            #   搬走的檔案還能用 sqlite3 .recover 救。
            moved = self.path.with_name(f"{self.path.name}.corrupt-{_stamp()}")
            for suffix in ("", "-wal", "-shm"):
                src = Path(str(self.path) + suffix)
                if src.exists():
                    src.replace(Path(str(moved) + suffix))
            self.startup["quarantined"] = {"file": str(moved), "problem": problem, "at": now()}
            print(f"[store] ★ 對話資料庫損壞（{problem}），已搬到 {moved}，"
                  f"改用新的空資料庫。舊檔可以用 sqlite3 .recover 救回")
        db = _connect(self.path)
        db.executescript(SCHEMA)
        self._set_meta(db, "schema_version", str(SCHEMA_VERSION))
        return db

    @staticmethod
    def _set_meta(db: sqlite3.Connection, key: str, value: str) -> None:
        db.execute("INSERT INTO meta(key, value) VALUES (?, ?) "
                   "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value))

    def meta(self, key: str) -> str | None:
        with self._lock:
            row = self._db.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    @contextmanager
    def _tx(self):
        """一個寫入交易。BEGIN IMMEDIATE：一開始就拿寫鎖，別的 process 等 busy_timeout。"""
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            else:
                self._db.execute("COMMIT")

    # -- 讀 -----------------------------------------------------------------
    @staticmethod
    def _conv_row(r: sqlite3.Row) -> dict[str, Any]:
        return {"id": str(r["id"]), "agent_id": r["agent_id"], "title": r["title"],
                "created_at": r["created_at"], "updated_at": r["updated_at"]}

    def owns(self, conv_id: str | None, sid: str) -> bool:
        """這段對話存在而且是這個 session 的。「不存在」與「別人的」刻意分不出來
        （conv_id 是流水號，可被猜到）。"""
        if not conv_id or not str(conv_id).isdigit():
            return False
        with self._lock:
            row = self._db.execute(
                "SELECT 1 FROM conversation WHERE id = ? AND session_id = ?",
                (int(conv_id), sid)).fetchone()
        return row is not None

    def list_conversations(self, sid: str) -> list[dict[str, Any]]:
        # 時間戳只到秒，同一秒內的先後用最後一則訊息的流水號決定，
        # 否則剛問的那段在清單上可能跟別段隨機換位置。
        with self._lock:
            rows = self._db.execute(
                """SELECT c.*,
                          (SELECT count(*) FROM message m WHERE m.conv_id = c.id) AS turns,
                          (SELECT max(m.id) FROM message m WHERE m.conv_id = c.id) AS last_msg
                   FROM conversation c WHERE c.session_id = ?
                   ORDER BY c.updated_at DESC, last_msg DESC, c.id DESC""", (sid,)).fetchall()
        return [{**self._conv_row(r), "turns": r["turns"]} for r in rows]

    def _messages(self, conv_id: int) -> list[dict[str, Any]]:
        # 依回合排序（回合的先後 = 問題送出的先後），同一回合先問題後回答。
        # 前端重播就是照這個順序：每個 user 開一輪，assistant 接在最後一輪。
        rows = self._db.execute(
            """SELECT m.*, (SELECT min(u.id) FROM message u
                            WHERE u.conv_id = m.conv_id AND u.turn_id = m.turn_id) AS turn_seq
               FROM message m WHERE m.conv_id = ?
               ORDER BY turn_seq, CASE m.role WHEN 'user' THEN 0 ELSE 1 END, m.id""",
            (conv_id,)).fetchall()
        out = []
        for r in rows:
            m: dict[str, Any] = {"role": r["role"], "content": r["content"], "at": r["at"],
                                 "turn_id": r["turn_id"]}
            if r["role"] == "assistant":
                m["agent_id"] = r["agent_id"]
                m["hits"] = json.loads(r["hits"]) if r["hits"] else []
                saved_trace = json.loads(r["trace"]) if r["trace"] else []
                m["trace"] = []
                for item in saved_trace:
                    if isinstance(item, dict) and item.get("phase") == "turn_metrics":
                        m["metrics"] = item.get("metrics")
                    else:
                        m["trace"].append(item)
                if r["status"]:
                    m["status"] = r["status"]
            out.append(m)
        return out

    def get_conversation(self, conv_id: str, sid: str) -> dict[str, Any] | None:
        if not self.owns(conv_id, sid):
            return None
        with self._lock:
            r = self._db.execute("SELECT * FROM conversation WHERE id = ?",
                                 (int(conv_id),)).fetchone()
            if r is None:
                return None
            return {**self._conv_row(r), "messages": self._messages(r["id"])}

    def counts(self) -> dict[str, int]:
        with self._lock:
            return _counts(self._db)

    def metrics_overview(self, sid: str, since: str, until: str, *, agent_names: dict,
                         limit: int = 10_000) -> dict:
        from metrics import summarize

        # 獨立唯讀 WAL snapshot，不占主連線的 Python lock；統計不讀取問題或答案。
        db = _connect(self.path, readonly=True)
        try:
            db.execute("BEGIN")
            where = "c.session_id = ? AND m.role = 'assistant' AND m.at >= ? AND m.at <= ?"
            params = (sid, since, until)
            total = db.execute(f"SELECT count(*) FROM message m JOIN conversation c ON c.id = m.conv_id WHERE {where}",
                               params).fetchone()[0]
            rows = db.execute(f"""SELECT m.id, m.conv_id, m.turn_id, m.agent_id, m.status, m.at,
                CASE WHEN json_valid(m.trace) THEN
                    CASE WHEN json_extract(m.trace, '$[#-1].phase') = 'turn_metrics'
                         THEN json_extract(m.trace, '$[#-1].metrics') END
                END AS metrics
                FROM message m JOIN conversation c ON c.id = m.conv_id WHERE {where}
                ORDER BY m.id DESC LIMIT ?""", (*params, limit))
            return summarize(rows, matched_turns=total, agent_names=agent_names)
        finally:
            db.close()

    # -- 寫 -----------------------------------------------------------------
    def begin_turn(self, conv_id: str | None, sid: str, agent_id: str, query: str,
                   title: str) -> tuple[dict[str, Any], str, list[dict[str, str]]]:
        """開一輪：（沒有就建）對話、記下這一題、回傳給 agent 的歷史。一個交易。

        回傳 (對話 {id, title}, turn_id, history)。history 只放**已經有回答**的回合，
        而且一問一答成對 —— 正在跑的那幾輪（包括同時在跑的另一輪）不會混進來。
        """
        turn_id = uuid.uuid4().hex
        ts = now()
        with self._tx() as db:
            row = None
            if conv_id and str(conv_id).isdigit():
                row = db.execute("SELECT * FROM conversation WHERE id = ? AND session_id = ?",
                                 (int(conv_id), sid)).fetchone()
            if row is None:
                cur = db.execute(
                    "INSERT INTO conversation(session_id, agent_id, title, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?)", (sid, agent_id, title, ts, ts))
                cid, conv_title = cur.lastrowid, title
            else:
                cid, conv_title = row["id"], row["title"]
            history = self._history(db, cid)
            # 切換 agent 但沿用同一段對話是允許的，只記錄最後一次是誰答的
            db.execute("UPDATE conversation SET agent_id = ?, updated_at = ? WHERE id = ?",
                       (agent_id, ts, cid))
            db.execute("INSERT INTO message(conv_id, turn_id, role, content, at) "
                       "VALUES (?, ?, 'user', ?, ?)", (cid, turn_id, query, ts))
        return {"id": str(cid), "title": conv_title}, turn_id, history

    @staticmethod
    def _history(db: sqlite3.Connection, cid: int) -> list[dict[str, str]]:
        rows = db.execute(
            """SELECT u.content AS q, a.content AS a
               FROM message u JOIN message a
                 ON a.conv_id = u.conv_id AND a.turn_id = u.turn_id AND a.role = 'assistant'
               WHERE u.conv_id = ? AND u.role = 'user' AND a.content != ''
               ORDER BY u.id""", (cid,)).fetchall()
        out: list[dict[str, str]] = []
        for r in rows:
            if r["q"]:
                out.append({"role": "user", "content": r["q"]})
            out.append({"role": "assistant", "content": r["a"]})
        return out

    def finish_turn(self, conv_id: str, turn_id: str, content: str, agent_id: str,
                    hits: list, trace: list, status: str, *, metrics: dict | None = None) -> bool:
        """記下這一輪的回答。對話在這段期間被刪掉了就回 False（不是錯誤）。"""
        ts = now()
        with self._tx() as db:
            if db.execute("SELECT 1 FROM conversation WHERE id = ?",
                          (int(conv_id),)).fetchone() is None:
                return False
            db.execute(
                """INSERT INTO message(conv_id, turn_id, role, content, agent_id, hits, trace,
                                       status, at)
                   VALUES (?, ?, 'assistant', ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(conv_id, turn_id, role) DO UPDATE SET
                     content = excluded.content, agent_id = excluded.agent_id,
                     hits = excluded.hits, trace = excluded.trace,
                     status = excluded.status, at = excluded.at""",
                (int(conv_id), turn_id, content, agent_id,
                 json.dumps(hits, ensure_ascii=False), json.dumps(_with_metrics(trace, metrics), ensure_ascii=False),
                 status, ts))
            db.execute("UPDATE conversation SET updated_at = ? WHERE id = ?", (ts, int(conv_id)))
        return True

    def delete(self, conv_id: str, sid: str) -> bool:
        if not str(conv_id).isdigit():
            return False
        with self._tx() as db:
            cur = db.execute("DELETE FROM conversation WHERE id = ? AND session_id = ?",
                             (int(conv_id), sid))
        return cur.rowcount > 0

    def rename(self, conv_id: str, sid: str, title: str) -> bool:
        if not str(conv_id).isdigit():
            return False
        with self._tx() as db:
            cur = db.execute("UPDATE conversation SET title = ? WHERE id = ? AND session_id = ?",
                             (title, int(conv_id), sid))
        return cur.rowcount > 0

    def claim_orphans(self, sid: str) -> list[str]:
        with self._tx() as db:
            ids = [str(r[0]) for r in db.execute(
                "SELECT id FROM conversation WHERE session_id IS NULL ORDER BY id")]
            db.execute("UPDATE conversation SET session_id = ? WHERE session_id IS NULL", (sid,))
        return ids

    # -- 備份 ---------------------------------------------------------------
    def backup(self, dest: Path) -> dict[str, Any]:
        """線上做一份一致的快照（sqlite backup API，寫入中也安全），做完就驗。"""
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".tmp")
        if tmp.exists():
            tmp.unlink()
        out = sqlite3.connect(tmp)
        try:
            with self._lock:
                self._db.backup(out)
            # 備份檔改回一般模式：WAL 模式的檔要配 -wal/-shm 才完整，唯讀打開還可能
            # 因為建不了 -shm 而失敗。單一檔案才好搬、好驗、好還原。
            out.execute("PRAGMA journal_mode=DELETE")
        finally:
            out.close()
        tmp.replace(dest)
        info = verify(dest)
        self.last_backup = {"file": str(dest), "at": now(), **info}
        return self.last_backup

    def snapshot(self, directory: Path, keep: int) -> dict[str, Any]:
        """備份到 directory，留最新的 keep 份。"""
        info = self.backup(directory / f"conversations-{_stamp()}.db")
        olds = sorted(directory.glob("conversations-*.db"))
        for p in olds[:-keep] if keep > 0 else []:
            p.unlink(missing_ok=True)
        return info

    # -- 從 conversations.json 遷移 -----------------------------------------
    def _migrate_json(self, path: Path) -> None:
        if not path.exists() or self.meta("legacy_json_imported"):
            return
        if self.counts()["conversations"]:
            self.startup["migration"] = {"ok": False, "skipped": True,
                                         "note": "資料庫已經有對話，沒有匯入 JSON（避免重複）"}
            return
        raw = path.read_bytes()
        try:
            data = json.loads(raw.decode("utf-8"))
            convs = data["conversations"]
        except Exception as e:  # noqa: BLE001
            # 讀不懂就不動它；新資料寫在資料庫，JSON 不會再被寫入。
            self.startup["migration"] = {"ok": False, "file": str(path),
                                         "error": f"{type(e).__name__}: {e}",
                                         "note": "JSON 讀不懂，原檔沒有動，請人工檢查"}
            print(f"[store] ★ {path} 讀不懂，沒有匯入（原檔沒有動）：{e}")
            return

        want_convs = want_msgs = 0
        try:
            with self._tx() as db:
                for cid, c in sorted(convs.items(), key=lambda kv: int(kv[0])):
                    db.execute(
                        "INSERT INTO conversation(id, session_id, agent_id, title, created_at, "
                        "updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (int(cid), c.get("session_id") or None, c.get("agent_id") or "",
                         c.get("title") or "", c.get("created_at") or now(),
                         c.get("updated_at") or c.get("created_at") or now()))
                    want_convs += 1
                    turn, has_answer = None, False
                    for m in c.get("messages") or []:
                        role = m.get("role")
                        if role == "user" or turn is None or (role == "assistant" and has_answer):
                            turn, has_answer = uuid.uuid4().hex, False
                        if role == "assistant":
                            has_answer = True
                        db.execute(
                            "INSERT INTO message(conv_id, turn_id, role, content, agent_id, hits, "
                            "trace, at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (int(cid), turn, role, m.get("content") or "", m.get("agent_id"),
                             json.dumps(m["hits"], ensure_ascii=False) if "hits" in m else None,
                             json.dumps(_with_metrics(m.get("trace", []), m.get("metrics")), ensure_ascii=False)
                             if "trace" in m or "metrics" in m else None,
                             m.get("at") or c.get("updated_at") or now()))
                        want_msgs += 1
                # 流水號要接在舊的後面（含已刪掉的號碼），舊連結不會指到別人的新對話
                seq = int(data.get("seq") or 0)
                db.execute("UPDATE sqlite_sequence SET seq = max(seq, ?) WHERE name = 'conversation'",
                           (seq,))
                got = _counts(db)
                if (got["conversations"], got["messages"]) != (want_convs, want_msgs):
                    raise ValueError(f"筆數對不上：JSON {want_convs} 段／{want_msgs} 則，"
                                     f"匯入 {got['conversations']} 段／{got['messages']} 則")
                # 抽樣核對內容：同一段對話的問題與回答依序一字不差
                for cid in random.Random(0).sample(sorted(convs), min(10, len(convs))):
                    want = [(m.get("role"), m.get("content") or "") for m in convs[cid]["messages"]]
                    rows = db.execute("SELECT role, content FROM message WHERE conv_id = ? "
                                      "ORDER BY id", (int(cid),)).fetchall()
                    if [(r[0], r[1]) for r in rows] != want:
                        raise ValueError(f"對話 {cid} 的內容匯入後對不上")
                info = {"file": str(path), "at": now(), "conversations": want_convs,
                        "messages": want_msgs, "seq": seq,
                        "sha256": hashlib.sha256(raw).hexdigest()}
                self._set_meta(db, "legacy_json_imported", json.dumps(info, ensure_ascii=False))
        except Exception as e:  # noqa: BLE001 -- 交易已經 rollback，資料庫保持空的
            self.startup["migration"] = {"ok": False, "file": str(path),
                                         "error": f"{type(e).__name__}: {e}",
                                         "note": "整批放棄，資料庫沒有寫入，JSON 沒有動"}
            print(f"[store] ★ 匯入 {path} 失敗，整批放棄：{e}")
            return
        self.startup["migration"] = {"ok": True, **info}
        print(f"[store] 已從 {path.name} 匯入 {want_convs} 段對話、{want_msgs} 則訊息"
              f"（原檔保留不動，退版時用得到）")

    def export_json(self) -> dict[str, Any]:
        """舊版 conversations.json 的格式。退回舊版 Hub 時用（見模組 docstring）。"""
        with self._lock:
            seq_row = self._db.execute(
                "SELECT seq FROM sqlite_sequence WHERE name = 'conversation'").fetchone()
            convs = {}
            for r in self._db.execute("SELECT * FROM conversation ORDER BY id").fetchall():
                c = {**self._conv_row(r), "messages": [
                    {k: v for k, v in m.items() if k not in ("turn_id", "status")}
                    for m in self._messages(r["id"])]}
                if r["session_id"]:
                    c["session_id"] = r["session_id"]
                convs[str(r["id"])] = c
        return {"seq": seq_row[0] if seq_row else 0, "conversations": convs}


def verify(path: Path) -> dict[str, Any]:
    """一份資料庫檔能不能用：完整性檢查 + 筆數。備份做完、還原之前都跑這個。"""
    db = _connect(path, readonly=True)
    try:
        ok = db.execute("PRAGMA integrity_check").fetchone()[0]
        return {"ok": ok == "ok", "integrity": ok, **_counts(db)}
    finally:
        db.close()


def start_backups(store: Store, directory: Path, every_h: float, keep: int) -> None:
    """背景定期備份。啟動後先做一份，之後每 every_h 小時一份。every_h <= 0 不啟動。

    ★ 同機備份只防得了檔案損壞或誤刪，防不了整台機器故障；異機複製要另外排程，
      見 README 的「對話備份與還原」。
    """
    if every_h <= 0:
        return

    def loop() -> None:
        while True:
            try:
                info = store.snapshot(directory, keep)
                if not info.get("ok"):
                    print(f"[store] ★ 備份做完但驗證沒過：{info}")
            except Exception as e:  # noqa: BLE001 -- 備份失敗不能讓 Hub 掛掉，但要看得到
                store.last_backup = {"ok": False, "at": now(),
                                     "error": f"{type(e).__name__}: {e}"}
                print(f"[store] ★ 備份失敗：{e}")
            time.sleep(every_h * 3600)

    threading.Thread(target=loop, daemon=True, name="hub-backup").start()


# --------------------------------------------------------------------------
# 命令列
# --------------------------------------------------------------------------
def _main(argv: list[str]) -> int:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    root = Path(__file__).parent
    db_path = Path(os.environ.get("HUB_DB_PATH") or root / "data" / "hub.db")
    if not argv or argv[0] not in ("backup", "verify", "export-json"):
        print(__doc__.split("★")[0].strip())
        return 2
    cmd, rest = argv[0], argv[1:]
    if cmd == "verify":
        if not rest:
            print("用法：python store.py verify <檔案.db>")
            return 2
        info = verify(Path(rest[0]))
        print(json.dumps(info, ensure_ascii=False, indent=1))
        return 0 if info["ok"] else 1
    store = Store(db_path)
    if cmd == "backup":
        directory = Path(rest[0]) if rest else db_path.parent / "backups"
        info = store.backup(directory / f"conversations-{_stamp()}.db")
        print(json.dumps(info, ensure_ascii=False, indent=1))
        return 0 if info["ok"] else 1
    if not rest:
        print("用法：python store.py export-json <輸出.json>")
        return 2
    out = Path(rest[0])
    out.write_text(json.dumps(store.export_json(), ensure_ascii=False, indent=1), "utf-8")
    print(f"已匯出到 {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
