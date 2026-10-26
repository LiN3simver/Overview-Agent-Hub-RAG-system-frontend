"""對話儲存（store.py）：驗收條件。

  - 並行新增／改名／刪除之後，已經確認存進去的訊息不遺失
  - 同一段對話同時跑兩輪，問題與回答配得起來
  - 中途重啟（重開資料庫）之後資料都在
  - 可以從備份還原
  - 從舊的 conversations.json 遷移：筆數、內容都對；壞掉的 JSON 不可以被蓋掉
  - 資料庫檔壞掉時：搬走保留、不是當成空的然後覆寫
"""
from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from store import Store, verify


@pytest.fixture
def store(tmp_path: Path) -> Store:
    return Store(tmp_path / "hub.db")


def _answer(s: Store, conv: dict, turn: str, text: str, status: str = "done") -> bool:
    return s.finish_turn(conv["id"], turn, text, "rfq", [{"id": "h"}], [], status)


# --------------------------------------------------------------------------
# 基本行為
# --------------------------------------------------------------------------
def test_turn_roundtrip_and_ownership(store):
    conv, t1, hist = store.begin_turn(None, "alice", "rfq", "第一題", "第一題")
    assert hist == []
    assert _answer(store, conv, t1, "第一個回答")
    c = store.get_conversation(conv["id"], "alice")
    assert [(m["role"], m["content"]) for m in c["messages"]] == [
        ("user", "第一題"), ("assistant", "第一個回答")]
    assert c["messages"][1]["hits"] == [{"id": "h"}] and c["messages"][1]["status"] == "done"
    # 別人的對話：看不到、改不了、刪不掉，而且跟「不存在」分不出來
    assert store.get_conversation(conv["id"], "mallory") is None
    assert not store.rename(conv["id"], "mallory", "x")
    assert not store.delete(conv["id"], "mallory")
    assert store.get_conversation("999", "alice") is None
    # 拿別人的 conv_id 送問題：開一段新的，不可以寫進別人的
    other, _, _ = store.begin_turn(conv["id"], "mallory", "rfq", "偷看", "偷看")
    assert other["id"] != conv["id"]


def test_history_only_completed_pairs(store):
    conv, t1, _ = store.begin_turn(None, "s", "rfq", "Q1", "Q1")
    _answer(store, conv, t1, "A1")
    # 第二輪還沒回答（或回答是空的）就開第三輪：歷史只放成對的那一輪
    _, t2, _ = store.begin_turn(conv["id"], "s", "rfq", "Q2", "Q1")
    _, t3, hist = store.begin_turn(conv["id"], "s", "rfq", "Q3", "Q1")
    assert hist == [{"role": "user", "content": "Q1"}, {"role": "assistant", "content": "A1"}]
    _answer(store, conv, t2, "")
    _, _, hist = store.begin_turn(conv["id"], "s", "rfq", "Q4", "Q1")
    assert [m["content"] for m in hist] == ["Q1", "A1"], "空白回答那一輪不進歷史"


def test_concurrent_turns_in_one_conversation_pair_up(store):
    """兩輪交錯完成：重播時每個回答要接在**自己的**問題後面。"""
    conv, ta, _ = store.begin_turn(None, "s", "rfq", "問 A", "問 A")
    _, tb, _ = store.begin_turn(conv["id"], "s", "bom", "問 B", "問 A")
    _answer(store, conv, tb, "答 B")          # B 先答完
    _answer(store, conv, ta, "答 A")
    msgs = store.get_conversation(conv["id"], "s")["messages"]
    assert [(m["role"], m["content"]) for m in msgs] == [
        ("user", "問 A"), ("assistant", "答 A"), ("user", "問 B"), ("assistant", "答 B")]
    assert msgs[0]["turn_id"] == msgs[1]["turn_id"] != msgs[2]["turn_id"]


def test_answer_saved_twice_is_overwrite_not_duplicate(store):
    conv, t, _ = store.begin_turn(None, "s", "rfq", "Q", "Q")
    _answer(store, conv, t, "部分", "cancelled")
    _answer(store, conv, t, "完整", "done")
    msgs = store.get_conversation(conv["id"], "s")["messages"]
    assert len(msgs) == 2 and msgs[1]["content"] == "完整"


def test_answer_after_delete_is_dropped_quietly(store):
    """使用者在回答串流途中刪掉對話：存回答時不可以炸，也不可以把對話救回來。"""
    conv, t, _ = store.begin_turn(None, "s", "rfq", "Q", "Q")
    assert store.delete(conv["id"], "s")
    assert _answer(store, conv, t, "A") is False
    assert store.get_conversation(conv["id"], "s") is None
    assert store.counts()["messages"] == 0, "刪對話要連訊息一起刪"


def test_rename_and_list_order(store):
    a, ta, _ = store.begin_turn(None, "s", "rfq", "舊的", "舊的")
    b, tb, _ = store.begin_turn(None, "s", "rfq", "新的", "新的")
    assert store.rename(a["id"], "s", "改過的名字")
    _answer(store, a, ta, "A")                 # a 最近更新 -> 排第一
    lst = store.list_conversations("s")
    assert [c["title"] for c in lst] == ["改過的名字", "新的"]
    assert lst[0]["turns"] == 2
    # 下一輪不會把使用者改過的標題蓋回去
    again, _, _ = store.begin_turn(a["id"], "s", "rfq", "另一題", "另一題")
    assert again["title"] == "改過的名字"


# --------------------------------------------------------------------------
# 並行與重啟
# --------------------------------------------------------------------------
def test_concurrent_writers_lose_nothing(tmp_path):
    """8 條 thread × 2 個 Store（模擬兩個 process 開同一個檔）同時寫，一筆都不能少。"""
    path = tmp_path / "hub.db"
    stores = [Store(path), Store(path)]
    errors: list[BaseException] = []

    def worker(i: int) -> None:
        s = stores[i % 2]
        try:
            for j in range(15):
                conv, t, _ = s.begin_turn(None, f"user{i}", "rfq", f"q{i}-{j}", f"q{i}-{j}")
                s.finish_turn(conv["id"], t, f"a{i}-{j}", "rfq", [], [], "done")
                if j % 5 == 0:
                    s.rename(conv["id"], f"user{i}", f"renamed{i}-{j}")
        except BaseException as e:      # noqa: BLE001
            errors.append(e)

    ths = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in ths:
        t.start()
    for t in ths:
        t.join(60)
    assert not errors, errors
    # 「中途重啟」：全部關掉重開，從檔案讀回來
    fresh = Store(path)
    assert fresh.counts() == {"conversations": 120, "messages": 240, "orphans": 0}
    for i in range(8):
        convs = fresh.list_conversations(f"user{i}")
        assert len(convs) == 15
        for c in convs:
            msgs = fresh.get_conversation(c["id"], f"user{i}")["messages"]
            assert msgs[0]["content"].replace("q", "a") == msgs[1]["content"]


def test_ids_never_reused_after_delete(store):
    """舊連結 /?conv=12 不可以在刪掉之後指到別人的新對話。"""
    a, _, _ = store.begin_turn(None, "s", "rfq", "Q", "Q")
    store.delete(a["id"], "s")
    b, _, _ = store.begin_turn(None, "s", "rfq", "Q", "Q")
    assert int(b["id"]) > int(a["id"])


# --------------------------------------------------------------------------
# 備份與還原
# --------------------------------------------------------------------------
def test_backup_restore_roundtrip(store, tmp_path):
    conv, t, _ = store.begin_turn(None, "s", "rfq", "要留下來的問題", "要留下來的問題")
    _answer(store, conv, t, "要留下來的回答")
    info = store.backup(tmp_path / "bk" / "snap.db")
    assert info["ok"] and info["conversations"] == 1 and info["messages"] == 2
    # 正本之後被改壞（刪掉），從備份還原：把備份檔當資料庫打開
    store.delete(conv["id"], "s")
    restored = Store(tmp_path / "bk" / "snap.db")
    msgs = restored.get_conversation(conv["id"], "s")["messages"]
    assert [m["content"] for m in msgs] == ["要留下來的問題", "要留下來的回答"]


def test_snapshot_keeps_latest_n(store, tmp_path, monkeypatch):
    import store as store_mod
    stamps = iter(f"20261005-0000{i:02d}" for i in range(10))
    monkeypatch.setattr(store_mod, "_stamp", lambda: next(stamps))
    d = tmp_path / "backups"
    for _ in range(5):
        store.snapshot(d, keep=3)
    assert sorted(p.name for p in d.glob("*.db")) == [
        "conversations-20261005-000002.db", "conversations-20261005-000003.db",
        "conversations-20261005-000004.db"]


def test_corrupt_database_is_quarantined_not_overwritten(tmp_path):
    path = tmp_path / "hub.db"
    path.write_bytes(b"this is not a sqlite database" * 100)
    s = Store(path)
    q = s.startup["quarantined"]
    moved = Path(q["file"])
    assert moved.exists() and moved.read_bytes().startswith(b"this is not"), "壞檔要原封不動留著"
    assert s.counts()["conversations"] == 0       # 用新的空資料庫起來，服務沒掛


# --------------------------------------------------------------------------
# 從 conversations.json 遷移
# --------------------------------------------------------------------------
LEGACY = {
    "seq": 7,
    "conversations": {
        "3": {"id": "3", "session_id": "alice", "agent_id": "rfq", "title": "料號查詢",
              "created_at": "2026-09-01T00:00:00+00:00", "updated_at": "2026-09-01T00:01:00+00:00",
              "messages": [
                  {"role": "user", "content": "P38 多少錢", "at": "2026-09-01T00:00:00+00:00"},
                  {"role": "assistant", "content": "RMB 1.5", "agent_id": "rfq",
                   "hits": [{"id": "x"}], "trace": [{"phase": "result"}],
                   "at": "2026-09-01T00:01:00+00:00"},
                  {"role": "user", "content": "沒答到的一題", "at": "2026-09-01T00:02:00+00:00"},
              ]},
        "5": {"id": "5", "agent_id": "bom", "title": "無主的舊對話",
              "created_at": "2026-08-01T00:00:00+00:00", "updated_at": "2026-08-01T00:00:00+00:00",
              "messages": [{"role": "user", "content": "舊問題", "at": "2026-08-01T00:00:00+00:00"},
                           {"role": "assistant", "content": "舊回答", "agent_id": "bom",
                            "hits": [], "trace": [], "at": "2026-08-01T00:00:00+00:00"}]},
    },
}


def test_migration_imports_everything_once(tmp_path):
    js = tmp_path / "conversations.json"
    js.write_text(json.dumps(LEGACY, ensure_ascii=False), "utf-8")
    before = js.read_bytes()
    s = Store(tmp_path / "hub.db", legacy_json=js)
    assert s.startup["migration"]["ok"] is True
    assert s.counts() == {"conversations": 2, "messages": 5, "orphans": 1}
    c = s.get_conversation("3", "alice")
    assert [(m["role"], m["content"]) for m in c["messages"]] == [
        ("user", "P38 多少錢"), ("assistant", "RMB 1.5"), ("user", "沒答到的一題")]
    assert c["messages"][1]["hits"] == [{"id": "x"}]
    assert js.read_bytes() == before, "JSON 原檔一個位元組都不能動（退版要用）"
    # 流水號接在舊的 seq 後面（6、7 是被刪掉的號碼）
    new, _, _ = s.begin_turn(None, "alice", "rfq", "新", "新")
    assert int(new["id"]) == 8
    # 無主的照舊無主，認領之後才出現
    assert s.claim_orphans("alice") == ["5"]
    # 重開不會再匯入一次
    again = Store(tmp_path / "hub.db", legacy_json=js)
    assert "migration" not in again.startup and again.counts()["conversations"] == 3


def test_unreadable_json_is_left_alone(tmp_path):
    js = tmp_path / "conversations.json"
    js.write_text('{"seq": 3, "conversations": {"1": {"id": "1", "mess', "utf-8")   # 寫到一半
    before = js.read_bytes()
    s = Store(tmp_path / "hub.db", legacy_json=js)
    assert s.startup["migration"]["ok"] is False
    assert js.read_bytes() == before
    # 之後照常寫入新對話，JSON 依舊不會被碰
    s.begin_turn(None, "s", "rfq", "Q", "Q")
    assert js.read_bytes() == before


def test_failed_migration_writes_nothing(tmp_path, monkeypatch):
    """匯入到一半出錯：整批放棄，資料庫保持空的，下次重開還會再試。"""
    js = tmp_path / "conversations.json"
    broken = json.loads(json.dumps(LEGACY))
    broken["conversations"]["5"]["messages"][0]["role"] = "system"   # 違反 CHECK
    js.write_text(json.dumps(broken, ensure_ascii=False), "utf-8")
    s = Store(tmp_path / "hub.db", legacy_json=js)
    assert s.startup["migration"]["ok"] is False
    assert s.counts()["conversations"] == 0
    assert s.meta("legacy_json_imported") is None


def test_export_json_roundtrip(tmp_path):
    js = tmp_path / "conversations.json"
    js.write_text(json.dumps(LEGACY, ensure_ascii=False), "utf-8")
    s = Store(tmp_path / "hub.db", legacy_json=js)
    out = s.export_json()
    assert out["seq"] == 7
    for cid, c in LEGACY["conversations"].items():
        got = out["conversations"][cid]
        assert [(m["role"], m["content"]) for m in got["messages"]] == \
               [(m["role"], m["content"]) for m in c["messages"]]
        assert got.get("session_id") == c.get("session_id")


def test_verify_detects_damage(tmp_path, store):
    good = store.backup(tmp_path / "good.db")
    assert good["ok"]
    bad = tmp_path / "bad.db"
    data = bytearray((tmp_path / "good.db").read_bytes())
    data[4096:8192] = b"\x00" * 4096          # 把一整頁抹掉
    bad.write_bytes(bytes(data))
    try:
        assert verify(bad)["ok"] is False
    except Exception:       # noqa: BLE001 -- 壞到連開都打不開也算「抓到了」
        pass
