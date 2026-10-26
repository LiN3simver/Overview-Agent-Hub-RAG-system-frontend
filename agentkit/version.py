"""agentkit 的原始碼識別；啟動時固定，修改檔案後必須重啟才會換版。"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from copy import deepcopy
from pathlib import Path, PurePosixPath
from typing import Any

VERSION = "1.1.0"
SCHEMA = 1
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def source_bytes(path: Path) -> bytes:
    # Git 在 Windows/Linux 可能換行不同，Python 原始碼的 LF/CRLF 視為同一版。
    return path.read_bytes().replace(b"\r\n", b"\n")


def combined_hash(files: dict[str, str]) -> str:
    lines = "".join(f"{files[name]}  {name}\n" for name in sorted(files))
    return hashlib.sha256(lines.encode("utf-8")).hexdigest()


def snapshot(root: Path | None = None) -> dict[str, Any]:
    root = root or Path(__file__).resolve().parent
    files = {}
    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(root)
        if any(part.startswith(".") or part == "__pycache__" for part in rel.parts):
            continue
        files[rel.as_posix()] = hashlib.sha256(source_bytes(path)).hexdigest()
    if "__init__.py" not in files or "version.py" not in files:
        raise ValueError("agentkit 原始碼不完整：缺少 __init__.py 或 version.py")
    return {"schema": SCHEMA, "version": VERSION,
            "source_sha256": combined_hash(files), "files": files}


def valid_manifest(value: Any) -> bool:
    if not isinstance(value, dict) or value.get("schema") != SCHEMA:
        return False
    if not isinstance(value.get("version"), str) or not value["version"]:
        return False
    files = value.get("files")
    if not isinstance(files, dict) or not 2 <= len(files) <= 256:
        return False
    for name, digest in files.items():
        if not isinstance(name, str) or not isinstance(digest, str):
            return False
        rel = PurePosixPath(name)
        if (rel.is_absolute() or ".." in rel.parts or "\\" in name or ":" in name
                or rel.as_posix() != name or rel.suffix != ".py" or not _SHA256.fullmatch(digest)):
            return False
    return ("__init__.py" in files and "version.py" in files
            and value.get("source_sha256") == combined_hash(files))


def compare(actual: Any, expected: dict[str, Any]) -> dict[str, Any]:
    """嚴格比對完整快照；只有版本字串相同還不足以通過。"""
    if actual is None:
        return {"ok": False, "status": "unversioned", "hint": "更新完整 agentkit 並重建／重啟服務。"}
    if not valid_manifest(actual):
        return {"ok": False, "status": "invalid", "hint": "版本清單格式不完整或雜湊不一致。"}
    wanted, got = expected["files"], actual["files"]
    missing = sorted(wanted.keys() - got.keys())
    extra = sorted(got.keys() - wanted.keys())
    changed = sorted(name for name in wanted.keys() & got.keys() if wanted[name] != got[name])
    ok = actual["version"] == expected["version"] and actual["source_sha256"] == expected["source_sha256"]
    return {"ok": ok, "status": "current" if ok else "mismatch",
            "version": actual["version"], "source_sha256": actual["source_sha256"],
            "missing_files": missing, "changed_files": changed, "extra_files": extra,
            "hint": "" if ok else "更新完整 agentkit 快照；鎖版專案須先更新鎖檔，再重建／重啟。"}


# 每個 process 只記錄啟動時讀到的版本，避免磁碟檔案更新後舊行程冒充新版。
_RUNTIME = snapshot()


def identity() -> dict[str, Any]:
    return deepcopy(_RUNTIME)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="輸出 agentkit 原始碼版本（不載入 LLM 或 .env）")
    parser.add_argument("--sha256", action="store_true")
    args = parser.parse_args()
    print(_RUNTIME["source_sha256"] if args.sha256 else json.dumps(_RUNTIME, ensure_ascii=False, indent=2))
