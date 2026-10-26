"""將完整 agentkit 快照封裝為可重現的 ZIP，供各專案更新 vendor 與鎖檔。"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from agentkit import version  # noqa: E402


def package(output_dir: Path, source: Path | None = None) -> Path:
    source = source or Path(version.__file__).resolve().parent
    manifest = version.snapshot(source)
    output_dir.mkdir(parents=True, exist_ok=True)
    archive = output_dir / f"agentkit-{manifest['version']}-{manifest['source_sha256'][:12]}.zip"
    # 先讀完並核對，避免打包期間有人改檔而產生不符合 manifest 的 ZIP。
    contents = {f"agentkit/{name}": version.source_bytes(source / name) for name in manifest["files"]}
    actual = {name.removeprefix("agentkit/"): hashlib.sha256(data).hexdigest()
              for name, data in contents.items()}
    if actual != manifest["files"]:
        raise ValueError("agentkit 在打包時發生變更，請重新執行")
    contents["agentkit-release.json"] = (json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zipped:
        for name, data in sorted(contents.items()):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            zipped.writestr(info, data)
    return archive


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).resolve().parents[1] / "dist")
    args = parser.parse_args(argv)
    print(package(args.output_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
