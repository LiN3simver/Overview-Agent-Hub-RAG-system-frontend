"""部署指令的只讀檢查與漏檔保護；SSH/Git 都用本機替身，絕不連伺服器。"""
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
GIT_BASH = Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "Git/bin/bash.exe"
BASH = str(GIT_BASH) if GIT_BASH.exists() else shutil.which("bash")
pytestmark = pytest.mark.skipif(not BASH, reason="需要 Git Bash／Bash")


@pytest.fixture
def shell_env(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_git = bin_dir / "git"
    fake_git.write_text('''#!/bin/bash
if [[ "$*" == "ls-files --others --exclude-standard -- agentkit/ scripts/" ]]; then
    printf '%s\\n' "$MOCK_UNTRACKED"
    exit 0
fi
printf '未預期的 git 呼叫：%s\\n' "$*" >&2
exit 2
''', encoding="utf-8")
    fake_ssh = bin_dir / "ssh"
    fake_ssh.write_text('''#!/bin/bash
printf '%s\\n' "$*" >> "$MOCK_SSH_LOG"
exit "${MOCK_SSH_EXIT:-0}"
''', encoding="utf-8")
    fake_git.chmod(0o755)
    fake_ssh.chmod(0o755)
    env = os.environ.copy()
    env.update({"MOCK_SSH_LOG": str(tmp_path / "ssh.log"), "MOCK_UNTRACKED": "",
                "NO_COLOR": "1", "MOCK_BIN": str(bin_dir)})
    return env


def run(shell_env, *args):
    # 在 Bash 內 prepend POSIX PATH，避免 Windows PATH 轉換讓真的 SSH 排在前面。
    command = 'export PATH="$(cygpath -u "$MOCK_BIN" 2>/dev/null || printf \'%s\' "$MOCK_BIN"):$PATH"; exec bash deploy/deploy.sh "$@"'
    return subprocess.run([BASH, "-c", command, "check-test", *args], cwd=ROOT,
                          env=shell_env, capture_output=True, timeout=20)


@pytest.mark.parametrize("missing", ["agentkit/cancellation.py", "scripts/check_deployment.py"])
def test_deploy_refuses_untracked_runtime_files_even_with_allow_dirty(shell_env, missing):
    shell_env["MOCK_UNTRACKED"] = missing
    result = run(shell_env, "deploy", "--allow-dirty", "-y")
    assert result.returncode == 1, result.stderr
    assert missing.encode() in result.stderr
    # 只有既有的唯讀 bootstrap 前提檢查，未進入 push 或遠端部署。
    calls = Path(shell_env["MOCK_SSH_LOG"]).read_text()
    assert "test -x" in calls and "apply.sh" not in calls


def test_check_passes_local_release_hash_and_selection_to_read_only_remote_command(shell_env):
    result = run(shell_env, "check", "rfq,partrag")
    assert result.returncode == 0, result.stderr
    calls = Path(shell_env["MOCK_SSH_LOG"]).read_text()
    assert re.search(r"--expected-sha256 [0-9a-f]{64}", calls)
    assert "--agents rfq,partrag" in calls and "exec -T hub python scripts/check_deployment.py" in calls
    assert "apply.sh" not in calls and "up --build" not in calls


def test_failed_remote_release_check_is_propagated(shell_env):
    shell_env["MOCK_SSH_EXIT"] = "1"
    assert run(shell_env, "check", "rfq").returncode == 1


def test_invalid_agent_argument_never_reaches_ssh(shell_env):
    result = run(shell_env, "check", "rfq;unexpected")
    assert result.returncode == 1
    assert not Path(shell_env["MOCK_SSH_LOG"]).exists()
