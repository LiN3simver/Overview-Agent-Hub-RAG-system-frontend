"""The committed locks must cover both deployment platforms and reviewed inputs."""
import re

import pytest
from packaging.markers import default_environment
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from scripts import lock_dependencies as lock


def locked(path, platform):
    env = {**default_environment(), "sys_platform": platform,
           "platform_system": "Windows" if platform == "win32" else "Linux"}
    result = {}
    for line in path.read_text(encoding="utf-8").replace("\\\n", " ").splitlines():
        if not line.strip() or line.startswith(("#", "--", "-r")):
            continue
        text, *hashes = line.split(" --hash=sha256:")
        assert hashes and all(re.fullmatch(r"[0-9a-f]{64}", h.strip()) for h in hashes)
        requirement = Requirement(text.strip())
        assert len(requirement.specifier) == 1
        assert next(iter(requirement.specifier)).operator == "=="
        if requirement.marker is None or requirement.marker.evaluate(env):
            result[canonicalize_name(requirement.name)] = (next(iter(requirement.specifier)).version, set(requirement.extras))
    return result


@pytest.mark.parametrize("platform", ["win32", "linux"])
def test_lock_covers_reviewed_direct_and_transitive_dependencies(platform, monkeypatch):
    # uvloop has no mandatory dependencies; never query the network in tests.
    monkeypatch.setattr(lock, "release", lambda name, version: {"info": {"requires_dist": []}})
    expected_runtime = lock.resolve(lock.inputs(lock.ROOT / "requirements.in"), platform)
    expected_dev = lock.resolve(lock.inputs(lock.ROOT / "requirements-dev.in"), platform)
    runtime = locked(lock.ROOT / "requirements.txt", platform)
    development = {**runtime, **locked(lock.ROOT / "requirements-dev.txt", platform)}
    assert runtime == expected_runtime
    assert development == expected_dev
    assert runtime["uvicorn"][1] == {"standard"}
    assert ("uvloop" in runtime) == (platform == "linux")
    assert ("colorama" in runtime) == (platform == "win32")
