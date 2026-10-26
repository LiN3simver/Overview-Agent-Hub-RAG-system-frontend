"""版本漂移、漏檔、舊 Hub、分批上線與部署檢查退出碼。只用本機假 HTTP。"""
import asyncio
import hashlib
import json
import zipfile
from copy import deepcopy

import httpx
import pytest
from fastapi import FastAPI

import main
from agentkit import AgentSpec, build_router, client, deployment, envelope, registry, version
from scripts import check_deployment, package_agentkit


def changed_manifest():
    actual = version.identity()
    actual["files"]["client.py"] = "a" * 64
    actual["source_sha256"] = version.combined_hash(actual["files"])
    return actual


def report(ids=("rfq",), manifest=None):
    manifest = manifest or version.identity()
    return {"ok": True, "scope": "agentkit_consistency", "hub": {"agentkit": manifest},
            "checked_agents": list(ids), "agents": [
                {"agent_id": aid, **version.compare(manifest, manifest)} for aid in ids]}


@pytest.fixture
def kit_source(tmp_path):
    root = tmp_path / "kit"
    root.mkdir()
    (root / "__init__.py").write_bytes(b"# init\r\n")
    (root / "version.py").write_bytes(b"VERSION = '1.1.0'\r\n")
    (root / "client.py").write_bytes(b"def work():\r\n    return 1\r\n")
    return root


def test_snapshot_normalizes_crlf_and_ignores_non_source(kit_source):
    first = version.snapshot(kit_source)
    (kit_source / "client.py").write_bytes(b"def work():\n    return 1\n")
    (kit_source / ".env").write_text("synthetic-secret")
    (kit_source / "BUILD_MANIFEST.json").write_text("synthetic-build")
    (kit_source / "__pycache__").mkdir()
    (kit_source / "__pycache__" / "junk.py").write_text("synthetic-cache")
    assert version.snapshot(kit_source) == first
    assert version.valid_manifest(first)


def test_runtime_identity_is_frozen_and_cannot_be_mutated(monkeypatch):
    before = version.identity()
    monkeypatch.setattr(version, "snapshot", lambda *args: pytest.fail("不得重讀磁碟"))
    exposed = version.identity()
    exposed["files"]["client.py"] = "changed"
    assert version.identity() == before


def test_same_version_with_different_source_is_rejected():
    result = version.compare(changed_manifest(), version.identity())
    assert result["status"] == "mismatch" and not result["ok"]
    assert result["changed_files"] == ["client.py"]


def test_missing_and_extra_files_are_reported():
    expected = version.identity()
    actual = deepcopy(expected)
    del actual["files"]["cancellation.py"]
    actual["files"]["obsolete.py"] = "b" * 64
    actual["source_sha256"] = version.combined_hash(actual["files"])
    result = version.compare(actual, expected)
    assert result["missing_files"] == ["cancellation.py"]
    assert result["extra_files"] == ["obsolete.py"] and not result["ok"]


@pytest.mark.parametrize("actual", [None, {}, {"version": "1.1.0"}, [], "old"])
def test_unversioned_or_invalid_manifests_fail(actual):
    assert version.compare(actual, version.identity())["ok"] is False


def test_manifest_cannot_claim_unchanged_hash_after_file_changes():
    actual = version.identity()
    actual["files"]["client.py"] = "b" * 64
    assert not version.valid_manifest(actual)


@pytest.mark.parametrize("filename", ["../other.py", "/private.py", "C:/file.py", "x\\file.py"])
def test_invalid_manifest_paths_are_rejected(filename):
    actual = version.identity()
    actual["files"][filename] = "b" * 64
    actual["source_sha256"] = version.combined_hash(actual["files"])
    assert not version.valid_manifest(actual)


def test_version_endpoint_is_independent_of_business_dependencies():
    def forbidden():
        pytest.fail("版本檢查不得呼叫 PLM/DB/LLM 或 status_fn")
    spec = AgentSpec("rfq", status_fn=forbidden)
    app = FastAPI()
    app.include_router(build_router(spec))
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
            return await http.get("/a2a/version")
    response = asyncio.run(run())
    assert response.status_code == 200
    assert response.json() == {"protocol": envelope.PROTOCOL, "agent_id": "rfq", "agentkit": version.identity()}


def test_deployment_reports_mixed_versions_offline_and_missing_endpoint():
    ids = ["rfq", "bom", "partrag", "car8d", "iso"]
    def handle(request):
        assert request.url.path == "/a2a/version"
        aid = request.url.host
        if aid == "partrag":
            raise httpx.ConnectError("synthetic", request=request)
        if aid == "car8d":
            return httpx.Response(404)
        if aid == "iso":
            return httpx.Response(200, json={"agent_id": "rfq", "protocol": envelope.PROTOCOL})
        manifest = changed_manifest() if aid == "bom" else version.identity()
        return httpx.Response(200, json={"agent_id": aid, "protocol": envelope.PROTOCOL, "agentkit": manifest})
    # transport 用固定 host 區分；不使用機器上真實 .env 的 URL。
    with pytest.MonkeyPatch.context() as patch:
        for aid in ids:
            patch.setenv(f"A2A_{aid.upper()}_URL", f"http://{aid}")
        result = asyncio.run(deployment.inspect_agents(ids, transport=httpx.MockTransport(handle)))
    assert result["ok"] is False
    assert [row["status"] for row in result["agents"]] == [
        "current", "mismatch", "unavailable", "unversioned", "invalid"]


def test_deployment_has_bounded_parallelism_and_hard_timeout(monkeypatch):
    monkeypatch.setattr(deployment, "TIMEOUT_S", 0.05)
    active = peak = closed = 0
    async def handle(request):
        nonlocal active, peak, closed
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(60)
            pytest.fail("應在硬期限取消")
        finally:
            active -= 1
            closed += 1
    ids = [ref.agent_id for ref in registry.AGENTS]
    result = asyncio.run(deployment.inspect_agents(ids, transport=httpx.MockTransport(handle)))
    assert peak <= deployment.MAX_CONCURRENT and active == 0 and closed == len(ids)
    assert all(row["status"] == "unavailable" for row in result["agents"])


@pytest.mark.parametrize("agents,expected_code", [("rfq,bom,rfq", 200), ("unknown", 400), ("", 400)])
def test_hub_deployment_selection_is_explicit_and_validated(monkeypatch, agents, expected_code):
    async def inspect(ids):
        assert ids == ["rfq", "bom"]
        return report(ids)
    monkeypatch.setattr(deployment, "inspect_agents", inspect)
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as http:
            return await http.get("/hub/deployment", params={"agents": agents})
    response = asyncio.run(run())
    assert response.status_code == expected_code


def test_hub_deployment_checks_all_registry_agents_by_default(monkeypatch):
    async def inspect(ids):
        assert ids == [ref.agent_id for ref in registry.AGENTS]
        return report(ids)
    monkeypatch.setattr(deployment, "inspect_agents", inspect)
    assert asyncio.run(main.deployment_check())["ok"] is True


def test_cli_catches_hub_and_agents_consistently_running_old_snapshot():
    old = changed_manifest()
    problems = check_deployment.evaluate(report(manifest=old), version.identity()["source_sha256"], ["rfq"])
    assert any("Hub 還不是" in problem for problem in problems)
    assert any("rfq" in problem for problem in problems)


@pytest.mark.parametrize("rows", [[], [{"agent_id": "rfq"}, {"agent_id": "rfq"}], [None]])
def test_cli_rejects_incomplete_or_duplicate_check_evidence(rows):
    data = report()
    data["agents"] = rows
    assert check_deployment.evaluate(data, version.identity()["source_sha256"], ["rfq"])


@pytest.mark.parametrize("fails", [False, True])
def test_cli_returns_nonzero_when_release_gate_fails(monkeypatch, fails, capsys):
    data = report(manifest=changed_manifest() if fails else None)
    ctor = httpx.Client
    def handle(request):
        assert request.url.path == "/hub/deployment" and request.url.params["agents"] == "rfq"
        return httpx.Response(200, json=data)
    monkeypatch.setattr(httpx, "Client", lambda **kw: ctor(transport=httpx.MockTransport(handle), **kw))
    assert check_deployment.main(["--hub-url", "http://hub.test", "--agents", "rfq", "--json"]) == int(fails)
    assert json.loads(capsys.readouterr().out)["ok"] is not fails


def test_cards_expose_version_drift_without_changing_agent_readiness(monkeypatch):
    actual = {"agent_id": "rfq", "status": "ready", "agentkit": changed_manifest()}
    monkeypatch.setattr(client.network, "request", lambda *a, **kw: httpx.Response(
        200, request=httpx.Request("GET", "http://agent.test"), json=actual))
    result = client.card("rfq", fresh=True)
    assert result["status"] == "ready" and result["agentkit_check"]["status"] == "mismatch"


def test_package_is_reproducible_complete_and_contains_no_secrets(kit_source, tmp_path):
    (kit_source / ".env.bak").write_text("synthetic-secret")
    archive = package_agentkit.package(tmp_path / "out", kit_source)
    first = archive.read_bytes()
    assert package_agentkit.package(tmp_path / "out", kit_source).read_bytes() == first
    with zipfile.ZipFile(archive) as zipped:
        manifest = json.loads(zipped.read("agentkit-release.json"))
        assert set(zipped.namelist()) == {"agentkit-release.json"} | {f"agentkit/{name}" for name in manifest["files"]}
        for name, digest in manifest["files"].items():
            assert hashlib.sha256(zipped.read(f"agentkit/{name}")).hexdigest() == digest
    assert version.valid_manifest(manifest)
