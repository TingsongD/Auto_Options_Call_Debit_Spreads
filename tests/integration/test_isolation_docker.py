"""Actual worker controls and mock RPC; mandatory in CI, never provider access."""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from collections.abc import Iterator

import pytest

from spx_research.isolation.client import DockerGateway
from spx_research.isolation.protocol import canonical
from tests.unit.test_isolation import model_request, wire

pytestmark = pytest.mark.skipif(
    os.environ.get("SPX_TEST_ISOLATION") != "1",
    reason="set SPX_TEST_ISOLATION=1 for Docker acceptance",
)
IMAGE = os.environ.get("SPX_ISOLATION_IMAGE", "spx-inference:2.1")


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], check=check, capture_output=True, text=True, timeout=30
    )


@pytest.fixture(scope="module")
def socket_volume() -> Iterator[str]:
    unique = "spx-isolation-test-" + uuid.uuid4().hex[:12]
    volume = unique + "-socket"
    docker("volume", "create", volume)
    try:
        docker(
            "run",
            "-d",
            "--name",
            unique,
            "--network",
            "none",
            "--read-only",
            "--user",
            "65532:65532",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--pids-limit",
            "64",
            "--memory",
            "128m",
            "--cpus",
            "1",
            "--mount",
            f"type=volume,src={volume},dst=/run/spx",
            "--entrypoint",
            "python",
            IMAGE,
            "/app/gateway_server.py",
            "--mode",
            "mock",
            "--models",
            "mock-1",
        )
        for _ in range(40):
            ready = docker(
                "exec",
                unique,
                "python",
                "-c",
                "import os; assert os.path.exists('/run/spx/gateway.sock')",
                check=False,
            )
            if ready.returncode == 0:
                break
            time.sleep(0.1)
        else:
            pytest.fail("offline gateway failed to become ready: " + docker("logs", unique).stdout)
        yield volume
    finally:
        docker("rm", "-f", unique, check=False)
        docker("volume", "rm", volume, check=False)


def test_offline_container_round_trip(socket_volume: str) -> None:
    gateway = DockerGateway(image=IMAGE, socket_volume=socket_volume, mock=True)
    req = model_request()
    response = gateway.complete(req)
    assert response.request_hash == req.request_hash()
    assert response.parsed["packet_token"] == req.packet["packet_token"]
    assert response.parsed["schema_version"] == "2.1"
    assert response.cost_usd == 0 and not response.billing_uncertain
    assert response.provider_metadata["worker_image"].startswith("sha256:")


PROBES = r"""
import json, os, socket
from pathlib import Path
result = {'uid':os.getuid(), 'gid':os.getgid()}
for label,path in {
  'archive':'/archive', 'repo':'/repo', 'workspace':'/workspace',
  'host_home':'/Users/tingsongdai',
  'host_repo':'/Users/tingsongdai/Codex Projects/Auto_Options_Call_Debit_Spreads',
  'provider_secret':'/run/secrets/provider_key', 'database_secret':'/run/secrets/database_url',
  'egress_socket':'/run/egress/provider.sock',
}.items():
  try:
    if Path(path).is_dir(): os.listdir(path)
    else:
      with open(path,'rb') as f: f.read(1)
    result[label] = False
  except (OSError, PermissionError): result[label] = True
for label,path in {'readonly_app':'/app/denial-probe','readonly_tmp':'/tmp/denial-probe'}.items():
  try:
    with open(path,'w') as f: f.write('probe')
    result[label] = False
  except OSError: result[label] = True
secret_keys=('OPENAI_API_KEY','SPX_DB_DSN','SPX_TEST_DSN','SPX_ISOLATION_SECRET_CANARY')
result['no_secrets'] = all(k not in os.environ for k in secret_keys)
try:
  os.setuid(0)
  result['cannot_become_root'] = False
except OSError: result['cannot_become_root'] = True
targets={'arbitrary_network':('192.0.2.1',443),
         'database_host':('host.docker.internal',5433)}
for label,address in targets.items():
  try:
    s=socket.create_connection(address,timeout=.2);s.close()
    result[label] = False
  except OSError: result[label] = True
status=Path('/proc/self/status').read_text()
lines=status.splitlines()
result['zero_capabilities'] = any(
    l.startswith('CapEff:') and int(l.split()[1],16)==0 for l in lines)
result['no_new_privileges'] = 'NoNewPrivs:\t1' in status
result['contracts_available'] = Path('/app/contracts/bundle.json').is_file()
print(json.dumps(result))
"""


def test_actual_worker_denies_files_secrets_network_and_privilege(socket_volume: str) -> None:
    gateway = DockerGateway(image=IMAGE, socket_volume=socket_volume, mock=True)
    name = "spx-worker-probe-" + uuid.uuid4().hex[:12]
    command = gateway.command()
    command[-1:-1] = ["--name", name]
    process = subprocess.Popen(
        command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    try:
        for _ in range(40):
            inspection = docker("inspect", name, check=False)
            if inspection.returncode == 0 and json.loads(inspection.stdout)[0]["State"]["Running"]:
                break
            time.sleep(0.1)
        else:
            pytest.fail("worker did not start")
        info = json.loads(inspection.stdout)[0]
        host = info["HostConfig"]
        assert host["NetworkMode"] == "none" and host["ReadonlyRootfs"]
        assert host["CapDrop"] == ["ALL"] and host["PidsLimit"] == 64
        assert host["Memory"] == 128 * 1024 * 1024 and host["NanoCpus"] == 1_000_000_000
        assert info["Config"]["User"] == "65532:65532"
        assert [(m["Destination"], m["RW"]) for m in info["Mounts"]] == [("/run/spx", False)]
        results = json.loads(docker("exec", name, "python", "-c", PROBES).stdout)
        assert results.pop("uid") == results.pop("gid") == 65532
        assert all(results.values()), results
        out, err = process.communicate(canonical(wire(model_request())) + b"\n", timeout=20)
        assert process.returncode == 0, err.decode()
        assert json.loads(out)["provider"]["id"] == "offline-mock"
    finally:
        if process.poll() is None:
            docker("rm", "-f", name, check=False)
            process.communicate(timeout=10)


def test_worker_rejects_url_capability_before_gateway(socket_volume: str) -> None:
    gateway = DockerGateway(image=IMAGE, socket_volume=socket_volume, mock=True)
    raw = wire(model_request())
    raw["url"] = "https://unapproved.invalid"
    result = subprocess.run(
        gateway.command(), input=canonical(raw) + b"\n", capture_output=True, check=True, timeout=20
    )
    assert json.loads(result.stdout) == {
        "error": "ISOLATED_GATEWAY_FAILED",
        "exception_type": "ValueError",
    }
