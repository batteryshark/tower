"""Shared fixtures: a throwaway nats-server with JetStream and token auth."""

import shutil
import socket
import subprocess
import time

import pytest

NATS_SERVER = shutil.which("nats-server")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def temp_bus(tmp_path):
    """Start a private nats-server; yields (url, token_file). Skips if nats-server is not installed."""
    if not NATS_SERVER:
        pytest.skip("nats-server not installed")
    port = _free_port()
    token_file = tmp_path / "nats.token"
    token_file.write_text("test-token\n")
    proc = subprocess.Popen(
        [NATS_SERVER, "-a", "127.0.0.1", "-p", str(port), "-js", "-sd", str(tmp_path / "js"), "--auth", "test-token"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                break
            except OSError:
                time.sleep(0.05)
        else:
            pytest.fail("temp nats-server did not start")
        yield f"nats://127.0.0.1:{port}", str(token_file)
    finally:
        proc.terminate()
        proc.wait(timeout=10)
