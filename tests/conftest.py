"""Global pytest configuration for QuantaFlow test suite.

Enforces offline fixture resolution and guards against unmocked external network egress.
"""

import os
import socket
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"
os.environ.setdefault("QUANTAFLOW_OFFLINE_FIXTURES_DIR", str(FIXTURES_DIR))
os.environ.setdefault("ENGINE_MODE", "duckdb")

_ORIGINAL_SOCKET_CONNECT = socket.socket.connect


@pytest.fixture(autouse=True)
def block_unmocked_external_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Guard against unmocked external network calls during test execution.

    Permits loopback/localhost connections for internal test servers while blocking
    any unmocked external network connections.
    """

    def guarded_connect(self: socket.socket, address: object) -> None:
        host = None
        if isinstance(address, tuple) and len(address) >= 1:
            host = address[0]
        elif isinstance(address, str):
            host = address

        allowed_hosts = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}
        if host in allowed_hosts or (isinstance(host, str) and host.startswith("127.")):
            _ORIGINAL_SOCKET_CONNECT(self, address)  # type: ignore[arg-type]
            return

        raise RuntimeError(
            f"External network call blocked by QuantaFlow test isolation: "
            f"attempted connection to {address}"
        )

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
