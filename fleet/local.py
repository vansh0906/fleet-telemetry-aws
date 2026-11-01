"""Starts the local stand-ins: a moto server (AWS API emulator) and the ingest API, as child processes."""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent


def wait_port(port: int, timeout_s: float = 30) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.1)
    raise TimeoutError(f"nothing listening on port {port}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Child:
    def __init__(self, args: list[str], env: dict, log: Path):
        self.log = open(log, "w")
        self.proc = subprocess.Popen(args, env=env, cwd=ROOT, stdout=self.log, stderr=subprocess.STDOUT)

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.log.close()


def start_moto(port: int, logdir: Path) -> Child:
    env = dict(os.environ)
    child = Child([sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(port)], env,
                  logdir / "moto.log")
    wait_port(port)
    return child


def start_api(port: int, env_extra: dict, logdir: Path) -> Child:
    env = dict(os.environ)
    env.update(env_extra)
    child = Child([sys.executable, "-m", "uvicorn", "fleet.api:app", "--host", "127.0.0.1", "--port", str(port),
                   "--log-level", "warning"], env, logdir / "api.log")
    wait_port(port)
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            if requests.get(f"http://127.0.0.1:{port}/health", timeout=2).status_code == 200:
                return child
        except requests.RequestException:
            pass
        time.sleep(0.2)
    child.stop()
    raise RuntimeError(f"API did not become healthy; see {logdir / 'api.log'}")
