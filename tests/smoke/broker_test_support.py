from __future__ import annotations

import json
import re
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

REACTOR_ARGUMENTS = (
    "--reactor-backend=epoll",
    "--smp=1",
    "--memory=128M",
    "--overprovisioned",
)


def reserve_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def write_config(
    template: Path,
    output: Path,
    data_directory: Path,
    port: int,
    *,
    schema_version: int = 1,
) -> None:
    contents = template.read_text(encoding="utf-8")
    replacements = (
        (r"(?m)^(\s*schema_version:)\s*\d+\s*$", rf"\g<1> {schema_version}"),
        (
            r"(?m)^(\s*data_directory:)\s*.*$",
            rf'\g<1> "{data_directory}"',
        ),
        (r"(?m)^(\s*port:)\s*\d+\s*$", rf"\g<1> {port}"),
    )
    for pattern, replacement in replacements:
        contents, count = re.subn(pattern, replacement, contents)
        if count != 1:
            raise AssertionError(f"unable to specialize {template} with {pattern}")
    output.write_text(contents, encoding="utf-8")


def http_get(port: int, path: str) -> tuple[int, str, str]:
    with urllib.request.urlopen(
        f"http://127.0.0.1:{port}{path}", timeout=2.0
    ) as response:
        return (
            response.status,
            response.headers.get_content_type(),
            response.read().decode("utf-8"),
        )


class BrokerProcess:
    def __init__(self, binary: Path, config: Path, log_path: Path) -> None:
        output = log_path.open("w", encoding="utf-8")
        try:
            self.process = subprocess.Popen(
                [binary, "--config", config, *REACTOR_ARGUMENTS],
                stdout=output,
                stderr=subprocess.STDOUT,
                text=True,
            )
        finally:
            output.close()
        self.log_path = log_path

    def output(self) -> str:
        return self.log_path.read_text(encoding="utf-8")

    def wait_until_ready(self, port: int, timeout: float = 15.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            return_code = self.process.poll()
            if return_code is not None:
                raise AssertionError(
                    f"broker exited with {return_code} before readiness:\n"
                    f"{self.output()}"
                )
            try:
                status, _, body = http_get(port, "/v1/health/ready")
                if status == 200 and json.loads(body) == {"status": "ready"}:
                    return
            except (ConnectionError, json.JSONDecodeError, urllib.error.URLError):
                pass
            time.sleep(0.05)
        raise AssertionError(f"broker did not become ready:\n{self.output()}")

    def stop(self, requested_signal: signal.Signals) -> str:
        if self.process.poll() is None:
            self.process.send_signal(requested_signal)
        try:
            return_code = self.process.wait(timeout=10.0)
        except subprocess.TimeoutExpired as error:
            self.kill_if_running()
            raise AssertionError(
                f"broker did not stop after {requested_signal.name}:\n{self.output()}"
            ) from error
        if return_code != 0:
            raise AssertionError(f"broker exited with {return_code}:\n{self.output()}")
        return self.output()

    def kill_if_running(self) -> None:
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=5.0)


def assert_clean_shutdown(output: str) -> None:
    requested = output.find("shutdown requested")
    completed = output.find("shutdown complete", requested + 1)
    if requested < 0 or completed < 0:
        raise AssertionError(f"missing ordered shutdown diagnostics:\n{output}")


def assert_json_response(
    port: int,
    path: str,
    expected: dict[str, Any],
) -> None:
    status, content_type, body = http_get(port, path)
    if status != 200 or content_type != "application/json":
        raise AssertionError(
            f"unexpected response metadata for {path}: {status} {content_type}"
        )
    actual = json.loads(body)
    if actual != expected:
        raise AssertionError(f"unexpected response for {path}: {actual}")
