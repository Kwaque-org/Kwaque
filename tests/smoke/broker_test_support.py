from __future__ import annotations

import json
import os
import random
import re
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from bazel.native_test_environment import normalized_environment

# The test macros export the reactor backend selected for this build.
REACTOR_BACKEND = os.environ.get("KWAQUE_REACTOR_BACKEND", "epoll")
REACTOR_ARGUMENTS = (
    f"--reactor-backend={REACTOR_BACKEND}",
    "--smp=1",
    "--memory=128M",
    "--overprovisioned",
)

# Documented process exit statuses (src/broker/exit_code.h).
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
EXIT_NOT_CONFIGURED = 6
EXIT_DATA_DIRECTORY_IN_USE = 10
EXIT_CRASH_LOOP = 11

ADMIN_PORT = 9644

# Leases live as long as this process, like the brokers that use them.
_leases: list[socket.socket] = []


@dataclass(frozen=True)
class Endpoint:
    address: str
    port: int

    def url(self, path: str) -> str:
        return f"http://{self.address}:{self.port}{path}"


def lease_loopback_endpoint() -> Endpoint:
    """Return an admin endpoint that no concurrent test uses.

    Linux routes all of 127.0.0.0/8 to the loopback interface, so each broker
    gets its own address and keeps a fixed port; nothing probes for a free
    port that another process could take before the broker binds it. The
    lease is an abstract Unix socket named after the address: every process in
    the network namespace sees it, unlike a sandboxed temporary directory, and
    the kernel releases it when this process exits. 127.0.0.0/16 is left to
    developer brokers and other tools.
    """
    for _ in range(64):
        address = "127.{}.{}.{}".format(
            random.randrange(1, 255), random.randrange(0, 256), random.randrange(1, 255)
        )
        lease = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            lease.bind(f"\0kwaque-test-endpoint-{address}")
        except OSError:
            lease.close()
            continue
        _leases.append(lease)
        return Endpoint(address, ADMIN_PORT)
    raise RuntimeError("unable to lease a loopback endpoint")


def test_directory() -> tempfile.TemporaryDirectory[str]:
    """A temporary directory inside the test's private Bazel directory."""
    return tempfile.TemporaryDirectory(dir=os.environ.get("TEST_TMPDIR"))


def log_path(directory: Path, name: str) -> Path:
    """Where to keep a broker log so it survives a failed test.

    Bazel keeps TEST_UNDECLARED_OUTPUTS_DIR after the test, and CI uploads it.
    """
    outputs = os.environ.get("TEST_UNDECLARED_OUTPUTS_DIR")
    return (Path(outputs) if outputs else directory) / name


def write_config(
    template: Path,
    output: Path,
    data_directory: Path,
    endpoint: Endpoint,
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
        (r"(?m)^(\s*address:)\s*.*$", rf'\g<1> "{endpoint.address}"'),
        (r"(?m)^(\s*port:)\s*\d+\s*$", rf"\g<1> {endpoint.port}"),
    )
    for pattern, replacement in replacements:
        contents, count = re.subn(pattern, replacement, contents)
        if count != 1:
            raise AssertionError(f"unable to specialize {template} with {pattern}")
    output.write_text(contents, encoding="utf-8")


def http_request(
    endpoint: Endpoint, path: str, method: str = "GET"
) -> tuple[int, str, dict[str, str], str]:
    """Return status, content type, headers and body, including errors."""
    request = urllib.request.Request(endpoint.url(path), method=method)
    try:
        with urllib.request.urlopen(request, timeout=2.0) as response:
            return (
                response.status,
                response.headers.get_content_type(),
                dict(response.headers.items()),
                response.read().decode("utf-8"),
            )
    except urllib.error.HTTPError as error:
        return (
            error.code,
            error.headers.get_content_type(),
            dict(error.headers.items()),
            error.read().decode("utf-8"),
        )


def http_get(endpoint: Endpoint, path: str) -> tuple[int, str, str]:
    status, content_type, _, body = http_request(endpoint, path)
    return status, content_type, body


def version_fields(binary: Path) -> dict[str, str]:
    """Parses the tab-separated name=value fields that --version prints."""
    result = subprocess.run(
        [binary, "--version"], check=True, capture_output=True, text=True, timeout=5.0
    )
    return dict(field.split("=", 1) for field in result.stdout.strip().split("\t"))


# A broker log line that is never part of a passing run: a sanitizer report,
# a fatal reactor stop, or an error-level log record. Host checks and the
# per-shard memory recommendation report host-dependent severities (error in
# the production profile on a small test host) and are allowed.
_BAD_LOG_LINE = re.compile(
    r"runtime error:|Sanitizer|Aborting on shard|^ERROR\s", re.MULTILINE
)
_ALLOWED_LOG_LINE = re.compile(
    r"\] kwaque-broker - (?:host check |per-shard memory is below the recommended )"
)


def unexpected_log_lines(output: str, allowed: Iterable[str] = ()) -> list[str]:
    allowed = tuple(allowed)
    return [
        line
        for line in output.splitlines()
        if _BAD_LOG_LINE.search(line)
        and not _ALLOWED_LOG_LINE.search(line)
        and not any(text in line for text in allowed)
    ]


def assert_clean_log(output: str, allowed: Iterable[str] = ()) -> None:
    lines = unexpected_log_lines(output, allowed)
    if lines:
        raise AssertionError(
            "unexpected error or sanitizer output:\n" + "\n".join(lines)
        )


def assert_one_error(output: str, expected: str) -> None:
    """A rejected start reports exactly one actionable error line."""
    errors = unexpected_log_lines(output)
    if len(errors) != 1 or expected not in errors[0]:
        raise AssertionError(
            f"expected one error reporting {expected!r}, got {errors}:\n{output}"
        )


def run_broker(
    binary: Path,
    config: Path | None,
    *arguments: str,
    cwd: Path | None = None,
    environment: dict[str, str] | None = None,
    timeout: float = 30.0,
) -> subprocess.CompletedProcess[str]:
    # The broker may run in another directory, so neither its path nor the
    # sanitizer files it opens can stay relative to this one.
    return subprocess.run(
        [binary.absolute(), *(("--config", config) if config else ()), *arguments],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        cwd=cwd,
        env=normalized_environment(environment),
    )


class BrokerProcess:
    def __init__(
        self,
        binary: Path,
        config: Path | None,
        log: Path,
        reactor_arguments: tuple[str, ...] = REACTOR_ARGUMENTS,
        cwd: Path | None = None,
        arguments: tuple[str, ...] = (),
        environment: dict[str, str] | None = None,
    ) -> None:
        selected = ["--config", str(config)] if config is not None else []
        output = log.open("w", encoding="utf-8")
        try:
            self.process = subprocess.Popen(
                [binary.absolute(), *selected, *arguments, *reactor_arguments],
                stdout=output,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=cwd,
                env=normalized_environment(environment),
            )
        finally:
            output.close()
        self.log_path = log

    def output(self) -> str:
        return self.log_path.read_text(encoding="utf-8")

    def wait_for(self, expected: str, timeout: float = 60.0) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            output = self.output()
            if expected in output:
                return output
            return_code = self.process.poll()
            if return_code is not None:
                raise AssertionError(
                    f"broker exited with {return_code} before {expected!r}:\n{output}"
                )
            time.sleep(0.05)
        raise AssertionError(f"timed out waiting for {expected!r}:\n{self.output()}")

    def wait_until_ready(self, endpoint: Endpoint, timeout: float = 15.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            return_code = self.process.poll()
            if return_code is not None:
                raise AssertionError(
                    f"broker exited with {return_code} before readiness:\n"
                    f"{self.output()}"
                )
            try:
                status, _, body = http_get(endpoint, "/v1/health/ready")
                if status == 200 and json.loads(body) == {"status": "ready"}:
                    return
            except (ConnectionError, json.JSONDecodeError, urllib.error.URLError):
                pass
            time.sleep(0.05)
        raise AssertionError(f"broker did not become ready:\n{self.output()}")

    def stop(self, requested_signal: signal.Signals) -> str:
        """Stop the broker, requiring a clean exit and a clean log."""
        if self.process.poll() is None:
            self.process.send_signal(requested_signal)
        try:
            return_code = self.process.wait(timeout=10.0)
        except subprocess.TimeoutExpired as error:
            self.kill_if_running()
            raise AssertionError(
                f"broker did not stop after {requested_signal.name}:\n{self.output()}"
            ) from error
        if return_code != EXIT_SUCCESS:
            raise AssertionError(f"broker exited with {return_code}:\n{self.output()}")
        output = self.output()
        assert_clean_log(output)
        return output

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
    endpoint: Endpoint,
    path: str,
    expected: dict[str, Any],
) -> None:
    status, content_type, body = http_get(endpoint, path)
    if status != 200 or content_type != "application/json":
        raise AssertionError(
            f"unexpected response metadata for {path}: {status} {content_type}"
        )
    actual = json.loads(body)
    if actual != expected:
        raise AssertionError(f"unexpected response for {path}: {actual}")
