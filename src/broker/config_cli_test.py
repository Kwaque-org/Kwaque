#!/usr/bin/env python3
"""Verify broker startup, configuration selection, and process locking."""

from __future__ import annotations

import json
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path


REACTOR_ARGUMENTS = ("--smp=1", "--memory=128M", "--overprovisioned")
STARTUP_ATTEMPTS = 5


def reserve_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def write_test_config(
    template: Path, output: Path, data_directory: Path, port: int
) -> None:
    contents = template.read_text(encoding="utf-8")
    contents, directory_count = re.subn(
        r'(?m)^(\s*data_directory:)\s*.*$',
        rf'\1 "{data_directory}"',
        contents,
    )
    contents, port_count = re.subn(
        r"(?m)^(\s*port:)\s*\d+\s*$", rf"\1 {port}", contents
    )
    if directory_count != 1 or port_count != 1:
        raise AssertionError(f"unable to specialize configuration template {template}")
    output.write_text(contents, encoding="utf-8")


def get(url: str) -> tuple[int, str, str]:
    with urllib.request.urlopen(url, timeout=5.0) as response:
        return (
            response.status,
            response.headers.get_content_type(),
            response.read().decode("utf-8"),
        )


def version_fields(binary: Path) -> dict[str, str]:
    result = subprocess.run(
        [binary, "--version"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=5.0,
    )
    return dict(
        field.split("=", maxsplit=1)
        for field in result.stdout.strip().split("\t")
    )


def metric_value(exposition: str, name: str) -> float:
    samples = [
        line
        for line in exposition.splitlines()
        if line == name
        or line.startswith(name + "{")
        or line.startswith(name + " ")
    ]
    if len(samples) != 1:
        raise AssertionError(
            f"expected one bounded-cardinality sample for {name!r}:\n{exposition}"
        )
    return float(samples[0].rsplit(maxsplit=1)[1])


class RunningBroker:
    def __init__(self, binary: Path, log_path: Path, *arguments: str) -> None:
        output = log_path.open("w", encoding="utf-8")
        try:
            self.process = subprocess.Popen(
                [binary, *arguments, *REACTOR_ARGUMENTS],
                stdout=output,
                stderr=subprocess.STDOUT,
                text=True,
            )
        finally:
            output.close()
        self.log_path = log_path

    def output(self) -> str:
        return self.log_path.read_text(encoding="utf-8")

    def wait_for(self, expected: str, timeout: float = 15.0) -> str:
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

    def stop(self) -> str:
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
        try:
            return_code = self.process.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5.0)
            raise AssertionError(f"broker did not stop after SIGTERM:\n{self.output()}")
        output = self.output()
        if return_code != 0:
            raise AssertionError(
                f"broker exited with {return_code}\noutput:\n{output}"
            )
        return output

    def kill_if_running(self) -> None:
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=5.0)


def start_broker(
    binary: Path,
    logs: Path,
    name: str,
    template: Path,
    data_directory: Path,
) -> tuple[RunningBroker, Path, int, str]:
    for attempt in range(1, STARTUP_ATTEMPTS + 1):
        port = reserve_loopback_port()
        config = logs / f"{name}-{attempt}.yaml"
        write_test_config(template, config, data_directory, port)
        broker = RunningBroker(
            binary,
            logs / f"{name}-{attempt}.log",
            "--config",
            str(config),
        )
        try:
            output = broker.wait_for("startup stage=admin state=ready")
        except AssertionError:
            output = broker.output()
            broker.kill_if_running()
            address_in_use = (
                "address already in use" in output.casefold()
                or "eaddrinuse" in output.casefold()
            )
            if not address_in_use or attempt == STARTUP_ATTEMPTS:
                raise
            continue
        return broker, config, port, output

    raise AssertionError("exhausted broker startup attempts")


def assert_ordered(output: str, expected: tuple[str, ...]) -> None:
    position = 0
    for value in expected:
        found = output.find(value, position)
        if found < 0:
            raise AssertionError(f"missing ordered value {value!r}:\n{output}")
        position = found + len(value)


def main() -> None:
    binary = Path(sys.argv[1])
    default_template = Path(sys.argv[2])
    alternate_template = Path(sys.argv[3])

    with tempfile.TemporaryDirectory() as directory:
        logs = Path(directory)
        default, default_config, default_port, output = start_broker(
            binary,
            logs,
            "default",
            default_template,
            logs / "default-data",
        )
        try:
            for expected in (
                f"configuration loaded path={default_config}",
                "node_id=0",
                "build version=",
                "runtime shards=1",
                "memory_bytes=",
                "reactor_backend=",
                "runtime service ready shard=0",
            ):
                if expected not in output:
                    raise AssertionError(f"missing startup field {expected!r}:\n{output}")
            assert_ordered(
                output,
                (
                    "startup stage=data_directory state=ready",
                    "startup stage=pid_file state=ready",
                    "startup stage=abort_sources state=ready",
                    "startup stage=runtime_service state=ready",
                    "startup stage=admin state=ready",
                ),
            )

            base_url = f"http://127.0.0.1:{default_port}"
            status, content_type, body = get(base_url + "/v1/health/live")
            if (status, content_type, json.loads(body)) != (
                200,
                "application/json",
                {"status": "live"},
            ):
                raise AssertionError(f"unexpected liveness response: {status} {body}")

            status, content_type, body = get(base_url + "/v1/health/ready")
            if (status, content_type, json.loads(body)) != (
                200,
                "application/json",
                {"status": "ready"},
            ):
                raise AssertionError(f"unexpected readiness response: {status} {body}")

            _, _, body = get(base_url + "/v1/version")
            version = json.loads(body)
            expected_version = version_fields(binary)
            for field in ("version", "revision", "build_mode"):
                if version.get(field) != expected_version[field]:
                    raise AssertionError(
                        f"version endpoint field {field!r} did not match CLI: {body}"
                    )

            status, content_type, metrics = get(base_url + "/metrics")
            if status != 200 or content_type != "text/plain":
                raise AssertionError(
                    f"unexpected metrics response: {status} {content_type}"
                )
            metric_prefix = "kwaque_broker_"
            if metric_value(metrics, metric_prefix + "process_readiness") != 1:
                raise AssertionError("readiness metric was not set")
            if metric_value(metrics, metric_prefix + "shard_count") != 1:
                raise AssertionError("shard-count metric did not match --smp")
            if metric_value(metrics, metric_prefix + "startup_duration_seconds") < 0:
                raise AssertionError("startup duration metric was negative")
            if metric_value(metrics, metric_prefix + "shutdown_total") != 0:
                raise AssertionError("shutdown counter changed before shutdown")
            if metric_value(metrics, metric_prefix + "http_requests_total") < 3:
                raise AssertionError("administrative request counter did not advance")

            incomplete_request = socket.create_connection(
                ("127.0.0.1", default_port), timeout=5.0
            )
            incomplete_request.sendall(
                b"GET /v1/health/live HTTP/1.1\r\nHost: localhost\r\n"
            )
            output = default.stop()
            incomplete_request.close()
            assert_ordered(output, ("shutdown requested", "shutdown complete"))
        finally:
            default.kill_if_running()

        alternate, alternate_config, alternate_port, output = start_broker(
            binary,
            logs,
            "alternate",
            alternate_template,
            logs / "alternate-data",
        )
        try:
            expected_path = f"configuration loaded path={alternate_config}"
            for expected in (
                expected_path,
                "node_id=7",
                f"admin_port={alternate_port}",
                "developer_mode=false",
            ):
                if expected not in output:
                    raise AssertionError(
                        f"alternate configuration missing {expected!r}:\n{output}"
                    )

            contender = subprocess.run(
                [
                    binary,
                    "--config",
                    str(alternate_config),
                    *REACTOR_ARGUMENTS,
                ],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=15.0,
            )
            if contender.returncode == 0:
                raise AssertionError("second broker unexpectedly acquired the PID file")
            if "PID file is already locked" not in contender.stdout:
                raise AssertionError(
                    f"second broker did not report PID lock ownership:\n{contender.stdout}"
                )

            output = alternate.stop()
            if "shutdown complete" not in output:
                raise AssertionError(f"alternate broker did not shut down:\n{output}")
        finally:
            alternate.kill_if_running()


if __name__ == "__main__":
    main()
