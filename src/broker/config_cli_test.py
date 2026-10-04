"""Verify broker startup, configuration selection, and process locking."""

from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import socket
import sys
from pathlib import Path

from tests.smoke.broker_test_support import (
    EXIT_CRASH_LOOP,
    EXIT_DATA_DIRECTORY_IN_USE,
    EXIT_FAILURE,
    EXIT_NOT_CONFIGURED,
    EXIT_USAGE,
    REACTOR_BACKEND,
    BrokerProcess,
    Endpoint,
    assert_one_error,
    http_get,
    lease_loopback_endpoint,
    log_path,
    run_broker,
    test_directory,
    version_fields,
    write_config,
)

REACTOR_ARGUMENTS = (
    f"--reactor-backend={REACTOR_BACKEND}",
    "--smp=2",
    "--memory=384M",
    "--overprovisioned",
)
ADMIN_EXPOSURE_WARNING = "admin API is exposed without authentication or TLS"

ADMIN_METRICS = frozenset(
    {
        "kwaque_broker_process_readiness",
        "kwaque_broker_draining",
        "kwaque_broker_shards",
        "kwaque_broker_startup_duration_seconds",
        "kwaque_broker_start_time_seconds",
    }
)
BUILD_METRICS = frozenset({"kwaque_build_info"})
BUILD_LABELS = frozenset({"version", "revision", "build_mode", "dirty"})
RUNTIME_METRICS = frozenset(
    {
        "kwaque_runtime_task_active",
        "kwaque_runtime_task_accepted_total",
        "kwaque_runtime_task_completed_total",
        "kwaque_runtime_task_failed_total",
        "kwaque_runtime_task_abort_requests_total",
        "kwaque_runtime_timer_active",
        "kwaque_runtime_timer_accepted_total",
        "kwaque_runtime_timer_completed_total",
        "kwaque_runtime_timer_rejected_total",
        "kwaque_runtime_file_active",
        "kwaque_runtime_file_accepted_total",
        "kwaque_runtime_file_completed_total",
        "kwaque_runtime_file_rejected_total",
        "kwaque_runtime_file_completed_bytes_total",
        "kwaque_runtime_network_active",
        "kwaque_runtime_network_accepted_total",
        "kwaque_runtime_network_completed_total",
        "kwaque_runtime_network_rejected_total",
        "kwaque_runtime_network_completed_bytes_total",
        "kwaque_runtime_dns_active",
        "kwaque_runtime_dns_accepted_total",
        "kwaque_runtime_dns_completed_total",
        "kwaque_runtime_dns_rejected_total",
    }
)
RESOURCE_METRICS = frozenset(
    {
        "kwaque_resource_manager_memory_available_bytes",
        "kwaque_resource_manager_memory_configured_bytes",
        "kwaque_resource_manager_memory_used_bytes",
        "kwaque_resource_manager_memory_waiters",
    }
)
# Admission counters added to the native HTTP server for the admin listener.
HTTP_ADMISSION_METRICS = (
    "kwaque_httpd_connections_rejected",
    "kwaque_httpd_deadline_terminations",
)
WORKLOAD_COUNT = 8
PRODUCT_METRICS = ADMIN_METRICS | BUILD_METRICS | RUNTIME_METRICS | RESOURCE_METRICS
PRODUCT_PREFIXES = (
    "kwaque_broker_",
    "kwaque_build_",
    "kwaque_runtime_task_",
    "kwaque_runtime_timer_",
    "kwaque_runtime_file_",
    "kwaque_runtime_network_",
    "kwaque_runtime_dns_",
    "kwaque_resource_manager_",
)
DEFERRED_PREFIXES = (
    "kwaque_bounded_queue_",
    "kwaque_simulation_",
)


def metric_value(exposition: str, name: str) -> float:
    samples = [
        line
        for line in exposition.splitlines()
        if line == name or line.startswith((name + "{", name + " "))
    ]
    if len(samples) != 1:
        raise AssertionError(
            f"expected one bounded-cardinality sample for {name!r}:\n{exposition}"
        )
    return float(samples[0].rsplit(maxsplit=1)[1])


def metric_samples(exposition: str, name: str) -> list[str]:
    return [
        line
        for line in exposition.splitlines()
        if line.startswith((name + "{", name + " "))
    ]


def sample_name(line: str) -> str:
    return line.split("{", maxsplit=1)[0].split(maxsplit=1)[0]


def label_names(line: str) -> set[str]:
    if "{" not in line:
        return set()
    labels = line.split("{", maxsplit=1)[1].split("}", maxsplit=1)[0]
    if not labels:
        return set()
    return {label.split("=", maxsplit=1)[0] for label in labels.split(",")}


def verify_product_metrics(exposition: str, *, aggregated: bool) -> None:
    samples = [
        line for line in exposition.splitlines() if line and not line.startswith("#")
    ]
    observed = {
        sample_name(line)
        for line in samples
        if sample_name(line).startswith(PRODUCT_PREFIXES)
    }
    if observed != PRODUCT_METRICS:
        raise AssertionError(
            "product metric inventory mismatch: "
            f"missing={sorted(PRODUCT_METRICS - observed)} "
            f"unexpected={sorted(observed - PRODUCT_METRICS)}"
        )
    shard = set() if aggregated else {"shard"}
    for name in PRODUCT_METRICS:
        matching = metric_samples(exposition, name)
        if name in RESOURCE_METRICS:
            expected_samples = WORKLOAD_COUNT if aggregated else 2 * WORKLOAD_COUNT
            expected_labels = {"workload"} | shard
        elif name in RUNTIME_METRICS:
            expected_samples = 1 if aggregated else 2
            expected_labels = shard
        elif name in BUILD_METRICS:
            # Process-wide values are owned by shard zero and aggregate away
            # the shard label.
            expected_samples = 1
            expected_labels = set(BUILD_LABELS) | shard
        else:
            expected_samples = 1
            expected_labels = shard
        if len(matching) != expected_samples:
            raise AssertionError(
                f"expected {expected_samples} sample(s) for {name!r}: {matching}"
            )
        for sample in matching:
            labels = label_names(sample)
            if labels != expected_labels:
                raise AssertionError(
                    f"metric {name!r} has labels {sorted(labels)}, "
                    f"expected {sorted(expected_labels)}"
                )
            if (
                not aggregated
                and 'shard="0"' not in sample
                and name in (ADMIN_METRICS | BUILD_METRICS)
            ):
                raise AssertionError(f"process metric {name!r} left shard zero")
        if not aggregated and name in RUNTIME_METRICS | RESOURCE_METRICS:
            for shard_id in (0, 1):
                shard_samples = [
                    sample for sample in matching if f'shard="{shard_id}"' in sample
                ]
                expected_shard_samples = (
                    WORKLOAD_COUNT if name in RESOURCE_METRICS else 1
                )
                if len(shard_samples) != expected_shard_samples:
                    raise AssertionError(
                        f"metric {name!r} expected {expected_shard_samples} "
                        f"sample(s) for shard {shard_id}: {matching}"
                    )
    deferred = {
        sample_name(line)
        for line in samples
        if sample_name(line).startswith(DEFERRED_PREFIXES)
    }
    if deferred:
        raise AssertionError(
            f"deferred metric families appeared in broker output: {sorted(deferred)}"
        )
    for name in HTTP_ADMISSION_METRICS:
        if not metric_samples(exposition, name):
            raise AssertionError(f"admin listener does not export {name!r}")


def start_broker(
    binary: Path,
    logs: Path,
    name: str,
    template: Path,
    data_directory: Path,
    *arguments: str,
    environment: dict[str, str] | None = None,
    endpoint: Endpoint | None = None,
) -> tuple[BrokerProcess, Path, Endpoint, str]:
    endpoint = endpoint or lease_loopback_endpoint()
    config = logs / f"{name}.yaml"
    write_config(template, config, data_directory, endpoint)
    broker = BrokerProcess(
        binary,
        config,
        log_path(logs, f"{name}.log"),
        REACTOR_ARGUMENTS,
        arguments=arguments,
        environment=environment,
    )
    try:
        output = broker.wait_for("startup stage=admin state=ready")
    except AssertionError:
        broker.kill_if_running()
        raise
    return broker, config, endpoint, output


def assert_ordered(output: str, expected: tuple[str, ...]) -> None:
    position = 0
    for value in expected:
        found = output.find(value, position)
        if found < 0:
            raise AssertionError(f"missing ordered value {value!r}:\n{output}")
        position = found + len(value)


def assert_startup_rejected(
    binary: Path,
    directory: Path,
    *arguments: str,
    expected: tuple[str, ...],
    environment: dict[str, str],
    before_reactor: bool = False,
    after_configuration: bool = False,
    after_memory_observation: bool = False,
    status: int = EXIT_NOT_CONFIGURED,
) -> None:
    before = set(directory.rglob("*"))
    result = run_broker(
        binary,
        None,
        *arguments,
        *REACTOR_ARGUMENTS,
        cwd=directory,
        environment=environment,
        timeout=15.0,
    )
    # The native option parser owns command-line errors.
    expected_status = EXIT_USAGE if before_reactor else status
    if result.returncode != expected_status:
        raise AssertionError(
            f"expected exit {expected_status} for {arguments}, "
            f"got exit {result.returncode}:\n{result.stdout}"
        )
    for value in expected:
        if value not in result.stdout:
            raise AssertionError(
                f"startup rejection did not report {value!r}:\n{result.stdout}"
            )
    if not before_reactor:
        assert_one_error(result.stdout, expected[0])
    if after_configuration:
        if result.stdout.count("configuration loaded ") != 1:
            raise AssertionError(
                f"profile rejection did not load one configuration:\n{result.stdout}"
            )
    elif "configuration loaded" in result.stdout:
        raise AssertionError(f"rejected startup loaded configuration:\n{result.stdout}")
    if after_memory_observation and "runtime shards=" not in result.stdout:
        raise AssertionError(
            f"startup rejection did not observe memory:\n{result.stdout}"
        )
    forbidden_fields = (
        ("startup stage=",)
        if after_memory_observation
        else ("startup stage=", "runtime shards=")
    )
    for forbidden in forbidden_fields:
        if forbidden in result.stdout:
            raise AssertionError(
                f"rejected startup reached {forbidden!r}:\n{result.stdout}"
            )
    after = set(directory.rglob("*"))
    if after != before:
        raise AssertionError(
            "rejected startup changed the working directory: "
            f"created={sorted(after - before)} removed={sorted(before - after)}"
        )


def verify_configuration_rejections(
    binary: Path, directory: Path, environment: dict[str, str]
) -> None:
    cases = (
        ("missing-file", None, "unable to read configuration file"),
        ("malformed", "kwaque: [", "unable to parse YAML"),
        (
            "missing-setting",
            "kwaque: {developer_mode: true}",
            "required configuration key is missing",
        ),
        (
            # The version is checked before keys a newer schema may add.
            "future-schema",
            "kwaque: {schema_version: 2, future_setting: 1}",
            "unsupported configuration schema version 2",
        ),
        (
            "octal-looking-port",
            "kwaque: {schema_version: 1, developer_mode: true, admin: {port: 010000}}",
            "expected an unquoted decimal integer without leading zeros",
        ),
        (
            "second-document",
            "kwaque: {schema_version: 1, developer_mode: true}\n---\nother: 1\n",
            "configuration must contain exactly one YAML document",
        ),
        (
            "relative-production-directory",
            "kwaque: {schema_version: 1, data_directory: ./data}",
            "data directory must be an absolute path",
        ),
        (
            "removed-log-level",
            "kwaque: {schema_version: 1, developer_mode: true, log_level: debug}",
            "unknown configuration key",
        ),
        (
            "duplicate-setting",
            "kwaque: {schema_version: 1, schema_version: 1}",
            "duplicate configuration key",
        ),
        (
            "unknown-setting",
            "kwaque: {schema_version: 1, unknown: true}",
            "unknown configuration key",
        ),
        (
            "oversized",
            "x" * (64 * 1024 + 1),
            "configuration exceeds the maximum supported size",
        ),
    )
    for name, contents, expected in cases:
        config = directory / f"{name}.yaml"
        if contents is not None:
            config.write_text(contents, encoding="utf-8")
        assert_startup_rejected(
            binary,
            directory,
            "--config",
            str(config),
            expected=(expected,),
            environment=environment,
        )

    config = directory / "missing-file.yaml"
    for arguments, expected in (
        (("--config", str(config), "--config", str(config)), "--config"),
        (("--unknown-broker-option",), "--unknown-broker-option"),
    ):
        assert_startup_rejected(
            binary,
            directory,
            *arguments,
            expected=(expected,),
            environment=environment,
            before_reactor=True,
        )


def verify_runtime_rejections(
    binary: Path,
    directory: Path,
    templates: tuple[Path, Path],
    environment: dict[str, str],
) -> None:
    unsafe_options = (
        (("--unsafe-bypass-fsync=true",), "unsafe-bypass-fsync"),
        (("--unsafe-bypass-fsync", "true"), "unsafe-bypass-fsync"),
        (("--kernel-page-cache=true",), "kernel-page-cache"),
        (("--kernel-page-cache", "true"), "kernel-page-cache"),
        (("--relaxed-dma",), "relaxed-dma"),
    )
    io_properties = directory / "io-properties.yaml"
    io_properties.write_text("disks: []\n", encoding="utf-8")
    for index, template in enumerate(templates):
        config = directory / f"runtime-policy-{index}.yaml"
        write_config(
            template,
            config,
            directory / f"rejected-data-{index}",
            lease_loopback_endpoint(),
        )
        for arguments, option in unsafe_options:
            assert_startup_rejected(
                binary,
                directory,
                "--config",
                str(config),
                *arguments,
                expected=(f"{option} is not supported by the broker",),
                environment=environment,
                before_reactor=True,
            )
        assert_startup_rejected(
            binary,
            directory,
            "--config",
            str(config),
            "--io-properties=disks: []",
            "--io-properties-file",
            str(io_properties),
            expected=("io-properties and io-properties-file cannot be used together",),
            environment=environment,
            before_reactor=True,
        )


def verify_startup_policy(output: str, config: Path, profile: str) -> dict[str, str]:
    policies = [
        line.partition("startup policy ")[2]
        for line in output.splitlines()
        if "startup policy " in line
    ]
    if len(policies) != 1:
        raise AssertionError(f"expected one resolved startup policy:\n{output}")
    fields = dict(re.findall(r"(?:^| )([a-z_]+)=([^ ]+)", policies[0]))
    contents = config.read_bytes()
    expected = {
        "profile": profile,
        "crash_loop_limit": "5",
        "crash_loop_limiting": "true" if profile == "production" else "false",
        "configuration_checksum_algorithm": "sha256",
        "configuration_checksum": hashlib.sha256(contents).hexdigest(),
        "configuration_bytes": str(len(contents)),
    }
    for key, value in expected.items():
        if fields.get(key) != value:
            raise AssertionError(
                f"startup policy expected {key}={value}: {policies[0]}"
            )
    allocator = fields.get("allocator")
    if allocator not in ("seastar", "system"):
        raise AssertionError(
            f"startup policy omitted allocator capability: {policies[0]}"
        )
    expected_stats = "native" if allocator == "seastar" else "synthetic"
    if fields.get("allocator_stats") != expected_stats:
        raise AssertionError(
            f"startup policy misreported allocator stats: {policies[0]}"
        )
    for key in ("allocation_injection", "oom_abort_effective"):
        if fields.get(key) not in ("true", "false"):
            raise AssertionError(f"startup policy omitted {key}: {policies[0]}")
    expected_abort = "true" if allocator == "seastar" else "false"
    expected_capability = "native" if allocator == "seastar" else "unavailable"
    if (
        fields["oom_abort_effective"] != expected_abort
        or fields.get("oom_abort_requested") != expected_abort
        or fields.get("oom_abort_capability") != expected_capability
    ):
        raise AssertionError(f"allocator OOM policy is inconsistent: {policies[0]}")
    if output.count("configuration loaded ") != 1:
        raise AssertionError(f"configuration was not loaded exactly once:\n{output}")
    assert_ordered(
        output, ("startup policy ", "startup stage=data_directory state=ready")
    )
    return fields


def verify_memory_and_mount_policies(
    binary: Path,
    directory: Path,
    template: Path,
    environment: dict[str, str],
    allocator: str,
) -> None:
    contents = template.read_text(encoding="utf-8")
    budget_pattern = r"(?m)^(\s*diagnostic_memory_per_shard_bytes:)\s*\d+\s*$"
    diagnostic_bytes = 96 * 1024 * 1024
    selected, count = re.subn(budget_pattern, rf"\g<1> {diagnostic_bytes}", contents)
    if count != 1:
        raise AssertionError("development template must contain one diagnostic budget")
    diagnostic_template = directory / "explicit-diagnostic-template.yaml"
    diagnostic_template.write_text(selected, encoding="utf-8")
    if allocator == "system":
        missing_template = directory / "missing-diagnostic-template.yaml"
        missing_template.write_text(
            re.sub(budget_pattern, "", contents), encoding="utf-8"
        )
        missing_config = directory / "missing-diagnostic.yaml"
        write_config(
            missing_template,
            missing_config,
            directory / "missing-diagnostic-data",
            lease_loopback_endpoint(),
        )
        assert_startup_rejected(
            binary,
            directory,
            "--config",
            str(missing_config),
            expected=(
                "system-allocator diagnostic broker requires diagnostic_memory_per_shard_bytes",
            ),
            environment=environment,
            after_configuration=True,
        )

    broker, configuration, endpoint, output = start_broker(
        binary,
        directory,
        "explicit-diagnostic",
        diagnostic_template,
        directory / "explicit-diagnostic-data",
        environment=environment,
    )
    try:
        policy = verify_startup_policy(output, configuration, "development")
        expected_budget = (
            diagnostic_bytes if allocator == "system" else 192 * 1024 * 1024
        )
        if int(policy["class_budget_input_bytes"]) != expected_budget:
            raise AssertionError(
                f"diagnostic and native memory sources were confused:\n{output}"
            )
        expected_source = (
            "explicit_diagnostic_budget" if allocator == "system" else "allocator_stats"
        )
        if policy["class_budget_source"] != expected_source:
            raise AssertionError(f"incorrect workload budget source:\n{output}")
        for name, expected in (
            ("admin_memory_reservation_bytes", 4 * 1024 * 1024),
            ("reactor_headroom_bytes", 16 * 1024 * 1024),
            ("production_suitability_floor_bytes", 132 * 1024 * 1024),
        ):
            if int(policy[name]) != expected:
                raise AssertionError(f"startup reservation {name} changed:\n{output}")
        status, content_type, exposition = http_get(endpoint, "/metrics")
        samples = metric_samples(
            exposition, "kwaque_resource_manager_memory_configured_bytes"
        )
        expected_total = 2 * (expected_budget - 20 * 1024 * 1024)
        actual_total = sum(float(sample.rsplit(maxsplit=1)[1]) for sample in samples)
        if (status, content_type, len(samples), actual_total) != (
            200,
            "text/plain",
            WORKLOAD_COUNT,
            expected_total,
        ):
            raise AssertionError(
                f"workload budgets do not conserve their explicit reservations:\n{exposition}"
            )
        broker.stop(signal.SIGTERM)
    finally:
        broker.kill_if_running()

    strict, count = re.subn(
        r"(?m)^(\s*storage_strict_data_init:)\s*false\s*$", r"\1 true", contents
    )
    if count != 1:
        raise AssertionError(
            "development template must explicitly disable strict data initialization"
        )
    strict_template = directory / "strict-mount-template.yaml"
    strict_template.write_text(strict, encoding="utf-8")
    missing_config = directory / "missing-mount-marker.yaml"
    missing_data = directory / "missing-mount-data"
    write_config(
        strict_template, missing_config, missing_data, lease_loopback_endpoint()
    )
    assert_startup_rejected(
        binary,
        directory,
        "--config",
        str(missing_config),
        expected=("data directory mount marker is missing",),
        environment=environment,
        after_configuration=True,
        after_memory_observation=True,
        # A missing mount is an environment failure, not a configuration error.
        status=EXIT_FAILURE,
    )
    if missing_data.exists():
        raise AssertionError(
            "strict initialization created the directory being validated"
        )

    marked_data = directory / "marked-mount-data"
    marked_data.mkdir()
    marker = marked_data / ".kwaque_data_dir"
    marker_contents = b"fixture mount intent\n"
    marker.write_bytes(marker_contents)
    broker, configuration, _, output = start_broker(
        binary,
        directory,
        "marked-mount",
        strict_template,
        marked_data,
        environment=environment,
    )
    try:
        policy = verify_startup_policy(output, configuration, "development")
        if policy["storage_strict_data_init"] != "true":
            raise AssertionError(f"strict initialization was not recorded:\n{output}")
        broker.stop(signal.SIGTERM)
        if (
            marker.read_bytes() != marker_contents
            or (marked_data / "kwaque.pid").exists()
        ):
            raise AssertionError(
                "broker shutdown changed the mount marker or retained its PID file"
            )
    finally:
        broker.kill_if_running()


def verify_explicit_io_sources(
    binary: Path,
    directory: Path,
    templates: tuple[Path, Path],
    environment: dict[str, str],
    alternate_profile: str,
) -> None:
    personal_config = directory / "hostile-home" / ".config" / "seastar"
    personal_config.mkdir(parents=True)
    (personal_config / "seastar.conf").write_text(
        "unsafe-bypass-fsync=true\nkernel-page-cache=true\nrelaxed-dma=true\n",
        encoding="utf-8",
    )
    (personal_config / "io.conf").write_text(
        f"io-properties-file={directory / 'missing-personal-io.yaml'}\n"
        "unknown-runtime-option=true\n",
        encoding="utf-8",
    )
    hostile_environment = {**environment, "HOME": str(personal_config.parent.parent)}
    io_properties = directory / "io-properties.yaml"
    io_properties.write_text("disks: []\n", encoding="utf-8")
    for name, template, profile, arguments in (
        ("inline-io", templates[0], "development", ("--io-properties=disks: []",)),
        (
            "file-io",
            templates[1],
            alternate_profile,
            ("--io-properties-file", str(io_properties)),
        ),
    ):
        broker, config, _, output = start_broker(
            binary,
            directory,
            name,
            template,
            directory / f"{name}-data",
            *arguments,
            environment=hostile_environment,
        )
        try:
            verify_startup_policy(output, config, profile)
            output = broker.stop(signal.SIGTERM)
            if output.count("configuration loaded ") != 1:
                raise AssertionError(
                    f"configuration was reread after startup:\n{output}"
                )
        finally:
            broker.kill_if_running()


def verify_crash_loop_refusal(
    binary: Path, logs: Path, template: Path, environment: dict[str, str]
) -> None:
    """An unclean exit past crash_loop_limit refuses the next start with 11."""
    contents, count = re.subn(
        r"(?m)^(\s*crash_loop_limit:)\s*\d+\s*$",
        r"\g<1> 0",
        template.read_text(encoding="utf-8"),
    )
    if count != 1:
        raise AssertionError("production template must set crash_loop_limit")
    limited = logs / "crash-loop-template.yaml"
    limited.write_text(contents, encoding="utf-8")
    data_directory = logs / "crash-loop-data"
    broker, config, _, _ = start_broker(
        binary, logs, "crash-loop", limited, data_directory, environment=environment
    )
    # A killed broker never records a clean shutdown, so its start stays counted.
    broker.kill_if_running()
    tracker = data_directory / ".kwaque-crash-loop"
    if not tracker.is_file():
        raise AssertionError("an unclean exit removed the crash-loop tracker")
    recorded = tracker.read_bytes()

    refused = run_broker(
        binary, config, *REACTOR_ARGUMENTS, environment=environment, timeout=15.0
    )
    if refused.returncode != EXIT_CRASH_LOOP:
        raise AssertionError(
            f"crash-looping broker exited {refused.returncode}, expected "
            f"{EXIT_CRASH_LOOP}:\n{refused.stdout}"
        )
    assert_one_error(refused.stdout, "crash loop detected")
    if "startup stage=crash_tracking" in refused.stdout:
        raise AssertionError(f"refused start passed crash tracking:\n{refused.stdout}")
    if tracker.read_bytes() != recorded:
        raise AssertionError("a refused start changed the crash-loop tracker")


def wildcard_endpoint() -> Endpoint:
    # A wildcard listener conflicts with every loopback lease on its port, so
    # it takes an ephemeral port instead of the shared admin port.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("0.0.0.0", 0))
        return Endpoint("0.0.0.0", probe.getsockname()[1])


def verify_remote_admin_warning(
    binary: Path,
    directory: Path,
    template: Path,
    environment: dict[str, str],
) -> None:
    for attempt in range(1, 6):
        try:
            broker, config, _, output = start_broker(
                binary,
                directory,
                f"remote-admin-{attempt}",
                template,
                directory / f"remote-admin-data-{attempt}",
                environment=environment,
                endpoint=wildcard_endpoint(),
            )
            break
        except AssertionError as error:
            # Another process may take the probed port before the broker.
            if "Address already in use" not in str(error) or attempt == 5:
                raise
    try:
        verify_startup_policy(output, config, "development")
        if output.count(ADMIN_EXPOSURE_WARNING) != 1:
            raise AssertionError(f"remote admin did not report its exposure:\n{output}")
        assert_ordered(
            output,
            (ADMIN_EXPOSURE_WARNING, "startup stage=data_directory state=ready"),
        )
        broker.stop(signal.SIGTERM)
    finally:
        broker.kill_if_running()


def verify_logger_options(
    binary: Path,
    directory: Path,
    template: Path,
    environment: dict[str, str],
) -> None:
    """The runtime's log options reach the broker logger before startup."""
    broker, _, _, output = start_broker(
        binary,
        directory,
        "debug-logging",
        template,
        directory / "debug-logging-data",
        "--logger-log-level",
        "kwaque-broker=debug",
        environment=environment,
    )
    try:
        broker.stop(signal.SIGTERM)
    finally:
        broker.kill_if_running()

    endpoint = lease_loopback_endpoint()
    config = directory / "quiet-logging.yaml"
    write_config(template, config, directory / "quiet-logging-data", endpoint)
    quiet = BrokerProcess(
        binary,
        config,
        log_path(directory, "quiet-logging.log"),
        REACTOR_ARGUMENTS,
        arguments=("--logger-log-level", "kwaque-broker=error"),
        environment=environment,
    )
    try:
        quiet.wait_until_ready(endpoint)
        output = quiet.stop(signal.SIGTERM)
    finally:
        quiet.kill_if_running()
    for suppressed in ("startup stage=", "configuration loaded", "shutdown complete"):
        if suppressed in output:
            raise AssertionError(
                f"kwaque-broker=error still logged {suppressed!r}:\n{output}"
            )


def main() -> None:
    binary = Path(sys.argv[1]).resolve()
    default_template = Path(sys.argv[2]).resolve()
    alternate_template = Path(sys.argv[3]).resolve()

    with test_directory() as directory:
        logs = Path(directory)
        home = logs / "home"
        home.mkdir()
        environment = {**os.environ, "HOME": str(home)}
        verify_configuration_rejections(binary, logs, environment)
        verify_runtime_rejections(
            binary, logs, (default_template, alternate_template), environment
        )
        default, default_config, default_endpoint, output = start_broker(
            binary,
            logs,
            "default",
            default_template,
            logs / "default-data",
            "--unsafe-bypass-fsync=false",
            "--kernel-page-cache=false",
            environment=environment,
        )
        try:
            policy = verify_startup_policy(output, default_config, "development")
            if ADMIN_EXPOSURE_WARNING in output:
                raise AssertionError(
                    f"loopback admin reported remote exposure:\n{output}"
                )
            for expected in (
                f"configuration loaded path={default_config}",
                f"data_directory={logs / 'default-data'}",
                "build version=",
                "host kernel=",
                "runtime shards=2",
                "minimum_shard_memory_bytes=",
                "reactor_backend=",
                "runtime environment ready shard=0",
                "runtime environment ready shard=1",
            ):
                if expected not in output:
                    raise AssertionError(
                        f"missing startup field {expected!r}:\n{output}"
                    )
            assert_ordered(
                output,
                (
                    "startup stage=data_directory state=ready",
                    "startup stage=pid_file state=ready",
                    "startup stage=resource_registry state=ready",
                    "startup stage=runtime_environment state=ready",
                    "startup stage=admin state=ready",
                ),
            )

            status, content_type, body = http_get(default_endpoint, "/v1/health/live")
            if (status, content_type, json.loads(body)) != (
                200,
                "application/json",
                {"status": "live"},
            ):
                raise AssertionError(f"unexpected liveness response: {status} {body}")

            status, content_type, body = http_get(default_endpoint, "/v1/health/ready")
            if (status, content_type, json.loads(body)) != (
                200,
                "application/json",
                {"status": "ready"},
            ):
                raise AssertionError(f"unexpected readiness response: {status} {body}")

            _, _, body = http_get(default_endpoint, "/v1/version")
            version = json.loads(body)
            expected_version = version_fields(binary)
            for field in ("version", "revision", "build_timestamp", "build_mode"):
                if version.get(field) != expected_version[field]:
                    raise AssertionError(
                        f"version endpoint field {field!r} did not match CLI: {body}"
                    )
            if version.get("dirty") != (expected_version["dirty"] == "true"):
                raise AssertionError(f"version endpoint hid the dirty state: {body}")

            status, content_type, metrics = http_get(default_endpoint, "/metrics")
            if status != 200 or content_type != "text/plain":
                raise AssertionError(
                    f"unexpected metrics response: {status} {content_type}"
                )
            verify_product_metrics(metrics, aggregated=True)
            metric_prefix = "kwaque_broker_"
            if metric_value(metrics, metric_prefix + "process_readiness") != 1:
                raise AssertionError("readiness metric was not set")
            if metric_value(metrics, metric_prefix + "shards") != 2:
                raise AssertionError("shard metric did not match --smp")
            if metric_value(metrics, metric_prefix + "startup_duration_seconds") < 0:
                raise AssertionError("startup duration metric was negative")
            if metric_value(metrics, metric_prefix + "draining") != 0:
                raise AssertionError("draining was reported before shutdown")
            if metric_value(metrics, metric_prefix + "start_time_seconds") <= 0:
                raise AssertionError("start time metric was not set")
            if metric_value(metrics, "kwaque_build_info") != 1:
                raise AssertionError("build information metric must be 1")

            status, content_type, unaggregated = http_get(
                default_endpoint, "/metrics?__aggregate__=false"
            )
            if status != 200 or content_type != "text/plain":
                raise AssertionError(
                    f"unexpected unaggregated metrics response: {status} {content_type}"
                )
            verify_product_metrics(unaggregated, aggregated=False)

            incomplete_request = socket.create_connection(
                (default_endpoint.address, default_endpoint.port), timeout=5.0
            )
            incomplete_request.sendall(
                b"GET /v1/health/live HTTP/1.1\r\nHost: localhost\r\n"
            )
            output = default.stop(signal.SIGTERM)
            incomplete_request.close()
            assert_ordered(output, ("shutdown requested", "shutdown complete"))
        finally:
            default.kill_if_running()

        verify_memory_and_mount_policies(
            binary, logs, default_template, environment, policy["allocator"]
        )
        alternate_profile = "production"
        if policy["allocator"] == "system":
            rejected = logs / "production-system-allocator.yaml"
            write_config(
                alternate_template,
                rejected,
                logs / "production-system-data",
                lease_loopback_endpoint(),
            )
            assert_startup_rejected(
                binary,
                logs,
                "--config",
                str(rejected),
                expected=("production broker requires the native Seastar allocator",),
                environment=environment,
                after_configuration=True,
            )
            contents, count = re.subn(
                r"(?m)^(\s*developer_mode:)\s*false\s*$",
                r"\1 true",
                alternate_template.read_text(encoding="utf-8"),
            )
            if count != 1:
                raise AssertionError(
                    "alternate template must explicitly select production"
                )
            alternate_template = logs / "alternate-development-template.yaml"
            alternate_template.write_text(
                contents + "\n  diagnostic_memory_per_shard_bytes: 134217728\n",
                encoding="utf-8",
            )
            alternate_profile = "development"

        alternate, alternate_config, alternate_endpoint, output = start_broker(
            binary,
            logs,
            "alternate",
            alternate_template,
            logs / "alternate-data",
            "--unsafe-bypass-fsync",
            "false",
            "--kernel-page-cache",
            "false",
            environment=environment,
        )
        try:
            verify_startup_policy(output, alternate_config, alternate_profile)
            if ADMIN_EXPOSURE_WARNING in output:
                raise AssertionError(
                    f"loopback admin reported remote exposure:\n{output}"
                )
            expected_path = f"configuration loaded path={alternate_config}"
            for expected in (
                expected_path,
                f"admin_port={alternate_endpoint.port}",
                f"developer_mode={'true' if alternate_profile == 'development' else 'false'}",
            ):
                if expected not in output:
                    raise AssertionError(
                        f"alternate configuration missing {expected!r}:\n{output}"
                    )

            crash_directory = logs / "alternate-data" / "crash_reports"
            tracker = logs / "alternate-data" / ".kwaque-crash-loop"
            before_reports = {
                path.name: path.read_bytes() for path in crash_directory.iterdir()
            }
            before_tracker = tracker.read_bytes() if tracker.exists() else None

            contender = run_broker(
                binary,
                alternate_config,
                *REACTOR_ARGUMENTS,
                environment=environment,
                timeout=15.0,
            )
            if contender.returncode != EXIT_DATA_DIRECTORY_IN_USE:
                raise AssertionError(
                    f"second broker exited {contender.returncode}, expected "
                    f"{EXIT_DATA_DIRECTORY_IN_USE}:\n{contender.stdout}"
                )
            assert_one_error(contender.stdout, "PID file is already locked")

            after_reports = {
                path.name: path.read_bytes() for path in crash_directory.iterdir()
            }
            after_tracker = tracker.read_bytes() if tracker.exists() else None
            if before_reports != after_reports or before_tracker != after_tracker:
                raise AssertionError("PID-lock contender changed crash bookkeeping")

            output = alternate.stop(signal.SIGTERM)
            if "shutdown complete" not in output:
                raise AssertionError(f"alternate broker did not shut down:\n{output}")
            if tracker.exists() or any(crash_directory.iterdir()):
                raise AssertionError("clean shutdown retained crash bookkeeping")
        finally:
            alternate.kill_if_running()

        if alternate_profile == "production":
            verify_crash_loop_refusal(binary, logs, alternate_template, environment)

        verify_explicit_io_sources(
            binary,
            logs,
            (default_template, alternate_template),
            environment,
            alternate_profile,
        )
        verify_remote_admin_warning(binary, logs, default_template, environment)
        verify_logger_options(binary, logs, default_template, environment)


if __name__ == "__main__":
    main()
