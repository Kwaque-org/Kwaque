"""Qualify one shard's storage on a real file system the harness builds.

The harness makes its own devices: loop devices over files it creates in the
work directory, device-mapper targets stacked on them, a file system made on
top. It never touches a device or a path it did not create, and it must run
as root. What a run was told lives in a record in the work directory, which
is on another file system than the one under test.

Three kinds of evidence, each for what it shows and no more:

  kill    The store's process is killed while it runs. Shows that a start
          recovers what a process death leaves, with every completed write
          present. Shows nothing about a flush: a kill loses no write the
          kernel already has.

  replay  A log of every write, flush and discard is kept under the file
          system, with a mark each time the store reports something durable.
          The log is replayed to a mark, the file system is mounted, the
          store is started and everything reported before the mark is read
          back; the same for flushes between two marks. Only writes a
          completed flush covered lie before a mark, so a report that no
          flush backed shows as a loss. Shows nothing about a torn sector, a
          subset of unflushed writes that is not a prefix, or the loss of a
          flushed write.

  fault   The device under the file system fails writes, fails everything,
          stalls, fills up or returns a block it cannot read, while the
          store runs. Shows what the store reports for each cause on this
          file system, and that what it reported durable before the fault
          is read back afterwards. Shows nothing about loss.

None of them is a power cut. A power cut needs a device that loses power.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import platform
import random
import re
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

MIB = 1024 * 1024
SECTOR = 512
MARK_PREFIX = "kwaque."
VERDICT_PREFIX = "kwaque-durability-verdict "
PROFILE_PREFIX = "kwaque-durability-profile "
# The probe's exit codes: its verdict holds; it does not; the run met a
# failure it reports. Anything else is a crash.
PROBE_HOLDS = 0
PROBE_VERDICT = 2
PROBE_FAILED = 3
QUEUE_FIELDS = (
    "write_cache",
    "fua",
    "scheduler",
    "nr_requests",
    "nomerges",
    "logical_block_size",
    "physical_block_size",
    "minimum_io_size",
    "rotational",
    "discard_granularity",
)
FAULTS = ("failed-writes", "gone", "stalled", "full", "unreadable")


class HarnessError(RuntimeError):
    pass


@dataclasses.dataclass(frozen=True)
class Filesystem:
    make: tuple[str, ...]
    check: tuple[str, ...]
    minimum_bytes: int


FILESYSTEMS = {
    "ext4": Filesystem(
        make=("mkfs.ext4", "-F", "-q"),
        check=("e2fsck", "-f", "-n"),
        minimum_bytes=64 * MIB,
    ),
    "xfs": Filesystem(
        make=("mkfs.xfs", "-f", "-q"),
        check=("xfs_repair", "-n"),
        minimum_bytes=300 * MIB,
    ),
}


@dataclasses.dataclass(frozen=True)
class Completed:
    returncode: int
    output: str


class Runner:
    """Runs the system's tools. A test substitutes its own."""

    def run(
        self,
        argv: Sequence[str],
        *,
        check: bool = True,
        timeout: float = 300.0,
    ) -> Completed:
        try:
            done = subprocess.run(
                list(argv),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=timeout,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise HarnessError(f"{argv[0]} did not run: {error}") from error
        if check and done.returncode != 0:
            raise HarnessError(
                f"{' '.join(argv)} exited {done.returncode}: {done.stdout.strip()}"
            )
        return Completed(done.returncode, done.stdout)


# Device-mapper tables. Every length is in 512-byte sectors.


def log_writes_table(sectors: int, device: str, log: str) -> str:
    return f"0 {sectors} log-writes {device} {log}"


def linear_table(sectors: int, device: str) -> str:
    return f"0 {sectors} linear {device} 0"


def error_table(sectors: int, device: str) -> str:
    return f"0 {sectors} error {device} 0"


def failed_writes_table(sectors: int, device: str) -> str:
    # Never up, always down, and a write that arrives while down is failed.
    return f"0 {sectors} flakey {device} 0 0 180 1 error_writes"


def delay_table(sectors: int, device: str, flush_ms: int) -> str:
    # Reads and writes are passed on at once; a flush waits.
    return f"0 {sectors} delay {device} 0 0 {device} 0 0 {device} 0 {flush_ms}"


def dust_table(sectors: int, device: str) -> str:
    return f"0 {sectors} dust {device} 0 {SECTOR}"


def parse_found(text: str) -> int:
    """The number of the entry the replay tool found. One build of the tool
    prints it with the log sector it lies at, `N@S`; another prints it
    alone."""
    match = re.fullmatch(r"(\d+)(?:@\d+)?", text.strip())
    if not match:
        raise HarnessError(f"the replay tool did not name an entry: {text!r}")
    return int(match.group(1))


def needed(
    kind: str, filesystem: str, cause: str | None
) -> tuple[list[str], list[str]]:
    """The tools and the device-mapper targets one kind of run takes."""
    made = FILESYSTEMS[filesystem]
    tools = [
        "losetup",
        "blockdev",
        "mount",
        "umount",
        "df",
        made.make[0],
        made.check[0],
    ]
    targets: list[str] = []
    if kind != "kill":
        tools += ["dmsetup", "udevadm"]
    if kind == "replay":
        tools += ["replay-log", "blkdiscard"]
        targets.append("log-writes")
    if kind == "fault":
        targets.append("dust" if cause == "unreadable" else "linear")
        targets += {
            "failed-writes": ["flakey"],
            "gone": ["error"],
            "stalled": ["delay"],
        }.get(cause or "", [])
        if cause == "unreadable":
            tools.append("filefrag")
        if cause == "full":
            tools.append("fallocate")
    return tools, targets


def missing(
    runner: Runner,
    tools: Sequence[str],
    targets: Sequence[str],
    which: Callable[[str], str | None] = shutil.which,
) -> list[str]:
    """What this machine lacks of them. The linear and error targets are part
    of device-mapper itself; every other target is a module of its own."""
    lacking = [tool for tool in tools if which(tool) is None]
    for target in targets:
        if target in ("linear", "error"):
            continue
        module = "dm_" + target.replace("-", "_")
        if runner.run(["modinfo", module], check=False).returncode != 0:
            lacking.append(f"the {target} target (module {module})")
    return lacking


def marks_in(lines: Sequence[str]) -> list[int]:
    """The marks a record holds, in the order they were taken."""
    marks = []
    for line in lines:
        match = re.fullmatch(r"mark (\d+)", line)
        if match:
            marks.append(int(match.group(1)))
    if marks != list(range(1, len(marks) + 1)):
        raise HarnessError("a record's marks are not 1, 2, 3 and so on")
    return marks


def sample(items: Sequence[Any], count: int) -> list[Any]:
    """At most `count` of the items, evenly apart, the last among them."""
    if count <= 0 or not items:
        return []
    if len(items) <= count:
        return list(items)
    step = len(items) / count
    chosen = [items[min(len(items) - 1, int((i + 1) * step) - 1)] for i in range(count)]
    return sorted(set(chosen), key=chosen.index)


def first_physical_sector(filefrag: str) -> int:
    """Where a file's first extent begins on its device, from the extent
    listing in 512-byte units."""
    for line in filefrag.splitlines():
        match = re.match(r"\s*0:\s*\d+\.\.\s*\d+:\s*(\d+)\.\.\s*\d+:", line)
        if match:
            return int(match.group(1))
    raise HarnessError("the extent listing names no first extent")


def scheduler_of(text: str) -> str:
    """The scheduler in use: the bracketed one, or the only one."""
    match = re.search(r"\[([^\]]+)\]", text)
    return match.group(1) if match else text.strip()


def device_layers(sysfs: Path, device: str, *, depth: int = 0) -> list[dict[str, Any]]:
    """Every layer of the stack under a block device, from the top down: a
    mapped device and what it maps, a partition and its disk, a loop device
    and the device that holds its file. `device` is major:minor."""
    if depth > 8:
        raise HarnessError("the device stack is deeper than any real one")
    try:
        entry = (sysfs / "dev" / "block" / device).resolve(strict=True)
    except OSError as error:
        raise HarnessError(f"no block device {device}") from error
    layer: dict[str, Any] = {"device": device, "name": entry.name}
    if (entry / "partition").exists():
        layer["partition_of"] = entry.parent.name
        below = (entry.parent / "dev").read_text().strip()
        return [layer, *device_layers(sysfs, below, depth=depth + 1)]
    for field in QUEUE_FIELDS:
        path = entry / "queue" / field
        if path.exists():
            value = path.read_text().strip()
            layer[field] = scheduler_of(value) if field == "scheduler" else value
    for extra in ("dm/name", "loop/backing_file"):
        path = entry / extra
        if path.exists():
            layer[extra] = path.read_text().strip()
    layers = [layer]
    slaves = entry / "slaves"
    if slaves.is_dir():
        for slave in sorted(slaves.iterdir()):
            below = (slave / "dev").read_text().strip()
            layers += device_layers(sysfs, below, depth=depth + 1)
    backing = layer.get("loop/backing_file")
    if backing:
        try:
            held = os.stat(backing).st_dev
        except OSError:
            held = 0
        if held:
            below = f"{os.major(held)}:{os.minor(held)}"
            if (sysfs / "dev" / "block" / below).exists():
                layers += device_layers(sysfs, below, depth=depth + 1)
            else:
                layers.append({"device": below, "name": "(no block device)"})
    return layers


def mount_entry(mountinfo: str, mount_point: str) -> dict[str, str]:
    """A mount as the kernel lists it: type, source and both option lists."""
    for line in mountinfo.splitlines():
        fields = line.split(" ")
        if len(fields) < 10 or fields[4] != mount_point or "-" not in fields:
            continue
        rest = fields[fields.index("-") + 1 :]
        if len(rest) < 3:
            continue
        return {
            "device": fields[2],
            "type": rest[0],
            "source": rest[1],
            "mount_options": fields[5],
            "super_options": rest[2],
        }
    raise HarnessError(f"{mount_point} is not mounted")


class Stack:
    """The devices and mounts of one run. Everything it makes it unmakes, the
    last made first, and it makes nothing over a path that exists."""

    def __init__(self, runner: Runner, work: Path, tag: str) -> None:
        self.runner = runner
        self.work = work
        self.tag = tag
        self._undo: list[tuple[str, Callable[[], None]]] = []

    def image(self, label: str, size: int) -> Path:
        path = self.work / f"{label}.img"
        # Exclusive: an image that is there is not this run's.
        with open(path, "xb") as made:
            made.truncate(size)
        self._undo.append((f"image {label}", path.unlink))
        return path

    def loop(self, image: Path) -> str:
        device = self.runner.run(
            ["losetup", "--find", "--show", str(image)]
        ).output.strip()
        if not device.startswith("/dev/loop"):
            raise HarnessError(f"losetup named no loop device: {device!r}")
        self._undo.append(
            (f"loop {device}", lambda: self._quiet(["losetup", "--detach", device]))
        )
        return device

    def sectors(self, device: str) -> int:
        return int(self.runner.run(["blockdev", "--getsz", device]).output.strip())

    def mapped(self, label: str, table: str) -> tuple[str, str]:
        name = f"kwaque-{self.tag}-{label}"
        self.runner.run(["dmsetup", "create", name, "--table", table])
        self.runner.run(["udevadm", "settle"], check=False)
        self._undo.append((f"map {name}", lambda: self.unmap(name)))
        return name, f"/dev/mapper/{name}"

    def unmap(self, name: str) -> None:
        self._forget(f"map {name}")
        self.runner.run(["dmsetup", "remove", "--retry", name])
        self.runner.run(["udevadm", "settle"], check=False)

    def reload(self, name: str, table: str) -> None:
        # The file system is not frozen first: what it has in flight meets
        # the new table as it would meet a device that changed under it.
        self.runner.run(["dmsetup", "suspend", "--nolockfs", name])
        try:
            self.runner.run(["dmsetup", "load", name, "--table", table])
        finally:
            self.runner.run(["dmsetup", "resume", name])

    def mount(self, device: str, kind: str, options: str = "") -> Path:
        point = self.work / "mnt"
        point.mkdir(exist_ok=True)
        argv = ["mount", "-t", kind]
        if options:
            argv += ["-o", options]
        self.runner.run([*argv, device, str(point)])
        self._undo.append(("mount", lambda: self._quiet(["umount", str(point)])))
        return point

    def unmount(self) -> None:
        self._forget("mount")
        self.runner.run(["umount", str(self.work / "mnt")])

    def close(self) -> None:
        while self._undo:
            _, undo = self._undo.pop()
            try:
                undo()
            except (HarnessError, OSError) as error:
                print(f"cleanup: {error}", file=sys.stderr)

    def _forget(self, label: str) -> None:
        self._undo = [one for one in self._undo if one[0] != label]

    def _quiet(self, argv: Sequence[str]) -> None:
        self.runner.run(argv, check=False)


@dataclasses.dataclass(frozen=True)
class Probed:
    returncode: int
    verdict: str
    output: str


def read_verdict(output: str) -> str:
    verdicts = [
        line[len(VERDICT_PREFIX) :].strip()
        for line in output.splitlines()
        if line.startswith(VERDICT_PREFIX)
    ]
    return verdicts[-1] if verdicts else ""


class Probe:
    """The native binary that runs a history over a store or checks a store
    against a record."""

    def __init__(self, binary: Path, backend: str, memory: str) -> None:
        self.binary = binary
        self.native = [
            f"--reactor-backend={backend}",
            # A reactor asks the kernel for ten thousand control blocks of
            # one allowance the host shares unless it is told otherwise.
            "--max-networking-io-control-blocks=1000",
            "--smp=1",
            f"--memory={memory}",
            "--overprovisioned",
        ]

    def run_arguments(
        self, root: Path, record: Path | None, history: str, mark: str = ""
    ) -> list[str]:
        argv = [
            str(self.binary),
            "--mode=run",
            f"--root={root}",
            f"--history={history}",
        ]
        if record is not None:
            argv.append(f"--record={record}")
        if mark:
            argv.append(f"--mark-command={mark}")
        return argv + self.native

    def check_arguments(
        self, root: Path, record: Path, answered: int = 0, asked: int = 0
    ) -> list[str]:
        argv = [
            str(self.binary),
            "--mode=check",
            f"--root={root}",
            f"--record={record}",
        ]
        if answered:
            argv += [f"--answered={answered}", f"--asked={asked or answered}"]
        return argv + self.native

    def start(self, argv: Sequence[str], log: Path) -> subprocess.Popen[bytes]:
        with log.open("wb") as sink:
            return subprocess.Popen(list(argv), stdout=sink, stderr=subprocess.STDOUT)

    def finish(
        self, process: subprocess.Popen[bytes], log: Path, timeout: float
    ) -> Probed:
        try:
            returncode = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise HarnessError("the store did not finish: a hang") from None
        output = log.read_text(errors="replace")
        return Probed(returncode, read_verdict(output), output)

    def call(self, argv: Sequence[str], log: Path, timeout: float = 600.0) -> Probed:
        return self.finish(self.start(argv, log), log, timeout)


def record_lines(record: Path) -> list[str]:
    if not record.exists():
        return []
    return record.read_text().splitlines()


def wait_for_marks(
    record: Path,
    process: subprocess.Popen[bytes],
    wanted: int,
    timeout: float,
) -> int:
    """Waits until the record holds `wanted` marks or the store's process has
    ended; the marks it holds then."""
    deadline = time.monotonic() + timeout
    while True:
        held = sum(1 for line in record_lines(record) if line.startswith("mark "))
        if held >= wanted or process.poll() is not None:
            return held
        if time.monotonic() > deadline:
            raise HarnessError("the store took no mark in time: a hang")
        time.sleep(0.002)


def profile_of(output: str) -> dict[str, str]:
    """What the store said of itself: one line of key=value words."""
    lines = [
        line[len(PROFILE_PREFIX) :]
        for line in output.splitlines()
        if line.startswith(PROFILE_PREFIX)
    ]
    if not lines:
        return {}
    return dict(word.split("=", 1) for word in lines[-1].split() if "=" in word)


def require_holds(what: str, probed: Probed) -> None:
    if probed.returncode != PROBE_HOLDS or probed.verdict != "holds":
        raise HarnessError(
            f"{what}: exit {probed.returncode}, verdict {probed.verdict!r}\n"
            f"{probed.output[-2000:]}"
        )


class Qualification:
    def __init__(self, arguments: argparse.Namespace, runner: Runner) -> None:
        self.arguments = arguments
        self.runner = runner
        self.kind = FILESYSTEMS[arguments.filesystem]
        self.work = Path(arguments.work)
        self.probe = Probe(Path(arguments.probe), arguments.reactor_backend, "512MiB")
        self.random = random.Random(arguments.seed)
        self.stack = Stack(runner, self.work, str(os.getpid()))
        self.logs = 0
        self.result: dict[str, Any] = {
            "kind": arguments.kind,
            "filesystem": arguments.filesystem,
            "seed": arguments.seed,
            "cases": [],
        }

    def log(self) -> Path:
        self.logs += 1
        return self.work / f"probe-{self.logs}.log"

    def make(self, device: str) -> list[str]:
        argv = [*self.kind.make, *self.arguments.mkfs_option, device]
        self.runner.run(argv)
        return argv

    def checked(self, device: str) -> None:
        self.runner.run([*self.kind.check, device])

    def environment(self, device: str, point: Path, made: Sequence[str]) -> None:
        """What the run stood on, read while it is mounted."""
        status = os.stat(point)
        sysfs = Path("/sys")
        self.result["environment"] = {
            "kernel": platform.release(),
            "command_line": Path("/proc/cmdline").read_text().strip(),
            "mkfs": list(made),
            "mount": mount_entry(Path("/proc/self/mountinfo").read_text(), str(point)),
            "stack": device_layers(
                sysfs, f"{os.major(status.st_dev)}:{os.minor(status.st_dev)}"
            ),
            "mounted_from": device,
            "space": self.runner.run(
                ["df", "--output=size,used,avail", "-B1", str(point)]
            ).output.split()[-3:],
            "discarded_first": True,
        }

    def profile(self, probed: Probed) -> None:
        said = profile_of(probed.output)
        if said:
            self.result["store"] = said
        # An appending write that blocks its submitter is a file system this
        # store is not qualified on, whatever else holds.
        if said.get("appending_io") == "bad":
            raise HarnessError(
                "an appending write blocks in the kernel on this file system"
            )

    def fill(self, point: Path) -> list[Path]:
        """Takes the file system's free space, down to less than a block."""
        taken: list[Path] = []
        chunk = 64 * MIB
        while chunk >= 4096:
            free = os.statvfs(point)
            path = point / f"ballast-{len(taken)}"
            if free.f_bavail * free.f_frsize < chunk or (
                self.runner.run(
                    ["fallocate", "-l", str(chunk), str(path)], check=False
                ).returncode
                != 0
            ):
                path.unlink(missing_ok=True)
                chunk //= 2
                continue
            taken.append(path)
        return taken

    # A process death, at a seeded count of marks, as many times as asked.
    def kill(self) -> None:
        size = max(self.arguments.size_mib * MIB, self.kind.minimum_bytes)
        device = self.stack.loop(self.stack.image("data", size))
        made = self.make(device)
        point = self.stack.mount(
            device, self.arguments.filesystem, self.arguments.mount_options
        )
        self.environment(device, point, made)
        # Once to its end first: the history runs here at all, and the store
        # says what it ran on and how long the kernel held it.
        whole = self.probe.call(
            # No record: every kernel stall it counts is the store's own.
            self.probe.run_arguments(point / "store-whole", None, "long"),
            self.log(),
            self.arguments.timeout,
        )
        require_holds("the run that is not killed", whole)
        self.profile(whole)
        for round_ in range(self.arguments.rounds):
            root = point / f"store-{round_}"
            record = self.work / f"record-{round_}"
            log = self.log()
            process = self.probe.start(
                self.probe.run_arguments(root, record, "long"), log
            )
            wanted = self.random.randint(1, self.arguments.marks)
            held = wait_for_marks(record, process, wanted, self.arguments.timeout)
            ended = process.poll()
            if ended is None:
                process.send_signal(signal.SIGKILL)
                process.wait()
            elif ended != PROBE_HOLDS:
                raise HarnessError(
                    f"the store ended by itself with {ended}:\n"
                    f"{log.read_text(errors='replace')[-2000:]}"
                )
            checked = self.probe.call(
                self.probe.check_arguments(root, record), self.log()
            )
            require_holds(f"after a kill at mark {held}", checked)
            self.result["cases"].append(
                {"killed_at_mark": held, "finished_first": ended is not None}
            )
        self.stack.unmount()
        self.checked(device)

    # The log under the file system, replayed to marks and between them.
    def replay(self) -> None:
        size = max(self.arguments.size_mib * MIB, self.kind.minimum_bytes)
        data = self.stack.loop(self.stack.image("data", size))
        log_device = self.stack.loop(self.stack.image("log", 4 * size))
        name, mapped = self.stack.mapped(
            "log", log_writes_table(self.stack.sectors(data), data, log_device)
        )
        mark = f"dmsetup message {name} 0 mark"
        made = self.make(mapped)
        self.runner.run([*mark.split(), "mkfs"])
        point = self.stack.mount(
            mapped, self.arguments.filesystem, self.arguments.mount_options
        )
        self.environment(mapped, point, made)
        root = point / "store"
        record = self.work / "record"
        ran = self.probe.call(
            self.probe.run_arguments(root, record, "life", mark),
            self.log(),
            self.arguments.timeout,
        )
        require_holds("the run under the log", ran)
        self.profile(ran)
        self.runner.run([*mark.split(), "last"])
        self.stack.unmount()
        self.runner.run([*mark.split(), "end"])
        # A log that ran out of room stops logging and says so only here.
        status = self.runner.run(["dmsetup", "status", name]).output
        if "logging_disabled" in status:
            raise HarnessError(
                f"the log stopped before the run ended: {status.strip()}"
            )
        # The log's own count of entries is complete once its target is gone.
        self.stack.unmap(name)

        marks = marks_in(record_lines(record))
        if not marks:
            raise HarnessError("the run took no mark")
        find = ["replay-log", "--log", log_device, "--find"]
        entries = {
            taken: parse_found(
                self.runner.run([*find, "--end-mark", f"{MARK_PREFIX}{taken}"]).output
            )
            for taken in marks
        }

        def restored(
            replay: Sequence[str], answered: int, asked: int, what: str
        ) -> None:
            # Emptied first: a replay to an earlier point must not find what
            # a later one left.
            self.runner.run(["blkdiscard", "-f", data])
            self.runner.run(
                ["replay-log", "--log", log_device, "--replay", data, *replay]
            )
            again = self.stack.mount(
                data, self.arguments.filesystem, self.arguments.mount_options
            )
            checked = self.probe.call(
                self.probe.check_arguments(again / "store", record, answered, asked),
                self.log(),
            )
            self.stack.unmount()
            require_holds(what, checked)
            self.checked(data)

        for taken in sample(marks, self.arguments.marks):
            restored(
                ["--end-mark", f"{MARK_PREFIX}{taken}"],
                taken,
                taken,
                f"replayed to mark {taken}",
            )
            self.result["cases"].append({"mark": taken, "entry": entries[taken]})
        # Between two marks: every flush and every FUA write the log holds
        # there is a point a device could have stopped at.
        between = []
        for taken in marks[:-1]:
            at = entries[taken] + 1
            while True:
                found = self.runner.run(
                    [*find, "--next-flush", "--next-fua", "--start-entry", str(at)],
                    check=False,
                )
                if found.returncode != 0:
                    break
                entry = parse_found(found.output)
                if entry >= entries[taken + 1]:
                    break
                between.append((taken, entry))
                at = entry + 1
        for taken, entry in sample(between, self.arguments.between):
            restored(
                ["--limit", str(entry + 1)],
                taken,
                taken + 1,
                f"replayed to entry {entry} after mark {taken}",
            )
            self.result["cases"].append({"after_mark": taken, "entry": entry})
        self.result["marks"] = len(marks)
        self.result["points_between_marks"] = len(between)
        restored(
            ["--end-mark", "last"], marks[-1], marks[-1], "replayed to the unmount"
        )
        restored(["--end-mark", "end"], marks[-1], marks[-1], "replayed to the end")

    # One cause under the file system while the store runs.
    def fault(self) -> None:
        cause = self.arguments.cause
        size = max(self.arguments.size_mib * MIB, self.kind.minimum_bytes)
        data = self.stack.loop(self.stack.image("data", size))
        sectors = self.stack.sectors(data)
        healthy = (
            dust_table(sectors, data)
            if cause == "unreadable"
            else linear_table(sectors, data)
        )
        name, mapped = self.stack.mapped("fault", healthy)
        made = self.make(mapped)
        point = self.stack.mount(
            mapped, self.arguments.filesystem, self.arguments.mount_options
        )
        self.environment(mapped, point, made)
        root = point / "store"
        record = self.work / "record"
        case: dict[str, Any] = {"cause": cause}

        def remounted() -> Path:
            self.stack.unmount()
            return self.stack.mount(
                mapped, self.arguments.filesystem, self.arguments.mount_options
            )

        if cause == "unreadable":
            ran = self.probe.call(
                self.probe.run_arguments(root, record, "life"),
                self.log(),
                self.arguments.timeout,
            )
            require_holds("the run before the fault", ran)
            listing = self.runner.run(
                ["filefrag", "-b512", "-v", str(self.first_data_file(root))]
            ).output
            # The segment's header fills its first 8 KiB; its first block
            # follows.
            bad = first_physical_sector(listing) + 8192 // SECTOR
            self.stack.unmount()
            self.runner.run(["dmsetup", "message", name, "0", "addbadblock", str(bad)])
            self.runner.run(["dmsetup", "message", name, "0", "enable"])
            again = self.stack.mount(
                mapped, self.arguments.filesystem, self.arguments.mount_options
            )
            failing = self.probe.call(
                self.probe.check_arguments(again / "store", record), self.log()
            )
            case["with_the_block_unreadable"] = {
                "exit": failing.returncode,
                "verdict": failing.verdict,
            }
            if failing.returncode not in (PROBE_HOLDS, PROBE_VERDICT, PROBE_FAILED):
                raise HarnessError(
                    f"an unreadable block crashed the store:\n{failing.output[-2000:]}"
                )
            self.stack.unmount()
            self.runner.run(["dmsetup", "message", name, "0", "disable"])
            again = self.stack.mount(
                mapped, self.arguments.filesystem, self.arguments.mount_options
            )
            require_holds(
                "once the block reads again",
                self.probe.call(
                    self.probe.check_arguments(again / "store", record), self.log()
                ),
            )
            self.result["cases"].append(case)
            return

        log = self.log()
        process = self.probe.start(self.probe.run_arguments(root, record, "long"), log)
        wanted = self.random.randint(1, self.arguments.marks)
        case["injected_at_mark"] = wait_for_marks(
            record, process, wanted, self.arguments.timeout
        )
        ballast: list[Path] = []
        if cause == "failed-writes":
            self.stack.reload(name, failed_writes_table(sectors, data))
        elif cause == "gone":
            self.stack.reload(name, error_table(sectors, data))
        elif cause == "stalled":
            self.stack.reload(name, delay_table(sectors, data, self.arguments.stall_ms))
            time.sleep(3 * self.arguments.stall_ms / 1000.0)
        else:
            ballast = self.fill(point)
        if cause == "stalled":
            self.stack.reload(name, healthy)
        ran = self.probe.finish(process, log, self.arguments.timeout)
        case["run"] = {"exit": ran.returncode, "verdict": ran.verdict}
        # A run that ended whole never met the cause.
        case["met"] = ran.returncode == PROBE_FAILED or cause == "stalled"
        failure = [
            line for line in ran.output.splitlines() if line.startswith("failed: ")
        ]
        case["reported"] = failure[-1] if failure else ""
        if ran.returncode not in (PROBE_HOLDS, PROBE_FAILED):
            raise HarnessError(
                f"the store did not end with a result it reports:\n{ran.output[-2000:]}"
            )
        if cause == "stalled":
            require_holds("a stalled flush", ran)
        # The device is whole again before the store is started on it.
        if cause in ("failed-writes", "gone"):
            self.stack.reload(name, healthy)
        for path in ballast:
            path.unlink(missing_ok=True)
        again = remounted()
        checked = self.probe.call(
            self.probe.check_arguments(again / "store", record), self.log()
        )
        require_holds(f"after {cause}", checked)
        self.result["cases"].append(case)

    def first_data_file(self, root: Path) -> Path:
        found = sorted(path for path in root.rglob("data") if "segments" in path.parts)
        if not found:
            raise HarnessError("the store holds no segment data file")
        return found[0]

    def run(self) -> int:
        failed = ""
        try:
            getattr(self, self.arguments.kind)()
        except HarnessError as error:
            failed = str(error)
        finally:
            self.stack.close()
        self.result["passed"] = not failed
        self.result["failure"] = failed
        (self.work / "result.json").write_text(
            json.dumps(self.result, indent=2, sort_keys=True) + "\n"
        )
        if failed:
            print(f"FAILED: {failed}", file=sys.stderr)
            return 1
        print(f"passed: {len(self.result['cases'])} cases; see {self.work}/result.json")
        return 0


def parse(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--kind", required=True, choices=("kill", "replay", "fault"))
    parser.add_argument("--filesystem", required=True, choices=sorted(FILESYSTEMS))
    parser.add_argument("--probe", required=True, help="the built durability probe")
    parser.add_argument(
        "--work",
        required=True,
        help="an empty directory of this run, not on the file system under test",
    )
    parser.add_argument("--cause", choices=FAULTS, help="the fault kind's cause")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--size-mib", type=int, default=1024)
    parser.add_argument("--rounds", type=int, default=8, help="kills")
    parser.add_argument(
        "--marks",
        type=int,
        default=24,
        help="marks replayed to, or the latest to act at",
    )
    parser.add_argument("--between", type=int, default=24, help="points between marks")
    parser.add_argument("--stall-ms", type=int, default=2000)
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument("--mount-options", default="")
    parser.add_argument("--mkfs-option", action="append", default=[])
    parser.add_argument("--reactor-backend", default="linux-aio")
    arguments = parser.parse_args(argv)
    if (arguments.kind == "fault") != (arguments.cause is not None):
        parser.error("--cause goes with --kind=fault, and only with it")
    return arguments


def main(argv: Sequence[str]) -> int:
    arguments = parse(argv)
    if os.geteuid() != 0:
        print("this harness makes devices and mounts: run it as root", file=sys.stderr)
        return 2
    work = Path(arguments.work)
    if not work.is_dir() or any(work.iterdir()):
        print("--work must be an empty directory of this run", file=sys.stderr)
        return 2
    if not Path(arguments.probe).is_file():
        print("--probe is not a file", file=sys.stderr)
        return 2
    runner = Runner()
    lacking = missing(
        runner, *needed(arguments.kind, arguments.filesystem, arguments.cause)
    )
    if lacking:
        print(
            "this machine cannot run this kind: it lacks " + ", ".join(lacking),
            file=sys.stderr,
        )
        return 2
    return Qualification(arguments, runner).run()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
