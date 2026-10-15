from __future__ import annotations

import argparse
import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path

try:
    from tools import storage_device_harness as harness
except ModuleNotFoundError:
    import storage_device_harness as harness


class FakeRunner(harness.Runner):
    """Records every command and answers the ones a test gives answers for."""

    def __init__(self, answers=None):
        self.commands: list[list[str]] = []
        self.answers = answers or {}

    def run(self, argv, *, check=True, timeout=300.0):
        self.commands.append(list(argv))
        for prefix, answer in self.answers.items():
            if tuple(argv[: len(prefix)]) == prefix:
                done = answer(argv) if callable(answer) else answer
                if check and done.returncode != 0:
                    raise harness.HarnessError(f"{argv[0]} exited {done.returncode}")
                return done
        return harness.Completed(0, "")


class TablesTest(unittest.TestCase):
    def test_tables_are_the_targets_own_lines(self):
        self.assertEqual(
            harness.log_writes_table(2048, "/dev/loop1", "/dev/loop2"),
            "0 2048 log-writes /dev/loop1 /dev/loop2",
        )
        self.assertEqual(
            harness.linear_table(2048, "/dev/loop1"), "0 2048 linear /dev/loop1 0"
        )
        self.assertEqual(
            harness.error_table(2048, "/dev/loop1"), "0 2048 error /dev/loop1 0"
        )
        self.assertEqual(
            harness.failed_writes_table(2048, "/dev/loop1"),
            "0 2048 flakey /dev/loop1 0 0 180 1 error_writes",
        )
        self.assertEqual(
            harness.delay_table(2048, "/dev/loop1", 2000),
            "0 2048 delay /dev/loop1 0 0 /dev/loop1 0 0 /dev/loop1 0 2000",
        )
        self.assertEqual(
            harness.dust_table(2048, "/dev/loop1"), "0 2048 dust /dev/loop1 0 512"
        )


class ParsingTest(unittest.TestCase):
    def test_a_found_entry_is_its_number(self):
        self.assertEqual(harness.parse_found("1024@8200\n"), 1024)
        self.assertEqual(harness.parse_found("1024\n"), 1024)
        for text in ("", "Couldn't find entry", "12@", "a@b", "1@2@3", "-1"):
            with self.assertRaises(harness.HarnessError):
                harness.parse_found(text)

    def test_what_a_kind_takes_and_what_a_machine_lacks(self):
        tools, targets = harness.needed("replay", "xfs", None)
        self.assertIn("replay-log", tools)
        self.assertIn("mkfs.xfs", tools)
        self.assertEqual(targets, ["log-writes"])
        tools, targets = harness.needed("kill", "ext4", None)
        self.assertNotIn("dmsetup", tools)
        self.assertEqual(targets, [])
        self.assertEqual(
            harness.needed("fault", "ext4", "gone")[1], ["linear", "error"]
        )
        self.assertEqual(
            harness.needed("fault", "ext4", "stalled")[1], ["linear", "delay"]
        )
        tools, targets = harness.needed("fault", "ext4", "unreadable")
        self.assertIn("filefrag", tools)
        self.assertEqual(targets, ["dust"])

        runner = FakeRunner({("modinfo", "dm_dust"): harness.Completed(1, "not found")})
        lacking = harness.missing(
            runner,
            ["mount", "replay-log"],
            ["linear", "log-writes", "dust"],
            which=lambda tool: None if tool == "replay-log" else "/bin/" + tool,
        )
        self.assertEqual(lacking, ["replay-log", "the dust target (module dm_dust)"])
        self.assertEqual(
            runner.commands, [["modinfo", "dm_log_writes"], ["modinfo", "dm_dust"]]
        )

    def test_marks_are_counted_from_one(self):
        lines = ["create_asked 1", "created 1", "mark 1", "offer 1 100 aa", "mark 2"]
        self.assertEqual(harness.marks_in(lines), [1, 2])
        self.assertEqual(harness.marks_in(["note mark 7", "marks 1"]), [])
        with self.assertRaises(harness.HarnessError):
            harness.marks_in(["mark 2"])
        with self.assertRaises(harness.HarnessError):
            harness.marks_in(["mark 1", "mark 1"])

    def test_a_sample_is_evenly_apart_and_ends_at_the_last(self):
        self.assertEqual(harness.sample([], 3), [])
        self.assertEqual(harness.sample([1, 2, 3], 0), [])
        self.assertEqual(harness.sample([1, 2, 3], 5), [1, 2, 3])
        self.assertEqual(harness.sample(list(range(1, 11)), 3), [3, 6, 10])
        self.assertEqual(harness.sample(list(range(1, 11)), 1), [10])
        many = harness.sample(list(range(100)), 24)
        self.assertEqual(len(many), 24)
        self.assertEqual(many[-1], 99)
        self.assertEqual(many, sorted(many))

    def test_the_first_extent_is_where_the_file_begins(self):
        listing = (
            "Filesystem type is: ef53\n"
            "File size of /mnt/store/data is 1048576 (2048 blocks of 512 bytes)\n"
            " ext:     logical_offset:        physical_offset: length:   expected: flags:\n"
            "   0:        0..    2047:      34816..     36863:   2048:             last,eof\n"
            "/mnt/store/data: 1 extent found\n"
        )
        self.assertEqual(harness.first_physical_sector(listing), 34816)
        with self.assertRaises(harness.HarnessError):
            harness.first_physical_sector("/mnt/store/data: 0 extents found\n")

    def test_the_scheduler_in_use_is_the_bracketed_one(self):
        self.assertEqual(harness.scheduler_of("mq-deadline kyber [none]\n"), "none")
        self.assertEqual(harness.scheduler_of("none\n"), "none")

    def test_a_mount_is_read_from_the_kernels_list(self):
        listing = (
            "25 1 8:2 / / rw,relatime shared:1 - ext4 /dev/sda2 rw,errors=remount-ro\n"
            "310 25 253:4 / /work/mnt rw,noatime shared:99 - xfs "
            "/dev/mapper/kwaque-7-log rw,attr2,inode64,logbufs=8,noquota\n"
        )
        self.assertEqual(
            harness.mount_entry(listing, "/work/mnt"),
            {
                "device": "253:4",
                "type": "xfs",
                "source": "/dev/mapper/kwaque-7-log",
                "mount_options": "rw,noatime",
                "super_options": "rw,attr2,inode64,logbufs=8,noquota",
            },
        )
        with self.assertRaises(harness.HarnessError):
            harness.mount_entry(listing, "/elsewhere")

    def test_a_verdict_and_a_profile_are_read_from_the_stores_output(self):
        output = (
            "INFO seastar - Reactor backend: linux-aio\n"
            "kwaque-durability-profile backend=linux-aio appending_io=good stalls=0\n"
            "kwaque-durability-verdict holds\n"
        )
        self.assertEqual(harness.read_verdict(output), "holds")
        self.assertEqual(harness.read_verdict("no verdict here\n"), "")
        self.assertEqual(
            harness.profile_of(output),
            {"backend": "linux-aio", "appending_io": "good", "stalls": "0"},
        )
        self.assertEqual(harness.profile_of(""), {})
        harness.require_holds("it", harness.Probed(0, "holds", output))
        for probed in (
            harness.Probed(2, "lost", ""),
            harness.Probed(0, "", ""),
            harness.Probed(3, "holds", ""),
            harness.Probed(-9, "", ""),
        ):
            with self.assertRaises(harness.HarnessError):
                harness.require_holds("it", probed)


def block_device(sysfs: Path, where: str, number: str, queue=None, extra=None) -> Path:
    """One block device in a sysfs made for a test."""
    entry = sysfs / "devices" / where
    (entry / "queue").mkdir(parents=True)
    (entry / "dev").write_text(number + "\n")
    for field, value in (queue or {}).items():
        (entry / "queue" / field).write_text(value + "\n")
    for field, value in (extra or {}).items():
        path = entry / field
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value + "\n")
    link = sysfs / "dev" / "block" / number
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to(entry)
    return entry


class DeviceLayersTest(unittest.TestCase):
    def test_every_layer_under_a_mapped_device_is_named(self):
        with tempfile.TemporaryDirectory() as directory:
            sysfs = Path(directory)
            disk = block_device(
                sysfs,
                "pci/nvme0n1",
                "259:0",
                {
                    "write_cache": "write back",
                    "fua": "1",
                    "scheduler": "[none] mq-deadline",
                    "nr_requests": "1023",
                    "logical_block_size": "512",
                    "physical_block_size": "4096",
                },
            )
            part = disk / "nvme0n1p2"
            part.mkdir()
            (part / "partition").write_text("2\n")
            (part / "dev").write_text("259:2\n")
            (sysfs / "dev" / "block" / "259:2").symlink_to(part)
            loop = block_device(
                sysfs,
                "virtual/block/loop3",
                "7:3",
                {"write_cache": "write through", "scheduler": "none"},
            )
            mapped = block_device(
                sysfs,
                "virtual/block/dm-4",
                "253:4",
                {"write_cache": "write back", "fua": "1"},
                {"dm/name": "kwaque-7-log"},
            )
            (mapped / "slaves").mkdir()
            (mapped / "slaves" / "loop3").symlink_to(loop)

            layers = harness.device_layers(sysfs, "253:4")
            self.assertEqual([layer["name"] for layer in layers], ["dm-4", "loop3"])
            self.assertEqual(layers[0]["dm/name"], "kwaque-7-log")
            self.assertEqual(layers[0]["write_cache"], "write back")
            self.assertEqual(layers[1]["scheduler"], "none")

            # A partition is named with the disk it is cut from.
            under = harness.device_layers(sysfs, "259:2")
            self.assertEqual(
                [layer["name"] for layer in under], ["nvme0n1p2", "nvme0n1"]
            )
            self.assertEqual(under[0]["partition_of"], "nvme0n1")
            self.assertEqual(under[1]["scheduler"], "none")
            self.assertEqual(under[1]["physical_block_size"], "4096")

            # A loop device goes on to the device that holds its file.
            image = sysfs / "image"
            image.write_bytes(b"")
            (loop / "loop").mkdir()
            (loop / "loop" / "backing_file").write_text(str(image) + "\n")
            held = os.stat(image).st_dev
            below = f"{os.major(held)}:{os.minor(held)}"
            through = harness.device_layers(sysfs, "7:3")
            self.assertEqual(through[0]["loop/backing_file"], str(image))
            self.assertEqual(through[1]["device"], below)

            with self.assertRaises(harness.HarnessError):
                harness.device_layers(sysfs, "1:1")


class StackTest(unittest.TestCase):
    def test_everything_made_is_unmade_the_last_first(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            runner = FakeRunner(
                {
                    ("losetup", "--find"): harness.Completed(0, "/dev/loop9\n"),
                    ("blockdev", "--getsz"): harness.Completed(0, "2048\n"),
                }
            )
            stack = harness.Stack(runner, work, "7")
            image = stack.image("data", 1024 * 1024)
            self.assertEqual(image.stat().st_size, 1024 * 1024)
            # An image that is there is not this run's.
            with self.assertRaises(FileExistsError):
                stack.image("data", 1)
            device = stack.loop(image)
            self.assertEqual(stack.sectors(device), 2048)
            name, mapped = stack.mapped("log", "0 2048 linear /dev/loop9 0")
            self.assertEqual(
                (name, mapped), ("kwaque-7-log", "/dev/mapper/kwaque-7-log")
            )
            stack.mount(mapped, "ext4", "noatime")
            stack.reload(name, "0 2048 error /dev/loop9 0")
            runner.commands.clear()
            stack.close()
            self.assertEqual(
                runner.commands,
                [
                    ["umount", str(work / "mnt")],
                    ["dmsetup", "remove", "--retry", "kwaque-7-log"],
                    ["udevadm", "settle"],
                    ["losetup", "--detach", "/dev/loop9"],
                ],
            )
            self.assertFalse(image.exists())

    def test_a_table_is_changed_with_the_device_suspended(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = FakeRunner()
            stack = harness.Stack(runner, Path(directory), "7")
            stack.reload("kwaque-7-fault", "0 8 error /dev/loop9 0")
            self.assertEqual(
                runner.commands,
                [
                    ["dmsetup", "suspend", "--nolockfs", "kwaque-7-fault"],
                    [
                        "dmsetup",
                        "load",
                        "kwaque-7-fault",
                        "--table",
                        "0 8 error /dev/loop9 0",
                    ],
                    ["dmsetup", "resume", "kwaque-7-fault"],
                ],
            )

    def test_what_was_unmade_by_hand_is_not_unmade_again(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = FakeRunner()
            stack = harness.Stack(runner, Path(directory), "7")
            name, mapped = stack.mapped("log", "0 8 linear /dev/loop9 0")
            stack.mount(mapped, "xfs")
            stack.unmount()
            stack.unmap(name)
            runner.commands.clear()
            stack.close()
            self.assertEqual(runner.commands, [])


class ProbeTest(unittest.TestCase):
    def test_arguments_name_the_mode_and_the_record(self):
        probe = harness.Probe(Path("/bin/probe"), "linux-aio", "512MiB")
        run = probe.run_arguments(
            Path("/mnt/store"), Path("/work/record"), "life", "dmsetup message x 0 mark"
        )
        self.assertEqual(
            run[:6],
            [
                "/bin/probe",
                "--mode=run",
                "--root=/mnt/store",
                "--history=life",
                "--record=/work/record",
                "--mark-command=dmsetup message x 0 mark",
            ],
        )
        self.assertIn("--reactor-backend=linux-aio", run)
        self.assertIn("--max-networking-io-control-blocks=1000", run)
        bare = " ".join(probe.run_arguments(Path("/a"), None, "long"))
        self.assertNotIn("--mark-command=", bare)
        self.assertNotIn("--record=", bare)
        whole = probe.check_arguments(Path("/mnt/store"), Path("/work/record"))
        self.assertEqual(
            whole[1:4], ["--mode=check", "--root=/mnt/store", "--record=/work/record"]
        )
        self.assertFalse(any(word.startswith("--answered") for word in whole))
        part = probe.check_arguments(Path("/a"), Path("/b"), 3, 4)
        self.assertIn("--answered=3", part)
        self.assertIn("--asked=4", part)
        same = probe.check_arguments(Path("/a"), Path("/b"), 3)
        self.assertIn("--asked=3", same)


class ArgumentsTest(unittest.TestCase):
    def test_a_cause_goes_with_the_fault_kind_only(self):
        base = ["--filesystem=ext4", "--probe=/p", "--work=/w"]
        self.assertEqual(
            harness.parse(["--kind=fault", "--cause=gone", *base]).cause, "gone"
        )
        self.assertIsInstance(
            harness.parse(["--kind=replay", *base]), argparse.Namespace
        )
        for bad in (["--kind=fault", *base], ["--kind=kill", "--cause=gone", *base]):
            with (
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                harness.parse(bad)


if __name__ == "__main__":
    unittest.main()
