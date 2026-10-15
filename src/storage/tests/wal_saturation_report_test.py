import copy
import json
import tempfile
import unittest
from pathlib import Path

from src.storage.tests import wal_saturation_report as report

PROFILE = {
    "allocator": "native",
    "injection": "false",
    "optimized": "true",
    "asan": "false",
    "ubsan": "false",
    "oom_abort": "true",
}
PROFILE_LINE = "kwaque-benchmark-profile-v1 " + " ".join(
    f"{key}={value}" for key, value in PROFILE.items()
)


def settings(**changes):
    values = {
        "first_rate": 500,
        "increment": 500,
        "step_requests": 128,
        "step_ns": 100_000_000,
        "maximum_steps": 12,
    }
    values.update(changes)
    return values


def step(number, rate, offered, refused=0, late=0, drain_ns=3_000_000):
    """One step: every accepted request but the late ones completed, a short
    drain after the planned period."""
    accepted = offered - refused
    completed = accepted - late
    elapsed = offered * report.NANOSECONDS // rate + drain_ns
    return {
        "row": "step",
        "step": number,
        "offered_rate": rate,
        "achieved_rate": round(completed * report.NANOSECONDS / elapsed, 3),
        "offered": offered,
        "accepted": accepted,
        "refused": refused,
        "completed": completed,
        "elapsed_ns": elapsed,
        "pass": report.passes(offered, completed),
        "reactor_flushes": (completed + 1) // 2,
        "reactor_writes": (accepted + 1) // 2,
        "reactor_write_bytes": 8192 * accepted,
    }


def limit_search():
    """Two passing steps and a third that refuses a quarter of its plan."""
    steps = [
        step(1, 500, 128),
        step(2, 1000, 128, refused=3),
        step(3, 2000, 200, refused=50),
    ]
    final = {
        "row": "final",
        "outcome": "limit",
        "steps": 3,
        "last_passing_rate": 1000,
        "first_failing_rate": 2000,
        **settings(),
    }
    return steps, final


def unreached_search(maximum_steps=3, bound="steps"):
    steps = [step(1, 500, 128), step(2, 1000, 128), step(3, 2000, 200)]
    final = {
        "row": "final",
        "outcome": "not_reached",
        "steps": 3,
        "bound": bound,
        **settings(maximum_steps=maximum_steps),
    }
    return steps, final


def log(*searches, profile=PROFILE_LINE):
    lines = ["startup diagnostics", profile]
    for steps, final in searches:
        lines += [report.PREFIX + json.dumps(row) for row in (*steps, final)]
    return "\n".join(lines) + "\n"


class SaturationReportTest(unittest.TestCase):
    def rejected(self, message, steps, final):
        with self.assertRaisesRegex(ValueError, message):
            report.validate_search(steps, final)

    def test_a_search_follows_from_its_steps(self):
        report.validate_search(*limit_search())
        report.validate_search(*unreached_search())
        self.assertEqual(
            [row["pass"] for row in limit_search()[0]], [True, True, False]
        )

    def test_one_tenth_rule_decides_a_step(self):
        # Of 128 planned, twelve missing is within a tenth and thirteen is not.
        self.assertTrue(report.passes(128, 116))
        self.assertFalse(report.passes(128, 115))
        self.assertTrue(report.passes(10, 9))
        self.assertFalse(report.passes(9, 8))
        self.assertTrue(report.passes(1, 1))
        # A step with no completion at all fails whatever its plan.
        self.assertFalse(report.passes(1, 0))
        self.assertFalse(report.passes(128, 0))
        # A refusal and a late result are both not achieved.
        self.assertTrue(step(1, 500, 128, refused=6, late=6)["pass"])
        self.assertFalse(step(1, 500, 128, refused=7, late=6)["pass"])
        for completed, verdict in ((116, False), (115, True)):
            row = step(1, 500, 128, late=128 - completed)
            row["pass"] = verdict
            with (
                self.subTest(completed=completed),
                self.assertRaisesRegex(ValueError, "one-tenth rule"),
            ):
                report.validate_step(row, 1)

    def test_step_counts_follow_from_one_another(self):
        report.validate_step(step(1, 500, 128, refused=20, late=2), 1)
        for change, message in (
            ({"accepted": 127}, "not the offered ones"),
            ({"refused": 1}, "not the offered ones"),
            ({"completed": 129}, "more requests completed"),
            ({"offered": 0, "accepted": 0, "completed": 0}, "offered nothing"),
            ({"offered_rate": 0}, "offered nothing"),
            ({"elapsed_ns": 0}, "offered nothing"),
            ({"elapsed_ns": 255_999_999}, "before its period"),
            ({"achieved_rate": 500.0}, "achieved rate"),
            ({"achieved_rate": "494.2"}, "achieved rate"),
            ({"achieved_rate": True}, "achieved rate"),
            ({"reactor_flushes": 0}, "no flush at the device"),
            ({"refused": -1}, "lacks a count"),
            ({"completed": 128.0}, "lacks a count"),
            ({"accepted": True}, "lacks a count"),
            ({"pass": 1}, "lacks a count"),
            ({"pass": "true"}, "lacks a count"),
        ):
            row = step(1, 500, 128)
            row.update(change)
            with (
                self.subTest(change=change),
                self.assertRaisesRegex(ValueError, message),
            ):
                report.validate_step(row, 1)
        for key in (*report.STEP_COUNTS, "pass", "achieved_rate"):
            row = step(1, 500, 128)
            row.pop(key)
            with self.subTest(missing=key), self.assertRaises(ValueError):
                report.validate_step(row, 1)
        with self.assertRaisesRegex(ValueError, "numbered in order"):
            report.validate_step(step(2, 500, 128), 1)
        # A step that completed nothing shows no flush and is still a row.
        empty = step(1, 500, 128, refused=128)
        self.assertEqual((empty["completed"], empty["reactor_flushes"]), (0, 0))
        report.validate_step(empty, 1)

    def test_rates_rise_by_the_stated_rule(self):
        # Each pass in a row doubles what the next one adds.
        steps = [
            step(number, rate, max(128, rate // 10))
            for number, rate in enumerate((500, 1000, 2000, 4000, 8000), 1)
        ]
        final = {
            "row": "final",
            "outcome": "not_reached",
            "steps": 5,
            "bound": "steps",
            **settings(maximum_steps=5),
        }
        report.validate_search(steps, final)
        other = [
            step(number, rate, max(64, -(-rate * 3 // 100)))
            for number, rate in enumerate((1000, 1100, 1300, 1700), 1)
        ]
        report.validate_search(
            other,
            {
                **final,
                "steps": 4,
                **settings(
                    first_rate=1000,
                    increment=100,
                    step_requests=64,
                    step_ns=30_000_000,
                    maximum_steps=4,
                ),
            },
        )
        for index, rate in ((1, 1500), (2, 1500), (2, 3000), (0, 1000)):
            changed = copy.deepcopy(steps)
            changed[index] = step(index + 1, rate, max(128, rate // 10))
            with self.subTest(index=index, rate=rate):
                self.rejected("stated rule", changed, final)
        # A step plans its own count, or as many as fill its shortest period.
        changed = copy.deepcopy(steps)
        changed[2] = step(3, 2000, 128)
        self.rejected("stated requests", changed, final)
        changed = copy.deepcopy(steps)
        changed[0] = step(1, 500, 129)
        self.rejected("stated requests", changed, final)

    def test_final_row_agrees_with_the_steps(self):
        steps, final = limit_search()
        for change, message in (
            ({"steps": 2}, "number of steps"),
            ({"first_failing_rate": 1000}, "final rates"),
            ({"last_passing_rate": 500}, "final rates"),
            ({"last_passing_rate": None}, "final rates"),
            ({"outcome": "not_reached"}, "outcome disagrees"),
            ({"outcome": "saturated"}, "lacks its outcome"),
            ({"bound": "steps"}, "inside the bound"),
            ({"maximum_steps": 2}, "past its step bound"),
            ({"increment": 0}, "lacks its outcome or a setting"),
            ({"step_ns": 1.0e8}, "lacks its outcome or a setting"),
        ):
            with self.subTest(change=change):
                self.rejected(message, steps, {**final, **change})
        for key in ("last_passing_rate", "first_failing_rate", "steps", "outcome"):
            changed = dict(final)
            changed.pop(key)
            with self.subTest(missing=key), self.assertRaises(ValueError):
                report.validate_search(steps, changed)
        for key in report.SETTINGS:
            changed = dict(final)
            changed.pop(key)
            with self.subTest(missing=key):
                self.rejected("lacks its outcome or a setting", steps, changed)
        self.rejected("number of steps", [], {**final, "steps": 0})
        # The search ends at its first failing step.
        longer = [*steps, step(4, 4000, 400, refused=300)]
        self.rejected(
            "after a failing step",
            longer,
            {
                **final,
                "steps": 4,
                "last_passing_rate": 2000,
                "first_failing_rate": 4000,
            },
        )

    def test_a_first_step_that_fails_has_no_passing_rate(self):
        steps = [step(1, 500, 128, refused=64)]
        final = {
            "row": "final",
            "outcome": "limit",
            "steps": 1,
            "first_failing_rate": 500,
            **settings(),
        }
        report.validate_search(steps, final)
        self.rejected("final rates", steps, {**final, "last_passing_rate": 500})
        self.rejected("final rates", steps, {**final, "last_passing_rate": 0})

    def test_no_limit_is_claimed_without_a_failing_step(self):
        steps, final = unreached_search()
        for key, value in (("last_passing_rate", 2000), ("first_failing_rate", 4000)):
            with self.subTest(key=key):
                self.rejected("final rates", steps, {**final, key: value})
        self.rejected("outcome disagrees", steps, {**final, "outcome": "limit"})
        # What ended the search is stated: its step bound, or the file's room.
        report.validate_search(*unreached_search(maximum_steps=12, bound="room"))
        for maximum_steps, bound in (
            (12, "steps"),
            (3, "room"),
            (3, "time"),
            (3, None),
        ):
            with self.subTest(maximum_steps=maximum_steps, bound=bound):
                self.rejected(
                    "ended early",
                    *unreached_search(maximum_steps=maximum_steps, bound=bound),
                )
        steps, final = unreached_search()
        final.pop("bound")
        self.rejected("ended early", steps, final)

    def test_log_holds_whole_searches_and_a_build_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.log"
            path.write_text(log(limit_search(), unreached_search()))
            searches = report.read_searches(path)
            self.assertEqual(
                [search["final"]["outcome"] for search in searches],
                ["limit", "not_reached"],
            )
            self.assertEqual(searches[0]["build_profile"], PROFILE)
            for text, message in (
                (log(limit_search(), profile="no profile"), "build profile"),
                (log(), "no completed saturation search"),
                (log((limit_search()[0], {"row": "final"})), "lacks its outcome"),
                (log(([], limit_search()[1])), "number of steps"),
                (
                    log(limit_search()) + report.PREFIX + json.dumps(step(1, 500, 128)),
                    "no final row",
                ),
                # A search that stopped before its final row runs into the next.
                (
                    log(limit_search()).rsplit(report.PREFIX, 1)[0]
                    + log(limit_search()),
                    "number of steps",
                ),
                (log(limit_search()) + report.PREFIX + '{"row":"cohort"}', "unknown"),
                (log(limit_search()) + report.PREFIX + "[]", "unknown"),
                (
                    log(limit_search(), profile=PROFILE_LINE + "\nallocator=system"),
                    "conflicting build profile",
                ),
            ):
                path.write_text(text)
                with (
                    self.subTest(message=message),
                    self.assertRaisesRegex(ValueError, message),
                ):
                    report.read_searches(path)

    def test_summary_gives_the_rates_and_what_was_counted(self):
        steps, final = limit_search()
        summary = report.summarize(
            {"steps": steps, "final": final, "build_profile": PROFILE}
        )
        self.assertEqual(
            (
                summary["outcome"],
                summary["steps"],
                summary["last_passing_rate"],
                summary["first_failing_rate"],
                summary["bound"],
            ),
            ("limit", 3, 1000, 2000, None),
        )
        self.assertEqual(
            [summary[key] for key in ("offered", "accepted", "refused", "completed")],
            [456, 403, 53, 403],
        )
        self.assertEqual(summary["reactor_write_bytes"], 8192 * 403)
        self.assertEqual(summary["settings"], settings())
        self.assertEqual(
            [(row["offered"], row["refused"], row["pass"]) for row in summary["rates"]],
            [(500, 0, True), (1000, 3, True), (2000, 50, False)],
        )
        unreached = report.summarize(
            {
                "steps": unreached_search()[0],
                "final": unreached_search()[1],
                "build_profile": PROFILE,
            }
        )
        self.assertEqual(
            (
                unreached["outcome"],
                unreached["last_passing_rate"],
                unreached["first_failing_rate"],
                unreached["bound"],
            ),
            ("not_reached", None, None, "steps"),
        )


if __name__ == "__main__":
    unittest.main()
