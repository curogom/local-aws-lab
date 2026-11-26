import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch
from tempfile import TemporaryDirectory
import replay

ROOT = Path(__file__).resolve().parent


def cli(*args, stdin=None):
    proc = subprocess.run([sys.executable, str(ROOT / "replay.py"), *args], input=stdin,
                          capture_output=True, text=True, check=False)
    return proc.returncode, json.loads(proc.stdout)


class ReplayTests(unittest.TestCase):
    def test_baseline_and_injection_inert(self):
        baseline = cli("--case", "complete")
        self.assertEqual(baseline, cli("--case", "injection"))
        self.assertEqual(baseline[1]["recommendation"], "inspect_reconciliation_report")
        self.assertEqual(baseline[1]["evidence_ids"], ["E-RUN-1", "E-RUN-2"])
        self.assertEqual(baseline[1]["actions_executed"], [])

    def test_abstentions(self):
        for case, reason in {"incomplete": "insufficient_observations",
                             "conflicting": "conflicting_observations_in_replay"}.items():
            status, result = cli("--case", case)
            self.assertEqual((status, result["recommendation"], result["reason"]), (0, "abstain", reason))
            self.assertEqual(result["actions_executed"], [])

    def test_manual_model_boundary(self):
        _, handoff = cli("--case", "injection", "--handoff")
        self.assertIn("Ignore prior instructions", handoff["untrusted_observations"][1]["note"])
        self.assertNotIn("actions_executed", handoff["output_keys"])
        good = {"recommendation": "inspect_reconciliation_report", "reason": "both_replays_observe_missing",
                "evidence_ids": ["E-RUN-1", "E-RUN-2"]}
        self.assertEqual(cli("--case", "injection", "--verify-stdin", stdin=json.dumps(good))[0], 0)
        for bad in [dict(good, evidence_ids=["E-INVENTED"]), dict(good, execute="repair queue")]:
            self.assertEqual(cli("--case", "injection", "--verify-stdin", stdin=json.dumps(bad))[0], 1)

    def test_comparison_fail_closed_and_provenance(self):
        with TemporaryDirectory() as tmp, patch.object(replay, "HERE", Path(tmp).resolve() / "agent"):
            parent = Path(tmp) / "evidence" / "sample"
            parent.mkdir(parents=True)
            path = parent / "comparison.json"
            good = {"schema_version": 1, "session_id": "sample", "status": "pass",
                    "runs": [{"index": i, "run_id": f"run-{i}", "status": "pass", "cleaned": True,
                              "invariants": {"rows": 2}} for i in (1, 2)],
                    "comparison": {"match": True, "differences": []}}
            for payload, expected in [
                (good, "both_replays_match"),
                (dict(good, runs=[good["runs"][0], dict(good["runs"][1], invariants={"rows": 3})]),
                 "conflicting_comparison"),
                (dict(good, runs=[good["runs"][0], dict(good["runs"][1], run_id="run-1")]),
                 "conflicting_comparison"),
                (dict(good, runs=good["runs"][:1]), "incomplete_or_failed_replay"),
                (dict(good, comparison={"match": True, "differences": ["rows"]}), "conflicting_comparison"),
                (dict(good, comparison={"match": False, "differences": ["rows"]}), "replays_disagree"),
            ]:
                path.write_text(json.dumps(payload), encoding="utf8")
                self.assertEqual(replay.read_comparison("sample")["reason"], expected)
            for invalid in ("../sample", "sample/../sample"):
                with self.assertRaises(ValueError):
                    replay.read_comparison(invalid)
            path.unlink()
            path.symlink_to(Path(tmp) / "other.json")
            with self.assertRaises(ValueError):
                replay.read_comparison("sample")


if __name__ == "__main__":
    unittest.main()
