import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import replay

ROOT = Path(__file__).resolve().parent

def fixture(n):
    return json.loads((ROOT/"fixtures"/f"archived-p4-run-{n}.json").read_text())

class ReplayTest(unittest.TestCase):
    def test_compare_archive_only_ignores_identity_and_row_order(self):
        one, two = fixture(1), fixture(2)
        self.assertNotEqual(one["run_id"], two["run_id"])
        two["final_rows"].reverse()
        self.assertEqual(replay.differences(replay.normalized(one), replay.normalized(two)), [])

    def test_same_wrong_outputs_in_two_runs_do_not_pass(self):
        raw = fixture(1)
        raw["final_rows"][0]["sha256"]["S"] = "0"*64
        with self.assertRaisesRegex(ValueError, "contents"):
            replay.normalized(raw)

    def test_manual_fallback_cannot_pass_as_native(self):
        raw = fixture(1)
        raw["manual_normal_event_id"] = "fixture"
        with self.assertRaisesRegex(ValueError, "automatic native"):
            replay.normalized(raw)

    def test_duplicate_receipt_cannot_count_as_two_messages(self):
        raw = fixture(1)
        raw["late_replays"]["observed"][1] = copy.deepcopy(raw["late_replays"]["observed"][0])
        with self.assertRaisesRegex(ValueError, "late replay"):
            replay.normalized(raw)

    @mock.patch.object(replay, "docker")
    def test_refuse_unowned_container(self, docker):
        docker.return_value = subprocess.CompletedProcess([],0,json.dumps(
            [{"Id":"other","Config":{"Labels":{replay.LABEL:"other-owner"}}}]),"")
        with self.assertRaisesRegex(ValueError, "does not belong"):
            replay.find_owned("fixture","owner")
        self.assertEqual(docker.call_count,1)

    @mock.patch.object(replay, "find_owned", side_effect=["own-id", None])
    @mock.patch.object(replay, "docker")
    def test_failed_launch_preserves_error_and_cleans_only_owned_id(self, docker, find_owned):
        docker.side_effect = [subprocess.CalledProcessError(125,["docker"],stderr="fixture launch error"),
                              subprocess.CompletedProcess([],0,"fixture emulator logs",""),
                              subprocess.CompletedProcess([],0,"","")]
        env={"image_id":"sha256:"+"0"*64}
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)
            record=replay.run_once(1,path,env,"owner",lambda _:None)
            self.assertEqual(record["stage"],"container_start")
            self.assertEqual(record["status"],"fail")
            self.assertIs(record["cleaned"],True)
            self.assertIn("fixture launch error",(path/"stage-1.log").read_text())
            self.assertIn("fixture emulator logs",(path/"emulator-1.log").read_text())
            self.assertFalse(any(path.glob("runtime-*")))
            docker.assert_any_call("rm","-f","own-id",timeout=15)

    @mock.patch.object(replay, "run_once")
    @mock.patch.object(replay, "shell")
    @mock.patch.object(replay, "docker")
    def test_no_second_run_if_first_cleanup_unverified(self, docker, shell, run_once):
        docker.return_value.stdout = json.dumps([{"Id":"sha256:"+"0"*64}])
        shell.return_value.stdout = "fixture aws-cli"
        args = mock.Mock(image=replay.IMAGE, expect_version="1.0.490", expected_image_id=None)
        def fail(index, directory, env, owner, save):
            record = {"index": index, "status": "fail", "cleaned": False}
            save(record)
            return record
        run_once.side_effect = fail
        with tempfile.TemporaryDirectory() as tmp:
            report = replay.execute(args, Path(tmp), "owner")
        self.assertEqual(report["status"], "fail")
        self.assertIsNone(report["comparison"]["match"])
        self.assertEqual(run_once.call_count, 1)

    @mock.patch.object(replay, "docker")
    def test_bad_endpoint_is_refused_before_docker_or_aws(self, docker):
        args=mock.Mock()
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(replay.scenario,"ENDPOINT","https://s3.amazonaws.com"):
            report=replay.execute(args, Path(tmp),"owner")
        docker.assert_not_called()
        self.assertEqual(report["reason"], "nonstandard scenario endpoint refused")
        self.assertEqual(report["status"],"fail")

if __name__ == "__main__":
    unittest.main()
