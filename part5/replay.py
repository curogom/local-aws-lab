#!/usr/bin/env python3
"""Part 5: run the Part 4 scenario in two fresh local Docker containers."""
import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from uuid import uuid4

import scenario

ROOT = Path(__file__).resolve().parent
IMAGE = "local-aws-lab-part3-emulator:latest"
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")
VERSION = re.compile(r"[0-9]+(?:\.[0-9]+){2}")
RUN_ID = re.compile(r"[0-9a-f]{12}")
LABEL = "local-aws-lab-part5.owner"

def write_json(path, payload):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)

def shell(args, timeout=10, check=True):
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=check,
                          env={"PATH": os.environ["PATH"], "HOME": os.environ["HOME"], "NO_PROXY": "*",
                               "DOCKER_CONFIG": str(ROOT / ".unused-docker-config"), "DOCKER_CLI_HINTS": "false"})

def docker(*args, **kwargs):
    socket = Path.home() / ".docker/run/docker.sock"
    return shell(["docker", "--host", f"unix://{socket}", *args], **kwargs)

def need(ok, why):
    if not ok:
        raise ValueError(why)

def normalized(raw):
    rid = raw["run_id"]
    need(isinstance(rid, str) and RUN_ID.fullmatch(rid), "invalid run identity")
    need(raw.get("endpoint") == scenario.ENDPOINT, "unexpected scenario endpoint")
    need(raw.get("notification_configuration") == "accepted", "notification setup not accepted")
    checks = raw["checks"]
    need(set(checks) == {"normal_payload_matches", "gap_absent_before_reconciliation",
         "gap_repaired_from_source", "two_injected_late_events_deduplicated",
         "missing_object_not_recorded", "exactly_two_result_rows"} and
         all(v is True for v in checks.values()), "Part 4 assertions failed or incomplete")
    auto, gap, late, missing = (raw[k] for k in ("automatic_probe", "gap", "late_replays", "missing_reference"))
    need(auto["observed"] is True and auto["manual_send_before_probe"] is False and
         auto["reconcile_before_probe"] is False and "manual_normal_event_id" not in raw and
         [(x["key"], x["outcome"], x["event_source"]) for x in auto["observations"]] ==
         [("incoming/normal.txt", "committed", "aws:s3")], "automatic native event was not demonstrated")
    need(gap["simulated_by"] == "put-object with notification disabled" and
         not gap["observations_before_reconciliation"] and gap["row_before_reconciliation"] is None and
         gap["repair"] == [{"key":"incoming/gap.txt","outcome":"committed"}], "gap or repair mismatch")
    seen = [x["message_id"] for x in late["observed"]
            if x["key"] == "incoming/gap.txt" and x["outcome"] == "duplicate"]
    need(len(late["observed"]) == len(seen) == len(late["injected"]) == len(set(seen)) == 2
         and sorted(seen) == sorted(late["injected"]) and late["row_unchanged"] is True,
         "late replay did not preserve exactly one row")
    need(missing["row"] is None and len(missing["observed"]) == 1 and
         [(x["message_id"], x["key"], x["outcome"]) for x in missing["observed"]] ==
         [(missing["injected"], "incoming/missing.txt", "missing_object")], "missing object mismatch")
    prefix = "aws-lab-p4-" + rid + "/"
    rows = {}
    for row in raw["final_rows"]:
        job = row["job_id"]["S"]
        need(job.startswith(prefix) and job[len(prefix):] not in rows, "foreign or duplicate row")
        rows[job[len(prefix):]] = {key: row[key][kind]
                                 for key,kind in (("bytes","N"),("sha256","S"),("source","S"))}
    expected = {key: {"bytes": str(len(payload)), "sha256": hashlib.sha256(payload).hexdigest(), "source": source}
                for key,payload,source in (("incoming/normal.txt",b"normal payload\n","queue"),
                                            ("incoming/gap.txt",b"notification gap\n","reconciler"))}
    need(rows == expected and gap["row_after_repair"] in raw["final_rows"], "unexpected result contents")
    return {"automatic_s3_event": True, "gap_repaired_from_s3": True, "duplicate_messages": 2,
            "missing_result_absent": True, "rows": rows, "assertions": checks}

def differences(first, second):
    return sorted(k for k in first.keys() | second.keys() if first.get(k) != second.get(k))

def preflight(ca):
    with tempfile.TemporaryDirectory(prefix="aws-p5-preflight-") as tmp:
        lab = scenario.Lab(ca, Path(tmp))
        return {key: len(lab.aws(service, operation, "--no-paginate").get(field, []))
                for key,service,operation,field in (
                    ("s3_buckets","s3api","list-buckets","Buckets"),
                    ("sqs_queues","sqs","list-queues","QueueUrls"),
                    ("dynamodb_tables","dynamodb","list-tables","TableNames"))}

def find_owned(name, owner):
    result = docker("container", "inspect", name, check=False)
    if result.returncode:
        if "No such container:" in result.stderr or "No such object:" in result.stderr:
            return None
        raise RuntimeError("unable to verify container ownership")
    row = json.loads(result.stdout)[0]
    need(row["Config"]["Labels"].get(LABEL) == owner, "container does not belong to this session")
    return row["Id"]

def reason(exc):
    if isinstance(exc, subprocess.TimeoutExpired):
        return "timeout"
    if isinstance(exc, subprocess.CalledProcessError):
        return "command_failed"
    return str(exc) if isinstance(exc, ValueError) else type(exc).__name__

def run_once(index, directory, environment, owner, save):
    name = f"local-aws-lab-p5-{owner}-{index}"
    record = {"index": index, "status":"fail", "stage":"container_start", "cleaned":None,
              "invariants":None, "container":name}
    save(record)
    started = False
    try:
        with tempfile.TemporaryDirectory(prefix="runtime-", dir=directory) as temp:
            # Keep logs/results; this mount alone holds the disposable generated credentials.
            try:
                started = True
                docker("run", "--pull", "never", "-d", "--name", name, "--label", f"{LABEL}={owner}",
                       "--memory=2g", "--memory-swap=2g", "-p", "127.0.0.1:19498:9298",
                       "-v", f"{temp}:/home/emulator/.aws/emulator", environment["image_id"], timeout=25)
                record["stage"] = "health"
                ca = Path(temp) / "9298/ca.pem"
                deadline = time.monotonic() + 35
                while time.monotonic() < deadline:
                    if ca.is_file() and shell(["curl","--noproxy","*","--silent","--fail","--max-time","2",
                                                "--cacert",str(ca), scenario.ENDPOINT+"/__admin/health"],
                                               timeout=3, check=False).returncode == 0:
                        break
                    time.sleep(1)
                else:
                    raise ValueError("health deadline exceeded")
                record["stage"] = "version"
                actual = docker("exec",name,"rpm","-q","--qf","%{VERSION}","aws-emulator").stdout.strip()
                record["emulator_version"] = actual
                need(actual == environment["emulator_expected_version"], "installed emulator version mismatch")
                record["stage"] = "fresh_state"
                record["preflight"] = preflight(ca)
                need(not any(record["preflight"].values()), "new emulator is not empty")
                record["stage"] = "scenario"
                destination = directory / f"run-{index}.json"
                proc = shell([sys.executable,str(ROOT/"scenario.py"),"--ca",str(ca),"--emulator-version",
                              actual,"--image-id",environment["image_id"],"--out",str(destination)],
                             timeout=105, check=False)
                (directory/f"scenario-{index}.log").write_text(proc.stdout+proc.stderr,encoding="utf-8")
                raw = json.loads(destination.read_text(encoding="utf-8")) if destination.exists() else {}
                record["run_id"] = raw.get("run_id")
                need(proc.returncode == 0, "scenario failed; see raw evidence and scenario log")
                record["stage"] = "invariants"
                need(all(raw.get(k) == environment.get(expected) for k,expected in
                         (("image_id","image_id"),("aws_cli","aws_cli"),("lab_source_sha256","scenario_sha256"))),
                     "scenario environment does not match pinned metadata")
                record["invariants"] = normalized(raw)
                record.update(stage="complete", status="pass")
            except (OSError, RuntimeError, subprocess.SubprocessError, ValueError, TypeError, KeyError) as exc:
                record["reason"] = reason(exc)
                if isinstance(exc, subprocess.SubprocessError):
                    output = getattr(exc, "stderr", None) or getattr(exc, "stdout", None) or ""
                    if isinstance(output, bytes):
                        output = output.decode("utf-8", errors="replace")
                    (directory/f"stage-{index}.log").write_text(output, encoding="utf-8")
            finally:
                if started:
                    try:
                        cid = find_owned(name, owner)
                        if cid:
                            try:
                                logs = docker("logs","--tail","60",cid,check=False)
                                (directory/f"emulator-{index}.log").write_text(logs.stdout+logs.stderr,encoding="utf-8")
                            finally:
                                docker("rm","-f",cid,timeout=15)
                        record["cleaned"] = find_owned(name, owner) is None
                    except (OSError, RuntimeError, subprocess.SubprocessError, ValueError, KeyError):
                        record["cleaned"] = False
                    if record["cleaned"] is not True:
                        record.update(status="fail", stage="cleanup", reason="own container absence not verified")
                save(record)
    finally:
        save(record)
    return record

def execute(args, directory, owner):
    report = {"schema_version":1, "session_id":owner, "status":"fail", "stage":"initial",
              "environment":{}, "runs":[], "comparison":{"match":None, "differences":[]}}
    target = directory/"comparison.json"
    def save(record=None):
        if record is not None and not any(run is record for run in report["runs"]):
            report["runs"].append(record)
        write_json(target, report)
    save()
    try:
        report["stage"] = "environment"
        need(scenario.ENDPOINT == "https://127.0.0.1:19498", "nonstandard scenario endpoint refused")
        inspect = json.loads(docker("image","inspect",args.image).stdout)[0]
        need(IMAGE_ID.fullmatch(inspect["Id"]), "image did not resolve to immutable local ID")
        if args.expected_image_id:
            need(args.expected_image_id == inspect["Id"], "local image ID does not match expected ID")
        report["environment"] = {"image_id":inspect["Id"], "repo_digests":inspect.get("RepoDigests") or [],
              "image_selector":args.image, "emulator_expected_version":args.expect_version,
              "aws_cli":shell(["aws","--version"]).stdout.strip(),
              "scenario_sha256":hashlib.sha256((ROOT/"scenario.py").read_bytes()).hexdigest(),
              "runner_sha256":hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
        report["stage"] = "replay"
        first = run_once(1,directory,report["environment"],owner,save)
        if first["cleaned"] is not True:
            return report
        second = run_once(2,directory,report["environment"],owner,save)
        if first["status"] == second["status"] == "pass":
            delta = differences(first["invariants"],second["invariants"])
            if first["run_id"] == second["run_id"]:
                delta.append("distinct_run_ids")
            report["comparison"] = {"match":not delta,"differences":delta}
            report.update(status="fail" if delta else "pass", stage="comparison")
    except (OSError, RuntimeError, subprocess.SubprocessError, ValueError, TypeError, KeyError) as exc:
        report["reason"] = reason(exc)
        if isinstance(exc, subprocess.CalledProcessError):
            (directory/"environment.log").write_text(exc.stderr or "",encoding="utf-8")
    finally:
        save()
    return report

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default=IMAGE)
    parser.add_argument("--expect-version", default="1.0.490")
    parser.add_argument("--expected-image-id")
    args = parser.parse_args()
    if not VERSION.fullmatch(args.expect_version) or (args.expected_image_id and not IMAGE_ID.fullmatch(args.expected_image_id)):
        parser.error("invalid expected emulator version or image ID")
    if args.image != IMAGE and not IMAGE_ID.fullmatch(args.image):
        parser.error("only the local lab tag or an explicit immutable SHA-256 image ID is accepted")
    os.umask(0o077)
    owner = uuid4().hex[:12]
    directory = ROOT/"evidence"/owner
    directory.mkdir(parents=True)
    def interrupt(signum, _):
        raise SystemExit(128+signum)
    signal.signal(signal.SIGTERM, interrupt)
    print(directory, flush=True)
    report = execute(args,directory,owner)
    print(report["status"] + ": " + report["stage"])
    return 0 if report["status"] == "pass" else 1

if __name__ == "__main__":
    raise SystemExit(main())
