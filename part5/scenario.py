#!/usr/bin/env python3
"""Part 4: local S3 notification delivery + source/state reconciliation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import time
from urllib.parse import unquote_plus, urlsplit
from uuid import uuid4

ENDPOINT = "https://127.0.0.1:19498"
REGION = "us-east-1"
PREFIX = "incoming/"

def j(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

def digest(data):
    return hashlib.sha256(data).hexdigest()

def attrs(**values):
    return {k: {"N" if isinstance(v, int) else "S": str(v)} for k, v in values.items()}

def event(bucket, key):
    return {"Records": [{"eventName": "ObjectCreated:Put", "s3": {
        "bucket": {"name": bucket}, "object": {"key": key}
    }}]}

def event_keys(payload, bucket):
    if not isinstance(payload, dict) or not isinstance(payload.get("Records"), list):
        return []
    keys = []
    for row in payload["Records"]:
        if not isinstance(row, dict) or not str(row.get("eventName", "")).startswith("ObjectCreated:"):
            continue
        try:
            s3 = row["s3"]
            encoded = s3["object"]["key"]
            if s3["bucket"]["name"] != bucket or not isinstance(encoded, str):
                continue
            key = unquote_plus(encoded, errors="strict")
        except (KeyError, TypeError, ValueError):
            continue
        if key.startswith(PREFIX) and len(key) > len(PREFIX):
            keys.append(key)
    return keys

class ApiError(RuntimeError):
    pass

class Lab:
    def __init__(self, ca, work):
        self.ca = str(ca)
        self.credentials_file = str(ca.with_name("emulator_credentials").resolve(strict=True))
        self.work = work
        self.run_id = uuid4().hex[:12]
        self.bucket = "aws-lab-p4-" + self.run_id
        self.table = "aws-lab-p4-" + self.run_id
        self.queue = None
        self.arn = None

    def aws(self, service, operation, *args):
        env = {"PATH": os.environ["PATH"], "HOME": str(self.work),
               "NO_PROXY": "127.0.0.1", "AWS_EC2_METADATA_DISABLED": "true",
               "AWS_SHARED_CREDENTIALS_FILE": self.credentials_file, "AWS_CONFIG_FILE": os.devnull,
               "AWS_MAX_ATTEMPTS": "1", "AWS_PAGER": "", "AWS_CLI_AUTO_PROMPT": "off"}
        cmd = ["aws", service, operation, "--region", REGION, "--endpoint-url", ENDPOINT,
               "--ca-bundle", self.ca, "--no-cli-pager", "--output", "json",
               "--cli-connect-timeout", "3", "--cli-read-timeout", "5", *args]
        result = subprocess.run(cmd, env=env, text=True, capture_output=True, timeout=12)
        if result.returncode:
            raise ApiError(f"{service}/{operation}: {result.stderr.strip()}")
        return json.loads(result.stdout) if result.stdout.strip() else {}

    def setup(self):
        response = self.aws("sqs", "create-queue", "--queue-name", self.bucket)
        offered = urlsplit(response["QueueUrl"])
        fragments = offered.path.strip("/").split("/")
        if len(fragments) != 2 or not fragments[0].isdigit() or fragments[1] != self.bucket:
            raise ValueError(f"Unexpected queue URL path: {offered.path!r}")
        # Never pass a URL returned by the emulator through to the CLI unvalidated.
        self.queue = ENDPOINT + offered.path
        self.arn = self.aws("sqs", "get-queue-attributes", "--queue-url", self.queue,
                            "--attribute-names", "QueueArn")["Attributes"]["QueueArn"]
        self.aws("s3api", "create-bucket", "--bucket", self.bucket)
        self.aws("dynamodb", "create-table", "--table-name", self.table,
                 "--attribute-definitions", "AttributeName=job_id,AttributeType=S",
                 "--key-schema", "AttributeName=job_id,KeyType=HASH", "--billing-mode", "PAY_PER_REQUEST")
        end = time.monotonic() + 20
        while time.monotonic() < end:
            try:
                table = self.aws("dynamodb", "describe-table", "--table-name", self.table)
            except ApiError as exc:
                if "ResourceNotFoundException" not in str(exc):
                    raise
            else:
                if table["Table"]["TableStatus"] == "ACTIVE":
                    break
            time.sleep(0.5)
        else:
            raise RuntimeError("DynamoDB table did not become ACTIVE before the deadline")


    def notify(self, enabled):
        if enabled:
            policy = {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": {"Service": "s3.amazonaws.com"},
                "Action": "sqs:SendMessage", "Resource": self.arn,
                "Condition": {"ArnEquals": {"aws:SourceArn": "arn:aws:s3:::" + self.bucket}}
            }]}
            self.aws("sqs", "set-queue-attributes", "--queue-url", self.queue,
                     "--attributes", j({"Policy": j(policy)}))
        config = {"QueueConfigurations": [{"Id": "part4-input", "QueueArn": self.arn,
                   "Events": ["s3:ObjectCreated:Put"], "Filter": {"Key": {"FilterRules": [
                       {"Name": "prefix", "Value": PREFIX}]}}}]} if enabled else {}
        self.aws("s3api", "put-bucket-notification-configuration", "--bucket", self.bucket,
                 "--notification-configuration", j(config))

    def upload(self, key, data):
        body = self.work / "input"
        body.write_bytes(data)
        self.aws("s3api", "put-object", "--bucket", self.bucket, "--key", key, "--body", str(body))

    def row(self, key):
        return self.aws("dynamodb", "get-item", "--table-name", self.table, "--consistent-read",
                        "--key", j(attrs(job_id=f"{self.bucket}/{key}"))).get("Item")

    def commit(self, key, source):
        existing = self.row(key)
        if existing:
            return "duplicate"
        output = self.work / "object"
        try:
            self.aws("s3api", "get-object", "--bucket", self.bucket, "--key", key, str(output))
        except ApiError as exc:
            if "NoSuchKey" in str(exc) or "Not Found" in str(exc):
                return "missing_object"
            raise
        data = output.read_bytes()
        item = attrs(job_id=f"{self.bucket}/{key}", source=source,
                     sha256=digest(data), bytes=len(data))
        try:
            self.aws("dynamodb", "put-item", "--table-name", self.table, "--item", j(item),
                     "--condition-expression", "attribute_not_exists(job_id)")
        except ApiError as exc:
            if "ConditionalCheckFailedException" in str(exc):
                return "duplicate"
            raise
        return "committed"

    def enqueue(self, payload):
        return self.aws("sqs", "send-message", "--queue-url", self.queue, "--message-body",
                        j(payload))["MessageId"]

    def drain(self, seconds, target=None):
        seen, controls = [], 0
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            messages = self.aws("sqs", "receive-message", "--queue-url", self.queue,
                                "--max-number-of-messages", "10", "--wait-time-seconds", "1",
                                "--visibility-timeout", "2").get("Messages", [])
            for msg in messages:
                parsed = json.loads(msg["Body"])
                keys = event_keys(parsed, self.bucket)
                if not keys:
                    controls += 1  # e.g. S3 test event, not object delivery
                for key in keys:
                    seen.append({"key": key, "outcome": self.commit(key, "queue"),
                                 "event_source": parsed["Records"][0].get("eventSource"),
                                 "message_id": msg["MessageId"]})
                # Missing-object terminal policy is intentional in this lab; no DLQ claim.
                self.aws("sqs", "delete-message", "--queue-url", self.queue,
                         "--receipt-handle", msg["ReceiptHandle"])
            if target is not None and any(x["key"] == target for x in seen):
                break
        return seen, controls

    def reconcile(self):
        result = self.aws("s3api", "list-objects-v2", "--bucket", self.bucket, "--prefix", PREFIX,
                          "--no-paginate", "--max-keys", "50")
        if result.get("IsTruncated"):
            raise RuntimeError("Tiny-lab scan refuses truncated input")
        return [{"key": obj["Key"], "outcome": self.commit(obj["Key"], "reconciler")}
                for obj in result.get("Contents", []) if not self.row(obj["Key"])]

    def run(self, evidence):
        normal, gap, missing = [PREFIX + x for x in ("normal.txt", "gap.txt", "missing.txt")]
        self.setup()
        evidence["run_id"] = self.run_id
        try:
            self.notify(True)
            evidence["notification_configuration"] = "accepted"
        except ApiError as exc:
            evidence["notification_configuration"] = str(exc)
        self.upload(normal, b"normal payload\n")
        native, controls = self.drain(8, normal)
        evidence["automatic_probe"] = {"observed": any(x["key"] == normal for x in native),
            "observations": native, "control_events": controls, "wait_budget_seconds": 8,
            "manual_send_before_probe": False, "reconcile_before_probe": False}
        if not evidence["automatic_probe"]["observed"]:
            evidence["manual_normal_event_id"] = self.enqueue(event(self.bucket, normal))
            evidence["manual_normal_observations"] = self.drain(8, normal)[0]
        if evidence["notification_configuration"] == "accepted":
            self.notify(False)
        self.upload(gap, b"notification gap\n")
        before, _ = self.drain(3)
        evidence["gap"] = {"simulated_by": "put-object with notification disabled",
                            "observations_before_reconciliation": before,
                            "row_before_reconciliation": self.row(gap)}
        evidence["gap"]["repair"] = self.reconcile()
        evidence["gap"]["row_after_repair"] = self.row(gap)
        baseline = self.row(gap)
        late_ids = [self.enqueue(event(self.bucket, gap)) for _ in range(2)]
        evidence["late_replays"] = {"injected": late_ids, "observed": self.drain(5)[0],
                                    "row_unchanged": baseline is not None and self.row(gap) == baseline}
        missing_id = self.enqueue(event(self.bucket, missing))
        evidence["missing_reference"] = {"injected": missing_id, "observed": self.drain(8, missing)[0],
                                         "row": self.row(missing)}
        scan = self.aws("dynamodb", "scan", "--table-name", self.table, "--consistent-read",
                        "--no-paginate").get("Items", [])
        evidence["final_rows"] = sorted(scan, key=lambda x: x["job_id"]["S"])
        normal_row, gap_row = self.row(normal), self.row(gap)
        replays = evidence["late_replays"]["observed"]
        expected = lambda row, data: bool(row and row.get("sha256") == {"S": digest(data)}
                                         and row.get("bytes") == {"N": str(len(data))})
        evidence["checks"] = {
            "normal_payload_matches": expected(normal_row, b"normal payload\n"),
            "gap_absent_before_reconciliation": evidence["gap"]["row_before_reconciliation"] is None
               and not any(x["key"] == gap for x in before),
            "gap_repaired_from_source": expected(gap_row, b"notification gap\n")
                 and gap_row.get("source") == {"S": "reconciler"},
            "two_injected_late_events_deduplicated": all(any(
                 r["message_id"] == m and r["key"] == gap and r["outcome"] == "duplicate"
                 for r in replays) for m in late_ids) and evidence["late_replays"]["row_unchanged"],
            "missing_object_not_recorded": self.row(missing) is None and any(
                 r["message_id"] == missing_id and r["outcome"] == "missing_object"
                 for r in evidence["missing_reference"]["observed"]),
            "exactly_two_result_rows": len(scan) == 2,
        }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ca", required=True, type=Path)
    parser.add_argument("--emulator-version", required=True)
    parser.add_argument("--image-id", required=True)
    parser.add_argument("--out", type=Path, default=Path("results.json"))
    args = parser.parse_args()
    evidence = {"emulator_version": args.emulator_version, "image_id": args.image_id,
        "endpoint": ENDPOINT, "host": f"{platform.system()} {platform.release()} {platform.machine()}",
        "aws_cli": subprocess.check_output(["aws", "--version"], text=True).strip(),
        "lab_source_sha256": digest(Path(__file__).read_bytes())}
    try:
        with tempfile.TemporaryDirectory(prefix="aws-lab-part4-") as temp:
            Lab(args.ca.resolve(strict=True), Path(temp)).run(evidence)
    except Exception as exc:
        evidence["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        args.out.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n")
    return 0 if all(evidence["checks"].values()) else 1

if __name__ == "__main__":
    raise SystemExit(main())
