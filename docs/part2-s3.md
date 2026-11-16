# Part 2 — S3 upload and file-processing retries

This implements the published series promise: **S3 file-processing pipelines and failures/retries after upload**. S3-to-SQS notifications belong to Part 4; durable ownership with DynamoDB belongs to Part 3. No automated notification, durable claim, exactly-once execution, or production performance is claimed here.

## Run

```bash
./setup.sh
./scripts/s3-run.sh
```

This starts/stops only the dedicated `queue-reliability-emulator` on TLS port 9298. It refuses an existing session. The always-on instance at 9297 is untouched. The container is ephemeral and must not contain other work. The SDK and presigned HTTP client use only explicit emulator credentials, the emulator CA, no proxy, and a fixed loopback endpoint. Redirects are rejected. Presigned URLs/credentials are not persisted in reports.

`results/s3-pipeline.json` contains synthetic case results. `examples/results/s3-pipeline.json` is a captured successful run on AWS Emulator 1.0.367 / ARM64 Mac. As elsewhere in this repo, the Dockerfile's `LTS/latest` download is not immutable; the version guard fails closed on drift. No AWS account was used.

## Observed cases

| Case | Observed result |
|---|---|
| CreateBucket / PutObject / HeadObject / GetObject | 25-byte UTF-8 fixture, readback SHA-256 matches |
| DeleteObject then HeadObject | 404 |
| Actual presigned HTTP PUT followed by SDK GET | HTTP 200 and equal bytes |
| Raw S3 same-key overwrite | replacement bytes returned |
| Sequential same request ID / same bytes | same input key reused |
| Sequential same request ID / different bytes | application conflict, original retained |
| Failure before read | no output before retry, one matching output after retry |
| Failure after read / before write | no output before retry, one matching output after retry |
| Failure after write / before response | one output already exists; retries preserve identical bytes/hash |
| Source mutated after upload validation | hash mismatch rejected |

Three processing failure cases each invoke the function once with an injected exception and twice normally. The report records input/output hashes and the count **before retry**, not just the final object count. Request IDs are stable across retries; a new UUID per retry would defeat the conflict demonstration. Processing uses immutable input bytes, a key-derived output path and processor version `v1`. It checks an expected application SHA-256 before writing. ETag is not treated as MD5.

## Boundaries and limits

- The original 10-case harness uses **Python exceptions**. The additional `s3_crash_probe.py` sends real SIGKILL to its own child process after a completed read or write, then retries in a fresh process. Neither harness tests in-flight HTTP disconnection, emulator failure, or a power failure. The after-write case loses the application completion response after a successful S3 write.
- Rewriting one deterministic object proves convergence for this fixture, not a single execution or no duplicate side effects. Versioned buckets are not tested.
- Upload's GET-before-PUT check is sequential and **not atomic**. It does not prevent two workers racing on the same request ID. The hash expectation is caller-provided test state, not a durable ledger. Part 3 addresses ownership/state separately.
- The processor accepts small UTF-8 text fixtures only. Size and SHA-256 are checked in the harness; production MIME validation, upload limits, malware scanning, streaming large files, multipart and browser CORS are not covered.
- Presigned success proves transfer compatibility in this emulator, not IAM, signature enforcement or expiry security. Do not copy this fixture into production unchanged.
- Cleanup deletes only the newly created random-prefix bucket and its own synthetic objects. Report `cleanup: true` means this cleanup returned successfully.

## Sources

- [S3 presigned uploads and same-key replacement](https://docs.aws.amazon.com/AmazonS3/latest/userguide/PresignedUrlUploadObject.html)
- [PutObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
- [HeadObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)
- [AWS Emulator limitations](https://docs.aws.amazon.com/emulator/latest/userguide/what-is-service.html)

No screenshots have yet been taken for Part 2. Reports are synthetic observations, not cloud service guarantees.

## Verification record

On 2026-11-16 KST (2026-11-15 UTC), the modified working tree based on `23ad6b1` passed `./run-all.sh --advanced`, including the existing SQS/Kafka scenarios and the new 10-case S3 harness. Final unit suite: 12 passing tests (7 S3 + 5 existing). Python compilation, Bash syntax, and `git diff --check` passed. Runtime: Darwin ARM64, Python 3.14.6, boto3/botocore 1.44.3, Kafka 4.4.0, AWS Emulator 1.0.367. The runner used deliberately invalid AWS profile/endpoint environment variables to check local isolation. This is local evidence; at that time CI had not run because nothing had been pushed.

## Independent rerun and process-crash validation

The standalone documented command `./scripts/s3-run.sh` was rerun with an invalid `AWS_PROFILE` and cloud endpoint override; all 10 S3 cases passed again. The runner now additionally executes `s3_crash_probe.py`. Its two synthetic cases passed against the isolated local emulator:

| Real process boundary | Child exit | Objects before restart | Fresh retry exit | Final objects |
|---|---|---|---|---|
| Completed GET, before processing/write | SIGKILL (-9) | 0 | 0 | 1 |
| Completed PUT, before function returns | SIGKILL (-9) | 1 | 0 | 1 |

The parent checks the actual signal return code, observes S3 before retry, starts a new Python process for retry, and compares output bytes/hash with a subsequent normal invocation. The already-written result stays byte-identical in the post-write crash case. This is convergence after a real worker crash, not exactly-once execution. The emulator itself remains running throughout the crash test. No forced termination is sent to an unrelated process.

`results/s3-crash-probe.json` records the observations; `examples/results/s3-crash-probe.json` is the captured passing result. The random synthetic bucket is removed by the parent after both cases. The standalone runner then stops its dedicated 9298 instance. The existing 9297 instance remains running. These observations extend the earlier exception-only scope; they do not establish production durability or network-fault behavior.

The expanded `./run-all.sh --advanced` also passed on 2026-11-15 at 15:10 UTC (2026-11-16 KST), including both SIGKILL cases, existing SQS/Kafka scenarios and all 12 unit tests. This rerun used the already-installed virtual environment and Docker build cache, not a clean-machine installation. That rerun did not include remote CI or cloud AWS execution.

## Emulator version refresh

On 2026-11-16 UTC, the first public CI run at `572cc1c` passed the unit job but stopped before integration scenarios because official LTS/latest had changed from 1.0.367 to 1.0.385. The version guard correctly rejected this drift. A fresh ARM64 image was built from the official package, and `EXPECTED_EMULATOR_VERSION=1.0.385 ./run-all.sh --advanced` passed locally, including all 12 unit tests, S3 basic and real SIGKILL probes, and SQS/Kafka scenarios. The default expected version was then updated to 1.0.385. Earlier sample results remain historical 1.0.367 evidence; this change does not make the latest download immutable.
