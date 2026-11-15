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

- The failure mechanism is a **Python exception**, not process SIGKILL, an in-flight HTTP disconnect, or a power failure. The after-write case models losing the application completion response after a successful S3 write.
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

On 2026-11-16 KST (2026-11-15 UTC), the modified working tree based on `23ad6b1` passed `./run-all.sh --advanced`, including the existing SQS/Kafka scenarios and the new 10-case S3 harness. Final unit suite: 12 passing tests (7 S3 + 5 existing). Python compilation, Bash syntax, and `git diff --check` passed. Runtime: Darwin ARM64, Python 3.14.6, boto3/botocore 1.44.3, Kafka 4.4.0, AWS Emulator 1.0.367. The runner used deliberately invalid AWS profile/endpoint environment variables to check local isolation. This is local evidence; CI for these new commits has not run because nothing has been pushed.
