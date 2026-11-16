#!/bin/bash
set -euo pipefail
. "$(dirname "$0")/session.sh"
cd "$(dirname "$0")/.."
# Never stop an existing session, even if owned by this repository.
if docker inspect queue-reliability-emulator >/dev/null 2>&1; then
 echo 'A dedicated emulator session already exists; refusing to replace it.' >&2
 exit 1
fi
.venv/bin/python -m unittest -v test_s3_pipeline
LAB_EMULATOR_STARTED=1
trap lab_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
./scripts/emulator-start.sh
.venv/bin/python s3_pipeline.py
.venv/bin/python s3_crash_probe.py
