#!/bin/bash
set -euo pipefail
. "$(dirname "$0")/scripts/session.sh"
cd "$(dirname "$0")"
mkdir -p results
.venv/bin/python -c 'import sys; sys.exit("Disable PYTHONOPTIMIZE for this lab" if not __debug__ else 0)'
.venv/bin/python -m unittest -v test_lab test_scenarios test_s3_pipeline test_runners
# Only our dedicated containers are touched; the existing AWS Emulator is left alone.
if docker inspect queue-reliability-emulator >/dev/null 2>&1; then
 echo 'An emulator session already exists; refusing to replace it.' >&2; exit 1
fi
if docker inspect queue-reliability-kafka >/dev/null 2>&1; then
 if [ "$(docker inspect -f '{{ index .Config.Labels "queue-reliability-lab" }}' queue-reliability-kafka)" != true ] ||
    [ "$(docker inspect -f '{{.State.Running}}' queue-reliability-kafka)" = true ]; then
  echo 'Kafka is running or is not owned by this lab; refusing to replace it.' >&2; exit 1
 fi
fi
LAB_EMULATOR_STARTED=1
LAB_KAFKA_STARTED=0
trap lab_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
./scripts/emulator-start.sh
.venv/bin/python lab.py sqs --count 500
.venv/bin/python probe_limit.py
.venv/bin/python s3_pipeline.py
.venv/bin/python s3_crash_probe.py
if [ "${1:-}" = --advanced ]; then .venv/bin/python sqs_scenarios.py all; fi
./scripts/emulator-stop.sh
LAB_EMULATOR_STARTED=0
LAB_KAFKA_STARTED=1
./kafka-start.sh
.venv/bin/python lab.py kafka --count 500

if [ "${1:-}" = --advanced ]; then .venv/bin/python kafka_scenarios.py --case all; fi

.venv/bin/python record_environment.py
