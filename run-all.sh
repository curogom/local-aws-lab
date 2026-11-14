#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results
.venv/bin/python -c 'import sys; sys.exit("Disable PYTHONOPTIMIZE for this lab" if not __debug__ else 0)'
.venv/bin/python -m unittest -v test_lab
# Only our dedicated containers are touched; the existing AWS Emulator is left alone.
for name in queue-reliability-emulator queue-reliability-kafka; do
 if docker inspect "$name" >/dev/null 2>&1 && [ "$(docker inspect -f '{{.State.Running}}' "$name")" = true ]; then
  echo "$name is already running; stop your lab session before running all tests." >&2; exit 1
 fi
done
cleanup() {
 ./scripts/emulator-stop.sh >/dev/null 2>&1 || true
 if [ "$(docker inspect -f '{{ index .Config.Labels "queue-reliability-lab" }}' queue-reliability-kafka 2>/dev/null)" = true ]; then docker stop queue-reliability-kafka >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT
./scripts/emulator-start.sh
.venv/bin/python lab.py sqs --count 500
.venv/bin/python probe_limit.py
./scripts/emulator-stop.sh
./kafka-start.sh
.venv/bin/python lab.py kafka --count 500
