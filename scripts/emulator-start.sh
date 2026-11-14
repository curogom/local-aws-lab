#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .runtime/emulator results
if docker inspect queue-reliability-emulator >/dev/null 2>&1; then
  echo 'Lab emulator container already exists. Run ./scripts/emulator-stop.sh first.' >&2
  exit 1
fi
docker build -t queue-reliability-emulator:local -f docker/Dockerfile.emulator .
docker run -d --rm --name queue-reliability-emulator --label queue-reliability-lab=true \
 --memory=2g --memory-swap=2g -p 127.0.0.1:9298:9298 \
 -v "$PWD/.runtime/emulator:/home/emulator/.aws/emulator" queue-reliability-emulator:local
for i in $(seq 30); do
 if curl -sf --cacert .runtime/emulator/9298/ca.pem https://127.0.0.1:9298/__admin/health; then
  printf '\nLab emulator ready.\n'; exit 0
 fi
 sleep 1
done
echo 'Lab emulator readiness timed out; inspect docker logs queue-reliability-emulator.' >&2
exit 1
