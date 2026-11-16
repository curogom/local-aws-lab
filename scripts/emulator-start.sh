#!/bin/bash
set -euo pipefail
. "$(dirname "$0")/session.sh"
cd "$(dirname "$0")/.."
mkdir -p .runtime/emulator results
if docker inspect queue-reliability-emulator >/dev/null 2>&1; then
  echo 'Lab emulator container already exists. Run ./scripts/emulator-stop.sh first.' >&2
  exit 1
fi
docker build -t queue-reliability-emulator:local -f docker/Dockerfile.emulator .
set --
if [ "$(uname -s)" = Linux ]; then set -- --user "$(id -u):$(id -g)"; fi
docker run "$@" -d --rm --name queue-reliability-emulator --label queue-reliability-lab=true \
 --memory=2g --memory-swap=2g -p 127.0.0.1:9298:9298 \
 -v "$PWD/.runtime/emulator:/home/emulator/.aws/emulator" queue-reliability-emulator:local
for i in $(seq 30); do
 if curl -sf --cacert .runtime/emulator/9298/ca.pem https://127.0.0.1:9298/__admin/health; then
  actual=$(docker exec queue-reliability-emulator rpm -q --qf '%{VERSION}' aws-emulator)
  expected=${EXPECTED_EMULATOR_VERSION:-1.0.385}
  if [ "$actual" != "$expected" ]; then
   echo "Emulator version drift: expected $expected, got $actual. Review before updating EXPECTED_EMULATOR_VERSION." >&2
   exit 1
  fi
  printf '\nLab emulator %s ready.\n' "$actual"; exit 0
 fi
 sleep 1
done
echo 'Lab emulator readiness timed out; inspect docker logs queue-reliability-emulator.' >&2
exit 1
