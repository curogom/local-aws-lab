#!/bin/bash
set -euo pipefail
name=queue-reliability-emulator
if docker inspect "$name" >/dev/null 2>&1; then
 test "$(docker inspect -f '{{ index .Config.Labels "queue-reliability-lab" }}' "$name")" = true
 docker stop "$name"
fi
