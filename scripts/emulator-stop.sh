#!/bin/bash
set -euo pipefail
. "$(dirname "$0")/session.sh"
name=queue-reliability-emulator
lab_stop_owned "$name"
