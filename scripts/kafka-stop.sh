#!/bin/bash
set -euo pipefail
. "$(dirname "$0")/session.sh"
lab_stop_owned queue-reliability-kafka
