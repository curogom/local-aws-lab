#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
mkdir -p results .runtime/emulator
printf 'Python environment ready. Docker engine must be running. Run ./run-all.sh next.\n'
