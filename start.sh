#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${DATABASE_PATH:-}" && -n "${RENDER:-}" ]]; then
  echo "DATABASE_PATH must point to a Render Persistent Disk." >&2
  exit 1
fi

if command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
else
  echo "Neither python nor python3 is available." >&2
  exit 1
fi

# Any migration or lock failure stops application startup.
"${PYTHON_BIN}" run_startup_migrations.py

uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
