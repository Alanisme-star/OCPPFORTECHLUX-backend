#!/usr/bin/env bash
set -euo pipefail

# （可選）若你的專案根目錄不是當前路徑，可取消註解這行
# cd /opt/render/project/src

if [[ -n "${DATABASE_PATH:-}" ]]; then
  echo "DATABASE_PATH is set: ${DATABASE_PATH}"
else
  echo "DATABASE_PATH is not set; using the project default."
fi

if command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
else
  echo "Neither python nor python3 is available." >&2
  exit 1
fi

# 部署/重啟時自動補齊 DB schema（可重複執行；失敗會停止部署）
"${PYTHON_BIN}" migrate_payments_schema.py

# 啟動後端服務（Render 會提供 $PORT；本地預設 8000）
uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
