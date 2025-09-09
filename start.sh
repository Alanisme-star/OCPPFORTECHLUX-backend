#!/usr/bin/env bash
set -euo pipefail

# （可選）若你的專案根目錄不是當前路徑，可取消註解這行
# cd /opt/render/project/src

# 部署/重啟時自動補齊 DB schema（可重複執行；失敗不擋服務啟動）
python migrate_payments_schema.py || python3 migrate_payments_schema.py || true

# 啟動後端服務（Render 會提供 $PORT；本地預設 8000）
uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
