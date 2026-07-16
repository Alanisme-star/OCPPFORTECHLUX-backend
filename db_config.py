import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DATABASE_PATH = BASE_DIR / "ocpp_data.db"


def get_database_path() -> str:
    configured_path = os.getenv("DATABASE_PATH")
    database_path = Path(configured_path) if configured_path else DEFAULT_DATABASE_PATH
    database_path = database_path.expanduser().resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    return str(database_path)
