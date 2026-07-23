"""Run startup migrations under one cross-process lock."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from pathlib import Path

from db_config import get_database_path


def _lock_timeout_seconds() -> float:
    try:
        return max(1.0, float(os.getenv("MIGRATION_LOCK_TIMEOUT_SECONDS", "120")))
    except ValueError:
        return 120.0


@contextmanager
def migration_file_lock(database_path: str):
    """Serialize migrations for processes that share the SQLite directory."""
    lock_path = Path(f"{database_path}.migration.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+b")
    if lock_path.stat().st_size == 0:
        handle.write(b"\0")
        handle.flush()
    deadline = time.monotonic() + _lock_timeout_seconds()
    locked = False
    try:
        while not locked:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
            except (BlockingIOError, OSError):
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"timed out waiting for migration lock: {lock_path}"
                    )
                time.sleep(0.1)
        yield lock_path
    finally:
        if locked:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def run() -> dict[str, object]:
    database_path = get_database_path()
    database = Path(database_path)
    print(
        "[STARTUP][DATABASE] "
        f"path={database_path} exists={database.exists()} directory={database.parent}"
    )
    with migration_file_lock(database_path) as lock_path:
        print(f"[STARTUP][MIGRATION_LOCK] acquired={lock_path}")
        from migrate_payments_schema import migrate as migrate_payments
        from migrate_household_accounts import migrate as migrate_households

        migrate_payments()
        report = migrate_households(database_path, create_backup=True)
        print(
            "[STARTUP][HOUSEHOLD_MIGRATION] "
            f"database={report['database']} backup={report['backup']} "
            f"accounts_created={report['accounts_created']} "
            f"cards_linked={report['cards_linked']}"
        )
        return report


if __name__ == "__main__":
    run()
