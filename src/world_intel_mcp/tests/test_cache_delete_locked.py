"""Cache.delete on a locked database: logged and swallowed, as Cache.set
does, never raised into the caller.

The review measured ``sqlite3.OperationalError: database is locked``
escaping Cache.delete (after the 5 s busy timeout) and failing the whole
tool call. Here the wait is set to zero so the test is instant; the
error raised is the same.
"""

import logging
import sqlite3
import time
from pathlib import Path

import pytest

from world_intel_mcp.cache import Cache


def test_delete_on_a_locked_database_is_logged_not_raised(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    cache = Cache(db_path=tmp_path / "locked.db")
    cache.set("k", {"v": 1}, 60)
    cache._get_conn().execute("PRAGMA busy_timeout=0")

    other = sqlite3.connect(str(cache.db_path), isolation_level=None)
    other.execute("BEGIN IMMEDIATE")
    now = time.time()
    other.execute(
        "INSERT INTO cache (key, value, expires_at, created_at) VALUES (?, ?, ?, ?)",
        ("written-meanwhile", "2", now + 60, now),
    )
    try:
        with caplog.at_level(logging.WARNING, logger="world-intel-mcp.cache"):
            cache.delete("k")
    finally:
        other.execute("COMMIT")
        other.close()

    assert "Cache delete failed for k" in caplog.text
    assert "locked" in caplog.text
    assert cache.get("k") == {"v": 1}
    # The connection is still usable once the lock is gone.
    assert cache.get("written-meanwhile") == 2
    cache.delete("k")
    assert cache.get("k") is None
    cache.close()
