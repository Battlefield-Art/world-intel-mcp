"""Opening a new cache database from several connections at once.

The switch to WAL needs an exclusive lock, and SQLite answers the losers
of a simultaneous first open with "database is locked" without waiting.
Before the retry, 8 processes opening one fresh file crashed 29 to 67
times in 80 to 160 opens, and the default-path cache treated that as an
unusable path and fell back to the temp dir.

Gaps / not covered: the stress test uses threads in one process, which
hit the race less often than separate processes do (measured about 3 to
4 failed opens per 1,600 with the old code, against 67 per 160 for 8
processes); a lock held past the retry window is tested with a shortened
window, not the real 10 s.
"""

import pathlib
import sqlite3
import threading

import pytest

from world_intel_mcp import cache as cache_mod
from world_intel_mcp.cache import Cache


class _LockedSwitch(sqlite3.Connection):
    """A connection whose first WAL switches report a held lock."""

    failures = 0
    message = "database is locked"

    def execute(self, sql, *args):
        if sql == "PRAGMA journal_mode=WAL" and type(self).failures > 0:
            type(self).failures -= 1
            raise sqlite3.OperationalError(type(self).message)
        return super().execute(sql, *args)


@pytest.fixture
def locked_connect(monkeypatch):
    real_connect = sqlite3.connect

    def connect(*args, **kwargs):
        return real_connect(*args, factory=_LockedSwitch, **kwargs)

    monkeypatch.setattr(cache_mod.sqlite3, "connect", connect)
    monkeypatch.setattr(cache_mod.time, "sleep", lambda _s: None)
    _LockedSwitch.message = "database is locked"
    yield _LockedSwitch
    _LockedSwitch.failures = 0


def test_wal_switch_retries_a_held_lock(tmp_path, locked_connect):
    locked_connect.failures = 3
    c = Cache(db_path=tmp_path / "c.db")
    try:
        assert locked_connect.failures == 0
        assert c.fell_back is False
        assert c._get_conn().execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        c.set("k", {"v": 1}, 60)
        assert c.get("k") == {"v": 1}
    finally:
        c.close()


def test_wal_switch_raises_other_errors_at_once(tmp_path, locked_connect):
    locked_connect.failures = 1
    locked_connect.message = "disk I/O error"
    with pytest.raises(sqlite3.OperationalError, match="disk I/O error"):
        Cache(db_path=tmp_path / "c.db")
    assert locked_connect.failures == 0


def test_wal_switch_gives_up_when_the_lock_outlasts_the_window(
    tmp_path, locked_connect, monkeypatch
):
    monkeypatch.setattr(cache_mod, "_WAL_RETRY_SECONDS", 0.0)
    locked_connect.failures = 1_000
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        Cache(db_path=tmp_path / "c.db")
    assert locked_connect.failures == 999


def test_simultaneous_first_opens_all_succeed(tmp_path):
    threads_per_round, rounds = 16, 200
    errors: list[BaseException] = []
    for r in range(rounds):
        path = pathlib.Path(tmp_path / f"round-{r}.db")
        barrier = threading.Barrier(threads_per_round)

        def open_once() -> None:
            barrier.wait()
            try:
                Cache(db_path=path).close()
            except Exception as exc:  # collected and asserted below
                errors.append(exc)

        threads = [threading.Thread(target=open_once) for _ in range(threads_per_round)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    assert errors == []
