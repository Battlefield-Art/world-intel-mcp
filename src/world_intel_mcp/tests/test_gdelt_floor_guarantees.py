"""What the cross-process GDELT floor guarantees, pinned one guarantee at a time.

Second review of the news signals (2026-10-17): the floor tests left four
mutants alive, and the floor itself let a slot predate its write lock,
let a cancelled wait jump its own queue, honoured slots up to 600 s
ahead and crashed on a row it could not read. Each test below names the
finding it covers. Clock-driven tests advance a fake clock only through
the patched ``asyncio.sleep`` and record the clock at each request, so
they measure request spacing, not requested sleeps.

Gaps / not covered: retries after a multi-attempt failure in one call are
spaced by the per-attempt throttle of the integration branch (its
test_fetcher_retry_spacing.py), not on this branch; real GDELT behaviour
(no network); processes on different cache DB paths (they do not
coordinate, by design).
"""

import asyncio
import logging
import os
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time

import httpx
import pytest
import respx

from world_intel_mcp import fetcher as fetcher_mod
from world_intel_mcp import shared_floor
from world_intel_mcp.analysis import news_signals as ns
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import news
from world_intel_mcp.tests.test_news_signals import _NOW_GDELT, _gdelt_router

_GDELT_URL = r".*api\.gdeltproject\.org.*"
_OTHER_PID = os.getpid() + 1
_T = 1_000_000.0
_FLOOR = fetcher_mod._SOURCE_RATE_LIMITS["gdelt"]
_REAL_SLEEP = asyncio.sleep


def _seed(db, source: str, slot, pid) -> None:
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS source_floor ("
            "source TEXT PRIMARY KEY, slot REAL NOT NULL, pid INTEGER NOT NULL)"
        )
        conn.execute(
            "INSERT OR REPLACE INTO source_floor (source, slot, pid) VALUES (?, ?, ?)",
            (source, slot, pid),
        )
        conn.commit()
    finally:
        conn.close()


def _row(db, source: str = "gdelt"):
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute(
            "SELECT slot, pid FROM source_floor WHERE source = ?", (source,)
        ).fetchone()
    finally:
        conn.close()


class _Clock:
    """Wall clock that moves only when someone sleeps."""

    def __init__(self, start: float) -> None:
        self.now = start
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    async def sleep(self, delay, *args, **kwargs) -> None:
        self.sleeps.append(delay)
        self.now += max(0.0, delay)
        await _REAL_SLEEP(0)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    c = _Clock(time.time())
    monkeypatch.setattr(time, "time", c.time)
    monkeypatch.setattr(asyncio, "sleep", c.sleep)
    return c


def _gaps(stamps: list[float]) -> list[float]:
    return [b - a for a, b in zip(stamps, stamps[1:])]


# ---------------------------------------------------------------------------
# reserve(): the row
# ---------------------------------------------------------------------------


def test_reserve_overwrites_a_slot_far_ahead_as_a_clock_jump(
    tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    # NS-2 / F9: a row 120 s ahead (a backward clock step) was honoured, so
    # every other process slept two minutes before its first GDELT call.
    db = tmp_path / "floor.db"
    _seed(db, "gdelt", slot=_T + 120, pid=_OTHER_PID)
    with caplog.at_level(logging.WARNING, logger="world-intel-mcp.shared_floor"):
        assert shared_floor.reserve(db, "gdelt", 5.0, now=_T) == _T
    assert _row(db) == (_T, os.getpid())
    assert any("clock jump" in r.message for r in caplog.records)
    # A real queue stays inside the bound and is still honoured.
    _seed(db, "gdelt", slot=_T + 30, pid=_OTHER_PID)
    assert shared_floor.reserve(db, "gdelt", 5.0, now=_T) == _T + 35


def test_reserve_after_an_hour_old_row_goes_now(tmp_path) -> None:
    # NS-5 (S2): without max(now, ...) an old row from another process,
    # the steady state of any long-lived cache DB, put the slot in the past.
    db = tmp_path / "floor.db"
    _seed(db, "gdelt", slot=_T - 3600, pid=_OTHER_PID)
    assert shared_floor.reserve(db, "gdelt", 5.0, now=_T) == _T


def test_reserve_keeps_its_own_queued_slot(tmp_path) -> None:
    # NS-3 / F2: a process queued behind another one, its wait cancelled,
    # used to go at once on its next call and rewind the shared row.
    db = tmp_path / "floor.db"
    _seed(db, "gdelt", slot=_T + 2, pid=_OTHER_PID)
    assert shared_floor.reserve(db, "gdelt", 5.0, now=_T) == _T + 7
    assert shared_floor.reserve(db, "gdelt", 5.0, now=_T + 1) == _T + 7
    assert _row(db) == (_T + 7, os.getpid())
    # Once that slot has passed, its own floor governs again.
    assert shared_floor.reserve(db, "gdelt", 5.0, now=_T + 9) == _T + 9


@pytest.mark.parametrize("bad_slot", ["2026-10-17T22:00:00Z", b"\x00"])
def test_reserve_overwrites_an_unreadable_row(
    tmp_path, caplog: pytest.LogCaptureFixture, bad_slot
) -> None:
    # NS-6: a text or blob slot raised ValueError on every GDELT call,
    # forever, since nothing rewrote the row.
    db = tmp_path / "floor.db"
    _seed(db, "gdelt", slot=bad_slot, pid=_OTHER_PID)
    with caplog.at_level(logging.WARNING, logger="world-intel-mcp.shared_floor"):
        assert shared_floor.reserve(db, "gdelt", 5.0, now=_T) == _T
    assert any("Unreadable gdelt floor row" in r.message for r in caplog.records)
    assert _row(db) == (_T, os.getpid())
    assert shared_floor.reserve(db, "gdelt", 5.0, now=_T + 6) == _T + 6


def test_reserve_stamps_the_slot_once_it_holds_the_write_lock(tmp_path) -> None:
    # NS-4 / F1: the slot was the time the caller started waiting for the
    # lock, so the next process was spaced from a moment that had passed.
    db = tmp_path / "floor.db"
    shared_floor.reserve(db, "gdelt", 5.0, now=_T)
    held = threading.Event()
    released: list[float] = []

    def _hold() -> None:
        conn = sqlite3.connect(str(db), isolation_level=None)
        try:
            conn.execute("BEGIN IMMEDIATE")
            held.set()
            time.sleep(1.0)
            released.append(time.time())
            conn.execute("COMMIT")
        finally:
            conn.close()

    holder = threading.Thread(target=_hold)
    holder.start()
    try:
        assert held.wait(5.0)
        asked = time.time()
        slot = shared_floor.reserve(db, "gdelt", 5.0)
    finally:
        holder.join()
    assert slot >= released[0]
    assert slot - asked >= 0.9


_PAUSING_CHILD = textwrap.dedent(
    """
    import sys, time
    from pathlib import Path
    from types import SimpleNamespace
    import sqlite3
    from world_intel_mcp import shared_floor

    db, role, marker, interval = sys.argv[1], sys.argv[2], Path(sys.argv[3]), float(sys.argv[4])
    if role == "slow":
        real = sqlite3.connect

        class _One:
            def __init__(self, row):
                self.row = row

            def fetchone(self):
                return self.row

        class _Pausing:
            def __init__(self, conn):
                self.conn = conn

            def execute(self, sql, *args):
                cur = self.conn.execute(sql, *args)
                if sql.startswith("SELECT"):
                    row = cur.fetchone()
                    marker.touch()
                    time.sleep(1.0)
                    return _One(row)
                return cur

            def close(self):
                self.conn.close()

        shared_floor.sqlite3 = SimpleNamespace(connect=lambda *a, **k: _Pausing(real(*a, **k)))
    else:
        deadline = time.time() + 30
        while not marker.exists():
            if time.time() > deadline:
                sys.exit("slow child never paused")
            time.sleep(0.01)
    print(repr(shared_floor.reserve(db, "gdelt", interval)))
    """
)


def test_begin_immediate_keeps_two_overlapping_reservations_apart(tmp_path) -> None:
    # F3 / NS-5 (S4): with a deferred BEGIN, a reservation made while
    # another sits between its read and its write either reads the same
    # old row or fails with "database is locked"; nothing caught it.
    db = tmp_path / "floor.db"
    marker = tmp_path / "paused"
    interval = 0.2
    shared_floor.reserve(db, "gdelt", interval, now=time.time() - 100)
    procs = []
    try:
        for role in ("slow", "fast"):
            procs.append(subprocess.Popen(
                [sys.executable, "-c", _PAUSING_CHILD, str(db), role, str(marker), str(interval)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            ))
        outs = [p.communicate(timeout=60) for p in procs]
    finally:
        for p in procs:
            if p.poll() is None:
                p.kill()
    for p, (_, err) in zip(procs, outs):
        assert p.returncode == 0, err
    slow, fast = (float(out.strip()) for out, _ in outs)
    # The fast child had to wait for the slow one's write lock.
    assert fast - slow >= 0.9


_THROTTLE_CHILD = textwrap.dedent(
    """
    import asyncio, json, sys, time
    from pathlib import Path
    from world_intel_mcp import fetcher as fetcher_mod
    from world_intel_mcp import shared_floor
    from world_intel_mcp.cache import Cache
    from world_intel_mcp.circuit_breaker import CircuitBreaker

    db, start, interval = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
    fetcher_mod._SOURCE_RATE_LIMITS["gdelt"] = interval
    slots = []
    real_reserve = shared_floor.reserve

    def _recording(*args, **kwargs):
        slots.append(real_reserve(*args, **kwargs))
        return slots[-1]

    shared_floor.reserve = _recording
    f = fetcher_mod.Fetcher(cache=Cache(db_path=Path(db)), breaker=CircuitBreaker())

    async def main():
        ready = time.time()
        while time.time() < start:
            await asyncio.sleep(0.005)
        await f._source_throttle("gdelt")
        print(json.dumps({"ready": ready, "slot": slots[0], "went": time.time()}))

    asyncio.run(main())
    """
)


def test_floor_holds_across_processes_while_another_writer_holds_the_lock(tmp_path) -> None:
    # NS-5 / F12: four processes ask at one instant while a cache write
    # holds the DB lock. Judged from the reserved slots, not wall gaps: each
    # slot is at least one interval after the one before, none predates the
    # lock's release, and no process fell back to its local floor.
    import json

    db = tmp_path / "shared.db"
    interval = 0.5
    # Switch the DB to WAL here, as a running deployment already is: four
    # children making the switch at once is a startup race (measured: one
    # child in four runs died "database is locked" in PRAGMA journal_mode).
    from world_intel_mcp.cache import Cache

    Cache(db_path=db).close()
    shared_floor.reserve(db, "gdelt", interval, now=time.time() - 100)
    start = time.time() + 4.0
    released: list[float] = []
    held = threading.Event()

    def _hold() -> None:
        conn = sqlite3.connect(str(db), isolation_level=None)
        try:
            while time.time() < start - 0.2:
                time.sleep(0.01)
            conn.execute("BEGIN IMMEDIATE")
            held.set()
            time.sleep(1.2)
            released.append(time.time())
            conn.execute("COMMIT")
        finally:
            conn.close()

    holder = threading.Thread(target=_hold)
    holder.start()
    procs = []
    try:
        for _ in range(4):
            procs.append(subprocess.Popen(
                [sys.executable, "-c", _THROTTLE_CHILD, str(db), repr(start), str(interval)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            ))
        outs = [p.communicate(timeout=60) for p in procs]
    finally:
        for p in procs:
            if p.poll() is None:
                p.kill()
        holder.join()
    assert held.is_set()
    for p, (_, err) in zip(procs, outs):
        assert p.returncode == 0, err
        assert "floor unavailable" not in err, err
    runs = sorted((json.loads(out) for out, _ in outs), key=lambda r: r["slot"])
    # Every child was waiting before the first slot, so the race was real.
    assert all(r["ready"] < start for r in runs), runs
    assert all(r["slot"] >= released[0] for r in runs), (runs, released)
    assert all(g >= interval - 1e-9 for g in _gaps([r["slot"] for r in runs])), runs
    # asyncio.sleep runs on the monotonic clock: allow 10 ms of drift.
    assert all(r["went"] >= r["slot"] - 0.01 for r in runs), runs


# ---------------------------------------------------------------------------
# The fetcher's use of the floor
# ---------------------------------------------------------------------------


async def test_a_far_ahead_row_costs_at_most_one_interval(fetcher, clock: _Clock) -> None:
    # NS-2: the wait the caller sees, end to end through the fetcher.
    shared_floor.reserve(fetcher.cache.db_path, "gdelt", _FLOOR)
    _seed(fetcher.cache.db_path, "gdelt", slot=clock.now + 120, pid=_OTHER_PID)
    await fetcher._source_throttle("gdelt")
    assert sum(clock.sleeps) <= _FLOOR


async def test_local_floor_counts_from_when_the_call_went(fetcher, clock: _Clock) -> None:
    # F4 / NS-5 (S8): stamping the in-process floor before the shared wait
    # spaced this process's next call from when the first one started
    # waiting, so the two went 0 s apart.
    _seed(fetcher.cache.db_path, "gdelt", slot=clock.now + 2, pid=_OTHER_PID)
    went = []
    for _ in range(2):
        await fetcher._source_throttle("gdelt")
        went.append(clock.now)
    assert went[1] - went[0] >= _FLOOR


async def test_a_cancelled_wait_still_holds_its_place_in_the_queue(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # NS-3 / F2 end to end, on the real clock: a caller's timeout cancels
    # the wait behind another process; the next call waits for the slot
    # this process had queued for, not 0 s.
    interval = 0.5
    monkeypatch.setitem(fetcher_mod._SOURCE_RATE_LIMITS, "gdelt", interval)
    other_slot = time.time() + 1.0
    _seed(fetcher.cache.db_path, "gdelt", slot=other_slot, pid=_OTHER_PID)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(fetcher._source_throttle("gdelt"), 0.2)
    await fetcher._source_throttle("gdelt")
    assert time.time() >= other_slot + interval


async def test_source_throttle_falls_back_when_the_cache_file_is_not_a_database(
    fetcher, monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    # NS-5 (S6): narrowing the fetcher's except to OperationalError made a
    # corrupt cache file raise DatabaseError on every GDELT call.
    junk = tmp_path / "junk.db"
    junk.write_bytes(b"this is not an sqlite database " * 200)
    monkeypatch.setattr(fetcher.cache, "db_path", junk)
    with caplog.at_level(logging.WARNING, logger="world-intel-mcp.fetcher"):
        await fetcher._source_throttle("gdelt")
    assert any(
        "Cross-process gdelt floor unavailable" in r.message and "not a database" in r.message
        for r in caplog.records
    )
    assert fetcher_mod._source_last_call["gdelt"] > 0


@respx.mock
async def test_429_retries_are_spaced_by_request_time(fetcher, clock: _Clock) -> None:
    # F10: the clock at each request, not the sleeps asked for.
    sent: list[float] = []
    answers = iter([429, 429, 200])

    def _answer(request: httpx.Request) -> httpx.Response:
        sent.append(clock.now)
        status = next(answers)
        if status == 429:
            return httpx.Response(429, text="Please limit requests to one every 5 seconds")
        return httpx.Response(200, json={"articles": []})

    respx.get(url__regex=_GDELT_URL).mock(side_effect=_answer)
    await news.fetch_gdelt_search(fetcher, "Iran")
    assert len(sent) == 3
    assert min(_gaps(sent)) >= _FLOOR, sent


@respx.mock
async def test_timeline_requests_are_spaced_and_the_retry_waits_its_pause(
    fetcher, clock: _Clock
) -> None:
    # F10 and F5 (M08): every timeline request follows the one before by at
    # least the floor, and the retry pass's request by its full pause.
    # One attempt per call, so each request is its own fetcher call.
    single = Fetcher(cache=fetcher.cache, breaker=CircuitBreaker(), max_retries=0)
    route, calls = _gdelt_router(fail_times={"sanction": 1})
    sent: list[tuple[str, float]] = []

    def _answer(request: httpx.Request) -> httpx.Response:
        response = route(request)
        sent.append((calls[-1][0], clock.now))
        return response

    respx.get(url__regex=_GDELT_URL).mock(side_effect=_answer)
    r = await ns.fetch_hostility_timeline(single, "Iran", "Israel", days=3, now=_NOW_GDELT)
    assert r["column_status"]["sanction"] == "ok"
    assert [c for c, _ in sent] == [
        "articles", "threat", "sanction", "force", "criticism", "tone", "sanction"
    ]
    stamps = [t for _, t in sent]
    assert min(_gaps(stamps)) >= _FLOOR, stamps
    assert stamps[-1] - stamps[-2] >= ns._GDELT_RETRY_WAIT_S, stamps


@respx.mock
async def test_timeline_retry_pass_is_skipped_while_the_breaker_is_open(
    fetcher, clock: _Clock
) -> None:
    # F5 (M10): three failed columns open the GDELT breaker; the retry pass
    # must not pause and ask again, and says why it did not.
    route, calls = _gdelt_router(fail={"threat", "sanction", "force"})
    respx.get(url__regex=_GDELT_URL).mock(side_effect=route)
    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    assert any(g.startswith("retry pass skipped: the GDELT circuit breaker is open")
               for g in r["data_gaps"]), r["data_gaps"]
    assert ns._GDELT_RETRY_WAIT_S not in clock.sleeps
    # articles once, three attempts for each failing column, nothing after.
    assert len(calls) == 1 + 3 * 3
    assert r["column_status"]["criticism"] == "failed"
    assert r["column_status"]["tone"] == "failed"
