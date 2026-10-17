"""GDELT request spacing: the 429 back-off and the cross-process floor.

GDELT's DOC API answers 429 "Please limit requests to one every 5 seconds"
(measured 2026-10-17) and limits per IP address. The fetcher's floor
(``_SOURCE_RATE_LIMITS["gdelt"]``) used to be an asyncio lock plus a
timestamp, so it spaced calls inside one process only, while this server
runs as several processes on one host (collector, one MCP server per
client, dashboard, CLI). ``shared_floor`` keeps the last reserved slot in
the shared cache DB. The cloud review of 2026-10-17 (finding M3) also
measured retries of one call going 1 s and 2 s apart on a 429.

Gaps / not covered: processes that resolve different cache DB paths (they
do not coordinate, by design of the shared file); a lock held longer than
the reservation's busy timeout under real write load (simulated only by a
raised error); real GDELT behaviour under the new spacing (no live probe).
"""

import asyncio
import logging
import os
import sqlite3
import subprocess
import sys
import textwrap
import time

import httpx
import pytest
import respx

from world_intel_mcp import fetcher as fetcher_mod
from world_intel_mcp import shared_floor
from world_intel_mcp.sources import news

_GDELT_URL = r".*api\.gdeltproject\.org.*"
_OTHER_PID = os.getpid() + 1
_T = 1_000_000.0


def _seed(db, source: str, slot: float, pid: int) -> None:
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT OR REPLACE INTO source_floor (source, slot, pid) VALUES (?, ?, ?)",
            (source, slot, pid),
        )
        conn.commit()
    finally:
        conn.close()


def test_reserve_defers_only_to_another_process(tmp_path) -> None:
    db = tmp_path / "floor.db"
    assert shared_floor.reserve(db, "gdelt", 5.0, now=_T) == _T
    # This process again: its own in-process floor spaces it, not the row.
    assert shared_floor.reserve(db, "gdelt", 5.0, now=_T + 1) == _T + 1
    # Another process went 1 s ago: this call goes 4 s from now.
    _seed(db, "gdelt", slot=_T + 1, pid=_OTHER_PID)
    assert shared_floor.reserve(db, "gdelt", 5.0, now=_T + 2) == _T + 6
    # Another process holds a slot still ahead: this one queues behind it.
    _seed(db, "gdelt", slot=_T + 20, pid=_OTHER_PID)
    assert shared_floor.reserve(db, "gdelt", 5.0, now=_T + 10) == _T + 25
    # Sources keep separate rows.
    assert shared_floor.reserve(db, "other", 5.0, now=_T + 10) == _T + 10


def test_reserve_ignores_a_slot_far_in_the_future(tmp_path) -> None:
    # A slot beyond MAX_AHEAD_S is a clock jump or a corrupt row, not a
    # queue: honouring it would stall every caller.
    db = tmp_path / "floor.db"
    shared_floor.reserve(db, "gdelt", 5.0, now=_T)
    _seed(db, "gdelt", slot=_T + shared_floor.MAX_AHEAD_S + 50, pid=_OTHER_PID)
    assert shared_floor.reserve(db, "gdelt", 5.0, now=_T + 1) == _T + 1


async def test_source_throttle_waits_out_another_process_slot(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    waits: list[float] = []

    async def _record(delay, *args, **kwargs) -> None:
        waits.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _record)
    shared_floor.reserve(fetcher.cache.db_path, "gdelt", 5.0)
    _seed(fetcher.cache.db_path, "gdelt", slot=time.time() + 2.0, pid=_OTHER_PID)

    await fetcher._source_throttle("gdelt")
    assert len(waits) == 1
    assert 6.5 < waits[0] <= 7.0


async def test_source_throttle_keeps_the_local_floor_when_the_db_fails(
    fetcher, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def _locked(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(shared_floor, "reserve", _locked)
    with caplog.at_level(logging.WARNING, logger="world-intel-mcp.fetcher"):
        await fetcher._source_throttle("gdelt")
    assert any("Cross-process gdelt floor unavailable" in r.message for r in caplog.records)
    assert fetcher_mod._source_last_call["gdelt"] > 0


_CHILD = textwrap.dedent(
    """
    import asyncio, sys, time
    from pathlib import Path
    from world_intel_mcp import fetcher as fetcher_mod
    from world_intel_mcp.cache import Cache
    from world_intel_mcp.circuit_breaker import CircuitBreaker

    db, start, interval = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
    fetcher_mod._SOURCE_RATE_LIMITS["gdelt"] = interval
    f = fetcher_mod.Fetcher(cache=Cache(db_path=Path(db)), breaker=CircuitBreaker())

    async def main():
        while time.time() < start:
            await asyncio.sleep(0.005)
        await f._source_throttle("gdelt")
        print(repr(time.time()))

    asyncio.run(main())
    """
)


def test_gdelt_floor_spaces_calls_across_processes(tmp_path) -> None:
    # Three processes on one cache DB all ask for a GDELT slot at the same
    # instant; each must go at least one interval after the one before.
    db = tmp_path / "shared.db"
    interval = 0.5
    start = time.time() + 3.0
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", _CHILD, str(db), repr(start), str(interval)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=dict(os.environ),
        )
        for _ in range(3)
    ]
    outs = [p.communicate(timeout=60) for p in procs]
    for p, (_, err) in zip(procs, outs):
        assert p.returncode == 0, err
    passed = sorted(float(out.strip()) for out, _ in outs)
    gaps = [b - a for a, b in zip(passed, passed[1:])]
    assert all(g >= interval - 0.05 for g in gaps), gaps


@respx.mock
async def test_a_429_is_retried_no_sooner_than_the_gdelt_floor(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Review M3: one GDELT call against an always-429 server went REQUEST,
    # sleep 1, REQUEST, sleep 2, REQUEST.
    events: list[tuple[str, float]] = []

    async def _record(delay, *args, **kwargs) -> None:
        events.append(("sleep", delay))

    def _answer(request: httpx.Request) -> httpx.Response:
        events.append(("request", 0.0))
        return httpx.Response(429, text="Please limit requests to one every 5 seconds")

    monkeypatch.setattr(asyncio, "sleep", _record)
    respx.get(url__regex=_GDELT_URL).mock(side_effect=_answer)

    result = await news.fetch_gdelt_search(fetcher, "Iran")
    assert result["reason"] == "gdelt_fetch_failed"
    gaps: list[float] = []
    waited: float | None = None
    for kind, delay in events:
        if kind == "request":
            if waited is not None:
                gaps.append(waited)
            waited = 0.0
        elif waited is not None:
            waited += delay
    assert len(gaps) == 2
    assert all(g >= fetcher_mod._SOURCE_RATE_LIMITS["gdelt"] for g in gaps), gaps
