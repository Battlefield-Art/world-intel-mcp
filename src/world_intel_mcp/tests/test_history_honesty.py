"""History honesty: stale-cache attribution, live-success health, caps,
lasting write-failure records and the rules the run log must keep.

The stale-cache tests drive a real ``Fetcher`` with a real ``Cache`` and
real source modules (``fetch_earthquakes``, ``fetch_market_quotes``)
against respx, because the failure they guard is a source module
rebuilding its dict and dropping the fetcher's ``_stale`` marker: a
hand-built ``{"_stale": True}`` payload would not exercise it. The health
tests then feed ``SourceOutcome`` values with the same ``stale_serves``
the collector attaches, at fixed times.
"""

import asyncio
import gc
import json
import os
import sqlite3
import stat
import threading
import time
import types
import warnings
from urllib.parse import unquote

import httpx
import pytest
import respx

from world_intel_mcp import cli, collector, history
from world_intel_mcp import fetcher as fetcher_mod
from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import StaleServe, recording_stale_serves
from world_intel_mcp.history import HistoryStore, Observation, SourceOutcome

from .test_history import T0, _history_tools, _importer, _invoke, _kp, _record, _VS

_USGS = "https://earthquake.usgs.gov/fdsnws/event/1/query"
_YAHOO = r"https://query1\.finance\.yahoo\.com/v8/finance/chart/.*"


@pytest.fixture(autouse=True)
def _default_history_env(monkeypatch):
    for var in (
        "WORLD_INTEL_HISTORY_DAYS",
        "WORLD_INTEL_HISTORY_PAYLOAD_DAYS",
        "WORLD_INTEL_HISTORY_CAPTURE_DAYS",
        "WORLD_INTEL_HISTORY_CADENCE_S",
        "WORLD_INTEL_HISTORY_CADENCES",
        "WORLD_INTEL_HISTORY_MAX_BODY_BYTES",
        "WORLD_INTEL_HISTORY_REPLAY_MAX_BYTES",
    ):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def store(tmp_path):
    s = HistoryStore(tmp_path / "history.db")
    yield s
    s.close()


@pytest.fixture
def cli_db(tmp_path, monkeypatch):
    """A cache DB for CLI commands; closes the cache the CLI opens on it."""
    db = tmp_path / "cli.db"
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(db))
    monkeypatch.setattr(cli, "_fetcher", None)
    yield db
    if cli._fetcher is not None:
        cli._fetcher.cache.close()


def _stale(age: float = 812.0) -> tuple:
    return (StaleServe("firms", "wildfire:firms:world", age),)


def _replay(store, source, at, payload, age: float = 812.0) -> dict:
    """Record a capture the fetcher answered from expired cache."""
    return store.record_cycle(
        [SourceOutcome(source, at, 1.0, payload, stale_serves=_stale(age))], now=at
    )


def _quotes(*prices) -> dict:
    return {"quotes": [{"symbol": f"^Q{i}", "price": p} for i, p in enumerate(prices)]}


FIRES = {"fires": [1]}


# ---------------------------------------------------------------------------
# H1: stale-cache serves are attributed to the collector source
# ---------------------------------------------------------------------------


def _geojson(magnitude: float) -> dict:
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "us7000hst1",
                "properties": {"mag": magnitude, "place": "Test Ridge", "time": 1_760_000_000_000},
                "geometry": {"type": "Point", "coordinates": [-120.5, 35.2, 10.0]},
            }
        ],
    }


def _real_fetcher(tmp_path, monkeypatch) -> fetcher_mod.Fetcher:
    # No retries and no rate floors: the test controls every response.
    monkeypatch.delitem(fetcher_mod._SOURCE_RATE_LIMITS, "usgs")
    monkeypatch.delitem(fetcher_mod._SOURCE_RATE_LIMITS, "yahoo-finance")
    monkeypatch.setattr(fetcher_mod, "_YAHOO_MIN_INTERVAL", 0.0)
    cache = Cache(db_path=tmp_path / "cache.db")
    return fetcher_mod.Fetcher(
        cache=cache, breaker=CircuitBreaker(), default_timeout=5.0, max_retries=0
    )


def _expire_cache(db_path, key: str | None = None) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        with conn:
            if key is None:
                conn.execute("UPDATE cache SET expires_at = 0")
            else:
                conn.execute("UPDATE cache SET expires_at = 0 WHERE key = ?", (key,))
    finally:
        conn.close()


@respx.mock
async def test_stale_serve_through_a_real_source_is_a_stale_capture(tmp_path, monkeypatch):
    """USGS answers, the cached copy expires, then USGS returns 503. The
    fetcher serves the expired copy and fetch_earthquakes rebuilds its dict
    without the _stale marker. The capture must still read as a stale-cache
    replay, and health must keep the live capture as the last success."""
    real = _real_fetcher(tmp_path, monkeypatch)
    route = respx.get(_USGS)
    route.mock(return_value=httpx.Response(200, json=_geojson(5.2)))
    await collector.collect_once(real, _VS(), source_filter={"earthquakes"})

    _expire_cache(real.cache.db_path)
    route.mock(return_value=httpx.Response(503))
    summary = await collector.collect_once(real, _VS(), source_filter={"earthquakes"})
    await real.close()
    real.cache.close()
    assert route.call_count == 2  # the second cycle did try USGS live
    assert summary["history"]["status_counts"] == {"ok": 0, "degraded": 1, "error": 0}

    store = HistoryStore(real.cache.db_path)
    try:
        stale, live = store.capture_history("earthquakes")["captures"]
        health = store.collection_health(["earthquakes"])["sources"][0]
    finally:
        store.close()
    assert live["status"] == "ok" and live["observations_stored"] == 1
    assert stale["status"] == "degraded"
    assert stale["reason"].startswith(history.STALE_REASON)
    assert stale["change"] == "unchanged"  # same upstream data, replayed
    assert stale["observations_stored"] == 0
    assert health["state"] == "stale"
    assert health["last_success_at"] == live["collected_at"]
    assert health["last_ok_at"] == live["collected_at"]
    assert health["consecutive_failures"] == 1


@respx.mock
async def test_stale_serve_inside_a_gathered_sub_fetch_is_attributed(tmp_path, monkeypatch):
    """fetch_market_quotes gathers one Yahoo fetch per symbol in sub-tasks;
    a stale serve in one of them still lands in market_quotes' log, and a
    concurrently running source that stayed live is not marked."""
    real = _real_fetcher(tmp_path, monkeypatch)
    failing = {"symbol": None}

    def _chart(request):
        if unquote(request.url.path.rsplit("/", 1)[-1]) == failing["symbol"]:
            return httpx.Response(503)
        return httpx.Response(200, json={"chart": {"result": [{"meta": {
            "regularMarketPrice": 100.0, "previousClose": 99.0, "currency": "USD",
        }}]}})

    respx.get(url__regex=_YAHOO).mock(side_effect=_chart)
    respx.get(_USGS).mock(return_value=httpx.Response(200, json=_geojson(5.2)))
    both = {"market_quotes", "earthquakes"}
    first = await collector.collect_once(real, _VS(), source_filter=both)
    assert first["history"]["status_counts"] == {"ok": 2, "degraded": 0, "error": 0}

    _expire_cache(real.cache.db_path, "markets:quotes:^N225")
    _expire_cache(real.cache.db_path, "seismology:quakes:4.5:24")
    failing["symbol"] = "^N225"
    await collector.collect_once(real, _VS(), source_filter=both)
    await real.close()
    real.cache.close()

    store = HistoryStore(real.cache.db_path)
    try:
        quotes = store.capture_history("market_quotes")["captures"][0]
        quakes = store.capture_history("earthquakes")["captures"][0]
    finally:
        store.close()
    assert quotes["status"] == "degraded"
    assert quotes["reason"].startswith(history.STALE_REASON)
    assert (quotes["observations_extracted"], quotes["observations_stored"]) == (7, 0)
    assert (quakes["status"], quakes["reason"]) == ("ok", None)


def test_stale_serve_log_is_task_scoped_and_nests(tmp_path) -> None:
    cache = Cache(db_path=tmp_path / "c.db")
    real = fetcher_mod.Fetcher(cache=cache, breaker=CircuitBreaker())
    cache.set("k:dict", {"v": 1}, ttl_seconds=-10)
    cache.set("k:list", [1, 2], ttl_seconds=-10)

    async def _serve(key):
        with recording_stale_serves() as served:
            await asyncio.sleep(0)
            real._stale_fallback(key, "src")
            await asyncio.sleep(0)
            return [s.cache_key for s in served]

    async def _main():
        with recording_stale_serves() as outer:
            a, b = await asyncio.gather(_serve("k:dict"), _serve("k:list"))
            real._stale_fallback("k:missing", "src")  # nothing cached: no serve
        return a, b, [s.cache_key for s in outer], fetcher_mod._STALE_SERVE_LOGS.get()

    a, b, outer, after = asyncio.run(_main())
    cache.close()
    assert a == ["k:dict"] and b == ["k:list"]  # each task sees only its own
    assert sorted(outer) == ["k:dict", "k:list"]  # the outer block sees both
    assert after == ()  # the block reset its log on exit


def test_several_stale_serves_and_a_bare_top_level_marker_read_stale(store) -> None:
    serves = tuple(StaleServe("polymarket", f"k{i}", age) for i, age in enumerate((30.0, 812.5)))
    store.record_cycle([
        SourceOutcome("prediction_markets", T0, 1.0, {"markets": [1]}, stale_serves=serves),
        SourceOutcome("gas_prices", T0, 1.0, {"prices": [1], "_stale": True}),
    ], now=T0)
    for source in ("prediction_markets", "gas_prices"):
        (cap,) = store.capture_history(source)["captures"]
        assert cap["status"] == "degraded"
        assert cap["reason"].startswith(history.STALE_REASON)


# ---------------------------------------------------------------------------
# M1: health counts success only from live captures
# ---------------------------------------------------------------------------


def test_stale_captures_do_not_clear_overdue_or_the_success_time(store) -> None:
    now = T0 + 10_000
    _record(store, "wildfires", now - 5000, FIRES)
    for ago in (600, 300, 10):
        _replay(store, "wildfires", now - ago, FIRES)
    row = store.collection_health(["wildfires"], now=now)["sources"][0]
    assert row["state"] == "stale"
    assert row["overdue"] is True and row["attempt_overdue"] is False
    assert row["last_success_at"] == history._iso(now - 5000)
    assert row["last_success_age_s"] == 5000.0
    assert (row["last_ok_at"], row["last_ok_age_s"]) == (history._iso(now - 5000), 5000.0)
    assert row["consecutive_failures"] == 3
    assert row["last_attempt_age_s"] == 10.0
    assert row["last_reason"].startswith(history.STALE_REASON)


def test_a_live_degraded_capture_still_counts_as_success(store) -> None:
    now = T0 + 10_000
    _record(store, "wildfires", now - 5000, FIRES)
    _record(store, "wildfires", now - 10, {"fires": [], "count": 0})
    row = store.collection_health(["wildfires"], now=now)["sources"][0]
    assert (row["state"], row["overdue"], row["consecutive_failures"]) == ("degraded", False, 0)
    assert row["last_success_age_s"] == 10.0
    assert row["last_ok_age_s"] == 5000.0


def test_consecutive_failures_reset_only_by_a_live_capture(store) -> None:
    """ok, error, ok, error reads 1 (counted from the LAST live capture);
    a stale replay does not reset the count."""
    for i, (payload, error) in enumerate([(FIRES, None), (None, "boom"), (FIRES, None), (None, "boom")]):
        _record(store, "wildfires", T0 + i, payload, error)
    row = store.collection_health(["wildfires"], now=T0 + 5)["sources"][0]
    assert row["consecutive_failures"] == 1
    _replay(store, "wildfires", T0 + 6, FIRES)
    _record(store, "wildfires", T0 + 7, None, "boom")
    row = store.collection_health(["wildfires"], now=T0 + 8)["sources"][0]
    assert (row["state"], row["consecutive_failures"]) == ("failing", 3)


def test_states_and_fields_are_explained(store) -> None:
    _replay(store, "wildfires", T0, FIRES)
    health = store.collection_health(["wildfires"], now=T0)
    assert {r["state"] for r in health["sources"]} == {"stale"}
    assert set(health["states_explained"]) == {
        "ok", "degraded", "stale", "failing", "not_attempted_recently", "never_run"
    }
    assert "stale-cache replay" in health["fields_explained"]["last_success_at"]


def test_cli_health_shows_stale_overdue_and_last_ok(cli_db, monkeypatch) -> None:
    monkeypatch.setenv("COLUMNS", "260")
    now = time.time()
    s = HistoryStore(cli_db)
    _record(s, "wildfires", now - 5000, FIRES)
    _replay(s, "wildfires", now - 30, FIRES)
    s.close()
    out = _invoke("history", "health").output
    row = next(line for line in out.splitlines() if line.strip().startswith("wildfires"))
    assert "stale, overdue" in row
    assert row.count(history._iso(now - 5000)) == 2  # Last success and Last ok
    assert "Last ok" in out
    assert "served stale cache (age 812.0 s)" in row


# ---------------------------------------------------------------------------
# M2: a stale replay is not a new observation
# ---------------------------------------------------------------------------


def test_stale_replay_stores_no_observations(store) -> None:
    _record(store, "market_quotes", T0 + 10, _quotes(100.0))
    _record(store, "market_quotes", T0 + 20, _quotes(101.0))
    _replay(store, "market_quotes", T0 + 30, _quotes(100.0))
    store.record_cycle([SourceOutcome("market_quotes", T0 + 40, 1.0,
                                      dict(_quotes(99.0), _stale=True))], now=T0 + 40)
    points = store.series_history("market_quotes:^Q0:price")["observations"]
    assert [p["value"] for p in points] == [101.0, 100.0]  # newest first
    caps = store.capture_history("market_quotes")["captures"]
    for cap in caps[:2]:
        assert (cap["observations_extracted"], cap["observations_stored"]) == (1, 0)
        assert "observations not stored: a stale-cache replay" in cap["reason"]


def test_a_live_partial_capture_is_degraded_and_keeps_its_points(store) -> None:
    """6 of the 7 default index quotes answer: degraded as partial, and the
    six live points are stored."""
    symbols = history._DEFAULT_INDEX_SYMBOLS
    quotes = [{"symbol": sym, "price": 100.0 + i} for i, sym in enumerate(symbols)]
    _record(store, "market_quotes", T0, {"quotes": quotes[:-1]})
    (cap,) = store.capture_history("market_quotes")["captures"]
    assert cap["status"] == "degraded"
    assert cap["reason"] == "partial: 6 of 7 expected series"
    assert cap["observations_stored"] == 6
    _record(store, "market_quotes", T0 + 300, {"quotes": quotes})
    assert store.capture_history("market_quotes")["captures"][0]["status"] == "ok"
    commodities = {"commodities": [{"symbol": "GC=F", "name": "Gold", "price": 1.0}]}
    _record(store, "commodity_quotes", T0, commodities)
    assert store.capture_history("commodity_quotes")["captures"][0]["reason"] == (
        "partial: 1 of 8 expected series"
    )


# ---------------------------------------------------------------------------
# M3 + L3 + L5: replay says whose body it is, and both sizes are capped
# ---------------------------------------------------------------------------


def test_replay_of_a_deduped_capture_names_the_capture_that_stored_the_body(store) -> None:
    first = dict(_kp(("2026-10-17T00:00:00Z", 2.0)), timestamp="2026-10-17T16:00:00Z")
    _record(store, "space_weather", T0, first)
    _record(store, "space_weather", T0 + 300, dict(first, timestamp="2026-10-17T16:05:00Z"))
    own = store.capture_history("space_weather", capture_id=1)["capture"]
    assert "payload_note" not in own and "payload_stored_at" not in own
    later = store.capture_history("space_weather", capture_id=2)["capture"]
    assert later["payload"]["timestamp"] == "2026-10-17T16:00:00Z"
    assert later["payload_stored_at"] == history._iso(T0)
    assert later["payload_note"].startswith(f"body stored by the capture at {history._iso(T0)}")
    assert "timestamp" in later["payload_note"] and "_stale_age_seconds" in later["payload_note"]


def test_fresh_then_stale_flip_of_the_same_data_is_unchanged(store) -> None:
    data = {"prices": [3.1], "region": "US"}
    _record(store, "gas_prices", T0, data)
    _record(store, "gas_prices", T0 + 300, dict(data, _stale=True, _stale_age_seconds=300.0))
    caps = store.capture_history("gas_prices")["captures"]
    assert [c["change"] for c in caps] == ["unchanged", "first"]
    assert store.storage_stats()["payload_bodies"] == 1


def test_body_over_the_cap_keeps_its_hash_and_says_why(store, monkeypatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_HISTORY_MAX_BODY_BYTES", "1000")
    big = {"rows": ["x" * 50] * 40}
    raw = len(json.dumps(big, separators=(",", ":")))
    summary = _record(store, "news_feed", T0, big)
    _record(store, "news_feed", T0 + 300, big)
    assert summary["payloads_stored"] == 0
    caps = store.capture_history("news_feed")["captures"]
    assert [c["change"] for c in caps] == ["unchanged", "first"]  # the hash still dates changes
    for cap in caps:
        assert cap["status"] == "ok"  # the fetch was fine; only the body was not kept
        assert cap["payload_hash"] is not None and cap["body_retained"] is False
        assert cap["reason"] == (
            f"payload body not stored: {raw:,} bytes of JSON, over the 1,000-byte cap "
            "(WORLD_INTEL_HISTORY_MAX_BODY_BYTES)"
        )
    replay = store.capture_history("news_feed", capture_id=1)["capture"]
    assert replay["payload"] is None
    assert "over WORLD_INTEL_HISTORY_MAX_BODY_BYTES" in replay["payload_note"]
    monkeypatch.setenv("WORLD_INTEL_HISTORY_MAX_BODY_BYTES", "0")  # 0 = no cap
    assert _record(store, "news_feed", T0 + 600, big)["payloads_stored"] == 1


def test_replay_over_the_replay_cap_returns_an_outline(store, monkeypatch) -> None:
    payload = {"rows": list(range(500)), "source": "x", "meta": {"a": 1, "b": 2}}
    _record(store, "news_feed", T0, payload)
    monkeypatch.setenv("WORLD_INTEL_HISTORY_REPLAY_MAX_BYTES", "100")
    capped = store.capture_history("news_feed", capture_id=1)["capture"]
    assert capped["payload"] is None
    assert capped["payload_outline"] == {
        "rows": "list of 500", "source": "str", "meta": "object with 2 keys"
    }
    raw = len(json.dumps(payload, separators=(",", ":")))
    assert capped["payload_note"].startswith(f"body is {raw:,} bytes of JSON, over the 100-byte")
    monkeypatch.setenv("WORLD_INTEL_HISTORY_REPLAY_MAX_BYTES", "0")
    assert store.capture_history("news_feed", capture_id=1)["capture"]["payload"] == payload


def test_cli_replay_prints_the_outline(cli_db, monkeypatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_HISTORY_REPLAY_MAX_BYTES", "10")
    s = HistoryStore(cli_db)
    _record(s, "news_feed", T0, {"rows": list(range(50))})
    s.close()
    out = _invoke("history", "captures", "news_feed", "--capture-id", "1").output
    assert "replay cap" in out and "list of 50" in out


def test_size_caps_defaults_and_bad_values(monkeypatch) -> None:
    defaults = {"max_body_bytes": 4_194_304, "replay_max_bytes": 262_144}
    assert history.size_caps() == defaults
    monkeypatch.setenv("WORLD_INTEL_HISTORY_MAX_BODY_BYTES", "-1")
    monkeypatch.setenv("WORLD_INTEL_HISTORY_REPLAY_MAX_BYTES", "lots")
    assert history.size_caps() == defaults


# ---------------------------------------------------------------------------
# L1 + L9: empty results name what was checked and where
# ---------------------------------------------------------------------------


def test_revisions_name_what_was_checked(store) -> None:
    empty = store.revisions()
    assert empty["checked"] == {"observations": 0, "series": 0, "dated_series": 0}
    assert empty["data_gaps"] == ["no observations recorded yet, so no revision can be seen"]
    _record(store, "market_quotes", T0, _quotes(1.0))
    undated = store.revisions()
    assert undated["checked"] == {"observations": 1, "series": 1, "dated_series": 0}
    assert undated["data_gaps"] == [
        "none of the 1 series checked has an observed_at, so none can be checked for revisions"
    ]
    _record(store, "space_weather", T0, _kp(("2026-10-17T00:00:00Z", 2.0)))
    mixed = store.revisions()
    assert mixed["data_gaps"] == [
        "1 of 2 series have no observed_at and cannot be checked for revisions"
    ]
    scoped = store.revisions(source="space_weather")
    assert (scoped["checked"]["series"], scoped["data_gaps"]) == (1, [])
    assert store.revisions(source="cyclones")["data_gaps"] == [
        "no observations recorded yet for source 'cyclones', so no revision can be seen"
    ]


def test_cli_revisions_prints_gaps(cli_db) -> None:
    out = _invoke("history", "revisions").output
    assert "Gap:" in out and "no observations recorded yet" in out


def test_empty_run_log_names_its_path_and_the_other_path_risk(store) -> None:
    health = store.collection_health(["wildfires", "earthquakes"], now=T0)
    assert health["data_gaps"][0].startswith(f"no captures in this run log ({store.db_path})")
    assert any("never_run here" in line for line in health["cannot_prove"])
    _record(store, "wildfires", T0, FIRES)
    health = store.collection_health(["wildfires", "earthquakes"], now=T0)
    assert health["data_gaps"] == ["1 source(s) have no capture in this run log"]


async def test_history_tools_pass_gaps_through(tmp_path, monkeypatch) -> None:
    tools_mod = _history_tools()
    monkeypatch.setattr(tools_mod.runtime, "cache", types.SimpleNamespace(db_path=tmp_path / "t.db"))
    out = await tools_mod.HANDLERS["intel_revisions"]({})
    assert out["data_gaps"] == ["no observations recorded yet, so no revision can be seen"]
    health = await tools_mod.HANDLERS["intel_collection_health"]({"source": "wildfires"})
    assert health["history_write_failures"]["count"] == 0
    assert health["data_gaps"][0].startswith("no captures in this run log")


# ---------------------------------------------------------------------------
# L4: observation dates must name real calendar periods
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        ("2026-13-45", None), ("2026-02-30", None), ("2026-02-29", None), ("0000", None),
        ("2026-00", None), ("2026-13", None), ("0000-01-01", None),
        ("2028-02-29", "2028-02-29"), ("2026-12", "2026-12"), ("0001", "0001"),
    ],
)
def test_observed_at_periods_are_calendar_checked(text, expected) -> None:
    assert history.normalize_observed_at(text) == expected


# ---------------------------------------------------------------------------
# L6: a failed history write leaves a lasting, visible record
# ---------------------------------------------------------------------------


async def _ok_fires(fetcher, **kw):
    return dict(FIRES)


async def test_locked_db_is_retried_then_recorded_as_a_lost_cycle(tmp_path, monkeypatch):
    db = tmp_path / "cache.db"
    s = HistoryStore(db)
    _record(s, "wildfires", T0, FIRES)
    s.close()
    monkeypatch.setattr(history, "BUSY_TIMEOUT_S", 0.05)
    monkeypatch.setattr(collector, "_import_fetch_fn", _importer({"wildfires": _ok_fires}))
    holder = sqlite3.connect(str(db), timeout=0.05)
    holder.execute("BEGIN IMMEDIATE")  # another writer holds the lock
    fetcher = types.SimpleNamespace(cache=types.SimpleNamespace(db_path=db))
    try:
        summary = await collector.collect_once(fetcher, _VS(), source_filter={"wildfires"})
    finally:
        holder.rollback()
        holder.close()
    assert summary["history"] == {
        "recorded": 0, "error": "history write failed: database is locked"
    }
    (line,) = history.write_failures_path(db).read_text().splitlines()
    record = json.loads(line)
    assert (record["error"], record["sources"]) == (
        "history write failed: database is locked", ["wildfires"]
    )

    s = HistoryStore(db)
    try:
        assert s.capture_history("wildfires")["total_captures"] == 1  # the cycle was lost
        health = s.collection_health(["wildfires", "earthquakes"])
    finally:
        s.close()
    failures = health["history_write_failures"]
    assert failures["count"] == 1
    assert failures["last_error"] == "history write failed: database is locked"
    assert failures["record_file"] == str(history.write_failures_path(db))
    assert any(gap.startswith("1 collector cycle(s) were not written") for gap in health["data_gaps"])
    rows = {r["source"]: r for r in health["sources"]}
    assert rows["wildfires"]["unrecorded_attempts"] == 1
    assert "unrecorded_attempts" not in rows["earthquakes"]  # not in the lost cycle


async def test_a_write_that_fails_once_is_retried(tmp_path, monkeypatch):
    real_record = HistoryStore.record_cycle
    calls = {"n": 0}

    def _flaky(self, outcomes, now=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return real_record(self, outcomes, now)

    monkeypatch.setattr(HistoryStore, "record_cycle", _flaky)
    monkeypatch.setattr(collector, "_import_fetch_fn", _importer({"wildfires": _ok_fires}))
    db = tmp_path / "cache.db"
    fetcher = types.SimpleNamespace(cache=types.SimpleNamespace(db_path=db))
    summary = await collector.collect_once(fetcher, _VS(), source_filter={"wildfires"})
    assert calls["n"] == 2 and summary["history"]["recorded"] == 1
    assert not history.write_failures_path(db).exists()


def test_lost_cycle_records_are_capped_retained_and_parsed(tmp_path, monkeypatch) -> None:
    db = tmp_path / "cache.db"
    outcome = [SourceOutcome("wildfires", T0)]
    for i in range(505):
        history.record_write_failure(db, outcome, f"e{i}", now=T0 + i)
    lines = history.write_failures_path(db).read_text().splitlines()
    assert len(lines) == 500 and json.loads(lines[0])["error"] == "e5"
    with history.write_failures_path(db).open("a") as fh:
        fh.write("not json\n[1, 2]\n")
    records, unreadable = history.read_write_failures(db, now=T0 + 600)
    assert (len(records), unreadable) == (500, 2)
    monkeypatch.setenv("WORLD_INTEL_HISTORY_CAPTURE_DAYS", "1")
    records, _ = history.read_write_failures(db, now=T0 + 86400 + 300)
    assert records[0]["error"] == "e300"  # older ones aged out with the run log
    s = HistoryStore(db)
    try:
        gaps = s.collection_health(["wildfires"], now=T0 + 600)["data_gaps"]
    finally:
        s.close()
    assert f"2 lost-cycle record(s) in {history.write_failures_path(db)} could not be read" in gaps


def test_unwritable_failure_record_is_logged_not_raised(tmp_path, caplog) -> None:
    missing_dir = tmp_path / "gone" / "cache.db"
    assert history.record_write_failure(missing_dir, [], "boom") is None
    assert "Could not record the history write failure" in caplog.text


# The symlink and FIFO tests below need POSIX (symlinks, os.mkfifo,
# O_NOFOLLOW), as CI (Ubuntu) and macOS provide.
_LOST = [SourceOutcome("wildfires", T0)]


def _without_hanging(fn, fifo):
    """(fn's result, whether it blocked). A reader blocked opening the FIFO
    is released by opening its write end, so a regression fails, not hangs."""
    out = {}
    thread = threading.Thread(target=lambda: out.update(result=fn()), daemon=True)
    thread.start()
    thread.join(5)
    blocked = thread.is_alive()
    if blocked:
        os.close(os.open(fifo, os.O_WRONLY | os.O_NONBLOCK))
        thread.join(5)
    return out.get("result"), blocked


def test_failure_record_is_never_written_through_a_symlink(tmp_path, caplog) -> None:
    db = tmp_path / "cache.db"
    path = history.write_failures_path(db)
    targets = [tmp_path / "other-file-1", tmp_path / "other-file-2"]
    for target in targets:
        target.write_text("not ours\n")
        target.chmod(0o644)
    (tmp_path / (path.name + ".tmp")).symlink_to(targets[0])  # the old fixed temp name
    path.symlink_to(targets[1])

    assert history.record_write_failure(db, _LOST, "boom", now=T0) == path
    for target in targets:
        assert target.read_text() == "not ours\n"
        assert stat.S_IMODE(target.stat().st_mode) == 0o644
    assert not path.is_symlink() and stat.S_IMODE(path.stat().st_mode) == 0o600
    (line,) = path.read_text().splitlines()
    record = json.loads(line)
    assert (record["error"], record["replaced_unread"]) == (
        "boom", f"{path} is a symlink; not followed"
    )
    assert f"Not reading the lost-cycle record {path}: it is a symlink; not followed" in caplog.text
    # Nothing else was created: no temp file is left behind.
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(
        [path.name, path.name + ".tmp", "other-file-1", "other-file-2"]
    )


def test_lost_cycle_reader_does_not_follow_a_symlink(tmp_path, caplog) -> None:
    db = tmp_path / "cache.db"
    path = history.write_failures_path(db)
    elsewhere = tmp_path / "elsewhere.jsonl"
    elsewhere.write_text(
        json.dumps({"failed_at_ts": T0, "error": "planted", "sources": ["wildfires"]}) + "\n"
    )
    path.symlink_to(elsewhere)
    assert history.read_write_failures(db, now=T0) == ([], 1)
    assert f"Not reading the lost-cycle record {path}: it is a symlink; not followed" in caplog.text
    s = HistoryStore(db)
    try:
        health = s.collection_health(["wildfires"], now=T0)
    finally:
        s.close()
    assert health["history_write_failures"]["count"] == 0
    assert (
        f"the lost-cycle record {path} is a symlink; not followed, so cycles whose history "
        "write failed are not counted here"
    ) in health["data_gaps"]


def test_a_name_swapped_after_the_check_is_still_refused(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(history, "_record_refusal", lambda path: None)  # the lstat check missed it
    (tmp_path / "elsewhere").write_text("x\n")
    link = tmp_path / "link.jsonl"
    link.symlink_to(tmp_path / "elsewhere")
    assert history._read_record(link) == (None, "is a symlink; not followed")
    fifo = tmp_path / "fifo.jsonl"
    os.mkfifo(fifo)
    result, blocked = _without_hanging(lambda: history._read_record(fifo), fifo)
    assert (result, blocked) == ((None, "is not a regular file; not read"), False)


def test_a_fifo_at_the_record_path_is_refused_without_blocking(tmp_path) -> None:
    db = tmp_path / "cache.db"
    path = history.write_failures_path(db)
    os.mkfifo(path)
    result, blocked = _without_hanging(lambda: history.read_write_failures(db, now=T0), path)
    assert (result, blocked) == (([], 1), False)
    assert history.record_write_failure(db, _LOST, "boom", now=T0) == path
    assert json.loads(path.read_text())["replaced_unread"] == f"{path} is not a regular file; not read"


def test_a_failed_record_write_leaves_no_temp_file(tmp_path, caplog) -> None:
    db = tmp_path / "cache.db"
    path = history.write_failures_path(db)
    path.mkdir()
    (path / "keep").write_text("x")
    assert history.record_write_failure(db, _LOST, "boom", now=T0) is None
    assert "Could not record the history write failure" in caplog.text
    assert [p.name for p in tmp_path.iterdir()] == [path.name]
    assert (path / "keep").read_text() == "x"
    s = HistoryStore(db)
    try:
        gaps = s.collection_health(["wildfires"], now=T0)["data_gaps"]
    finally:
        s.close()
    assert (
        f"the lost-cycle record {path} is not a regular file; not read, so cycles whose "
        "history write failed are not counted here"
    ) in gaps


def test_undecodable_lost_cycle_lines_count_as_unreadable(tmp_path) -> None:
    db = tmp_path / "cache.db"
    history.record_write_failure(db, _LOST, "boom", now=T0)
    with history.write_failures_path(db).open("ab") as fh:
        fh.write(b"\xff\xfe not utf-8\n")
    records, unreadable = history.read_write_failures(db, now=T0)
    assert ([r["error"] for r in records], unreadable) == (["boom"], 1)
    assert history.record_write_failure(db, _LOST, "again", now=T0 + 1) is not None


def test_health_reports_a_record_it_cannot_inspect(tmp_path) -> None:
    db = tmp_path / ("d" * 236 + ".db")  # the record's file name is over NAME_MAX
    s = HistoryStore(db)
    try:
        gaps = s.collection_health(["wildfires"], now=T0)["data_gaps"]
    finally:
        s.close()
    assert f"1 lost-cycle record(s) in {history.write_failures_path(db)} could not be read" in gaps


# ---------------------------------------------------------------------------
# Surviving mutants from the review
# ---------------------------------------------------------------------------


def test_a_body_seen_continuously_outlives_payload_retention(store, monkeypatch) -> None:
    """Payload retention counts from the LAST sighting: a body first stored
    1.5 days ago but seen again 0.6 days ago survives a 1-day window."""
    monkeypatch.setenv("WORLD_INTEL_HISTORY_PAYLOAD_DAYS", "1")
    payload = _kp(("2026-10-17T00:00:00Z", 2.0))
    _record(store, "space_weather", T0, payload)
    _record(store, "space_weather", T0 + 0.9 * 86400, payload)  # unchanged: bumps last_seen_at
    summary = _record(store, "wildfires", T0 + 1.5 * 86400, FIRES)
    assert summary["pruned"]["payloads"] == 0
    assert store.capture_history("space_weather", capture_id=2)["capture"]["payload"] == payload
    summary = _record(store, "wildfires", T0 + 2 * 86400, FIRES)
    assert summary["pruned"]["payloads"] == 1  # 1.1 days after its last sighting


def test_replace_true_swaps_in_a_different_extractor(store, monkeypatch) -> None:
    monkeypatch.setattr(history, "_EXTRACTORS", dict(history._EXTRACTORS))
    monkeypatch.setattr(history, "_EXPECTED", dict(history._EXPECTED))
    history.register_extractor("test_feed", lambda p: [Observation("a", 1.0)])
    history.register_extractor("test_feed", lambda p: [Observation("b", 2.0)], replace=True)
    _record(store, "test_feed", T0, {"rows": [1]})
    assert [s["series_key"] for s in store.series_history()["series"]] == ["test_feed:b"]


def test_expected_count_is_per_registration(monkeypatch) -> None:
    monkeypatch.setattr(history, "_EXTRACTORS", dict(history._EXTRACTORS))
    monkeypatch.setattr(history, "_EXPECTED", dict(history._EXPECTED))
    history.register_extractor("test_feed", lambda p: [], expected=3)
    assert history._EXPECTED["test_feed"] == 3
    history.register_extractor("test_feed", lambda p: [], replace=True)
    assert "test_feed" not in history._EXPECTED


class _Abort(BaseException):
    """Not an Exception: gather() returns it, and nothing above catches it."""


async def test_collector_records_a_source_that_raised_a_base_exception(tmp_path, monkeypatch):
    async def _aborts(fetcher, **kw):
        raise _Abort("source aborted")

    monkeypatch.setattr(collector, "_import_fetch_fn", _importer({
        "wildfires": _ok_fires, "acled_events": _aborts,
    }))
    db = tmp_path / "cache.db"
    fetcher = types.SimpleNamespace(cache=types.SimpleNamespace(db_path=db))
    summary = await collector.collect_once(
        fetcher, _VS(), source_filter={"wildfires", "acled_events"}
    )
    assert (summary["successes"], summary["failures"]) == (1, 1)
    assert summary["history"]["status_counts"] == {"ok": 1, "degraded": 0, "error": 1}
    s = HistoryStore(db)
    try:
        (cap,) = s.capture_history("acled_events")["captures"]
    finally:
        s.close()
    assert (cap["status"], cap["reason"], cap["record_count"]) == ("error", "source aborted", None)
    assert cap["duration_s"] is not None


# ---------------------------------------------------------------------------
# ASCII-only observation dates; a stable stale reason with the age apart
# ---------------------------------------------------------------------------

_FULLWIDTH_2026 = "２０２６"


@pytest.mark.parametrize(
    "text",
    [
        _FULLWIDTH_2026,
        _FULLWIDTH_2026 + "-10",
        "2026-1０",  # one fullwidth digit in the month
        "٢٠٢٦",  # 2026 in Arabic-Indic digits
        _FULLWIDTH_2026 + "-10-17T00:00:00Z",
    ],
)
def test_observed_at_must_use_ascii_digits(text) -> None:
    assert history.normalize_observed_at(text) is None


def test_a_point_dated_in_fullwidth_digits_is_skipped_and_counted(store, monkeypatch) -> None:
    monkeypatch.setattr(history, "_EXTRACTORS", dict(history._EXTRACTORS))
    monkeypatch.setattr(history, "_EXPECTED", dict(history._EXPECTED))
    history.register_extractor("test_feed", lambda payload: [
        Observation("level", 1.0, observed_at=_FULLWIDTH_2026 + "-10"),
        Observation("level", 2.0, observed_at="2026-10"),
    ])
    _record(store, "test_feed", T0, {"rows": [1]})
    (cap,) = store.capture_history("test_feed")["captures"]
    assert (cap["observations_extracted"], cap["observations_stored"]) == (2, 1)
    assert cap["extract_error"] == "1 invalid point(s) skipped"
    (point,) = store.series_history("test_feed:level")["observations"]
    assert (point["observed_at"], point["value"]) == ("2026-10", 2.0)


def test_stale_reason_is_stable_and_the_age_is_a_field(store) -> None:
    serves = tuple(StaleServe("polymarket", f"k{i}", age) for i, age in enumerate((30.0, 812.5)))
    store.record_cycle([
        SourceOutcome("prediction_markets", T0, 1.0, {"markets": [1]}, stale_serves=serves),
        SourceOutcome("gas_prices", T0, 1.0, {"prices": [1], "_stale": True,
                                              "_stale_age_seconds": 64.5}),
        SourceOutcome("cyclones", T0, 1.0, {"cyclones": [1], "_stale": True}),
    ], now=T0)
    _replay(store, "wildfires", T0, FIRES, age=812.0)
    _replay(store, "wildfires", T0 + 300, FIRES, age=1112.0)
    _record(store, "wildfires", T0 + 600, FIRES)

    def _fields(source):
        return [(c["reason"], c["stale_age_s"], c["stale_fetches"])
                for c in store.capture_history(source)["captures"]]

    assert _fields("wildfires") == [
        (None, None, None),  # live
        ("served stale cache", 1112.0, 1),
        ("served stale cache", 812.0, 1),
    ]
    assert _fields("prediction_markets") == [("served stale cache", 812.5, 2)]
    # Seen only through the payload's marker: the fetch count is unknown.
    assert _fields("gas_prices") == [("served stale cache", 64.5, None)]
    assert _fields("cyclones") == [("served stale cache", None, None)]


def test_stale_reasons_with_points_stay_one_reason(store) -> None:
    _replay(store, "market_quotes", T0, _quotes(1.0), age=10.0)
    _replay(store, "market_quotes", T0 + 300, _quotes(1.0), age=310.0)
    reasons = {c["reason"] for c in store.capture_history("market_quotes")["captures"]}
    assert reasons == {
        "served stale cache; partial: 1 of 7 expected series; observations not stored: "
        "a stale-cache replay is not a new observation"
    }


def test_health_reports_the_stale_copy_age_apart_from_the_reason(store) -> None:
    now = T0 + 10_000
    _record(store, "wildfires", now - 5000, FIRES)
    _replay(store, "wildfires", now - 10, FIRES, age=4990.0)
    health = store.collection_health(["wildfires"], now=now)
    row = health["sources"][0]
    assert row["last_reason"] == "served stale cache"
    assert (row["last_stale_age_s"], row["last_stale_fetches"]) == (4990.0, 1)
    assert "last_stale_age_s" in health["fields_explained"]
    _record(store, "wildfires", now - 5, FIRES)
    row = store.collection_health(["wildfires"], now=now)["sources"][0]
    assert "last_stale_age_s" not in row and "last_stale_fetches" not in row


def test_cli_shows_the_stale_age_beside_the_stable_reason(cli_db, monkeypatch) -> None:
    monkeypatch.setenv("COLUMNS", "260")
    serves = tuple(StaleServe("polymarket", f"k{i}", age) for i, age in enumerate((30.0, 812.5)))
    s = HistoryStore(cli_db)
    _replay(s, "wildfires", T0, FIRES, age=812.0)
    s.record_cycle([SourceOutcome("prediction_markets", T0, 1.0, {"markets": [1]},
                                  stale_serves=serves)], now=T0)
    s.close()
    out = _invoke("history", "captures", "wildfires").output
    assert "served stale cache (age 812.0 s)" in out
    out = _invoke("history", "captures", "prediction_markets").output
    assert "served stale cache (2 fetches, oldest 812.5 s)" in out
    out = _invoke("history", "captures", "wildfires", "--capture-id", "1").output
    assert "Reason: served stale cache (age 812.0 s)" in out


# The run-log table as de73264 created it, before the stale columns.
_CAPTURES_BEFORE_STALE_COLUMNS = """
CREATE TABLE history_captures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    collected_at REAL NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ok', 'degraded', 'error')),
    duration_s REAL,
    record_count INTEGER,
    error TEXT,
    payload_hash TEXT,
    changed INTEGER,
    observations_extracted INTEGER,
    observations_stored INTEGER,
    extract_error TEXT
);
"""


def _capture_columns(db) -> list[str]:
    conn = sqlite3.connect(str(db))
    try:
        return [row[1] for row in conn.execute("PRAGMA table_info(history_captures)")]
    finally:
        conn.close()


def test_an_existing_run_log_gains_the_stale_columns_in_place(tmp_path) -> None:
    db = tmp_path / "before.db"
    conn = sqlite3.connect(str(db))
    conn.executescript(_CAPTURES_BEFORE_STALE_COLUMNS)
    with conn:
        conn.execute(
            "INSERT INTO history_captures (source, collected_at, status, error) "
            "VALUES ('gas_prices', ?, 'degraded', 'served stale cache (age 812.0 s)')",
            (T0,),
        )
    conn.close()
    for _ in range(2):  # opening again is a no-op
        HistoryStore(db).close()
    columns = _capture_columns(db)
    assert columns.count("stale_age_s") == 1 and columns.count("stale_fetches") == 1

    s = HistoryStore(db)
    try:
        _replay(s, "gas_prices", T0 + 300, {"prices": [1]}, age=99.0)
        new, old = s.capture_history("gas_prices")["captures"]
        row = s.collection_health(["gas_prices"], now=T0 + 400)["sources"][0]
    finally:
        s.close()
    assert (old["reason"], old["stale_age_s"], old["stale_fetches"]) == (
        "served stale cache (age 812.0 s)", None, None
    )
    assert (new["reason"], new["stale_age_s"], new["stale_fetches"]) == (
        "served stale cache", 99.0, 1
    )
    # The old row's reason still marks it a stale replay.
    assert (row["state"], row["consecutive_failures"], row["last_success_at"]) == ("stale", 2, None)


def test_migration_tolerates_a_column_another_process_just_added(tmp_path) -> None:
    db = tmp_path / "race.db"
    HistoryStore(db).close()
    conn = sqlite3.connect(str(db))

    class _ReadBeforeTheOtherAlter:
        """The table_info read happened before another process's ALTER."""

        def execute(self, sql, *args):
            if sql.startswith("PRAGMA table_info"):
                return iter(())
            return conn.execute(sql, *args)

    try:
        history._add_missing_capture_columns(_ReadBeforeTheOtherAlter())
        empty = sqlite3.connect(str(tmp_path / "empty.db"))
        with pytest.raises(sqlite3.OperationalError, match="no such table"):
            history._add_missing_capture_columns(empty)
        empty.close()
    finally:
        conn.close()
    assert _capture_columns(db).count("stale_age_s") == 1


def test_a_file_that_is_not_a_database_raises_and_leaks_no_connection(tmp_path, monkeypatch):
    db = tmp_path / "corrupt.db"
    db.write_bytes(b"this is not an SQLite database\n" * 64)
    opened = []
    real_connect = sqlite3.connect

    def _spy(*args, **kwargs):
        opened.append(real_connect(*args, **kwargs))
        return opened[-1]

    monkeypatch.setattr(history.sqlite3, "connect", _spy)
    gc.collect()  # connections other tests leaked warn now, not inside the block
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(sqlite3.DatabaseError, match="not a database"):
            HistoryStore(db)
        gc.collect()
    (conn,) = opened
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        conn.execute("SELECT 1")
    assert [w for w in caught if issubclass(w.category, ResourceWarning)] == []


# ---------------------------------------------------------------------------
# M4: a raising extractor degrades the capture. L2: quote units.
# ---------------------------------------------------------------------------


def test_a_raising_extractor_degrades_the_capture_and_keeps_it(store, monkeypatch) -> None:
    monkeypatch.setattr(history, "_EXTRACTORS", dict(history._EXTRACTORS))
    monkeypatch.setattr(history, "_EXPECTED", dict(history._EXPECTED))
    history.register_extractor("test_feed", lambda payload: payload["missing"])
    _record(store, "test_feed", T0, {"rows": [1]})
    (cap,) = store.capture_history("test_feed")["captures"]
    assert cap["status"] == "degraded"
    assert cap["reason"] == cap["extract_error"] == "extractor raised: KeyError: 'missing'"
    assert (cap["record_count"], cap["observations_extracted"]) == (1, None)
    replay = store.capture_history("test_feed", capture_id=cap["capture_id"])["capture"]
    assert replay["payload"] == {"rows": [1]}
    (row,) = store.collection_health(["test_feed"], now=T0)["sources"]
    assert (row["state"], row["consecutive_failures"]) == ("degraded", 0)
    assert (row["last_success_at"], row["last_ok_at"]) == (cap["collected_at"], None)
    assert row["last_reason"] == row["last_extract_error"] == cap["reason"]

    def _long(payload):
        raise ValueError("x" * 300)

    history.register_extractor("test_feed", _long, replace=True)
    _record(store, "test_feed", T0 + 300, {"rows": [1]})
    newest = store.capture_history("test_feed")["captures"][0]
    assert newest["extract_error"] == "extractor raised: ValueError: " + "x" * 200
    assert newest["reason"] == newest["extract_error"][:150]


def test_a_raising_quote_extractor_is_not_called_partial(store, monkeypatch) -> None:
    monkeypatch.setattr(history, "_EXTRACTORS", dict(history._EXTRACTORS))
    monkeypatch.setattr(history, "_EXPECTED", dict(history._EXPECTED))

    def _broken(payload):
        raise TypeError("quotes moved")

    history.register_extractor("market_quotes", _broken, replace=True, expected=7)
    _record(store, "market_quotes", T0, _quotes(1.0, 2.0))
    (cap,) = store.capture_history("market_quotes")["captures"]
    assert (cap["status"], cap["reason"]) == ("degraded", "extractor raised: TypeError: quotes moved")


def test_the_builtin_kp_extractor_raising_on_list_rows_degrades_the_capture(store) -> None:
    """Upstream Kp rows that turn from objects into lists make the built-in
    extractor raise; that capture is degraded, not a healthy ok."""
    _record(store, "space_weather", T0, {"kp_recent": [["2026-10-16T21:00:00Z", 2.33]], "alerts": []})
    (cap,) = store.capture_history("space_weather")["captures"]
    assert (cap["status"], cap["reason"]) == (
        "degraded", "extractor raised: AttributeError: 'list' object has no attribute 'get'"
    )
    assert (cap["extract_error"], cap["observations_extracted"]) == (cap["reason"], None)


def test_quote_units_say_what_the_number_is() -> None:
    assert set(history.COMMODITY_UNITS) == set(history._COMMODITY_SYMBOLS)
    quotes = {"quotes": [
        {"symbol": "^GSPC", "price": 6_123.5, "currency": "USD"},
        {"symbol": "^N225", "price": 48_000.0, "currency": "JPY"},
        {"symbol": "^HSI", "price": 26_000.0},
        {"symbol": "^TNX", "price": 4.1, "currency": "USD"},
        {"symbol": "AAPL", "price": 250.0, "currency": "USD"},
    ]}
    points = {p.series_key: (p.unit, p.label) for p in history._EXTRACTORS["market_quotes"](quotes)}
    assert points == {
        "^GSPC:price": ("index points", "^GSPC (USD)"),
        "^N225:price": ("index points", "^N225 (JPY)"),
        "^HSI:price": ("index points", "^HSI"),
        "^TNX:price": (None, "^TNX"),
        "AAPL:price": ("USD", "AAPL"),
    }
    commodities = {"commodities": [
        {"symbol": "ZC=F", "name": "Corn", "price": 420.25},
        {"symbol": "XX=F", "price": 1.0},
    ]}
    points = {
        p.series_key: (p.unit, p.label)
        for p in history._EXTRACTORS["commodity_quotes"](commodities)
    }
    assert points == {"ZC=F:price": ("US cents per bushel", "Corn"), "XX=F:price": (None, "XX=F")}
