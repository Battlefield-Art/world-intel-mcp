"""Tests for history.py, the collector's history recording, the history MCP
tools and the ``intel history`` CLI group.

Every test drives a real ``HistoryStore`` on a tmp SQLite file and asserts
actual values: dates as stored, dedupe counts, overdue arithmetic at a
fixed ``now``, revision pairs. The collector tests fake only the fetch
functions (the ``_import_fetch_fn`` boundary) and give the fake fetcher a
real ``cache.db_path``, so ``collect_once`` writes through the production
history path.
"""

import asyncio
import json
import types

import pytest
from click.testing import CliRunner

from world_intel_mcp import cli, collector, history
from world_intel_mcp.history import HistoryStore, Observation, SourceOutcome

T0 = 1_760_000_000.0  # 2025-10-09T08:53:20Z


def _kp(*rows):
    return {"kp_recent": [{"time": t, "kp": v} for t, v in rows], "alerts": []}


@pytest.fixture
def store(tmp_path):
    s = HistoryStore(tmp_path / "history.db")
    yield s
    s.close()


@pytest.fixture(autouse=True)
def _default_history_env(monkeypatch):
    for var in (
        "WORLD_INTEL_HISTORY_DAYS",
        "WORLD_INTEL_HISTORY_PAYLOAD_DAYS",
        "WORLD_INTEL_HISTORY_CAPTURE_DAYS",
        "WORLD_INTEL_HISTORY_CADENCE_S",
        "WORLD_INTEL_HISTORY_CADENCES",
    ):
        monkeypatch.delenv(var, raising=False)


def _record(store, source, at, payload=None, error=None, duration=1.0):
    return store.record_cycle([SourceOutcome(source, at, duration, payload, error)], now=at)


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------


def test_normalize_observed_at_forms() -> None:
    n = history.normalize_observed_at
    assert n("2026-10-15") == "2026-10-15"
    assert n("2026-10") == "2026-10"
    assert n("2026") == "2026"
    assert n("2026-10-17T03:00:00Z") == "2026-10-17T03:00:00Z"
    assert n("2026-10-17T05:00:00+02:00") == "2026-10-17T03:00:00Z"
    # A naive instant is refused unless the publisher's zone is known.
    assert n("2026-10-17 03:00:00.000") is None
    assert n("2026-10-17 03:00:00.000", assume_utc=True) == "2026-10-17T03:00:00Z"
    assert n("not a date") is None
    assert n(None) is None
    assert n(True) is None


def test_kp_observed_at_comes_from_payload_not_collection_time(store) -> None:
    _record(store, "space_weather", T0, _kp(("2026-10-17 03:00:00.000", 3.0)))
    rows = store.series_history("space_weather:kp")["observations"]
    assert rows == [
        {
            "observed_at": "2026-10-17T03:00:00Z",
            "published_at": None,
            "collected_at": "2025-10-09T08:53:20Z",
            "value": 3.0,
            "unit": "Kp index",
            "label": "NOAA planetary K-index",
            "capture_id": 1,
        }
    ]


def test_payload_without_observation_date_stores_null_not_collected_at(store) -> None:
    quotes = {"quotes": [{"symbol": "^GSPC", "price": 6_123.5, "currency": "USD"}]}
    _record(store, "market_quotes", T0, quotes)
    result = store.series_history("market_quotes:^GSPC:price")
    (obs,) = result["observations"]
    assert obs["observed_at"] is None
    assert obs["collected_at"] == "2025-10-09T08:53:20Z"
    assert obs["value"] == 6_123.5 and obs["unit"] == "USD"
    assert any("no observed_at" in gap for gap in result["data_gaps"])
    listing = store.series_history()["series"]
    assert listing[0]["points_without_observed_at"] == 1
    assert listing[0]["first_observed_at"] is None


# ---------------------------------------------------------------------------
# Status classification: a failure is never an ok capture with zero records
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payload,error,reason",
    [
        (None, "timeout (45.0s)", "timeout (45.0s)"),
        (None, None, "fetch returned no payload"),
        ({"error": "EIA_API_KEY not configured"}, None, "EIA_API_KEY not configured"),
    ],
)
def test_failures_are_error_captures_with_no_record_count(store, payload, error, reason):
    summary = _record(store, "energy_prices", T0, payload, error)
    assert summary["status_counts"] == {"ok": 0, "degraded": 0, "error": 1}
    (cap,) = store.capture_history("energy_prices")["captures"]
    assert cap["status"] == "error"
    assert cap["reason"] == reason
    assert cap["record_count"] is None
    assert cap["payload_hash"] is None and cap["change"] is None
    assert store.storage_stats()["payload_bodies"] == 0


def test_empty_payload_is_degraded_not_ok(store) -> None:
    _record(store, "earthquakes", T0, {"earthquakes": [], "count": 0})
    (cap,) = store.capture_history("earthquakes")["captures"]
    assert cap["status"] == "degraded"
    assert cap["record_count"] == 0
    assert "quiet period" in cap["reason"]


def test_stale_and_marked_degraded_payloads(store) -> None:
    _record(store, "gas_prices", T0, {"prices": [1], "_stale": True, "_stale_age_seconds": 812.0})
    _record(store, "cyclones", T0, {"cyclones": [1], "degraded": True})
    assert store.capture_history("gas_prices")["captures"][0]["reason"] == (
        "served stale cache (age 812.0 s)"
    )
    assert store.capture_history("cyclones")["captures"][0]["status"] == "degraded"


def test_extractor_that_finds_nothing_marks_degraded(store) -> None:
    curated_only = {"rates": [{"bank": "Bank of Japan", "rate": 0.5, "as_of": "2025-01-24",
                               "source": "curated", "label": "BoJ Policy Rate"}]}
    _record(store, "central_bank_rates", T0, curated_only)
    (cap,) = store.capture_history("central_bank_rates")["captures"]
    assert cap["status"] == "degraded"
    assert cap["observations_extracted"] == 0
    assert "found no measures" in cap["reason"]


def test_record_count_hint() -> None:
    assert history.record_count([1, 2, 3]) == 3
    assert history.record_count({"a": [1], "b": [1, 2]}) == 2
    assert history.record_count({"count": 4, "zones": {}}) == 4
    assert history.record_count({"oil": {"brent": None}}) is None
    assert history.record_count({"count": True}) is None


# ---------------------------------------------------------------------------
# Dedupe
# ---------------------------------------------------------------------------


def test_unchanged_payload_body_stored_once_despite_new_fetch_timestamp(store) -> None:
    first = dict(_kp(("2026-10-17T00:00:00Z", 2.0)), timestamp="2026-10-17T16:00:00Z")
    again = dict(first, timestamp="2026-10-17T16:05:00Z")
    s1 = _record(store, "space_weather", T0, first)
    s2 = _record(store, "space_weather", T0 + 300, again)
    assert (s1["payloads_stored"], s2["payloads_stored"]) == (1, 0)
    assert (s1["observations_stored"], s2["observations_stored"]) == (1, 0)
    caps = store.capture_history("space_weather")["captures"]
    assert [c["change"] for c in caps] == ["unchanged", "first"]
    assert caps[0]["payload_hash"] == caps[1]["payload_hash"]
    assert store.storage_stats()["payload_bodies"] == 1


def test_changed_then_reverted_payload_keeps_two_bodies(store) -> None:
    a = _kp(("2026-10-17T00:00:00Z", 2.0))
    b = _kp(("2026-10-17T00:00:00Z", 2.0), ("2026-10-17T03:00:00Z", 1.0))
    for i, payload in enumerate((a, b, a)):
        _record(store, "space_weather", T0 + 300 * i, payload)
    result = store.capture_history("space_weather")
    assert [c["change"] for c in result["captures"]] == ["changed", "changed", "first"]
    assert result["distinct_payloads"] == 2
    assert result["changed_captures"] == 2
    assert store.storage_stats()["payload_bodies"] == 2
    # The unchanged 00:00 point was stored once across all three captures.
    assert len(store.series_history("space_weather:kp")["observations"]) == 2


def test_capture_replay_returns_stored_body(store) -> None:
    payload = _kp(("2026-10-17T00:00:00Z", 2.0))
    _record(store, "space_weather", T0, payload)
    replay = store.capture_history("space_weather", capture_id=1)
    assert replay["capture"]["payload"] == payload
    assert store.capture_history("space_weather", capture_id=99)["error"].startswith("no capture 99")
    assert "integer" in store.capture_history("space_weather", capture_id="x")["error"]


# ---------------------------------------------------------------------------
# Revisions
# ---------------------------------------------------------------------------


def test_revisions_detect_changed_value_for_same_observed_at(store) -> None:
    _record(store, "space_weather", T0, _kp(("2026-10-17T00:00:00Z", 2.33), ("2026-10-17T03:00:00Z", 3.0)))
    _record(store, "space_weather", T0 + 600, _kp(("2026-10-17T00:00:00Z", 2.67), ("2026-10-17T03:00:00Z", 3.0)))
    quake = {"earthquakes": [{"id": "us7000tst1", "magnitude": 4.6, "time": "2026-10-16T22:10:00Z", "place": "Test Ridge"}]}
    _record(store, "earthquakes", T0, quake)
    quake["earthquakes"][0]["magnitude"] = 4.9
    _record(store, "earthquakes", T0 + 900, quake)
    # A quote whose price moves has no observed_at: never a revision.
    _record(store, "market_quotes", T0, {"quotes": [{"symbol": "^DJI", "price": 1.0}]})
    _record(store, "market_quotes", T0 + 300, {"quotes": [{"symbol": "^DJI", "price": 2.0}]})

    result = store.revisions()
    assert result["count"] == 2
    by_key = {r["series_key"]: r for r in result["revisions"]}
    kp = by_key["space_weather:kp"]
    assert kp["observed_at"] == "2026-10-17T00:00:00Z"
    assert (kp["previous_value"], kp["revised_value"], kp["delta"]) == (2.33, 2.67, 0.34)
    assert kp["previous_collected_at"] == "2025-10-09T08:53:20Z"
    assert kp["revised_collected_at"] == "2025-10-09T09:03:20Z"
    mag = by_key["earthquakes:us7000tst1:magnitude"]
    assert (mag["previous_value"], mag["revised_value"]) == (4.6, 4.9)
    assert mag["label"] == "Test Ridge"
    assert store.revisions(source="earthquakes")["count"] == 1
    assert store.revisions(series_key="space_weather:kp")["count"] == 1


def test_revisions_ignore_a_repeated_equal_value_from_any_writer(store) -> None:
    """record_cycle never stores an unchanged value twice, but a future
    writer (a backfill) might: a repeated value must not read as a revision."""
    _record(store, "space_weather", T0, _kp(("2026-10-17T00:00:00Z", 2.0)))
    with store._conn:
        store._conn.execute(
            "INSERT INTO history_observations (capture_id, source, series_key, observed_at, "
            "collected_at, value) VALUES (1, 'space_weather', 'space_weather:kp', "
            "'2026-10-17T00:00:00Z', ?, 2.0)",
            (T0 + 60,),
        )
    assert store.revisions()["count"] == 0


def test_consecutive_failures_follow_insertion_order_not_clock(store) -> None:
    """An error logged after a success counts even if the clock stepped back."""
    _record(store, "wildfires", T0 - 100, None, "earlier failure, before the success")
    _record(store, "wildfires", T0, {"fires": [1]})
    _record(store, "wildfires", T0 - 30, None, "boom")
    row = store.collection_health(["wildfires"], now=T0 + 10)["sources"][0]
    assert row["consecutive_failures"] == 1


# ---------------------------------------------------------------------------
# Collection health
# ---------------------------------------------------------------------------


def test_overdue_math_and_consecutive_failures(store) -> None:
    now = T0 + 10_000
    _record(store, "earthquakes", now - 899, {"earthquakes": [{"id": "a", "magnitude": 5.0, "time": "2026-10-16T22:10:00Z"}]})
    _record(store, "wildfires", now - 901, {"fires": [1]})
    _record(store, "wildfires", now - 120, None, "timeout (45.0s)")
    _record(store, "wildfires", now - 60, None, "boom")
    _record(store, "acled_events", now - 950, {"events": [1]})

    health = store.collection_health(
        ["earthquakes", "wildfires", "acled_events", "cyclones"], now=now
    )
    rows = {r["source"]: r for r in health["sources"]}
    quake = rows["earthquakes"]
    assert quake["overdue_after_s"] == 900.0
    assert quake["last_success_age_s"] == 899.0
    assert quake["overdue"] is False and quake["state"] == "ok"
    assert quake["latest_observed_at"] == "2026-10-16T22:10:00Z"
    fires = rows["wildfires"]
    assert fires["consecutive_failures"] == 2
    assert fires["overdue"] is True  # last payload 901 s ago > 900 s
    assert fires["state"] == "failing"
    assert fires["last_reason"] == "boom"
    assert rows["acled_events"]["state"] == "not_attempted_recently"
    assert rows["cyclones"]["state"] == "never_run"
    assert rows["cyclones"]["attempts"] == 0
    assert health["state_counts"] == {
        "ok": 1, "failing": 1, "not_attempted_recently": 1, "never_run": 1
    }
    assert any("observations are current" in line for line in health["cannot_prove"])


def test_cadence_overrides_from_env(store, monkeypatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_HISTORY_CADENCE_S", "600")
    monkeypatch.setenv("WORLD_INTEL_HISTORY_CADENCES", "wildfires=60, junk, bad=x")
    _record(store, "wildfires", T0, {"fires": [1]})
    _record(store, "earthquakes", T0, {"earthquakes": [{"id": "a", "magnitude": 5.0, "time": "2026-10-16T22:10:00Z"}]})
    health = store.collection_health(["wildfires", "earthquakes"], now=T0 + 200)
    rows = {r["source"]: r for r in health["sources"]}
    assert rows["wildfires"]["overdue_after_s"] == 180.0
    assert rows["wildfires"]["state"] == "not_attempted_recently"
    assert rows["earthquakes"]["overdue_after_s"] == 1800.0
    assert rows["earthquakes"]["state"] == "ok"


def test_health_unknown_source_and_off_roster_source(store) -> None:
    _record(store, "retired_source", T0, {"x": [1]})
    assert "unknown source" in store.collection_health(["earthquakes"], source="nope", now=T0)["error"]
    health = store.collection_health(["earthquakes"], now=T0)
    retired = [r for r in health["sources"] if r["source"] == "retired_source"][0]
    assert retired["in_collector_roster"] is False
    one = store.collection_health(["earthquakes"], source="earthquakes", now=T0)
    assert [r["source"] for r in one["sources"]] == ["earthquakes"]


# ---------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------


def test_retention_defaults() -> None:
    assert history.retention_days() == {
        "payload_days": 7.0, "capture_days": 30.0, "history_days": 365.0
    }


def test_run_log_retention_is_separate_from_observations(store, monkeypatch) -> None:
    """Captures (the run log) age out at capture_days while the
    observations they produced stay until history_days."""
    monkeypatch.setenv("WORLD_INTEL_HISTORY_CAPTURE_DAYS", "2")
    monkeypatch.setenv("WORLD_INTEL_HISTORY_DAYS", "10")
    _record(store, "space_weather", T0, _kp(("2026-10-17T00:00:00Z", 2.0)))
    summary = _record(store, "space_weather", T0 + 3 * 86400, _kp(("2026-10-20T00:00:00Z", 1.0)))
    assert summary["pruned"]["captures"] == 1
    assert summary["pruned"]["observations"] == 0
    observed = [o["observed_at"] for o in store.series_history("space_weather:kp")["observations"]]
    assert observed == ["2026-10-20T00:00:00Z", "2026-10-17T00:00:00Z"]
    assert store.capture_history("space_weather")["total_captures"] == 1


def test_retention_prunes_bodies_then_rows(store, monkeypatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_HISTORY_PAYLOAD_DAYS", "1")
    monkeypatch.setenv("WORLD_INTEL_HISTORY_CAPTURE_DAYS", "3")
    monkeypatch.setenv("WORLD_INTEL_HISTORY_DAYS", "3")
    _record(store, "space_weather", T0, _kp(("2026-10-17T00:00:00Z", 2.0)))
    later = T0 + 2 * 86400
    summary = _record(store, "space_weather", later, _kp(("2026-10-18T00:00:00Z", 1.0)))
    assert summary["pruned"] == {"payloads": 1, "captures": 0, "observations": 0}
    replay = store.capture_history("space_weather", capture_id=1)["capture"]
    assert replay["payload"] is None and "pruned" in replay["payload_note"]
    assert store.capture_history("space_weather")["captures"][1]["body_retained"] is False

    summary = _record(store, "space_weather", T0 + 4 * 86400, _kp(("2026-10-18T00:00:00Z", 1.0)))
    assert summary["pruned"]["captures"] == 1
    assert summary["pruned"]["observations"] == 1


def test_retention_zero_keeps_forever_and_bad_values_fall_back(store, monkeypatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_HISTORY_PAYLOAD_DAYS", "0")
    monkeypatch.setenv("WORLD_INTEL_HISTORY_CAPTURE_DAYS", "0")
    monkeypatch.setenv("WORLD_INTEL_HISTORY_DAYS", "-5")
    assert history.retention_days() == {
        "payload_days": 0.0, "capture_days": 0.0, "history_days": 365.0
    }
    _record(store, "space_weather", T0, _kp(("2026-10-17T00:00:00Z", 2.0)))
    summary = _record(store, "space_weather", T0 + 200 * 86400, _kp(("2026-10-18T00:00:00Z", 1.0)))
    assert summary["pruned"] == {"payloads": 0, "captures": 0, "observations": 0}


# ---------------------------------------------------------------------------
# Extractor registry
# ---------------------------------------------------------------------------


def test_register_extractor_api(store, monkeypatch) -> None:
    monkeypatch.setattr(history, "_EXTRACTORS", dict(history._EXTRACTORS))

    @history.register_extractor("test_feed")
    def _extract(payload):
        return [
            Observation("level", payload["level"], observed_at="2026-10-15", unit="m"),
            Observation("bad_date", 1.0, observed_at="2026-10-15 03:00"),
            Observation("nan", float("nan"), observed_at="2026-10-15"),
        ]

    with pytest.raises(ValueError, match="already registered"):
        history.register_extractor("test_feed", lambda p: [])
    history.register_extractor("test_feed", _extract, replace=True)
    assert "test_feed" in history.registered_extractors()

    _record(store, "test_feed", T0, {"level": 3.25, "rows": [1]})
    (cap,) = store.capture_history("test_feed")["captures"]
    assert cap["observations_extracted"] == 3
    assert cap["observations_stored"] == 1
    assert cap["extract_error"] == "2 invalid point(s) skipped"
    assert cap["status"] == "ok"
    (obs,) = store.series_history("test_feed:level")["observations"]
    assert (obs["observed_at"], obs["value"], obs["unit"]) == ("2026-10-15", 3.25, "m")


def test_raising_extractor_keeps_capture_and_reports(store, monkeypatch) -> None:
    monkeypatch.setattr(history, "_EXTRACTORS", dict(history._EXTRACTORS))
    history.register_extractor("test_feed", lambda payload: payload["missing"])
    _record(store, "test_feed", T0, {"rows": [1]})
    (cap,) = store.capture_history("test_feed")["captures"]
    assert cap["status"] == "ok"
    assert cap["extract_error"].startswith("extractor raised: KeyError")
    health = store.collection_health(["test_feed"], now=T0)
    assert health["sources"][0]["last_extract_error"].startswith("extractor raised")


def test_builtin_extractors() -> None:
    rates = {"rates": [
        {"bank": "Federal Reserve", "label": "Federal Funds Rate", "rate": 4.33,
         "as_of": "2026-10-15", "source": "fred"},
        {"bank": "Bank of Japan", "label": "BoJ Policy Rate", "rate": 0.5,
         "as_of": "2025-01-24", "source": "curated"},
    ]}
    (fed,) = history._EXTRACTORS["central_bank_rates"](rates)
    assert (fed.series_key, fed.value, fed.observed_at, fed.unit) == (
        "federal_funds_rate", 4.33, "2026-10-15", "percent"
    )
    quakes = {"earthquakes": [
        {"id": "us1", "magnitude": 5.1, "time": "2026-10-16T22:10:00Z", "place": "P"},
        {"id": "us2", "magnitude": None, "time": "2026-10-16T22:10:00Z"},
        {"id": None, "magnitude": 4.0, "time": "2026-10-16T22:10:00Z"},
    ]}
    (q,) = history._EXTRACTORS["earthquakes"](quakes)
    assert (q.series_key, q.observed_at, q.label) == ("us1:magnitude", "2026-10-16T22:10:00Z", "P")
    commodities = {"commodities": [{"symbol": "GC=F", "name": "Gold", "price": 2_400.0}]}
    (gold,) = history._EXTRACTORS["commodity_quotes"](commodities)
    assert (gold.series_key, gold.label, gold.observed_at, gold.unit) == (
        "GC=F:price", "Gold", None, None
    )


# ---------------------------------------------------------------------------
# Collector integration
# ---------------------------------------------------------------------------


class _VS:
    _store_queue = None

    async def store(self, name, data):
        pass

    async def collection_stats(self):
        return {"points_count": 0}


def _importer(fakes):
    name_by_ref = {(mod, fn): name for name, mod, fn, _ in collector.SOURCES}

    def _import(module_path, fn_name):
        return fakes[name_by_ref[(module_path, fn_name)]]

    return _import


async def test_collect_once_records_failed_fetch_as_failure_capture(tmp_path, monkeypatch):
    """The error-dict source counts as a cycle 'success' (pinned in
    test_collector_runtime) but the history records what happened: an error
    capture with the reason, not an ok capture with zero records."""

    async def _quakes(fetcher, **kw):
        return {"earthquakes": [{"id": "x1", "magnitude": 5.0, "time": "2026-10-16T22:10:00Z"}], "count": 1}

    async def _raises(fetcher, **kw):
        raise ValueError("acled exploded")

    async def _none(fetcher, **kw):
        return None

    async def _hangs(fetcher, **kw):
        await asyncio.sleep(5)

    async def _error_dict(fetcher, **kw):
        return {"error": "EIA_API_KEY not configured"}

    monkeypatch.setattr(collector, "_import_fetch_fn", _importer({
        "earthquakes": _quakes, "acled_events": _raises, "wildfires": _none,
        "nav_warnings": _hangs, "energy_prices": _error_dict,
    }))
    db = tmp_path / "cache.db"
    fetcher = types.SimpleNamespace(cache=types.SimpleNamespace(db_path=db))
    summary = await collector.collect_once(
        fetcher, _VS(),
        source_filter={"earthquakes", "acled_events", "wildfires", "nav_warnings", "energy_prices"},
        timeout=0.05,
    )
    assert summary["successes"] == 2  # earthquakes + the error dict, as before
    assert summary["history"]["recorded"] == 5
    assert summary["history"]["status_counts"] == {"ok": 1, "degraded": 0, "error": 4}

    store = HistoryStore(db)
    try:
        reasons = {
            src: store.capture_history(src)["captures"][0]
            for src in ("acled_events", "wildfires", "nav_warnings", "energy_prices", "earthquakes")
        }
    finally:
        store.close()
    assert reasons["acled_events"]["reason"] == "acled exploded"
    assert reasons["wildfires"]["reason"] == "fetch returned no payload"
    assert reasons["nav_warnings"]["reason"] == "timeout (0.05s)"
    assert reasons["energy_prices"]["reason"] == "EIA_API_KEY not configured"
    for src in ("acled_events", "wildfires", "nav_warnings", "energy_prices"):
        assert reasons[src]["status"] == "error"
        assert reasons[src]["record_count"] is None
    ok = reasons["earthquakes"]
    assert (ok["status"], ok["record_count"], ok["observations_stored"]) == ("ok", 1, 1)
    assert all(r["duration_s"] is not None and r["duration_s"] >= 0 for r in reasons.values())


def test_unserializable_payload_loses_only_its_body(store) -> None:
    """Mixed-type dict keys break sort_keys hashing; that capture keeps
    its status and says what was lost, and the cycle's other captures
    are still recorded."""
    summary = store.record_cycle([
        SourceOutcome("weird_source", T0, 1.0, {"rows": [1], "by_key": {1: "a", "b": 2}}),
        SourceOutcome("space_weather", T0, 1.0, _kp(("2026-10-17T00:00:00Z", 2.0))),
    ], now=T0)
    assert summary["recorded"] == 2
    assert summary["payloads_stored"] == 1
    (weird,) = store.capture_history("weird_source")["captures"]
    assert weird["status"] == "ok"
    assert weird["payload_hash"] is None and weird["change"] is None
    assert weird["reason"].startswith("payload not stored in history: TypeError")
    assert store.capture_history("space_weather")["captures"][0]["change"] == "first"


async def test_collect_once_survives_an_unexpected_history_bug(tmp_path, monkeypatch):
    async def _ok(fetcher, **kw):
        return {"earthquakes": [1]}

    def _boom(self, outcomes, now=None):
        raise RuntimeError("history bug")

    monkeypatch.setattr(collector, "_import_fetch_fn", _importer({"earthquakes": _ok}))
    monkeypatch.setattr(HistoryStore, "record_cycle", _boom)
    fetcher = types.SimpleNamespace(cache=types.SimpleNamespace(db_path=tmp_path / "c.db"))
    summary = await collector.collect_once(fetcher, _VS(), source_filter={"earthquakes"})
    assert summary["successes"] == 1
    assert summary["history"] == {
        "recorded": 0, "error": "history write failed: RuntimeError: history bug"
    }


async def test_collect_once_reports_unrecordable_history(monkeypatch):
    async def _ok(fetcher, **kw):
        return {"earthquakes": [1]}

    monkeypatch.setattr(collector, "_import_fetch_fn", _importer({"earthquakes": _ok}))
    summary = await collector.collect_once(object(), _VS(), source_filter={"earthquakes"})
    assert summary["history"]["recorded"] == 0
    assert "no cache db_path" in summary["history"]["error"]

    bad = types.SimpleNamespace(cache=types.SimpleNamespace(db_path="/dev/null/x/cache.db"))
    summary = await collector.collect_once(bad, _VS(), source_filter={"earthquakes"})
    assert summary["history"]["recorded"] == 0
    assert summary["history"]["error"].startswith("history write failed")


def test_collector_main_prints_history_line(monkeypatch, capsys) -> None:
    async def _fake_run_once(source_filter=None):
        return {"successes": 1, "sources_attempted": 1, "cycle_time_s": 0.1,
                "vector_store_points": 0, "errors": [], "failures": 0,
                "history": {"recorded": 1, "status_counts": {"ok": 0, "degraded": 0, "error": 1},
                            "observations_stored": 0}}

    monkeypatch.setattr(collector, "run_once", _fake_run_once)
    monkeypatch.setattr("sys.argv", ["intel-collector"])
    with pytest.raises(SystemExit):
        collector.main()
    assert "History: 1 captures (ok 0, degraded 0, error 1), 0 new observations" in capsys.readouterr().out

    async def _unrecorded(source_filter=None):
        out = await _fake_run_once()
        out["history"] = {"recorded": 0, "error": "history write failed: disk"}
        return out

    monkeypatch.setattr(collector, "run_once", _unrecorded)
    with pytest.raises(SystemExit):
        collector.main()
    assert "History: NOT recorded (history write failed: disk)" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------


def _history_tools():
    """Import the tools package under the suite's temp cache path (the
    test_aoi.py pattern): importing it imports runtime, which opens a
    Cache() at WORLD_INTEL_CACHE_DB."""
    import importlib
    import os

    from .test_server_registry import _TMP_CACHE

    prior = os.environ.get("WORLD_INTEL_CACHE_DB")
    os.environ["WORLD_INTEL_CACHE_DB"] = str(_TMP_CACHE)
    try:
        importlib.import_module("world_intel_mcp.tools")
        return importlib.import_module("world_intel_mcp.tools.history")
    finally:
        if prior is None:
            os.environ.pop("WORLD_INTEL_CACHE_DB", None)
        else:
            os.environ["WORLD_INTEL_CACHE_DB"] = prior


@pytest.fixture
def tool_db(tmp_path, monkeypatch):
    db = tmp_path / "tools.db"
    tools_mod = _history_tools()
    monkeypatch.setattr(tools_mod.runtime, "cache", types.SimpleNamespace(db_path=db))
    s = HistoryStore(db)
    _record(s, "space_weather", T0, _kp(("2026-10-17T00:00:00Z", 2.33)))
    _record(s, "space_weather", T0 + 600, _kp(("2026-10-17T00:00:00Z", 2.67)))
    _record(s, "energy_prices", T0, {"error": "EIA_API_KEY not configured"})
    s.close()
    return db


async def test_history_tools_read_the_runtime_cache_db(tool_db) -> None:
    h = _history_tools().HANDLERS
    health = await h["intel_collection_health"]({})
    rows = {r["source"]: r for r in health["sources"]}
    assert set(rows) == {name for name, _, _, _ in collector.SOURCES}
    assert rows["energy_prices"]["state"] in {"failing", "not_attempted_recently"}
    assert rows["energy_prices"]["last_reason"] == "EIA_API_KEY not configured"
    assert rows["cyclones"]["state"] == "never_run"
    one = await h["intel_collection_health"]({"source": "space_weather"})
    assert one["sources"][0]["latest_observed_at"] == "2026-10-17T00:00:00Z"

    caps = await h["intel_capture_history"]({"source": "space_weather", "limit": 1})
    assert len(caps["captures"]) == 1 and caps["total_captures"] == 2
    assert caps["captures"][0]["change"] == "changed"
    replay = await h["intel_capture_history"]({"source": "space_weather", "capture_id": 1})
    assert replay["capture"]["payload"]["kp_recent"][0]["kp"] == 2.33
    assert "required" in (await h["intel_capture_history"]({}))["error"]

    listing = await h["intel_series_history"]({})
    assert [s["series_key"] for s in listing["series"]] == ["space_weather:kp"]
    series = await h["intel_series_history"]({"series_key": "space_weather:kp"})
    assert [o["value"] for o in series["observations"]] == [2.67, 2.33]
    missing = await h["intel_series_history"]({"series_key": "nope"})
    assert "no observations" in missing["error"]

    revs = await h["intel_revisions"]({"source": "space_weather"})
    assert revs["count"] == 1 and revs["revisions"][0]["delta"] == 0.34


def test_tool_descriptions_state_history_accrues_only_while_collecting() -> None:
    history_tools = _history_tools()
    names = {t.name for t in history_tools.TOOLS}
    assert names == {
        "intel_collection_health", "intel_capture_history",
        "intel_series_history", "intel_revisions",
    }
    for tool in history_tools.TOOLS:
        assert "accrues only while the collector" in tool.description


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@pytest.fixture
def cli_db(tmp_path, monkeypatch):
    db = tmp_path / "cli-cache.db"
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(db))
    monkeypatch.setenv("COLUMNS", "220")
    monkeypatch.setattr(cli, "_fetcher", None)
    s = HistoryStore(db)
    _record(s, "space_weather", T0, _kp(("2026-10-17T00:00:00Z", 2.33)))
    _record(s, "space_weather", T0 + 600, _kp(("2026-10-17T00:00:00Z", 2.67)))
    _record(s, "energy_prices", T0, {"error": "EIA_API_KEY [red]not[/red] configured"})
    s.close()
    return db


def _invoke(*args):
    return CliRunner().invoke(cli.main, list(args), catch_exceptions=False)


def test_cli_history_registry() -> None:
    assert set(cli.history_group.commands) == {"health", "captures", "series", "revisions"}


def test_cli_history_health_table_and_json(cli_db) -> None:
    out = _invoke("history", "health").output
    assert "space_weather" in out and "never_run" in out
    assert "EIA_API_KEY [red]not[/red] configured" in out  # rendered literally
    assert "Cannot prove:" in out
    data = json.loads(_invoke("--json-output", "history", "health", "--source", "energy_prices").output)
    assert data["sources"][0]["consecutive_failures"] == 1
    bad = _invoke("history", "health", "--source", "nope").output
    assert bad.startswith("Error:") and "unknown source" in bad


def test_cli_history_captures(cli_db) -> None:
    out = _invoke("history", "captures", "space_weather").output
    assert "2 total, 1 changed, 2 distinct" in out
    assert "changed" in out and "first" in out
    replay = _invoke("history", "captures", "space_weather", "--capture-id", "1").output
    assert "Capture 1 of space_weather" in replay and "2.33" in replay
    assert "Error:" in _invoke("history", "captures", "space_weather", "--capture-id", "9").output
    empty = _invoke("history", "captures", "cyclones").output
    assert "Gap:" in empty and "no captures recorded" in empty


def test_cli_history_series_and_revisions(cli_db) -> None:
    listing = _invoke("history", "series").output
    assert "space_weather:kp" in listing
    one = _invoke("history", "series", "space_weather:kp").output
    assert "2026-10-17T00:00:00Z" in one and "2.67" in one and "Gap:" in one
    assert "Error:" in _invoke("history", "series", "nope").output
    revs = _invoke("history", "revisions").output
    assert "+0.34" in revs and "2.33" in revs
    none = _invoke("history", "revisions", "--source", "earthquakes").output
    assert "No revisions seen" in none
    data = json.loads(_invoke("--json-output", "history", "revisions").output)
    assert data["count"] == 1


def test_cli_history_empty_series_listing(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "empty.db"))
    monkeypatch.setattr(cli, "_fetcher", None)
    out = _invoke("history", "series").output
    assert "Gap:" in out and "no observations recorded yet" in out
