"""Tests for the day timeline (history_timeline.py), the intel_day_timeline
tool and the ``intel history day`` command.

Every store here is a real ``HistoryStore`` on a tmp SQLite file, written
through ``record_cycle`` (the collector's write path, retention pruning
included) and read back through the day timeline's read-only connection.
Assertions are exact values at a fixed ``now``: which day a capture at
23:59:59Z or 00:00:00Z lands on, per-source states and counts, retention
verdicts, truncation counts. The tool and CLI tests use days relative to
the real clock because those paths read ``time.time()``.
"""

import contextlib
import hashlib
import json
import os
import re
import sqlite3
import types
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from click.testing import CliRunner

from world_intel_mcp import cli, collector, history, history_timeline
from world_intel_mcp.history import HistoryStore, Observation, SourceOutcome
from world_intel_mcp.history_timeline import day_timeline


def at(text: str) -> float:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


NOON = at("2026-10-17T12:00:00Z")  # "now" while 2026-10-17 is in progress
LATER = at("2026-10-20T12:00:00Z")  # "now" once 2026-10-17 is complete
# 2026 in fullwidth digits: a regex \d and int() accept them; the day pattern is [0-9].
_FULLWIDTH_YEAR = "".join(chr(0xFF10 + int(d)) for d in "2026")


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


@pytest.fixture
def db(tmp_path):
    return tmp_path / "history.db"


@pytest.fixture
def store(db):
    s = HistoryStore(db)
    yield s
    s.close()


@pytest.fixture
def feeds(monkeypatch):
    """Register a test extractor for feed_a and feed_b: the payload lists
    (series, value, observed_at) points."""
    monkeypatch.setattr(history, "_EXTRACTORS", dict(history._EXTRACTORS))

    def _extract(payload):
        return [Observation(key, value, observed_at=obs) for key, value, obs in payload["points"]]

    history.register_extractor("feed_a", _extract)
    history.register_extractor("feed_b", _extract)
    return lambda *points: {"points": [list(p) for p in points], "rows": [1]}


def out(source, when, payload=None, error=None):
    return SourceOutcome(source, at(when), 1.0, payload, error)


def cycle(store, when, *outcomes):
    """One collector cycle, pruned as the collector would at ``when``."""
    return store.record_cycle(list(outcomes), now=at(when))


def kp(*rows):
    return {"kp_recent": [{"time": t, "kp": v} for t, v in rows], "alerts": []}


def by_source(result):
    return {row["source"]: row for row in result["sources"]}


# ---------------------------------------------------------------------------
# Input: YYYY-MM-DD, UTC, never in the future
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "day",
    [
        "2026-10-17T00:00:00Z", "2026/10/17", "20261017", "2026-W42-6", "2026-1-07",
        "", " 2026-10-17", "2026-10-17 ", _FULLWIDTH_YEAR + "-10-17", None, 20261017,
    ],
)
def test_malformed_day_is_an_error_naming_the_format(db, day) -> None:
    result = day_timeline(db, day, now=NOON)
    assert result == {
        "error": f"day must be YYYY-MM-DD, a UTC calendar date; got {day!r}",
        "accepted_format": "YYYY-MM-DD",
        "timezone": "UTC",
    }
    assert not db.exists()  # a refused request opens nothing


def test_impossible_and_future_days_are_errors(db) -> None:
    assert day_timeline(db, "2026-02-30", now=NOON)["error"] == (
        "day must be a real calendar date in YYYY-MM-DD; got '2026-02-30'"
    )
    assert day_timeline(db, "2026-10-18", now=NOON)["error"] == (
        "day 2026-10-18 is in the future (today is 2026-10-17 UTC)"
    )
    # Today is UTC today: one second before midnight UTC, tomorrow is refused.
    assert "in the future" in day_timeline(db, "2026-10-18", now=at("2026-10-17T23:59:59Z"))["error"]
    assert "error" not in day_timeline(db, "2026-10-18", now=at("2026-10-18T00:00:00Z"))


def test_other_arguments_are_checked(db) -> None:
    assert day_timeline(db, "2026-10-17", by="hourly", now=NOON)["error"] == (
        "by must be 'collected' or 'observed'; got 'hourly'"
    )
    assert day_timeline(db, "2026-10-17", source=5, now=NOON)["error"] == (
        "source must be a source name; got 5"
    )


def test_window_is_stated_in_utc_and_today_reads_in_progress(store, db) -> None:
    cycle(store, "2026-10-17T01:00:00Z", out("space_weather", "2026-10-17T01:00:00Z", kp(("2026-10-17T00:00:00Z", 2.0))))
    today = day_timeline(db, "2026-10-17", now=NOON)
    assert (today["day"], today["timezone"], today["by"]) == ("2026-10-17", "UTC", "collected")
    assert today["window"] == {
        "start": "2026-10-17T00:00:00Z", "end": "2026-10-18T00:00:00Z", "end_exclusive": True,
        "complete": False, "through": "2026-10-17T12:00:00Z",
    }
    assert today["data_gaps"][-1].startswith("The day is in progress: counts run through 2026-10-17T12:00:00Z")
    # In progress, the longest gap runs to now, not to midnight.
    assert (today["collector"]["longest_gap_from"], today["collector"]["longest_gap_to"]) == (
        "2026-10-17T01:00:00Z", "2026-10-17T12:00:00Z"
    )
    done = day_timeline(db, "2026-10-17", now=LATER)
    assert done["window"]["complete"] is True and "through" not in done["window"]
    assert done["collector"]["longest_gap_to"] == "2026-10-18T00:00:00Z"
    assert not any("in progress" in gap for gap in done["data_gaps"])


# ---------------------------------------------------------------------------
# The day boundary
# ---------------------------------------------------------------------------


def test_midnight_boundary_puts_each_record_on_exactly_one_day(store, db) -> None:
    cycle(store, "2026-10-16T23:59:59Z",
          out("space_weather", "2026-10-16T23:59:59Z", kp(("2026-10-16T23:59:59Z", 1.0))))
    cycle(store, "2026-10-17T00:00:00Z",
          out("space_weather", "2026-10-17T00:00:00Z",
              kp(("2026-10-16T23:59:59Z", 1.0), ("2026-10-17T00:00:00Z", 2.0))))
    roster = ["space_weather"]
    before = day_timeline(db, "2026-10-16", roster=roster, now=LATER)
    after = day_timeline(db, "2026-10-17", roster=roster, now=LATER)
    b, a = by_source(before)["space_weather"], by_source(after)["space_weather"]
    assert (b["attempts"], b["first_capture_at"], b["last_capture_at"]) == (
        1, "2026-10-16T23:59:59Z", "2026-10-16T23:59:59Z"
    )
    assert (a["attempts"], a["first_capture_at"], a["last_capture_at"]) == (
        1, "2026-10-17T00:00:00Z", "2026-10-17T00:00:00Z"
    )
    assert (before["collector"]["captures"], after["collector"]["captures"]) == (1, 1)
    assert [(o["collected_at"], o["observed_at"]) for o in before["observations"]] == [
        ("2026-10-16T23:59:59Z", "2026-10-16T23:59:59Z")
    ]
    assert [(o["collected_at"], o["observed_at"]) for o in after["observations"]] == [
        ("2026-10-17T00:00:00Z", "2026-10-17T00:00:00Z")
    ]
    seen_16 = day_timeline(db, "2026-10-16", by="observed", now=LATER)["day_values"]
    seen_17 = day_timeline(db, "2026-10-17", by="observed", now=LATER)["day_values"]
    assert [p["observed_at"] for p in seen_16] == ["2026-10-16T23:59:59Z"]
    assert [p["observed_at"] for p in seen_17] == ["2026-10-17T00:00:00Z"]


# ---------------------------------------------------------------------------
# by=collected: per-source states from the run log
# ---------------------------------------------------------------------------

ROSTER = ["space_weather", "earthquakes", "gas_prices", "market_quotes", "wildfires", "cyclones"]


def _busy_day(store) -> None:
    calm = kp(("2026-10-16T09:00:00Z", 2.0))
    cycle(store, "2026-10-16T12:00:00Z",
          out("space_weather", "2026-10-16T12:00:00Z", calm),
          out("wildfires", "2026-10-16T12:00:00Z", {"fires": [1]}))
    cycle(store, "2026-10-17T01:00:00Z",
          out("space_weather", "2026-10-17T01:00:00Z", calm),
          out("earthquakes", "2026-10-17T01:00:00Z", error="timeout (45.0s)"),
          out("gas_prices", "2026-10-17T01:00:00Z",
              {"prices": [1], "_stale": True, "_stale_age_seconds": 812.0}),
          out("market_quotes", "2026-10-17T01:00:00Z", {"quotes": [{"symbol": "^GSPC", "price": 6000.0}]}),
          out("weird_source", "2026-10-17T01:00:00Z", {"rows": [1], "by_key": {1: "a", "b": 2}}))
    cycle(store, "2026-10-17T01:05:00Z",
          out("space_weather", "2026-10-17T01:05:00Z", calm),
          out("earthquakes", "2026-10-17T01:05:00Z", error="boom"),
          out("gas_prices", "2026-10-17T01:05:00Z",
              {"prices": [1], "_stale": True, "_stale_age_seconds": 1112.0}),
          out("market_quotes", "2026-10-17T01:05:00Z", {"quotes": [{"symbol": "^GSPC", "price": 6001.0}]}))
    cycle(store, "2026-10-17T01:10:00Z",
          out("earthquakes", "2026-10-17T01:10:00Z", error="timeout (45.0s)"))


def test_collected_view_distinguishes_every_source_state(store, db) -> None:
    _busy_day(store)
    result = day_timeline(db, "2026-10-17", roster=ROSTER, now=LATER)
    rows = by_source(result)
    assert [r["source"] for r in result["sources"]] == ROSTER + ["weird_source"]

    sw = rows["space_weather"]
    assert sw["day_state"] == "unchanged"
    assert (sw["attempts"], sw["status_counts"]) == (2, {"ok": 2, "degraded": 0, "error": 0})
    assert sw["payload_changes"] == {"changed": 0, "unchanged": 2, "first": 0, "not_stored": 0}
    # Its extractor ran and found nothing new: 0, not "no extractor".
    assert (sw["observations_stored"], sw["observations_retained"]) == (0, 0)
    assert (sw["captures_with_body_retained"], sw["captures_with_body_pruned"]) == (2, 0)

    quakes = rows["earthquakes"]
    assert quakes["day_state"] == "failed"
    assert quakes["status_counts"] == {"ok": 0, "degraded": 0, "error": 3}
    assert [(r["status"], r["reason"], r["count"]) for r in quakes["reasons"]] == [
        ("error", "timeout (45.0s)", 2), ("error", "boom", 1)
    ]
    assert (quakes["reasons"][0]["first_at"], quakes["reasons"][0]["last_at"]) == (
        "2026-10-17T01:00:00Z", "2026-10-17T01:10:00Z"
    )
    assert quakes["observations_stored"] is None  # no capture ran an extractor

    gas = rows["gas_prices"]
    assert gas["day_state"] == "degraded"
    assert gas["payload_changes"] == {"changed": 0, "unchanged": 1, "first": 1, "not_stored": 0}
    assert [r["reason"] for r in gas["reasons"]] == [
        "served stale cache (age 1112.0 s)", "served stale cache (age 812.0 s)"
    ]

    quotes = rows["market_quotes"]
    assert quotes["day_state"] == "changed"
    assert quotes["payload_changes"] == {"changed": 1, "unchanged": 0, "first": 1, "not_stored": 0}
    assert (quotes["first_capture_at"], quotes["last_capture_at"]) == (
        "2026-10-17T01:00:00Z", "2026-10-17T01:05:00Z"
    )
    assert (quotes["observations_stored"], quotes["observations_retained"]) == (2, 2)

    fires = rows["wildfires"]
    assert (fires["day_state"], fires["attempts"]) == ("not_attempted", 0)
    assert (fires["last_capture_before"], fires["next_capture_after"]) == ("2026-10-16T12:00:00Z", None)
    assert rows["cyclones"]["day_state"] == "never_run"

    weird = rows["weird_source"]
    assert (weird["day_state"], weird["in_collector_roster"]) == ("answered", False)
    assert weird["payload_changes"]["not_stored"] == 1
    assert weird["reasons"][0]["status"] == "ok"
    assert weird["reasons"][0]["reason"].startswith("payload not stored in history: TypeError")

    assert result["state_counts"] == {
        "unchanged": 1, "failed": 1, "degraded": 1, "changed": 1, "not_attempted": 1,
        "never_run": 1, "answered": 1,
    }
    assert set(result["state_counts"]) <= set(result["states_explained"])
    c = result["collector"]
    assert (c["state"], c["captures"], c["sources_captured"]) == ("ran", 10, 5)
    assert (c["first_capture_at"], c["last_capture_at"]) == ("2026-10-17T01:00:00Z", "2026-10-17T01:10:00Z")
    assert (c["longest_gap_s"], c["longest_gap_from"], c["longest_gap_to"]) == (
        82_200.0, "2026-10-17T01:10:00Z", "2026-10-18T00:00:00Z"
    )
    # Saved observations: the two quote prices, each with its three dates.
    assert [(o["series_key"], o["value"], o["observed_at"], o["published_at"], o["collected_at"])
            for o in result["observations"]] == [
        ("market_quotes:^GSPC:price", 6000.0, None, None, "2026-10-17T01:00:00Z"),
        ("market_quotes:^GSPC:price", 6001.0, None, None, "2026-10-17T01:05:00Z"),
    ]
    assert result["data_gaps"] == []


def test_reasons_are_capped_per_source_and_the_rest_counted(store, db) -> None:
    for minute in range(7):
        when = f"2026-10-17T01:{minute:02d}:00Z"
        cycle(store, when, out("earthquakes", when, error=f"failure {minute}"))
    row = by_source(day_timeline(db, "2026-10-17", source="earthquakes", now=LATER))["earthquakes"]
    assert [r["reason"] for r in row["reasons"]] == [f"failure {m}" for m in (6, 5, 4, 3, 2)]
    assert row["more_reasons"] == 2


def test_source_filter_keeps_the_collector_block_whole(store, db) -> None:
    _busy_day(store)
    result = day_timeline(db, "2026-10-17", source="market_quotes", roster=ROSTER, now=LATER)
    assert [r["source"] for r in result["sources"]] == ["market_quotes"]
    assert result["source_filter"] == "market_quotes"
    assert result["collector"]["captures"] == 10  # every source, not just the filtered one
    unknown = day_timeline(db, "2026-10-17", source="nope", roster=ROSTER, now=LATER)
    assert unknown["error"].startswith("unknown source 'nope'")
    assert "weird_source" in unknown["known_sources"] and "cyclones" in unknown["known_sources"]


def test_day_without_any_collector_cycle(store, db) -> None:
    cycle(store, "2026-10-15T12:00:00Z",
          out("space_weather", "2026-10-15T12:00:00Z", kp(("2026-10-15T09:00:00Z", 2.0))),
          out("earthquakes", "2026-10-15T12:00:00Z", error="timeout (45.0s)"))
    cycle(store, "2026-10-17T12:00:00Z",
          out("space_weather", "2026-10-17T12:00:00Z", kp(("2026-10-17T09:00:00Z", 1.0))))
    roster = ["space_weather", "earthquakes", "cyclones"]
    quiet = day_timeline(db, "2026-10-16", roster=roster, now=LATER)
    c = quiet["collector"]
    assert (c["state"], c["context"], c["captures"]) == ("did_not_run", "between_captures", 0)
    assert c["explanation"] == (
        "The collector recorded no capture of any source that day: captures exist before "
        "and after this day."
    )
    rows = by_source(quiet)
    assert rows["space_weather"]["day_state"] == "collector_did_not_run"
    assert (rows["space_weather"]["last_capture_before"], rows["space_weather"]["next_capture_after"]) == (
        "2026-10-15T12:00:00Z", "2026-10-17T12:00:00Z"
    )
    assert (rows["earthquakes"]["day_state"], rows["earthquakes"]["next_capture_after"]) == (
        "collector_did_not_run", None
    )
    assert rows["cyclones"]["day_state"] == "never_run"
    assert quiet["state_counts"] == {"collector_did_not_run": 2, "never_run": 1}
    assert quiet["observations"] == [] and quiet["data_gaps"] == []

    early = day_timeline(db, "2026-10-14", roster=roster, now=LATER)["collector"]
    assert (early["context"], early["run_log_first_capture_at"]) == (
        "before_first_capture", "2026-10-15T12:00:00Z"
    )
    late = day_timeline(db, "2026-10-19", roster=roster, now=LATER)["collector"]
    assert (late["context"], late["run_log_last_capture_at"]) == (
        "after_last_capture", "2026-10-17T12:00:00Z"
    )


def test_empty_store_missing_tables_and_missing_file(tmp_path) -> None:
    empty = tmp_path / "empty.db"
    HistoryStore(empty).close()
    result = day_timeline(empty, "2026-10-17", roster=["space_weather"], now=LATER)
    assert result["collector"]["context"] == "empty_run_log"
    assert by_source(result)["space_weather"]["day_state"] == "never_run"
    assert result["data_gaps"] == []

    bare = tmp_path / "bare.db"
    sqlite3.connect(bare).close()
    for by in ("collected", "observed"):
        result = day_timeline(bare, "2026-10-17", by=by, roster=["space_weather"], now=LATER)
        assert result["data_gaps"][0].startswith(f"No history tables in {bare}")
    assert by_source(day_timeline(bare, "2026-10-17", roster=["space_weather"], now=LATER))[
        "space_weather"]["day_state"] == "never_run"

    missing = tmp_path / "nowhere" / "missing.db"
    result = day_timeline(missing, "2026-10-17", roster=["space_weather"], now=LATER)
    assert result["data_gaps"][0].startswith("No history tables")
    assert not missing.exists() and not missing.parent.exists()


# ---------------------------------------------------------------------------
# by=observed
# ---------------------------------------------------------------------------


def test_observed_view_lists_day_values_and_separates_periods(store, db, feeds) -> None:
    points = [
        ("daily", 1.0, "2026-10-17"),
        ("hourly", 2.0, "2026-10-17T06:00:00Z"),
        ("late", 8.0, "2026-10-17T23:59:59Z"),
        ("monthly", 3.0, "2026-10"),
        ("annual", 4.0, "2026"),
        ("yesterday", 5.0, "2026-10-16"),
        ("next_month", 6.0, "2026-11"),
        ("undated", 7.0, None),
    ]
    cycle(store, "2026-10-17T08:00:00Z", out("feed_a", "2026-10-17T08:00:00Z", feeds(*points)))
    revised = [p if p[0] != "hourly" else ("hourly", 2.5, p[2]) for p in points]
    cycle(store, "2026-10-17T09:00:00Z", out("feed_a", "2026-10-17T09:00:00Z", feeds(*revised)))
    # The undated value changes the next day: collected then, not on the 17th.
    next_day = [p if p[0] != "undated" else ("undated", 7.5, None) for p in revised]
    cycle(store, "2026-10-18T09:00:00Z", out("feed_a", "2026-10-18T09:00:00Z", feeds(*next_day)))

    result = day_timeline(db, "2026-10-17", by="observed", now=LATER)
    assert [(p["series_key"], p["observed_at"], p["precision"], p["value"]) for p in result["day_values"]] == [
        ("feed_a:daily", "2026-10-17", "date", 1.0),
        ("feed_a:hourly", "2026-10-17T06:00:00Z", "instant", 2.5),
        ("feed_a:late", "2026-10-17T23:59:59Z", "instant", 8.0),
    ]
    hourly = result["day_values"][1]
    assert (hourly["stored_values"], hourly["first_value"], hourly["first_collected_at"]) == (
        2, 2.0, "2026-10-17T08:00:00Z"
    )
    assert hourly["collected_at"] == "2026-10-17T09:00:00Z"
    assert result["day_values"][0]["stored_values"] == 1 and "first_value" not in result["day_values"][0]
    assert [(p["series_key"], p["observed_at"], p["precision"]) for p in result["period_values"]] == [
        ("feed_a:annual", "2026", "year"), ("feed_a:monthly", "2026-10", "month"),
    ]
    for item in result["day_values"] + result["period_values"]:
        assert {"observed_at", "published_at", "collected_at"} <= set(item)
    assert by_source(result)["feed_a"] == {
        "source": "feed_a", "in_collector_roster": False, "day_values": 3, "day_values_revised": 1,
        "period_values": 2, "unrecognized_observed_at": 0,
        "without_observed_at_collected_that_day": 1,
    }
    assert result["without_observed_at"]["collected_that_day"] == 1
    assert result["without_observed_at"]["by_source"] == {"feed_a": 1}
    assert "no observation date" in result["without_observed_at"]["reason"]
    assert result["data_gaps"][0].startswith(
        "1 observation(s) collected this day have no observed_at (feed_a 1) and are excluded"
    )
    assert "2026-10-17" in result["period_values_note"] and "not 2026-10-17" in result["period_values_note"]


def test_month_boundary_never_reads_a_month_value_as_a_day_value(store, db, feeds) -> None:
    points = [("oct31", 1.0, "2026-10-31"), ("oct", 2.0, "2026-10"), ("nov", 3.0, "2026-11"),
              ("nov1", 4.0, "2026-11-01"), ("year", 5.0, "2026")]
    cycle(store, "2026-11-01T08:00:00Z", out("feed_a", "2026-11-01T08:00:00Z", feeds(*points)))
    now = at("2026-11-02T00:00:00Z")
    oct31 = day_timeline(db, "2026-10-31", by="observed", now=now)
    assert [p["series_key"] for p in oct31["day_values"]] == ["feed_a:oct31"]
    assert [p["series_key"] for p in oct31["period_values"]] == ["feed_a:year", "feed_a:oct"]
    nov1 = day_timeline(db, "2026-11-01", by="observed", now=now)
    assert [p["series_key"] for p in nov1["day_values"]] == ["feed_a:nov1"]
    assert [p["series_key"] for p in nov1["period_values"]] == ["feed_a:year", "feed_a:nov"]


def test_unrecognized_observed_at_forms_are_counted_not_shown(store, db, feeds) -> None:
    cycle(store, "2026-10-17T08:00:00Z",
          out("feed_a", "2026-10-17T08:00:00Z", feeds(("ok", 1.0, "2026-10-17T06:00:00Z"))))
    with store._conn:  # a writer that bypasses record_cycle's validation
        for odd in ("2026-10-17T06:00:00.500Z", "2026-10-17 06:00:00"):
            store._conn.execute(
                "INSERT INTO history_observations (capture_id, source, series_key, observed_at, "
                "collected_at, value) VALUES (1, 'feed_a', 'feed_a:odd', ?, ?, 1.0)",
                (odd, at("2026-10-17T08:00:00Z")),
            )
    result = day_timeline(db, "2026-10-17", by="observed", now=LATER)
    assert [p["series_key"] for p in result["day_values"]] == ["feed_a:ok"]
    assert by_source(result)["feed_a"]["unrecognized_observed_at"] == 2
    assert any(g.startswith("2 stored observed_at value(s) on this day are in neither recognized form")
               for g in result["data_gaps"])


def test_observed_view_source_filter(store, db, feeds) -> None:
    cycle(store, "2026-10-17T08:00:00Z",
          out("feed_a", "2026-10-17T08:00:00Z", feeds(("x", 1.0, "2026-10-17"))),
          out("feed_b", "2026-10-17T08:00:00Z", feeds(("y", 2.0, "2026-10-17"))))
    result = day_timeline(db, "2026-10-17", by="observed", source="feed_b", now=LATER)
    assert [p["series_key"] for p in result["day_values"]] == ["feed_b:y"]
    assert [r["source"] for r in result["sources"]] == ["feed_b"]


# ---------------------------------------------------------------------------
# Retention: pruned records read as pruned, never as a quiet day
# ---------------------------------------------------------------------------


def test_day_past_payload_retention_names_the_pruned_bodies(store, db) -> None:
    cycle(store, "2026-10-01T12:00:00Z",
          out("space_weather", "2026-10-01T12:00:00Z", kp(("2026-10-01T09:00:00Z", 2.0))),
          out("election_calendar", "2026-10-01T12:00:00Z", {"elections": [1]}))
    # Eight days on, space weather changed and the calendar did not. The
    # cycle's pruning drops bodies last seen before 2026-10-02T12:00Z; the
    # calendar's body was seen again just now, so it stays.
    summary = cycle(store, "2026-10-09T12:00:00Z",
                    out("space_weather", "2026-10-09T12:00:00Z", kp(("2026-10-09T09:00:00Z", 3.0))),
                    out("election_calendar", "2026-10-09T12:00:00Z", {"elections": [1]}))
    assert summary["pruned"]["payloads"] == 1
    result = day_timeline(db, "2026-10-01", roster=["space_weather", "election_calendar"],
                          now=at("2026-10-09T12:00:00Z"))
    assert result["retention"]["payload_bodies"] == {
        "days": 7.0, "cutoff": "2026-10-02T12:00:00Z", "day": "past_window"
    }
    assert result["retention"]["run_log"]["day"] == "within_window"
    rows = by_source(result)
    assert rows["space_weather"]["day_state"] == "changed"  # the run log itself is kept
    assert (rows["space_weather"]["captures_with_body_retained"],
            rows["space_weather"]["captures_with_body_pruned"]) == (0, 1)
    assert (rows["election_calendar"]["captures_with_body_retained"],
            rows["election_calendar"]["captures_with_body_pruned"]) == (1, 0)
    assert result["data_gaps"] == [
        "Payload bodies of 1 of this day's captures were pruned (bodies are kept 7 days after "
        "the payload was last seen); intel_capture_history can replay only captures whose body "
        "is retained (1 of 2)."
    ]


def test_day_past_run_log_retention_reads_unknown_not_quiet(store, db, monkeypatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_HISTORY_CAPTURE_DAYS", "2")
    monkeypatch.setenv("WORLD_INTEL_HISTORY_DAYS", "10")
    cycle(store, "2026-10-10T12:00:00Z",
          out("space_weather", "2026-10-10T12:00:00Z", kp(("2026-10-10T09:00:00Z", 2.0))))
    summary = cycle(store, "2026-10-13T12:00:00Z",
                    out("space_weather", "2026-10-13T12:00:00Z", kp(("2026-10-13T09:00:00Z", 1.0))))
    assert summary["pruned"]["captures"] == 1

    result = day_timeline(db, "2026-10-10", roster=["space_weather", "cyclones"],
                          now=at("2026-10-13T12:00:00Z"))
    assert result["retention"]["run_log"] == {
        "days": 2.0, "cutoff": "2026-10-11T12:00:00Z", "day": "past_window"
    }
    assert result["collector"]["state"] == "unknown_past_retention"
    assert {r["day_state"] for r in result["sources"]} == {"unknown_past_retention"}
    assert result["data_gaps"][0] == (
        "The run log keeps captures 2 days (cutoff 2026-10-11T12:00:00Z); this day is past it, "
        "so its captures are gone. Whether the collector or any source ran that day cannot be "
        "read here: absence is not evidence that nothing happened."
    )
    # The observation collected that day outlived its capture.
    (item,) = result["observations"]
    assert (item["observed_at"], item["capture_retained"]) == ("2026-10-10T09:00:00Z", False)
    assert by_source(result)["space_weather"]["observations_retained"] == 1

    observed = day_timeline(db, "2026-10-10", by="observed", now=at("2026-10-13T12:00:00Z"))
    assert [p["capture_retained"] for p in observed["day_values"]] == [False]
    assert any(g.startswith("1 listed value(s) outlived the capture") for g in observed["data_gaps"])


def test_captures_past_the_window_but_not_yet_pruned_are_flagged(store, db, monkeypatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_HISTORY_CAPTURE_DAYS", "2")
    cycle(store, "2026-10-10T12:00:00Z",
          out("space_weather", "2026-10-10T12:00:00Z", kp(("2026-10-10T09:00:00Z", 2.0))))
    # No cycle since, so nothing pruned them yet.
    result = day_timeline(db, "2026-10-10", roster=["space_weather"], now=at("2026-10-13T12:00:00Z"))
    assert (result["collector"]["state"], by_source(result)["space_weather"]["attempts"]) == ("ran", 1)
    assert result["data_gaps"][0].startswith(
        "This day is past the 2-day run-log retention (cutoff 2026-10-11T12:00:00Z); its 1 "
        "captures are still here only because pruning runs at the end of a collector cycle"
    )


def test_day_straddling_the_run_log_cutoff(store, db, monkeypatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_HISTORY_CAPTURE_DAYS", "2")
    cycle(store, "2026-10-11T20:00:00Z",
          out("space_weather", "2026-10-11T20:00:00Z", kp(("2026-10-11T18:00:00Z", 2.0))))
    result = day_timeline(db, "2026-10-11", roster=["space_weather", "cyclones"],
                          now=at("2026-10-13T12:00:00Z"))
    assert result["retention"]["run_log"]["day"] == "partly_past_window"
    rows = by_source(result)
    assert rows["space_weather"]["day_state"] == "changed"
    assert rows["cyclones"]["day_state"] == "unknown_past_retention"
    # Before the cutoff the day is unknown, so the gap starts at the cutoff.
    c = result["collector"]
    assert (c["longest_gap_from"], c["longest_gap_to"], c["longest_gap_s"]) == (
        "2026-10-11T12:00:00Z", "2026-10-11T20:00:00Z", 28_800.0
    )
    assert result["data_gaps"][0].startswith(
        "Captures before 2026-10-11T12:00:00Z (the 2-day run-log cutoff) are pruned"
    )


def test_observation_retention_is_stated_for_old_days(store, db, monkeypatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_HISTORY_DAYS", "10")
    monkeypatch.setenv("WORLD_INTEL_HISTORY_PAYLOAD_DAYS", "0")
    now = at("2026-10-13T12:00:00Z")
    result = day_timeline(db, "2026-10-02", by="observed", now=now)
    assert result["retention"]["observations"]["day"] == "past_window"
    assert result["retention"]["payload_bodies"] == {"days": 0.0, "cutoff": None, "day": "kept_forever"}
    assert any(g.startswith("Observations are pruned 10 days after collection (cutoff "
                            "2026-10-03T12:00:00Z)") for g in result["data_gaps"])
    collected = day_timeline(db, "2026-10-02", now=now)
    assert any(g.startswith("Observations collected before 2026-10-03T12:00:00Z are past the "
                            "10-day retention") for g in collected["data_gaps"])


# ---------------------------------------------------------------------------
# Caps
# ---------------------------------------------------------------------------


def test_caps_bound_both_views_and_disclose_truncation(store, db, feeds) -> None:
    many = [(f"a{i:02d}", float(i), f"2026-10-17T00:{i:02d}:00Z") for i in range(30)]
    few = [(f"b{i}", float(i), f"2026-10-17T12:0{i}:00Z") for i in range(5)]
    cycle(store, "2026-10-17T13:00:00Z",
          out("feed_a", "2026-10-17T13:00:00Z", feeds(*many)),
          out("feed_b", "2026-10-17T13:00:00Z", feeds(*few)))

    observed = day_timeline(db, "2026-10-17", by="observed", limit=12, per_source_limit=10, now=LATER)
    assert [p["series_key"] for p in observed["day_values"]] == (
        [f"feed_a:a{i:02d}" for i in range(10)] + ["feed_b:b0", "feed_b:b1"]
    )
    assert observed["truncation"]["day_values"] == {
        "matched": 35, "returned": 12, "truncated": True, "limit": 12, "per_source_limit": 10,
        "sources_truncated": {"feed_a": {"matched": 30, "returned": 10},
                              "feed_b": {"matched": 5, "returned": 2}},
    }
    assert {r["source"]: r["day_values"] for r in observed["sources"]} == {"feed_a": 30, "feed_b": 5}
    assert any(g.startswith("Day values: 12 of 35 returned (by observed_at, then series; at most "
                            "10 per source and 12 in all)") for g in observed["data_gaps"])
    assert observed["truncation"]["period_values"]["truncated"] is False

    collected = day_timeline(db, "2026-10-17", limit=12, per_source_limit=10, now=LATER)
    assert [o["series_key"] for o in collected["observations"]] == (
        [f"feed_a:a{i:02d}" for i in range(10)] + ["feed_b:b0", "feed_b:b1"]
    )
    assert (collected["truncation"]["matched"], collected["truncation"]["returned"]) == (35, 12)
    assert any(g.startswith("Observations saved that day: 12 of 35 returned (earliest first")
               for g in collected["data_gaps"])

    defaults = day_timeline(db, "2026-10-17", now=LATER)["truncation"]
    assert (defaults["limit"], defaults["per_source_limit"], defaults["returned"]) == (100, 10, 15)
    full = day_timeline(db, "2026-10-17", per_source_limit=50, now=LATER)
    assert full["truncation"]["truncated"] is False and len(full["observations"]) == 35


@pytest.mark.parametrize(
    "limit,per_source,expected",
    [(0, -3, (1, 1)), (10**9, 10**9, (5000, 1000)), ("x", None, (100, 10)), (7, "7", (7, 7))],
)
def test_limits_are_clamped(store, db, limit, per_source, expected) -> None:
    trunc = day_timeline(db, "2026-10-17", limit=limit, per_source_limit=per_source, now=LATER)["truncation"]
    assert (trunc["limit"], trunc["per_source_limit"]) == expected


# ---------------------------------------------------------------------------
# Read-only, and the indexes the queries use
# ---------------------------------------------------------------------------


def test_reads_leave_the_store_untouched(store, db) -> None:
    _busy_day(store)
    store.close()  # checkpoint: the main file now holds everything
    before = hashlib.sha256(db.read_bytes()).hexdigest()
    for by in ("collected", "observed"):
        assert "error" not in day_timeline(db, "2026-10-17", by=by, roster=ROSTER, now=LATER)
    assert hashlib.sha256(db.read_bytes()).hexdigest() == before
    wal = db.with_name(db.name + "-wal")
    assert not wal.exists() or wal.stat().st_size == 0
    conn = history_timeline._connect(db)
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("DELETE FROM history_captures")
    finally:
        conn.close()


class _RecordingConnection:
    def __init__(self, conn, log):
        self._conn, self._log = conn, log

    def execute(self, sql, params=()):
        self._log.append((sql, params))
        return self._conn.execute(sql, params)

    def close(self):
        self._conn.close()


def test_queries_seek_the_history_indexes_and_never_scan_a_history_table(store, db, monkeypatch, feeds) -> None:
    """EXPLAIN QUERY PLAN of every statement the timeline runs: no full
    scan of a history table (named, or by the aliases c, o and p used in
    the SQL), and the three indexes the design rests on are used."""
    _busy_day(store)
    cycle(store, "2026-10-17T13:00:00Z", out("feed_a", "2026-10-17T13:00:00Z", feeds(("x", 1.0, "2026-10"))))
    log: list = []
    real = history_timeline._connect
    monkeypatch.setattr(history_timeline, "_connect", lambda path: _RecordingConnection(real(path), log))
    for by in ("collected", "observed"):
        for source in (None, "space_weather"):
            assert "error" not in day_timeline(db, "2026-10-17", by=by, source=source, roster=ROSTER, now=LATER)
    assert len(log) > 20
    forbidden = {"history_captures", "history_observations", "history_payloads", "c", "o", "p"}
    scans, indexes = [], set()
    with contextlib.closing(sqlite3.connect(db)) as conn:
        for sql, params in log:
            for row in conn.execute("EXPLAIN QUERY PLAN " + sql, params):
                detail = row[-1]
                indexes.update(re.findall(r"INDEX (\w+)", detail))
                match = re.match(r"SCAN (?:TABLE )?(\w+)", detail)
                if match and match.group(1) in forbidden:
                    scans.append((sql.split("\n")[1] if sql.startswith("\n") else sql[:60], detail))
    assert scans == []
    assert {"idx_history_captures_time", "idx_history_obs_time", "idx_history_obs_series"} <= indexes


# ---------------------------------------------------------------------------
# MCP tool through the real dispatcher
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def server():
    """server.py imported under a temp cache (test_server_registry's pattern).

    The shared runtime is not rebound here: the tool test replaces
    ``runtime.cache`` with its own store, and rebinding would drop an
    earlier module's open connections unclosed."""
    import importlib

    from .test_server_registry import _TMP_CACHE

    prior = os.environ.get("WORLD_INTEL_CACHE_DB")
    os.environ["WORLD_INTEL_CACHE_DB"] = str(_TMP_CACHE)
    try:
        module = importlib.import_module("world_intel_mcp.server")
        yield module
    finally:
        if prior is None:
            os.environ.pop("WORLD_INTEL_CACHE_DB", None)
        else:
            os.environ["WORLD_INTEL_CACHE_DB"] = prior


def _recent_day(days_ago: int = 2) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).date().isoformat()


async def test_tool_through_the_real_dispatcher_never_fetches(server, tmp_path, monkeypatch) -> None:
    from world_intel_mcp import runtime

    db = tmp_path / "dispatch.db"
    monkeypatch.setattr(runtime, "cache", types.SimpleNamespace(db_path=db))
    day = _recent_day()
    s = HistoryStore(db)
    s.record_cycle([
        SourceOutcome("space_weather", at(f"{day}T01:00:00Z"), 1.0, kp((f"{day}T00:00:00Z", 2.0))),
        SourceOutcome("energy_prices", at(f"{day}T01:00:00Z"), 1.0, {"error": "EIA_API_KEY not configured"}),
    ], now=at(f"{day}T01:00:00Z"))
    s.close()

    def _no_network(*args, **kwargs):
        raise AssertionError("the day timeline must never fetch")

    monkeypatch.setattr(httpx.AsyncClient, "send", _no_network)
    monkeypatch.setattr(httpx.Client, "send", _no_network)

    result = await server._dispatch("intel_day_timeline", {"day": day})
    rows = by_source(result)
    assert set(rows) == {name for name, _, _, _ in collector.SOURCES}
    assert rows["space_weather"]["day_state"] == "changed"
    assert rows["energy_prices"]["day_state"] == "failed"
    assert rows["energy_prices"]["reasons"][0]["reason"] == "EIA_API_KEY not configured"
    assert rows["cyclones"]["day_state"] == "never_run"
    assert result["db_path"] == str(db)

    observed = await server._dispatch(
        "intel_day_timeline", {"day": day, "by": "observed", "source": "space_weather"}
    )
    assert [p["observed_at"] for p in observed["day_values"]] == [f"{day}T00:00:00Z"]

    capped = await server._dispatch("intel_day_timeline", {"day": day, "limit": 3, "per_source_limit": 2})
    assert (capped["truncation"]["limit"], capped["truncation"]["per_source_limit"]) == (3, 2)
    bad = await server._dispatch("intel_day_timeline", {"day": "17-10-2026"})
    assert bad["accepted_format"] == "YYYY-MM-DD"
    assert (await server._dispatch("intel_day_timeline", {}))["error"].startswith("day is required")


def test_tool_registration(server) -> None:
    (tool,) = [t for t in server.TOOLS if t.name == "intel_day_timeline"]
    assert tool.inputSchema["required"] == ["day"]
    assert tool.inputSchema["properties"]["by"]["enum"] == ["collected", "observed"]
    assert re.fullmatch(tool.inputSchema["properties"]["day"]["pattern"], "2026-10-17")
    assert "accrues only while the collector" in tool.description
    assert chr(0x2014) not in tool.description  # no em dash in user-facing text


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@pytest.fixture
def cli_day(tmp_path, monkeypatch):
    db = tmp_path / "cli-day.db"
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(db))
    monkeypatch.setenv("COLUMNS", "220")
    monkeypatch.setattr(cli, "_fetcher", None)
    day = _recent_day()
    s = HistoryStore(db)
    first, second = at(f"{day}T01:00:00Z"), at(f"{day}T01:05:00Z")
    quote = {"quotes": [{"symbol": "^GSPC", "price": 6000.0}]}
    rates = {"rates": [{"bank": "Federal Reserve", "label": "Federal Funds Rate", "rate": 4.33,
                        "as_of": day[:7], "source": "fred"}]}
    s.record_cycle([
        SourceOutcome("space_weather", first, 1.0, kp((f"{day}T00:00:00Z", 2.0))),
        SourceOutcome("energy_prices", first, 1.0, {"error": "EIA_API_KEY [red]not[/red] configured"}),
        SourceOutcome("market_quotes", first, 1.0, quote),
        SourceOutcome("central_bank_rates", first, 1.0, rates),
    ], now=first)
    s.record_cycle([
        SourceOutcome("space_weather", second, 1.0, kp((f"{day}T00:00:00Z", 2.0))),
        SourceOutcome("market_quotes", second, 1.0, quote),
    ], now=second)
    s.close()
    yield day
    if cli._fetcher is not None:  # the shared Cache the command opened
        cli._fetcher.cache.close()


def _invoke(*args):
    return CliRunner().invoke(cli.main, list(args), catch_exceptions=False)


def test_cli_history_day_collected(cli_day) -> None:
    assert "day" in cli.history_group.commands
    result = _invoke("history", "day", cli_day)
    assert result.exit_code == 0
    text = result.output
    assert f"{cli_day} (UTC) by collected date: {cli_day}T00:00:00Z to" in text
    assert "Collector: ran, 6 captures of 4 source(s)" in text
    rows = {line.split()[0]: line for line in text.splitlines() if line.strip()}
    assert "changed" in rows["space_weather"] and "0 +1 first" in rows["space_weather"]
    assert "failed" in rows["energy_prices"]
    assert "EIA_API_KEY [red]not[/red] configured (x1)" in rows["energy_prices"]  # literal
    assert "never_run" in rows["cyclones"]
    assert "Observations saved that day (3 of 3)" in text
    assert "Cannot prove:" in text

    data = json.loads(_invoke("--json-output", "history", "day", cli_day, "--source", "market_quotes").output)
    assert [r["source"] for r in data["sources"]] == ["market_quotes"]
    assert data["sources"][0]["payload_changes"] == {"changed": 0, "unchanged": 1, "first": 1, "not_stored": 0}


def test_cli_history_day_observed(cli_day) -> None:
    text = _invoke("history", "day", cli_day, "--by", "observed").output
    assert f"Values observed on {cli_day} (1 of 1)" in text
    assert f"{cli_day}T00:00:00Z" in text and "instant" in text
    assert "Period values containing the day, not values for it (1 of 1)" in text
    assert "central_bank_rates:federal_funds_rate" in text and "month" in text
    assert "Gap: 1 observation(s) collected this day have no observed_at (market_quotes 1)" in text


def test_cli_history_day_errors(cli_day) -> None:
    bad = CliRunner().invoke(cli.main, ["history", "day", "2026/10/17"])
    assert bad.exit_code == 2 and "YYYY-MM-DD" in bad.output
    tomorrow = _recent_day(-1)
    future = CliRunner().invoke(cli.main, ["history", "day", tomorrow])
    assert future.exit_code == 2 and "in the future" in future.output
    wrong_view = CliRunner().invoke(cli.main, ["history", "day", cli_day, "--by", "hourly"])
    assert wrong_view.exit_code == 2
    unknown = _invoke("history", "day", cli_day, "--source", "nope").output
    assert unknown.startswith("Error:") and "unknown source" in unknown
