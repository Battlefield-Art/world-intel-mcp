"""History backfill (history_backfill.py) into the history store.

Upstream bodies are the real responses under fixtures/liquidity/ (captured
2026-10-17, see test_sources_liquidity.py): FRED graph CSVs for WALCL (9
weeks), M2SL (14 months) and RRPONTSYD (45 business days with its two real
blank cells, 2026-09-07 and 2026-10-12), the ECB BSI money aggregates
response (12 series, 2026-06 to 2026-08, 2026-08 provisional), the ILM
Eurosystem total assets response (weeks 2026-W39 to W41) and the portal's
real 404 problem body. Variants (one changed value, a "." cell, a NaN
cell, empty and HTML bodies) are made in the test from those files and
say so. Every expected value is read off the fixture files.

Gaps / not covered: live requests (run by hand, see the CHANGELOG entry);
concurrent writers (the collector writing the same database during a
backfill); FRED or ECB responses beyond the fixtures' size.
"""

from __future__ import annotations

import importlib
import json
import os
import sqlite3
import time
import types
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx
from click.testing import CliRunner

import world_intel_mcp.fetcher as fetcher_mod
from world_intel_mcp import cli
from world_intel_mcp import history_backfill as hb
from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.history import BACKFILL_VINTAGE_NOTE, HistoryStore, SourceOutcome
from world_intel_mcp.sources import liquidity

_FIX = Path(__file__).parent / "fixtures" / "liquidity"
_FRED = {sid: (_FIX / f"fred_{sid}.csv").read_text() for sid in ("WALCL", "RRPONTSYD", "M2SL")}
_BSI = json.loads((_FIX / "ecb_bsi_money.json").read_text())
_ILM = json.loads((_FIX / "ecb_ilm_total.json").read_text())
_ECB_404 = (_FIX / "ecb_404_problem.json").read_text()

_FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
_BSI_KEY = "BSI." + liquidity._ECB_MONEY_KEY  # 18 combinations, 12 series answer
_ILM_KEY = "ILM." + liquidity._ECB_EUROSYSTEM_KEY
_BSI_URL = f"https://data-api.ecb.europa.eu/service/data/BSI/{liquidity._ECB_MONEY_KEY}"
_ILM_URL = f"https://data-api.ecb.europa.eu/service/data/ILM/{liquidity._ECB_EUROSYSTEM_KEY}"

NOW = datetime(2026, 10, 17, 20, 0, tzinfo=timezone.utc).timestamp()
NOW_ISO = "2026-10-17T20:00:00Z"

WALCL = [
    ("2026-08-19", 6745699.0), ("2026-08-26", 6730912.0), ("2026-09-02", 6737204.0),
    ("2026-09-09", 6740619.0), ("2026-09-16", 6746548.0), ("2026-09-23", 6747704.0),
    ("2026-09-30", 6743031.0), ("2026-10-07", 6747560.0), ("2026-10-14", 6755639.0),
]


@pytest.fixture(autouse=True)
def _no_fred_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(fetcher_mod._SOURCE_RATE_LIMITS, liquidity.FRED_SOURCE, 0.0)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return tmp_path / "cache.db"


@pytest.fixture
def fetcher(db: Path):
    # The history store lives in the cache DB in production; same file here.
    f = Fetcher(cache=Cache(db_path=db), breaker=CircuitBreaker(),
                default_timeout=5.0, max_retries=0)
    yield f
    f.cache.close()


@pytest.fixture
def store(db: Path):
    s = HistoryStore(db)
    yield s
    s.close()


def _fred_responder(bodies: dict[str, httpx.Response] | None = None):
    bodies = bodies or {}

    def _respond(request: httpx.Request) -> httpx.Response:
        sid = request.url.params.get("id")
        if sid in bodies:
            return bodies[sid]
        if sid in _FRED:
            return httpx.Response(200, text=_FRED[sid])
        return httpx.Response(404, text="<html>Error - St. Louis Fed</html>")

    return _respond


def _fred_route(bodies: dict[str, httpx.Response] | None = None):
    return respx.get(_FRED_URL).mock(side_effect=_fred_responder(bodies))


def _series(result: dict, request: str) -> dict:
    (req,) = [r for r in result["requests"] if r["request"] == request]
    assert req["status"] == "ok", req
    (s,) = req["series"]
    return s


def _rows(store: HistoryStore, sql: str, *params) -> list[tuple]:
    return store._conn.execute(sql, params).fetchall()


# ---------------------------------------------------------------------------
# FRED
# ---------------------------------------------------------------------------


@respx.mock
async def test_fred_backfill_stores_every_value_with_three_dates(fetcher, store) -> None:
    route = _fred_route()
    out = await hb.backfill_fred(fetcher, store, ["walcl"], now=NOW)
    assert route.call_count == 1 and "error" not in out
    s = _series(out, "WALCL")
    assert s["series_key"] == "backfill:fred:WALCL"
    assert (s["points_in_response"], s["loaded"], s["stored_new"], s["stored_revised"],
            s["unchanged"], s["missing_skipped"], s["invalid_skipped"]) == (9, 9, 9, 0, 0, 0, 0)
    assert (s["first_observed_at"], s["last_observed_at"]) == ("2026-08-19", "2026-10-14")
    assert out["collected_at"] == NOW_ISO and out["totals"]["failed"] == 0

    hist = store.series_history("backfill:fred:WALCL")
    assert hist["source"] == "backfill:fred"
    got = [(o["observed_at"], o["value"]) for o in reversed(hist["observations"])]
    assert got == WALCL
    assert {o["published_at"] for o in hist["observations"]} == {None}
    assert {o["collected_at"] for o in hist["observations"]} == {NOW_ISO}
    assert {o["unit"] for o in hist["observations"]} == {"USD millions"}
    assert hist["observations"][0]["label"] == "FRED WALCL: Federal Reserve total assets"
    assert BACKFILL_VINTAGE_NOTE in hist["data_gaps"]
    assert any("graph CSV carries observation dates and values only" in g
               for g in out["data_gaps"])

    (cap,) = store.capture_history("backfill:fred")["captures"]
    assert (cap["status"], cap["change"], cap["observations_stored"]) == ("ok", "first", 9)
    (log,) = _rows(store, "SELECT source, request, start, status, ran_at FROM history_backfills")
    assert log == ("backfill:fred", "WALCL", None, "ok", NOW)


@respx.mock
async def test_fred_rerun_stores_nothing_and_reuses_the_cached_response(fetcher, store) -> None:
    route = _fred_route()
    first = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW)
    second = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW + 60)
    assert route.call_count == 1  # second read the 1 h cache copy, no request
    s = _series(second, "WALCL")
    assert (s["stored_new"], s["stored_revised"], s["unchanged"]) == (0, 0, 9)
    req = second["requests"][0]
    assert req["payload_changed"] is False and req["previous_backfill_at"] == NOW_ISO
    assert req["copy_fetched_at"] == first["requests"][0]["copy_fetched_at"]
    copy_at = datetime.strptime(req["copy_fetched_at"], "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc).timestamp()
    assert abs(copy_at - time.time()) < 120  # the real fetch time, not the backfill clock

    # Refetched (cache expired): still nothing new.
    fetcher.cache.delete("liquidity:fred:WALCL")
    third = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW + 120)
    assert route.call_count == 2
    assert _series(third, "WALCL")["stored_new"] == 0
    assert _rows(store, "SELECT COUNT(*) FROM history_observations") == [(9,)]
    changes = [c["change"] for c in store.capture_history("backfill:fred")["captures"]]
    assert changes == ["unchanged", "unchanged", "first"]


@respx.mock
async def test_changed_upstream_value_is_exactly_one_revision(fetcher, store) -> None:
    route = _fred_route()
    await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW)
    revised = _FRED["WALCL"].replace("2026-10-07,6747560", "2026-10-07,6747561")
    assert revised != _FRED["WALCL"]  # variant of the real CSV: one value revised
    fetcher.cache.delete("liquidity:fred:WALCL")
    route.side_effect = _fred_responder({"WALCL": httpx.Response(200, text=revised)})
    out = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW + 3600)
    s = _series(out, "WALCL")
    assert (s["stored_new"], s["stored_revised"], s["unchanged"]) == (0, 1, 8)
    assert out["requests"][0]["payload_changed"] is True
    revs = store.revisions(series_key="backfill:fred:WALCL")
    assert revs["count"] == 1
    (rev,) = revs["revisions"]
    assert (rev["observed_at"], rev["previous_value"], rev["revised_value"], rev["delta"]) == (
        "2026-10-07", 6747560.0, 6747561.0, 1.0)
    assert rev["previous_collected_at"] == NOW_ISO


@respx.mock
async def test_collected_at_is_stamped_after_the_fetches_it_stores(fetcher, store) -> None:
    """Without a fixed clock, one stamp per call taken after its last fetch:
    no stored row is dated before the response it came from."""
    _fred_route()
    out = await hb.backfill_fred(fetcher, store, ["WALCL", "M2SL"])
    logs = _rows(store, "SELECT ran_at, copy_fetched_at FROM history_backfills")
    assert len(logs) == 2 and all(ran >= fetched for ran, fetched in logs)
    assert len({ran for ran, _ in logs}) == 1
    assert _rows(store, "SELECT DISTINCT collected_at FROM history_observations") == [(logs[0][0],)]
    assert out["collected_at"] >= max(r["copy_fetched_at"] for r in out["requests"])


@respx.mock
async def test_repeated_ids_in_any_case_are_one_request(fetcher, store) -> None:
    route = _fred_route()
    out = await hb.backfill_fred(fetcher, store, "walcl, WALCL M2SL", now=NOW)
    assert [r["request"] for r in out["requests"]] == ["WALCL", "M2SL"]
    assert route.call_count == 2 and out["totals"]["stored_new"] == 9 + 14


@respx.mock
async def test_missing_values_are_skipped_and_counted_never_zero(fetcher, store) -> None:
    _fred_route()
    out = await hb.backfill_fred(fetcher, store, ["RRPONTSYD"], now=NOW)
    s = _series(out, "RRPONTSYD")
    assert (s["points_in_response"], s["missing_skipped"], s["loaded"]) == (45, 2, 43)
    stored = dict(_rows(store, "SELECT observed_at, value FROM history_observations"))
    assert "2026-09-07" not in stored and "2026-10-12" not in stored
    assert stored["2026-10-16"] == 0.1 and 0.0 not in stored.values()
    cap = store.capture_history("backfill:fred")["captures"][0]
    assert cap["extract_error"] == "RRPONTSYD: 2 missing value(s) skipped"
    assert out["totals"]["missing_skipped"] == 2


@respx.mock
async def test_dot_and_nan_cells_are_counted_apart(fetcher, store) -> None:
    # Variant of the real CSV: FRED's other missing marker "." and a NaN cell.
    body = _FRED["WALCL"].replace("2026-09-02,6737204", "2026-09-02,.").replace(
        "2026-09-09,6740619", "2026-09-09,NaN")
    _fred_route({"WALCL": httpx.Response(200, text=body)})
    s = _series(await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW), "WALCL")
    assert (s["missing_skipped"], s["invalid_skipped"], s["loaded"]) == (1, 1, 7)
    observed = {r[0] for r in _rows(store, "SELECT observed_at FROM history_observations")}
    assert observed == {d for d, _ in WALCL} - {"2026-09-02", "2026-09-09"}


@pytest.mark.parametrize(
    "response, reason",
    [
        (httpx.Response(500, text="upstream down"), "unavailable"),
        (httpx.Response(200, text=""), "not in the expected format"),
        (httpx.Response(200, text="<html><body>Error</body></html>"), "not in the expected format"),
        (httpx.Response(404, text="<html>Error</html>"), "unavailable"),
        (httpx.Response(200, text="observation_date,WALCL\n2026-10-14,\n"), "no observations"),
    ],
    ids=["http-500", "empty-body", "html-body", "unknown-id-404", "all-blank"],
)
@respx.mock
async def test_fetch_failures_are_errors_never_an_empty_success(
    fetcher, store, response, reason
) -> None:
    _fred_route({"WALCL": response})
    out = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW)
    assert out["error"].startswith("no request succeeded: WALCL: ")
    (req,) = out["requests"]
    assert req["status"] == "error" and reason in req["error"]
    assert out["totals"]["failed"] == 1 and out["totals"]["loaded"] == 0
    assert _rows(store, "SELECT COUNT(*) FROM history_observations") == [(0,)]
    (cap,) = store.capture_history("backfill:fred")["captures"]
    assert cap["status"] == "error" and reason in cap["reason"]
    assert _rows(store, "SELECT status FROM history_backfills") == [("error",)]


@respx.mock
async def test_partial_failure_is_reported_not_hidden(fetcher, store) -> None:
    _fred_route()
    out = await hb.backfill_fred(fetcher, store, ["WALCL", "NOSUCHSERIES"], now=NOW)
    assert "error" not in out and out["totals"]["failed"] == 1
    assert out["data_gaps"][0].startswith("1 of 2 request(s) failed and stored nothing: NOSUCHSERIES")
    assert _series(out, "WALCL")["stored_new"] == 9


@respx.mock
async def test_stale_cache_copy_is_refused(fetcher, store) -> None:
    fetcher.cache.set("liquidity:fred:WALCL", _FRED["WALCL"], -60)  # expired copy
    _fred_route({"WALCL": httpx.Response(503, text="maintenance")})
    out = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW)
    (req,) = out["requests"]
    assert req["status"] == "error" and "only a stale cached copy" in req["error"]
    assert _rows(store, "SELECT COUNT(*) FROM history_observations") == [(0,)]


@respx.mock
async def test_start_window_and_unknown_units(fetcher, store) -> None:
    _fred_route({"DGS10": httpx.Response(200, text=_FRED["WALCL"].replace("WALCL", "DGS10"))})
    out = await hb.backfill_fred(fetcher, store, ["WALCL", "DGS10"], "2026-10-01", now=NOW)
    s = _series(out, "WALCL")
    assert (s["points_in_response"], s["in_window"], s["loaded"]) == (9, 2, 2)
    assert (s["first_observed_at"], s["last_observed_at"]) == ("2026-10-07", "2026-10-14")
    other = _series(out, "DGS10")
    assert other["unit"] is None and other["label"] == "FRED DGS10"
    assert any(g.startswith("DGS10: units unknown") for g in out["data_gaps"])
    # A window with no values is an error, not an empty success.
    empty = await hb.backfill_fred(fetcher, store, ["M2SL"], "2026-09-01", now=NOW)
    assert "no values on or after 2026-09-01" in empty["requests"][0]["error"]


@pytest.mark.parametrize(
    "ids, start, message",
    [
        ([], None, "at least one"),
        (["A", "B", "C", "D", "E", "F"], None, "at most 5"),
        (["WAL-CL"], None, "not a FRED series id"),
        (["WALCL"], "2026-13-01", "YYYY-MM-DD"),
        (["WALCL"], "20261001", "YYYY-MM-DD"),
        (["WALCL"], "2999-01-01", "in the future"),
    ],
)
@respx.mock
async def test_bad_arguments_are_refused_before_any_request(fetcher, store, ids, start, message) -> None:
    route = _fred_route()
    out = await hb.backfill_fred(fetcher, store, ids, start, now=NOW)
    assert message in out["error"] and out["requests"] == []
    assert route.call_count == 0
    assert _rows(store, "SELECT COUNT(*) FROM history_captures") == [(0,)]


@respx.mock
async def test_point_caps_keep_the_newest_and_say_so(fetcher, store, monkeypatch) -> None:
    _fred_route()
    monkeypatch.setattr(hb, "MAX_POINTS", 5)
    monkeypatch.setattr(hb, "MAX_POINTS_PER_CALL", 10)
    out = await hb.backfill_fred(fetcher, store, ["WALCL", "M2SL", "RRPONTSYD"], now=NOW)
    walcl, m2 = _series(out, "WALCL"), _series(out, "M2SL")
    assert (walcl["loaded"], walcl["truncated"], walcl["first_observed_at"]) == (5, 4, "2026-09-16")
    assert (m2["loaded"], m2["truncated"], m2["first_observed_at"]) == (5, 9, "2026-04-01")
    (rrp,) = [r for r in out["requests"] if r["request"] == "RRPONTSYD"]
    assert rrp["status"] == "error" and "point budget" in rrp["error"]  # none of 10 left
    assert any(g.startswith("Point cap:") for g in out["data_gaps"])


async def test_rrp_gets_what_is_left_of_the_call_budget(fetcher, store, monkeypatch) -> None:
    with respx.mock:
        _fred_route()
        monkeypatch.setattr(hb, "MAX_POINTS_PER_CALL", 12)
        out = await hb.backfill_fred(fetcher, store, ["WALCL", "RRPONTSYD"], now=NOW)
    rrp = _series(out, "RRPONTSYD")
    assert (rrp["loaded"], rrp["truncated"], rrp["last_observed_at"]) == (3, 40, "2026-10-16")


@respx.mock
async def test_values_pruned_past_retention_are_stored_again(fetcher, store, monkeypatch) -> None:
    _fred_route()
    await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW)
    later = NOW + 400 * 86400
    out = await hb.backfill_fred(fetcher, store, ["WALCL"], now=later)
    assert out["pruned"]["observations"] == 9 and out["pruned"]["backfill_log"] == 1
    assert _series(out, "WALCL")["stored_new"] == 9
    assert out["requests"][0]["payload_changed"] is None  # its earlier log row was pruned
    hist = store.series_history("backfill:fred:WALCL")
    assert {o["collected_at"] for o in hist["observations"]} == {
        datetime.fromtimestamp(later, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}


# ---------------------------------------------------------------------------
# ECB Data Portal
# ---------------------------------------------------------------------------


@respx.mock
async def test_ecb_backfill_loads_every_series_of_the_key(fetcher, store) -> None:
    route = respx.get(_BSI_URL).mock(return_value=httpx.Response(200, json=_BSI))
    out = await hb.backfill_ecb(fetcher, store, [_BSI_KEY], now=NOW)
    assert "lastNObservations" not in str(route.calls.last.request.url)  # full history
    (req,) = out["requests"]
    assert req["status"] == "ok" and len(req["series"]) == 12
    by_key = {s["series_key"]: s for s in req["series"]}
    m3 = by_key["backfill:ecb:BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.E"]
    assert (m3["loaded"], m3["provisional"], m3["missing_skipped"]) == (3, 1, None)
    expected_units = {
        s["key"]: s["units"] for s in liquidity.parse_sdmx_json(_BSI, "BSI")
    }["BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.E"]
    hist = store.series_history("backfill:ecb:BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.E")
    assert [(o["observed_at"], o["value"], o["unit"]) for o in hist["observations"]] == [
        ("2026-08", 17646463.0, expected_units),
        ("2026-07", 17615092.0, expected_units),
        ("2026-06", 17611985.0, expected_units),
    ]
    assert out["totals"]["missing_skipped"] is None  # unknown, not zero
    assert any("12 point(s) the ECB marks provisional" in g for g in out["data_gaps"])
    assert any("published_at is NULL" in g for g in out["data_gaps"])


@respx.mock
async def test_ecb_weeks_are_dated_by_the_sunday_that_ends_them(fetcher, store) -> None:
    route = respx.get(_ILM_URL).mock(return_value=httpx.Response(200, json=_ILM))
    out = await hb.backfill_ecb(fetcher, store, [_ILM_KEY], "2026-09-01", now=NOW)
    assert "startPeriod=2026-09-01" in str(route.calls.last.request.url)
    s = _series(out, _ILM_KEY)
    assert s["series_key"] == "backfill:ecb:ILM.W.U2.C.T000000.Z5.Z01"
    rows = _rows(store, "SELECT observed_at, value FROM history_observations ORDER BY observed_at")
    assert rows == [("2026-09-27", 5897262.0), ("2026-10-04", 5947826.0), ("2026-10-11", 5943911.0)]
    again = await hb.backfill_ecb(fetcher, store, [_ILM_KEY], "2026-09-01", now=NOW + 60)
    assert route.call_count == 1 and _series(again, _ILM_KEY)["unchanged"] == 3
    assert again["requests"][0]["payload_changed"] is False
    # Both runs read one cached copy, found under the liquidity cache key.
    assert out["requests"][0]["copy_fetched_at"] is not None
    assert again["requests"][0]["copy_fetched_at"] == out["requests"][0]["copy_fetched_at"]


@pytest.mark.parametrize(
    "response, reason",
    [
        (httpx.Response(404, text=_ECB_404), "no series matches the key"),
        (httpx.Response(200, text=""), "no observations on or after"),
        (httpx.Response(200, text="{not json"), "not in the expected SDMX-JSON shape"),
        (httpx.Response(500, text="oops"), "upstream fetch failed"),
    ],
    ids=["404", "empty", "malformed", "http-500"],
)
@respx.mock
async def test_ecb_failures_are_errors(fetcher, store, response, reason) -> None:
    respx.get(_ILM_URL).mock(return_value=response)
    out = await hb.backfill_ecb(fetcher, store, [_ILM_KEY], now=NOW)
    assert out["requests"][0]["status"] == "error" and reason in out["requests"][0]["error"]
    assert "error" in out
    assert _rows(store, "SELECT COUNT(*) FROM history_observations") == [(0,)]


@pytest.mark.parametrize(
    "keys, message",
    [
        (["XYZ.M.U2"], "starts with its dataset"),
        (["BSI.M.U2.Y.V.M30"], "names all 11 dimensions"),
        (["ILM.W.U2.C..Z5.Z01"], "names all 6 dimensions"),
        (["BSI.M.U2.Y.V.M10+M20+M30.X.1+4+I.U2.2300.Z01+Z02.E+A"], "36 series combinations"),
        (["BSI.M.U2;DROP"], "letters, digits"),
        ([_BSI_KEY, _ILM_KEY, "BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.E"], "at most 2"),
    ],
)
@respx.mock
async def test_ecb_bad_keys_are_refused_before_any_request(fetcher, store, keys, message) -> None:
    route = respx.route(host="data-api.ecb.europa.eu").mock(
        return_value=httpx.Response(200, json=_ILM))
    out = await hb.backfill_ecb(fetcher, store, keys, now=NOW)
    assert message in out["error"] and route.call_count == 0


# ---------------------------------------------------------------------------
# Store integration: migration, health, per-request change
# ---------------------------------------------------------------------------


@respx.mock
async def test_migrates_a_database_made_by_the_current_schema(db, fetcher) -> None:
    old = HistoryStore(db)  # the schema as the store creates it, no backfill table
    old.record_cycle(
        [SourceOutcome("space_weather", NOW - 600, payload={
            "kp_recent": [{"time": "2026-10-17T18:00:00Z", "kp": 2.33}], "alerts": []})],
        now=NOW - 600,
    )
    before = _rows(old, "SELECT * FROM history_observations")
    tables = {r[0] for r in _rows(old, "SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert "history_backfills" not in tables and len(before) == 1
    old.close()

    _fred_route()
    store = HistoryStore(db)
    try:
        await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW)
        hb.ensure_schema(store)  # idempotent on a migrated database
        assert _rows(store, "SELECT * FROM history_observations WHERE source = 'space_weather'") == before
        assert _rows(store, "SELECT COUNT(*) FROM history_backfills") == [(1,)]
    finally:
        store.close()
    reopened = HistoryStore(db)
    try:
        health = reopened.collection_health(["space_weather"], now=NOW)
        assert [r["source"] for r in health["sources"]] == ["space_weather"]
        (bf,) = health["backfills"]
        assert (bf["source"], bf["captures"], bf["series"], bf["observations"]) == (
            "backfill:fred", 1, 1, 9)
        assert (bf["first_observed_at"], bf["latest_observed_at"]) == ("2026-08-19", "2026-10-14")
        assert "backfill" not in json.dumps(health["state_counts"])
        one = reopened.collection_health(["space_weather"], source="backfill:fred", now=NOW)
        assert one["sources"] == [] and one["backfills"][0]["source"] == "backfill:fred"
        bad = reopened.collection_health(["space_weather"], source="nope", now=NOW)
        assert "backfill:fred" in bad["known_sources"]
        collector_series = reopened.series_history("space_weather:kp")
        assert BACKFILL_VINTAGE_NOTE not in collector_series["data_gaps"]
    finally:
        reopened.close()


@respx.mock
async def test_capture_change_compares_the_same_request_only(fetcher, store) -> None:
    _fred_route()
    for sid, at in (("WALCL", NOW), ("M2SL", NOW + 1), ("WALCL", NOW + 2)):
        await hb.backfill_fred(fetcher, store, [sid], now=at)
    changes = [c["change"] for c in store.capture_history("backfill:fred")["captures"]]
    assert changes == ["unchanged", "first", "first"]  # newest first: WALCL, M2SL, WALCL


async def test_store_error_is_reported_with_what_was_committed(fetcher, store, monkeypatch) -> None:
    with respx.mock:
        _fred_route()
        real = hb._Run.store_request
        calls = {"n": 0}

        def _flaky(self, *args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 2:
                raise sqlite3.OperationalError("database is locked")
            return real(self, *args, **kwargs)

        monkeypatch.setattr(hb._Run, "store_request", _flaky)
        out = await hb.backfill_fred(fetcher, store, ["WALCL", "M2SL"], now=NOW)
    assert out["error"] == ("history store write failed: database is locked; "
                            "1 earlier request(s) were committed")
    assert [r["request"] for r in out["requests"]] == ["WALCL"]


# ---------------------------------------------------------------------------
# MCP tool and CLI
# ---------------------------------------------------------------------------


def _backfill_tools():
    from .test_server_registry import _TMP_CACHE

    prior = os.environ.get("WORLD_INTEL_CACHE_DB")
    os.environ["WORLD_INTEL_CACHE_DB"] = str(_TMP_CACHE)
    try:
        importlib.import_module("world_intel_mcp.tools")
        return importlib.import_module("world_intel_mcp.tools.history_backfill")
    finally:
        if prior is None:
            os.environ.pop("WORLD_INTEL_CACHE_DB", None)
        else:
            os.environ["WORLD_INTEL_CACHE_DB"] = prior


@respx.mock
async def test_mcp_tool_writes_the_runtime_store_within_the_caps(db, fetcher, monkeypatch) -> None:
    tools_mod = _backfill_tools()
    monkeypatch.setattr(tools_mod.runtime, "cache", types.SimpleNamespace(db_path=db), raising=False)
    monkeypatch.setattr(tools_mod.runtime, "fetcher", fetcher, raising=False)
    handler = tools_mod.HANDLERS["intel_history_backfill"]
    _fred_route()
    out = await handler({"provider": "FRED", "series": ["WALCL"], "start": "2026-10-01"})
    assert _series(out, "WALCL")["loaded"] == 2 and out["db_path"] == str(db)
    assert "provider must be" in (await handler({"provider": "bls", "series": ["X"]}))["error"]
    too_many = await handler({"provider": "fred", "series": ["A", "B", "C", "D", "E", "F"]})
    assert "at most 5" in too_many["error"]
    (tool,) = tools_mod.TOOLS
    assert "at most 5 FRED series or 2 ECB keys" in tool.description
    assert "\u2014" not in tool.description  # no em dash in user-facing text


@pytest.fixture
def cli_env(db, monkeypatch):
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(db))
    monkeypatch.setenv("COLUMNS", "220")
    monkeypatch.setattr(cli, "_fetcher", None)
    yield db
    if cli._fetcher is not None:
        cli._fetcher.cache.close()


def _invoke(*args):
    return CliRunner().invoke(cli.main, list(args), catch_exceptions=False)


def test_cli_backfill_registry() -> None:
    assert set(cli.backfill_group.commands) == {"fred", "ecb"}
    assert cli.history_group.commands["backfill"] is cli.backfill_group
    assert "backfill" not in cli.main.commands  # under intel history only


@respx.mock
def test_cli_backfill_fred_table_json_and_exit_codes(cli_env) -> None:
    _fred_route()
    first = _invoke("history", "backfill", "fred", "WALCL", "RRPONTSYD")
    assert first.exit_code == 0, first.output
    assert "backfill:fred:WALCL" in first.output and "backfill:fred:RRPONTSYD" in first.output
    assert "Stored 52 new and 0 revised values, 0 unchanged" in first.output
    again = json.loads(_invoke("--json-output", "history", "backfill", "fred", "WALCL").output)
    assert again["totals"]["unchanged"] == 9 and again["totals"]["stored_new"] == 0

    failed = _invoke("history", "backfill", "fred", "NOSUCHSERIES")
    assert failed.exit_code == 1 and "error" in failed.output
    refused = _invoke("history", "backfill", "fred", "WALCL", "--start", "yesterday")
    assert refused.exit_code == 1 and "YYYY-MM-DD" in refused.output

    series = _invoke("history", "series", "backfill:fred:WALCL").output
    assert "2026-10-14" in series and "6.75564e+06" in series and "Backfilled:" in series
    health = _invoke("history", "health").output
    assert "Backfill backfill:fred: 4 capture(s)" in health
    assert "52 observations in 2 series, observed 2026-08-17 to 2026-10-16" in health


@respx.mock
def test_cli_backfill_ecb(cli_env) -> None:
    respx.get(_ILM_URL).mock(return_value=httpx.Response(200, json=_ILM))
    out = _invoke("history", "backfill", "ecb", _ILM_KEY)
    assert out.exit_code == 0, out.output
    assert "backfill:ecb:ILM.W.U2.C.T000000.Z5.Z01" in out.output
    assert "2026-09-27" in out.output and "2026-10-11" in out.output
    assert "unknown" in out.output  # ECB missing values cannot be counted
