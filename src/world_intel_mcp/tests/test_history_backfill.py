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

import copy
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
async def fetcher(db: Path):
    # The history store lives in the cache DB in production; same file here.
    f = Fetcher(cache=Cache(db_path=db), breaker=CircuitBreaker(),
                default_timeout=5.0, max_retries=0)
    try:
        yield f
    finally:
        await f.close()
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
        assert health["state_counts"] == {"ok": 1}  # the collector roster only
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


def _refuse_runtime_init() -> None:
    raise AssertionError("the test initialized the runtime (default cache DB)")


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
    # Set the runtime's attributes in its namespace: reading them first (as
    # setattr does) would initialize the runtime and open the default DB.
    monkeypatch.setattr(tools_mod.runtime, "_initialize", _refuse_runtime_init)
    monkeypatch.setitem(vars(tools_mod.runtime), "cache", types.SimpleNamespace(db_path=db))
    monkeypatch.setitem(vars(tools_mod.runtime), "fetcher", fetcher)
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


# ---------------------------------------------------------------------------
# Review round 2: each test names the mutant it kills
# ---------------------------------------------------------------------------


def _iso_at(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ecb_honouring_start(body: dict):
    """An ECB mock that applies startPeriod the way the Data Portal does:
    to each period's first day (variant of a real response, made here)."""

    def _respond(request: httpx.Request) -> httpx.Response:
        start = request.url.params.get("startPeriod")
        data = copy.deepcopy(body)
        periods = data["structure"]["dimensions"]["observation"][0]["values"]
        keep = {str(i) for i, v in enumerate(periods) if not start or v["start"][:10] >= start}
        for series in data["dataSets"][0]["series"].values():
            series["observations"] = {
                k: v for k, v in series["observations"].items() if k in keep
            }
        return httpx.Response(200, json=data)

    return _respond


def _quarterly_bsi() -> dict:
    """The real BSI response with its three months relabelled as quarters
    (2025-Q4 to 2026-Q2, start and end as the portal writes them)."""
    data = copy.deepcopy(_BSI)
    data["structure"]["dimensions"]["series"][0]["values"] = [{"id": "Q", "name": "Quarterly"}]
    data["structure"]["dimensions"]["observation"][0]["values"] = [
        {"id": "2025-Q4", "name": "2025-Q4", "start": "2025-10-01T00:00:00.000+02:00",
         "end": "2025-12-31T23:59:59.999+01:00"},
        {"id": "2026-Q1", "name": "2026-Q1", "start": "2026-01-01T00:00:00.000+01:00",
         "end": "2026-03-31T23:59:59.999+02:00"},
        {"id": "2026-Q2", "name": "2026-Q2", "start": "2026-04-01T00:00:00.000+02:00",
         "end": "2026-06-30T23:59:59.999+02:00"},
    ]
    return data


_Q_M3 = "BSI.Q.U2.Y.V.M30.X.1.U2.2300.Z01.E"


def _m3_rows(store: HistoryStore, key: str) -> list[tuple]:
    return _rows(store, "SELECT observed_at, value FROM history_observations WHERE series_key = ? "
                 "ORDER BY observed_at", f"backfill:ecb:{key}")


@respx.mock
async def test_backfill_prunes_only_its_own_rows(fetcher, store, monkeypatch) -> None:
    """BF-1. Kills: _Run.prune calling HistoryStore._prune (every source)."""
    for name in ("WORLD_INTEL_HISTORY_DAYS", "WORLD_INTEL_HISTORY_CAPTURE_DAYS",
                 "WORLD_INTEL_HISTORY_PAYLOAD_DAYS"):
        monkeypatch.setenv(name, "0")  # the collector keeps everything
    for age_days, kp in ((400, 2.0), (40, 3.0), (10, 4.0)):
        at = NOW - age_days * 86400
        store.record_cycle([SourceOutcome("space_weather", at, payload={
            "kp_recent": [{"time": _iso_at(at), "kp": kp}], "alerts": []})], now=at)
    counts = "SELECT COUNT(*) FROM {} WHERE source = 'space_weather'"
    tables = ("history_captures", "history_payloads", "history_observations")
    before = [_rows(store, counts.format(t))[0][0] for t in tables]
    assert before == [3, 3, 3]
    for name in ("WORLD_INTEL_HISTORY_DAYS", "WORLD_INTEL_HISTORY_CAPTURE_DAYS",
                 "WORLD_INTEL_HISTORY_PAYLOAD_DAYS"):
        monkeypatch.delenv(name)  # the backfill runs under the defaults
    _fred_route()
    out = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW)
    assert [_rows(store, counts.format(t))[0][0] for t in tables] == before
    assert out["pruned"] == {"payloads": 0, "captures": 0, "observations": 0, "backfill_log": 0}
    assert any("prunes only backfill rows" in g for g in out["data_gaps"])


def _lock_after_first_write(db: Path, monkeypatch) -> list[sqlite3.Connection]:
    """After the first request commits, a second connection takes the write
    lock (BEGIN IMMEDIATE) and holds it: a real lock, not a raised error."""
    real = hb._Run.store_request
    holders: list[sqlite3.Connection] = []

    def _then_lock(self, *args, **kwargs):
        out = real(self, *args, **kwargs)
        if not holders:
            self.store._conn.execute("PRAGMA busy_timeout = 50")
            holder = sqlite3.connect(str(db), timeout=0.05)
            holder.execute("BEGIN IMMEDIATE")
            holders.append(holder)
        return out

    monkeypatch.setattr(hb._Run, "store_request", _then_lock)
    return holders


@respx.mock
def test_cli_store_failure_names_what_it_did_not_write(cli_env, monkeypatch) -> None:
    """BF-2. Kills: _print_backfill reading collected_at/totals of a store
    failure (KeyError), and _store_failure not naming the unwritten ones."""
    _fred_route()
    holders = _lock_after_first_write(cli_env, monkeypatch)
    try:
        out = _invoke("history", "backfill", "fred", "WALCL", "M2SL", "RRPONTSYD", "NOSUCHSERIES")
    finally:
        for h in holders:
            h.rollback()
            h.close()
    assert out.exit_code == 1
    assert ("Error: history store write failed: database is locked; "
            "1 earlier request(s) were committed") in out.output
    assert "Write failed on: M2SL" in out.output
    assert "Fetched but not written: RRPONTSYD" in out.output
    assert "Upstream failures not logged: NOSUCHSERIES" in out.output
    assert "backfill:fred:WALCL" in out.output  # the committed request's row
    check = sqlite3.connect(str(cli_env))
    try:
        assert check.execute("SELECT request, status FROM history_backfills").fetchall() == [
            ("WALCL", "ok")]
    finally:
        check.close()


async def test_store_failure_logs_later_upstream_failures_when_it_can(
    fetcher, store, monkeypatch
) -> None:
    """BF-2. Kills: _store_failure not writing the deferred error captures."""
    real = hb._Run.store_request

    def _m2sl_fails(self, request, *args, **kwargs):
        if request == "M2SL":
            raise sqlite3.OperationalError("disk I/O error")
        return real(self, request, *args, **kwargs)

    monkeypatch.setattr(hb._Run, "store_request", _m2sl_fails)
    with respx.mock:
        _fred_route()
        out = await hb.backfill_fred(fetcher, store, ["WALCL", "M2SL", "NOSUCHSERIES"], now=NOW)
    assert (out["failed_request"], out["fetched_not_written"]) == ("M2SL", [])
    assert (out["upstream_failures_logged"], out["upstream_failures_not_logged"]) == (
        ["NOSUCHSERIES"], [])
    assert _rows(store, "SELECT request, status FROM history_backfills ORDER BY capture_id") == [
        ("WALCL", "ok"), ("NOSUCHSERIES", "error")]
    assert out["collected_at"] == NOW_ISO


async def test_point_cap_gaps_say_which_values_no_backfill_can_load(
    fetcher, store, monkeypatch
) -> None:
    """BF-3. Kills: every truncation reported as the series cap (budget flag
    dropped), and the old "pass a later start or split the call" advice."""
    monkeypatch.setattr(hb, "MAX_POINTS", 5)
    with respx.mock:
        _fred_route()
        capped = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW)
    assert ("Point cap: backfill:fred:WALCL: 4 older value(s) before 2026-09-16 cannot be "
            "loaded by a backfill, which keeps only the newest 5 values of a series."
            ) in capped["data_gaps"]
    assert not any("later start" in g for g in capped["data_gaps"])
    monkeypatch.setattr(hb, "MAX_POINTS", 20_000)
    monkeypatch.setattr(hb, "MAX_POINTS_PER_CALL", 12)
    with respx.mock:
        _fred_route()
        budget = await hb.backfill_fred(fetcher, store, ["WALCL", "RRPONTSYD"], now=NOW + 60)
    assert ("Point cap: backfill:fred:RRPONTSYD: 40 older value(s) before 2026-10-14 were not "
            "loaded because this call's 12-point budget ran out; backfill it in a call of "
            "its own.") in budget["data_gaps"]
    assert _series(budget, "RRPONTSYD")["truncated_by"] == "call budget"


@pytest.mark.parametrize("order", [["NOSUCHSERIES", "WALCL"], ["WALCL", "NOSUCHSERIES"]])
@respx.mock
async def test_health_reports_a_partial_failure_in_either_order(fetcher, store, order) -> None:
    """BF-4. Kills: _backfill_row taking the last-written capture's status."""
    _fred_route()
    await hb.backfill_fred(fetcher, store, order, now=NOW)
    (row,) = store.collection_health([], now=NOW)["backfills"]
    assert (row["last_status"], row["last_run_requests"], row["last_run_failed"]) == ("error", 2, 1)
    assert "NOSUCHSERIES" in row["last_reason"]


@respx.mock
async def test_ecb_stale_cache_copy_is_refused(fetcher, store) -> None:
    """BF-5 / F1. Kills: the ECB stale check (`if stale:`) switched off."""
    fetcher.cache.set(f"liquidity:ecb:ILM:{liquidity._ECB_EUROSYSTEM_KEY}:0:None", _ILM, -60)
    respx.get(_ILM_URL).mock(return_value=httpx.Response(503, text="maintenance"))
    out = await hb.backfill_ecb(fetcher, store, [_ILM_KEY], now=NOW)
    (req,) = out["requests"]
    assert req["status"] == "error" and "only a stale cached copy" in req["error"]
    assert _rows(store, "SELECT COUNT(*) FROM history_observations") == [(0,)]


@respx.mock
async def test_change_flag_compares_runs_with_the_same_start_only(fetcher, store) -> None:
    """BF-5 / F3. Kills: `start IS ?` dropped from the previous-run lookup."""
    _fred_route()
    await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW)
    windowed = await hb.backfill_fred(fetcher, store, ["WALCL"], "2026-10-01", now=NOW + 10)
    assert windowed["requests"][0]["payload_changed"] is None  # first run with this start
    assert store.capture_history("backfill:fred")["captures"][0]["change"] == "first"
    again = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW + 20)
    assert again["requests"][0]["payload_changed"] is False
    assert again["requests"][0]["previous_backfill_at"] == NOW_ISO


@respx.mock
async def test_ecb_quarters_are_dated_by_the_period_end(fetcher, store) -> None:
    """BF-5 / F8. Kills: the period_end fallback for other periods dropped
    (or reading the period id instead)."""
    respx.route(host="data-api.ecb.europa.eu").mock(side_effect=_ecb_honouring_start(_quarterly_bsi()))
    out = await hb.backfill_ecb(fetcher, store, [_Q_M3], now=NOW)
    assert "error" not in out
    assert _m3_rows(store, _Q_M3) == [
        ("2025-12-31", 17611985.0), ("2026-03-31", 17615092.0), ("2026-06-30", 17646463.0)]


@respx.mock
async def test_ecb_start_keeps_periods_dated_on_or_after_it(fetcher, store) -> None:
    """F13. Kills: no lookback (startPeriod = start), and no client-side cut."""
    route = respx.route(host="data-api.ecb.europa.eu")
    route.mock(side_effect=_ecb_honouring_start(_ILM))
    # 2026-W39 runs 09-21 to 09-27 and is dated 09-27, on or after 09-22.
    out = await hb.backfill_ecb(fetcher, store, [_ILM_KEY], "2026-09-22", now=NOW)
    assert "startPeriod=2026-09-16" in str(route.calls.last.request.url)
    assert [r[0] for r in _rows(store, "SELECT observed_at FROM history_observations "
                                       "ORDER BY observed_at")] == [
        "2026-09-27", "2026-10-04", "2026-10-11"]
    assert _series(out, _ILM_KEY)["first_observed_at"] == "2026-09-27"
    route.side_effect = _ecb_honouring_start(_quarterly_bsi())
    # 2026-Q1 (dated 03-31) is returned for the early request, then cut.
    q = await hb.backfill_ecb(fetcher, store, [_Q_M3], "2026-04-01", now=NOW + 10)
    assert "startPeriod=2025-12-30" in str(route.calls.last.request.url)
    assert _m3_rows(store, _Q_M3) == [("2026-06-30", 17646463.0)]
    assert q["requests"][0]["status"] == "ok"
    q2 = await hb.backfill_ecb(fetcher, store, [_Q_M3], "2026-02-15", now=NOW + 20)
    assert [s for s in q2["requests"][0]["series"]
            if s["series_key"] == f"backfill:ecb:{_Q_M3}"][0]["loaded"] == 2


@respx.mock
async def test_ecb_plus_codes_that_match_no_series_are_named(fetcher, store) -> None:
    """BF-6. Kills: _unmatched_codes returning nothing."""
    m30_only = copy.deepcopy(_BSI)
    m30_only["dataSets"][0]["series"] = {
        k: v for k, v in m30_only["dataSets"][0]["series"].items()
        if k == "0:0:0:0:2:0:0:0:0:0:0"
    }  # variant: only the M3 stock series answers
    route = respx.route(host="data-api.ecb.europa.eu")
    route.mock(return_value=httpx.Response(200, json=m30_only))
    key = "BSI.M.U2.Y.V.M30+M3Q.X.1.U2.2300.Z01.E"
    out = await hb.backfill_ecb(fetcher, store, [key], now=NOW)
    assert f"{key}: '+' alternative(s) that matched no series: M3Q" in out["data_gaps"]
    assert out["requests"][0]["status"] == "ok"
    route.return_value = httpx.Response(200, json=_BSI)
    full = await hb.backfill_ecb(fetcher, store, [_BSI_KEY], now=NOW + 10)
    assert not any("matched no series" in g for g in full["data_gaps"])  # all 7 codes answer


@respx.mock
async def test_values_the_publisher_no_longer_serves_are_reported(fetcher, store) -> None:
    """BF-7. Kills: the absent-from-response comparison removed."""
    route = _fred_route()
    await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW)
    # Variant of the real CSV: 2026-09-02 withdrawn, 2026-09-09 now ".".
    body = _FRED["WALCL"].replace("2026-09-02,6737204\n", "").replace(
        "2026-09-09,6740619", "2026-09-09,.")
    assert body.count("\n") == _FRED["WALCL"].count("\n") - 1
    fetcher.cache.delete("liquidity:fred:WALCL")
    route.side_effect = _fred_responder({"WALCL": httpx.Response(200, text=body)})
    out = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW + 60)
    s = _series(out, "WALCL")
    assert (s["absent_from_response"], s["absent_first"], s["absent_last"]) == (
        2, "2026-09-02", "2026-09-09")
    assert out["totals"]["absent_from_response"] == 2
    assert any(g.startswith("backfill:fred:WALCL: 2 stored value(s) from 2026-09-02 to "
                            "2026-09-09 are absent") for g in out["data_gaps"])


@respx.mock
async def test_backfill_payload_bodies_are_a_summary(fetcher, store) -> None:
    """BF-8. Kills: the full response stored as the body (replay size grows
    with the series)."""
    rows = "\n".join(f"{date_}, {i}" for i, date_ in enumerate(
        datetime.fromtimestamp(NOW - d * 86400, tz=timezone.utc).date().isoformat()
        for d in range(15_000, 0, -1)))
    big = "observation_date,DFFX\n" + rows.replace(", ", ",") + "\n"  # synthetic, 15,000 rows
    _fred_route({"DFFX": httpx.Response(200, text=big)})
    out = await hb.backfill_fred(fetcher, store, ["DFFX"], now=NOW)
    assert _series(out, "DFFX")["loaded"] == 15_000
    cap_id = out["requests"][0]["capture_id"]
    replay = store.capture_history("backfill:fred", capture_id=cap_id)["capture"]["payload"]
    assert replay["body"] == hb.BODY_NOTE and "rows" not in replay
    assert replay["series"][0]["points_in_response"] == 15_000
    assert len(json.dumps(replay)) < 2_000
    (logged,) = _rows(store, "SELECT payload_hash FROM history_backfills WHERE capture_id = ?",
                      cap_id)
    assert replay["payload_hash"] == logged[0] and len(logged[0]) == 64  # the full body's hash


@respx.mock
async def test_one_transaction_per_request(fetcher, store, monkeypatch) -> None:
    """F2. Kills: a commit inside store_request before the observations."""

    def _boom(*args, **kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(HistoryStore, "_store_observations", _boom)
    _fred_route()
    out = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW)
    assert out["error"].startswith("history store write failed")
    for table in ("history_captures", "history_payloads", "history_backfills"):
        assert _rows(store, f"SELECT COUNT(*) FROM {table}") == [(0,)], table


async def test_previous_run_is_the_latest_ok_run(fetcher, store) -> None:
    """F3. Kills: `status = 'ok'` dropped (a failed run becomes the previous
    one), and ORDER BY ran_at ASC (the oldest run becomes the previous one)."""
    revised = _FRED["WALCL"].replace("2026-10-07,6747560", "2026-10-07,6747561")
    with respx.mock:
        route = _fred_route()
        await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW)  # A
        fetcher.cache.delete("liquidity:fred:WALCL")
        route.side_effect = _fred_responder({"WALCL": httpx.Response(500, text="down")})
        failed = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW + 10)
        assert failed["requests"][0]["status"] == "error"
        route.side_effect = _fred_responder({"WALCL": httpx.Response(200, text=revised)})
        b1 = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW + 20)  # B
        b2 = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW + 30)  # B again
    assert b1["requests"][0]["payload_changed"] is True
    assert b1["requests"][0]["previous_backfill_at"] == NOW_ISO  # not the failed run
    assert b2["requests"][0]["payload_changed"] is False
    assert b2["requests"][0]["previous_backfill_at"] == _iso_at(NOW + 20)


@respx.mock
async def test_history_days_zero_keeps_the_backfill_log(fetcher, store, monkeypatch) -> None:
    """F3. Kills: `history_days > 0` loosened to `>= 0` (0 = keep forever)."""
    monkeypatch.setenv("WORLD_INTEL_HISTORY_DAYS", "0")
    _fred_route()
    await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW)
    later = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW + 400 * 86400)
    assert later["pruned"]["backfill_log"] == 0 and later["pruned"]["observations"] == 0
    assert later["requests"][0]["payload_changed"] is False


@respx.mock
async def test_copy_fetched_at_is_the_copys_own_fetch_time(fetcher, store) -> None:
    """F4. Kills: _copy_at returning the read time instead of created_at."""
    route = _fred_route()
    fetcher.cache.set("liquidity:fred:WALCL", _FRED["WALCL"], 3600)
    fetched = time.time() - 1800
    fetcher.cache._get_conn().execute(
        "UPDATE cache SET created_at = ? WHERE key = 'liquidity:fred:WALCL'", (fetched,))
    fetcher.cache._get_conn().commit()
    out = await hb.backfill_fred(fetcher, store, ["WALCL"], now=NOW)
    assert route.call_count == 0  # the cached copy was read
    assert out["requests"][0]["copy_fetched_at"] == _iso_at(fetched)


@respx.mock
async def test_capture_row_carries_the_call_stamp(fetcher, store) -> None:
    """F5. Kills: the ok capture stamped with time.time() instead of the
    call's collected_at."""
    _fred_route()
    await hb.backfill_fred(fetcher, store, ["WALCL", "M2SL"], now=NOW)
    caps = store.capture_history("backfill:fred")["captures"]
    assert [c["collected_at"] for c in caps] == [NOW_ISO, NOW_ISO]


@respx.mock
def test_cli_partial_failure_exits_1_and_health_says_so(cli_env) -> None:
    """F6, and BF-4 on the CLI. Kills: the exit check reduced to
    `"error" in data`, and health printing only the last capture's status."""
    _fred_route()
    out = _invoke("history", "backfill", "fred", "WALCL", "NOSUCHSERIES")
    assert out.exit_code == 1 and "backfill:fred:WALCL" in out.output
    health = _invoke("history", "health").output
    assert "(error, 1 of 2 request(s) failed)" in health
    assert "Last run failed: FRED graph CSV for NOSUCHSERIES" in health


@respx.mock
async def test_start_on_an_observation_date_keeps_it(fetcher, store) -> None:
    """F7. Kills: `d >= start_d` changed to `d > start_d`."""
    _fred_route()
    out = await hb.backfill_fred(fetcher, store, ["WALCL"], "2026-10-07", now=NOW)
    s = _series(out, "WALCL")
    assert (s["loaded"], s["first_observed_at"]) == (2, "2026-10-07")


@respx.mock
async def test_caps_accept_exactly_the_documented_maximum(fetcher, store) -> None:
    """F9. Kills: `>` changed to `>=` for 5 FRED ids, 2 ECB keys and 20
    combinations."""
    _fred_route()
    five = await hb.backfill_fred(fetcher, store, ["WALCL", "M2SL", "RRPONTSYD", "AAA", "BBB"],
                                  now=NOW)
    assert len(five["requests"]) == 5 and "at most" not in str(five.get("error"))
    ecb = respx.route(host="data-api.ecb.europa.eu").mock(
        return_value=httpx.Response(200, json=_ILM))
    two = await hb.backfill_ecb(
        fetcher, store, [_ILM_KEY, "ILM.W.U2.C.T000000.Z5.Z01+Z06"], now=NOW + 10)
    assert len(two["requests"]) == 2 and "error" not in two
    twenty = "BSI.M.U2.Y.V.M10+M20+M30+M3Q+M3R.X.1+4+I+X.U2.2300.Z01.E"  # 5 x 4 = 20
    out = await hb.backfill_ecb(fetcher, store, [twenty], now=NOW + 20)
    assert out["requests"] and "combinations" not in str(out.get("error"))
    assert ecb.call_count == 3


@respx.mock
async def test_ecb_keys_repeated_in_any_case_are_one_request(fetcher, store) -> None:
    """F10. Kills: the ECB dict.fromkeys dedupe removed."""
    route = respx.get(_ILM_URL).mock(return_value=httpx.Response(200, json=_ILM))
    out = await hb.backfill_ecb(
        fetcher, store, [_ILM_KEY, "ilm." + liquidity._ECB_EUROSYSTEM_KEY], now=NOW)
    assert [r["request"] for r in out["requests"]] == [_ILM_KEY]
    assert route.call_count == 1


def test_tool_description_states_what_a_capture_and_missing_count_are() -> None:
    """BF-9. Kills: the old "one capture per series" / "missing values are
    skipped and counted" wording."""
    (tool,) = _backfill_tools().TOOLS
    assert "one capture per request (a FRED id, or an ECB key that may match" in tool.description
    assert "ECB missing values cannot be counted (null)" in tool.description
    assert "one capture per series" not in tool.description
