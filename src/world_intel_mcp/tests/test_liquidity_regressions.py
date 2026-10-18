"""Mocked regressions for liquidity data honesty and bounded requests."""

import copy
import csv
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
import respx

from world_intel_mcp.sources import liquidity
from world_intel_mcp.tests.test_sources_liquidity import (
    _BSI, _ECB_404, _FRED, _FRED_URL, _ECB_BSI_URL, _ECB_ILM_URL,
    _fixed_clock_and_no_throttle, _fred_router, _mock_all, fetcher,
)


def _expire(fetcher):
    conn = fetcher.cache._get_conn()
    conn.execute("UPDATE cache SET expires_at = ?, created_at = ?", (1, 1))
    conn.commit()


@respx.mock
async def test_stale_cache_markers_reach_every_surface(fetcher):
    _mock_all()
    await liquidity.fetch_liquidity_board(fetcher)
    await liquidity.fetch_net_liquidity(fetcher)
    await liquidity.fetch_money_supply(fetcher)
    await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY)
    _expire(fetcher)
    respx.get(_FRED_URL).respond(503)
    respx.get(_ECB_BSI_URL).respond(503)
    respx.get(_ECB_ILM_URL).respond(503)
    single = await liquidity.fetch_fed_liquidity_series(fetcher)
    net = await liquidity.fetch_net_liquidity(fetcher)
    money = await liquidity.fetch_money_supply(fetcher)
    ecb = await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY)
    board = await liquidity.fetch_liquidity_board(fetcher)
    series = [single, net, *net["components"].values(), money["us"]["M2"],
              *ecb["series"], *board["indicators"]]
    series += [s for roles in money["euro_area"]["aggregates"].values() for s in roles.values()]
    for s in series:
        assert s["served_from_stale_cache"] is True
        assert s["cache_age_seconds"] > 3600
        assert s["freshness"]["stale"] is True
    for out in (single, net, money, ecb, board):
        assert any("upstream fetch failed" in gap for gap in out["data_gaps"])


@pytest.mark.parametrize("wednesday, earlier, expected", [
    ("2024-12-25", "2024-12-24", True),
    ("2003-02-12", "2003-02-07", False),
    ("2013-09-18", "2013-09-17", False),
    ("2024-12-25", "2024-12-23", False),
])
def test_rrp_only_tuesday_after_daily_operations_begin(wednesday, earlier, expected):
    day = date.fromisoformat(wednesday)
    points, skipped = liquidity.compute_net_liquidity(
        [(day, 10000)], [(day, 1000)],
        [(date.fromisoformat(earlier), 2), (day, None)],
    )
    assert bool(points) is expected
    assert skipped == ([] if expected else [wednesday])
    if expected:
        assert points[0]["reverse_repo_observed_at"] == earlier
        assert points[0]["value"] == 7


def test_alignment_documentation_states_cutoff_and_tuesday():
    for text in (liquidity.__doc__, liquidity.NET_LIQUIDITY_ALIGNMENT,
                 liquidity.NET_LIQUIDITY_METHOD["components"]["RRPONTSYD"]["alignment"]):
        assert "2013-09-23" in text and "Tuesday" in text
    changelog = (Path(__file__).parents[3] / "CHANGELOG.md").read_text()
    assert "Tuesday only" in changelog
    assert "Net liquidity therefore starts at 2003-02-12" not in changelog


# Only the portal's own no-series 404 (the captured problem body) is a
# no-data answer; any other 404 takes the outage path (test_liquidity_ecb_404.py).
@pytest.mark.parametrize("status, body, message", [
    pytest.param(404, _ECB_404.encode(), "no series matches the key", id="404-portal-no-series"),
    (200, b"", "no observations on or after 2026-10-01"),
])
@respx.mock
async def test_ecb_no_data_never_retries_or_trips_breaker(fetcher, monkeypatch, status, body, message):
    fetcher.max_retries = 2
    sleep = AsyncMock()
    monkeypatch.setattr("world_intel_mcp.fetcher.asyncio.sleep", sleep)
    route = respx.get(_ECB_BSI_URL).respond(status, content=body)
    for _ in range(4):
        out = await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY, start="2026-10-01")
        assert message in out["error"]
        assert "outage" not in out["error"]
    assert route.call_count == 4
    sleep.assert_not_awaited()
    state = fetcher.breaker.status()[liquidity.ECB_SOURCE]
    assert state["total_failures"] == 0 and state["status"] == "closed"
    _mock_all()
    board = await liquidity.fetch_liquidity_board(fetcher)
    assert {r["indicator"] for r in board["indicators"]} >= {"euro_area_m1", "eurosystem_total_assets"}


@pytest.mark.parametrize("failure", [503, "timeout", "connection"])
@respx.mock
async def test_ecb_real_outages_retry_and_trip(fetcher, monkeypatch, failure):
    fetcher.max_retries = 1
    monkeypatch.setattr("world_intel_mcp.fetcher.asyncio.sleep", AsyncMock())
    route = respx.get(_ECB_BSI_URL)
    if failure == 503:
        route.respond(503)
    else:
        route.mock(side_effect=httpx.ReadTimeout("offline") if failure == "timeout" else httpx.ConnectError("offline"))
    for _ in range(3):
        out = await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY)
        assert "upstream fetch failed" in out["error"]
    assert route.call_count == 6
    assert fetcher.breaker.status()[liquidity.ECB_SOURCE]["status"] == "open"


@pytest.mark.parametrize("missing_role", [None, "1", "4", "I"])
@respx.mock
async def test_partial_ecb_names_each_missing_role_and_board_row(fetcher, missing_role):
    data = copy.deepcopy(_BSI)
    dims = data["structure"]["dimensions"]["series"]
    for key in list(data["dataSets"][0]["series"]):
        codes = {d["id"]: d["values"][int(i)]["id"] for d, i in zip(dims, key.split(":"))}
        if codes["BS_ITEM"] == "M30" and (missing_role is None or codes["DATA_TYPE"] == missing_role):
            del data["dataSets"][0]["series"][key]
    _mock_all(bsi=httpx.Response(200, json=data))
    money = await liquidity.fetch_money_supply(fetcher, region="euro_area")
    role = {"1": "stock", "4": "flow", "I": "annual_growth_rate"}.get(missing_role)
    for expected in ([role] if role else ["stock", "flow", "annual_growth_rate"]):
        assert any(f"M3 {expected}: missing" in gap for gap in money["data_gaps"])
    board = await liquidity.fetch_liquidity_board(fetcher)
    if missing_role in (None, "1"):
        assert any(r["indicator"] == "euro_area_m3" and r["error"] for r in board["unavailable"])
        assert any("euro_area_m3" in gap for gap in board["data_gaps"])


@respx.mock
async def test_all_expected_board_rows_accounted_for_on_outage(fetcher):
    _mock_all(bsi=httpx.Response(503), ilm=httpx.Response(503))
    board = await liquidity.fetch_liquidity_board(fetcher)
    assert {r["indicator"] for r in board["indicators"] + board["unavailable"]} == {
        "fed_total_assets", "treasury_general_account", "reverse_repo", "net_liquidity",
        "us_m2", "euro_area_m1", "euro_area_m2", "euro_area_m3", "eurosystem_total_assets",
    }


def test_index_base_overrides_currency():
    series = liquidity.parse_sdmx_json(_BSI, "BSI")
    indexes = [s for s in series if s["measure"] == "index"]
    assert indexes
    for s in indexes:
        assert s["units"] == "index (Dec10 = 100)"
        assert all(p["units"] == s["units"] for p in s["points"])


@respx.mock
async def test_fred_bad_body_refetches_and_preserves_last_good(fetcher):
    route = respx.get(_FRED_URL).mock(side_effect=_fred_router())
    good = await liquidity.fetch_fed_liquidity_series(fetcher)
    _expire(fetcher)
    key = "liquidity:fred:WALCL"
    original = fetcher.cache.get_stale_meta(key)
    route.respond(200, text="<html>Unavailable</html>")
    assert "error" in await liquidity.fetch_fed_liquidity_series(fetcher)
    assert fetcher.cache.get(key) is None
    assert fetcher.cache.get_stale_meta(key) == original
    route.respond(503)
    stale = await liquidity.fetch_fed_liquidity_series(fetcher)
    assert stale["latest"] == good["latest"] and stale["served_from_stale_cache"]
    route.mock(side_effect=_fred_router())
    recovered = await liquidity.fetch_fed_liquidity_series(fetcher)
    assert "error" not in recovered and not recovered.get("served_from_stale_cache")
    assert route.call_count == 4


@respx.mock
async def test_fred_cold_html_does_not_poison_cache(fetcher):
    route = respx.get(_FRED_URL).respond(200, text="<html>Unavailable</html>")
    assert "error" in await liquidity.fetch_fed_liquidity_series(fetcher)
    assert fetcher.cache.get_stale("liquidity:fred:WALCL") is None
    route.mock(side_effect=_fred_router())
    assert "latest" in await liquidity.fetch_fed_liquidity_series(fetcher)
    assert route.call_count == 2


@respx.mock
async def test_malformed_csv_and_ecb_series_are_isolated_on_board(fetcher):
    data = copy.deepcopy(_BSI)
    data["dataSets"][0]["series"] = []
    oversized = "observation_date,WALCL\n2026-10-14," + "9" * (csv.field_size_limit() + 1)
    _mock_all(fred_overrides={"WALCL": httpx.Response(200, text=oversized)}, bsi=httpx.Response(200, json=data))
    board = await liquidity.fetch_liquidity_board(fetcher)
    missing = {r["indicator"]: r["error"] for r in board["unavailable"]}
    assert "expected format" in missing["fed_total_assets"]
    assert "SDMX-JSON shape" in missing["euro_area_m1"]
    assert any(r["indicator"] == "us_m2" for r in board["indicators"])


@respx.mock
async def test_biggest_move_uses_amount_not_near_zero_percent(fetcher):
    _mock_all()
    board = await liquidity.fetch_liquidity_board(fetcher)
    rows = board["indicators"]
    amounts = [abs(r["change"]) / (1000 if r["units"].endswith(" millions") else 1) for r in rows]
    assert amounts == sorted(amounts, reverse=True)
    assert rows[0]["indicator"] != "reverse_repo"
    assert "billions" in board["ranking_rule"] and "no FX conversion" in board["ranking_rule"]


@respx.mock
async def test_board_status_and_growth_change_units(fetcher):
    _mock_all()
    board = await liquidity.fetch_liquidity_board(fetcher)
    for row in board["indicators"]:
        if row["indicator"].startswith("euro_area_m"):
            assert row["status"] == "Provisional value"
    money = await liquidity.fetch_money_supply(fetcher, region="euro_area")
    for roles in money["euro_area"]["aggregates"].values():
        assert roles["annual_growth_rate"]["change"]["units"] == "percentage points"
        assert roles["stock"]["change"]["units"] == "EUR millions"


def test_unknown_measure_documented_and_unused_regex_removed():
    root = Path(__file__).parents[3]
    assert "`unknown`" in (root / "README.md").read_text()
    assert not hasattr(liquidity, "_FRED_ID_RE")
    assert liquidity._ecb_measure("BSI", {}) == "unknown"


@pytest.mark.parametrize("key", [
    "M.U2..........", "M.U2.Y.V.M30.X.1.U2.2300.Z01.",
    "M.U2.Y.V.M30+.X.1.U2.2300.Z01.E", "M.U2",
    "M.U2.Y.V." + "+".join(f"M{i}" for i in range(51)) + ".X.1.U2.2300.Z01.E",
    "M.U2.Y.V.A+B+C+D.X.1+4+I.U2.2300.Z01.A+B+C+D+E",
])
@respx.mock
async def test_unbounded_ecb_keys_rejected_before_request(fetcher, key):
    route = respx.get(url__startswith="https://data-api.ecb.europa.eu/").respond(503)
    out = await liquidity.fetch_ecb_series(fetcher, key=key)
    assert "key must" in out["error"]
    assert route.call_count == 0


@respx.mock
async def test_empty_ecb_stock_is_unavailable_and_missing_per_role(fetcher):
    data = copy.deepcopy(_BSI)
    dims = data["structure"]["dimensions"]["series"]
    for key, body in data["dataSets"][0]["series"].items():
        codes = {d["id"]: d["values"][int(i)]["id"] for d, i in zip(dims, key.split(":"))}
        if codes["BS_ITEM"] == "M30" and codes["DATA_TYPE"] == "1":
            body["observations"] = {}
    _mock_all(bsi=httpx.Response(200, json=data))
    board = await liquidity.fetch_liquidity_board(fetcher)
    assert "euro_area_m3" not in {r["indicator"] for r in board["indicators"]}
    assert "euro_area_m3" in {r["indicator"] for r in board["unavailable"]}
    money = await liquidity.fetch_money_supply(fetcher, region="euro_area")
    assert any("M3 stock: missing" in g for g in money["data_gaps"])


@respx.mock
async def test_only_failed_component_marks_net_stale_without_restamping(fetcher):
    _mock_all()
    await liquidity.fetch_liquidity_board(fetcher)
    key = "liquidity:fred:WALCL"
    conn = fetcher.cache._get_conn()
    conn.execute("UPDATE cache SET expires_at = ?, created_at = ? WHERE key = ?", (1, 1, key))
    conn.commit()
    original = fetcher.cache.get_stale_meta(key)
    respx.get(_FRED_URL).mock(side_effect=_fred_router({"WALCL": httpx.Response(503)}))
    board = await liquidity.fetch_liquidity_board(fetcher)
    for row in board["indicators"]:
        assert bool(row.get("served_from_stale_cache")) == (row["indicator"] in ("fed_total_assets", "net_liquidity"))
    net = await liquidity.fetch_net_liquidity(fetcher)
    assert net["served_from_stale_cache"] is True
    assert not net["components"]["WDTGAL"].get("served_from_stale_cache")
    assert fetcher.cache.get_stale_meta(key) == original


@respx.mock
async def test_ecb_no_data_does_not_serve_an_old_cached_answer(fetcher):
    _mock_all()
    await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY, start="2026-10-01")
    _expire(fetcher)
    respx.get(_ECB_BSI_URL).respond(200, content=b"")
    out = await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY, start="2026-10-01")
    assert "no observations on or after 2026-10-01" in out["error"]
    assert "series" not in out


@respx.mock
async def test_cached_invalid_bodies_are_evicted(fetcher):
    key = "liquidity:fred:WALCL"
    fetcher.cache.set(key, "<html>old error</html>", 3600)
    assert "error" in await liquidity.fetch_fed_liquidity_series(fetcher)
    assert fetcher.cache.get_stale(key) is None
    ecb_key = f"liquidity:ecb:BSI:{liquidity._ECB_MONEY_KEY}:25:None"
    fetcher.cache.set(ecb_key, {"bad": "shape"}, 3600)
    assert "error" in await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY)
    assert fetcher.cache.get_stale(ecb_key) is None


@respx.mock
async def test_ecb_invalid_json_is_a_series_error(fetcher):
    _mock_all(bsi=httpx.Response(200, content=b"{broken"))
    out = await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY)
    assert "expected SDMX-JSON shape" in out["error"]
    assert fetcher.cache.get_stale(f"liquidity:ecb:BSI:{liquidity._ECB_MONEY_KEY}:25:None") is None


@pytest.mark.parametrize("status", [404, 200])
@pytest.mark.parametrize("metric", ["attempts", "breaker"])
@respx.mock
async def test_ecb_no_data_transport_metrics(fetcher, monkeypatch, status, metric):
    fetcher.max_retries = 2
    monkeypatch.setattr("world_intel_mcp.fetcher.asyncio.sleep", AsyncMock())
    # 404: the portal's no-series problem body; 200: an empty body (an
    # unexpected payload, but the transport worked, so no retry or breaker failure).
    route = respx.get(_ECB_BSI_URL).respond(status, content=_ECB_404.encode() if status == 404 else b"")
    for _ in range(4):
        await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY)
    if metric == "attempts":
        assert route.call_count == 4
    else:
        assert fetcher.breaker.status()[liquidity.ECB_SOURCE]["total_failures"] == 0
        assert fetcher.breaker.is_available(liquidity.ECB_SOURCE)
