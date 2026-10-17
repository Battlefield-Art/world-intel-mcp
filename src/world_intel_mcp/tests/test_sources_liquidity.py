"""Tests for sources/liquidity.py (FRED graph CSV, ECB Data Portal SDMX-JSON).

Fixtures under fixtures/liquidity/ are live responses captured 2026-10-17:
the FRED CSVs are the newest rows of each series' full download (header
unchanged; RRPONTSYD keeps its real blank cells for 2026-09-07 and
2026-10-12), the ECB files are complete responses for the keys the
module queries, and ecb_404_problem.json is the portal's real answer to
a key that matches no series.

Gaps / not covered: live calls (run by hand, see the CHANGELOG entry);
ECB responses with more than 50 series; FRED full-history downloads
(the arithmetic is the same per week, pinned here on real weeks).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import date
from pathlib import Path

import httpx
import pytest
import respx

import world_intel_mcp.fetcher as fetcher_mod
from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import liquidity

_FIX = Path(__file__).parent / "fixtures" / "liquidity"
_FRED = {sid: (_FIX / f"fred_{sid}.csv").read_text() for sid in ("WALCL", "WDTGAL", "RRPONTSYD", "M2SL")}
_BSI = json.loads((_FIX / "ecb_bsi_money.json").read_text())
_ILM = json.loads((_FIX / "ecb_ilm_total.json").read_text())
_ECB_404 = (_FIX / "ecb_404_problem.json").read_text()
_FRED_FLOOR_AT_IMPORT = fetcher_mod._SOURCE_RATE_LIMITS.get(liquidity.FRED_SOURCE)

_FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
_ECB_BSI_URL = f"https://data-api.ecb.europa.eu/service/data/BSI/{liquidity._ECB_MONEY_KEY}"
_ECB_ILM_URL = f"https://data-api.ecb.europa.eu/service/data/ILM/{liquidity._ECB_EUROSYSTEM_KEY}"


@pytest.fixture(autouse=True)
def _fixed_clock_and_no_throttle(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(liquidity, "_today", lambda: date(2026, 10, 17))
    monkeypatch.setitem(fetcher_mod._SOURCE_RATE_LIMITS, liquidity.FRED_SOURCE, 0.0)


@pytest.fixture
async def fetcher(tmp_path: Path) -> AsyncIterator[Fetcher]:
    fetcher = Fetcher(
        cache=Cache(db_path=tmp_path / "liq.db"),
        breaker=CircuitBreaker(),
        default_timeout=5.0,
        max_retries=0,
    )
    try:
        yield fetcher
    finally:
        await fetcher.close()
        fetcher.cache.close()


def _fred_router(overrides: dict[str, httpx.Response] | None = None):
    overrides = overrides or {}

    def _route(request: httpx.Request) -> httpx.Response:
        sid = request.url.params.get("id")
        if sid in overrides:
            return overrides[sid]
        if sid in _FRED:
            return httpx.Response(200, text=_FRED[sid], headers={"content-type": "application/csv"})
        return httpx.Response(404, text="<html>Error - St. Louis Fed</html>")

    return _route


def _mock_all(fred_overrides=None, bsi=None, ilm=None) -> None:
    respx.get(_FRED_URL).mock(side_effect=_fred_router(fred_overrides))
    respx.get(_ECB_BSI_URL).mock(return_value=bsi or httpx.Response(200, json=_BSI))
    respx.get(_ECB_ILM_URL).mock(return_value=ilm or httpx.Response(200, json=_ILM))


def _rows(text: str, sid: str):
    return liquidity.parse_fred_csv(text, sid)


# ---------------------------------------------------------------------------
# FRED CSV parsing
# ---------------------------------------------------------------------------


def test_parse_fred_csv_keeps_blank_cells_missing_not_zero() -> None:
    rows = dict(_rows(_FRED["RRPONTSYD"], "RRPONTSYD"))
    assert rows[date(2026, 10, 12)] is None  # Columbus Day: blank in the live CSV
    assert rows[date(2026, 9, 7)] is None  # Labor Day
    assert rows[date(2026, 10, 14)] == 0.201


@pytest.mark.parametrize(
    "text",
    [
        "observation_date,WDTGAL\n2026-10-14,885823\n",  # another series' header
        "<!DOCTYPE html><html><title>Error - St. Louis Fed</title></html>",
        "observation_date,WALCL\n2026-10-14,n/a\n",
        "",
    ],
)
def test_parse_fred_csv_rejects_wrong_shapes(text: str) -> None:
    with pytest.raises(ValueError):
        liquidity.parse_fred_csv(text, "WALCL")


# ---------------------------------------------------------------------------
# Net liquidity arithmetic, unit conversion, alignment
# ---------------------------------------------------------------------------


def test_compute_net_liquidity_known_week_and_unit_conversion() -> None:
    points, skipped = liquidity.compute_net_liquidity(
        _rows(_FRED["WALCL"], "WALCL"),
        _rows(_FRED["WDTGAL"], "WDTGAL"),
        _rows(_FRED["RRPONTSYD"], "RRPONTSYD"),
    )
    by_day = {p["observed_at"]: p for p in points}
    week = by_day["2026-10-14"]
    # WALCL 6,755,639 and WDTGAL 885,823 are USD millions; RRP 0.201 is USD billions.
    assert week["fed_total_assets"] == 6755.639
    assert week["treasury_general_account"] == 885.823
    assert week["reverse_repo"] == 0.201
    assert week["value"] == 5869.615
    assert week["units"] == "USD billions"
    assert week["reverse_repo_observed_at"] == "2026-10-14"
    # Quarter-end week: RRP 11.539 on 2026-09-30.
    assert by_day["2026-09-30"]["value"] == round(6743.031 - 984.046 - 11.539, 3)
    assert skipped == []
    assert [p["observed_at"] for p in points] == sorted(by_day)


def test_rrp_blank_on_the_wednesday_uses_latest_earlier_day_and_names_it() -> None:
    rrp = [(d, None if d == date(2026, 10, 7) else v) for d, v in _rows(_FRED["RRPONTSYD"], "RRPONTSYD")]
    points, _ = liquidity.compute_net_liquidity(
        _rows(_FRED["WALCL"], "WALCL"), _rows(_FRED["WDTGAL"], "WDTGAL"), rrp
    )
    week = {p["observed_at"]: p for p in points}["2026-10-07"]
    assert week["reverse_repo_observed_at"] == "2026-10-06"
    assert week["reverse_repo"] == 0.414
    assert week["value"] == round(6747.560 - 885.783 - 0.414, 3)


def test_rrp_is_never_taken_from_after_the_wednesday_and_gaps_are_skipped() -> None:
    walcl = [(date(2026, 10, 14), 6755639.0)]
    tga = [(date(2026, 10, 14), 885823.0)]
    later_only = [(date(2026, 10, 15), 1.201)]
    points, skipped = liquidity.compute_net_liquidity(walcl, tga, later_only)
    assert points == [] and skipped == ["2026-10-14"]
    # TGA missing for the week: skipped, not treated as a zero TGA.
    points, skipped = liquidity.compute_net_liquidity(walcl, [(date(2026, 10, 7), 885783.0)], [(date(2026, 10, 14), 0.201)])
    assert points == [] and skipped == ["2026-10-14"]


@respx.mock
async def test_fetch_net_liquidity_states_formula_and_returns_newest_first(fetcher: Fetcher) -> None:
    respx.get(_FRED_URL).mock(side_effect=_fred_router())
    out = await liquidity.fetch_net_liquidity(fetcher, limit=3)
    assert "error" not in out
    assert out["formula"].startswith("net_liquidity = WALCL - WDTGAL - RRPONTSYD")
    assert "divided by 1,000" in out["formula"]
    assert "never filled with zero" in out["alignment"]
    assert [p["observed_at"] for p in out["points"]] == ["2026-10-14", "2026-10-07", "2026-09-30"]
    assert out["latest"]["value"] == 5869.615
    assert out["change"]["absolute"] == round(5869.615 - (6747.560 - 885.783 - 2.338), 3)
    assert out["change"]["from"] == "2026-10-07"
    assert out["components"]["WALCL"]["units"] == "USD millions"
    assert out["components"]["RRPONTSYD"]["units"] == "USD billions"
    assert out["published_at"] is None
    assert any("WDTGAL (Wednesday level), not" in g for g in out["data_gaps"])
    # start filter and full history
    since = await liquidity.fetch_net_liquidity(fetcher, limit=0, start="2026-09-23")
    assert [p["observed_at"] for p in since["points"]] == ["2026-10-14", "2026-10-07", "2026-09-30", "2026-09-23"]
    full = await liquidity.fetch_net_liquidity(fetcher, limit=0)
    assert full["count"] == 9 and full["first_observed_at"] == "2026-08-19"


@respx.mock
@pytest.mark.parametrize("failing", ["WALCL", "WDTGAL", "RRPONTSYD"])
async def test_net_liquidity_component_outage_is_an_error_never_a_zero_term(
    fetcher: Fetcher, failing: str
) -> None:
    respx.get(_FRED_URL).mock(side_effect=_fred_router({failing: httpx.Response(503)}))
    out = await liquidity.fetch_net_liquidity(fetcher)
    assert "error" in out
    assert out["components_failed"] == [failing]
    assert "never treated as zero" in out["error"]
    assert "points" not in out and "latest" not in out
    assert any(failing in g for g in out["data_gaps"])


@respx.mock
async def test_fred_html_error_page_is_an_error_not_data(fetcher: Fetcher) -> None:
    html = httpx.Response(200, text="<!DOCTYPE html><title>Error</title>", headers={"content-type": "text/html"})
    respx.get(_FRED_URL).mock(side_effect=_fred_router({"WALCL": html}))
    out = await liquidity.fetch_fed_liquidity_series(fetcher, "WALCL")
    assert "not in the expected format" in out["error"]
    assert "points" not in out


@respx.mock
async def test_net_liquidity_with_no_aligned_week_is_an_error(fetcher: Fetcher) -> None:
    only_later_rrp = "observation_date,RRPONTSYD\n2026-12-01,0.5\n"
    respx.get(_FRED_URL).mock(
        side_effect=_fred_router({"RRPONTSYD": httpx.Response(200, text=only_later_rrp)})
    )
    out = await liquidity.fetch_net_liquidity(fetcher)
    assert "no week had all three components aligned" in out["error"]
    assert out["skipped_weeks"] == 9


# ---------------------------------------------------------------------------
# One FRED series
# ---------------------------------------------------------------------------


@respx.mock
async def test_fed_liquidity_series_points_carry_units_and_blank_count(fetcher: Fetcher) -> None:
    respx.get(_FRED_URL).mock(side_effect=_fred_router())
    out = await liquidity.fetch_fed_liquidity_series(fetcher, "rrpontsyd", limit=4)
    assert out["series_id"] == "RRPONTSYD"
    assert out["points"] == [
        {"observed_at": "2026-10-16", "value": 0.1, "units": "USD billions"},
        {"observed_at": "2026-10-15", "value": 1.201, "units": "USD billions"},
        {"observed_at": "2026-10-14", "value": 0.201, "units": "USD billions"},
        {"observed_at": "2026-10-13", "value": 0.275, "units": "USD billions"},
    ]
    assert out["blank_observations"] == 2
    assert any("2 blank RRPONTSYD dates" in g for g in out["data_gaps"])
    assert out["measure"] == "amount_outstanding" and out["published_at"] is None
    assert out["freshness"] == {"latest_age_days": 1, "expected_max_age_days": 6, "stale": False}
    assert out["change"]["absolute"] == round(0.1 - 1.201, 3)


@respx.mock
async def test_monthly_series_freshness_uses_period_end_and_flags_stale(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    respx.get(_FRED_URL).mock(side_effect=_fred_router())
    out = await liquidity.fetch_fed_liquidity_series(fetcher, "M2SL", limit=0, start="2026-06-01")
    assert [p["observed_at"] for p in out["points"]] == ["2026-08-01", "2026-07-01", "2026-06-01"]
    assert out["freshness"]["latest_age_days"] == (date(2026, 10, 17) - date(2026, 8, 31)).days
    assert out["freshness"]["stale"] is False
    monkeypatch.setattr(liquidity, "_today", lambda: date(2027, 1, 15))
    out = await liquidity.fetch_fed_liquidity_series(fetcher, "M2SL")
    assert out["freshness"]["stale"] is True
    assert any("M2SL: newest observation 2026-08-01 is" in g for g in out["data_gaps"])


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"series_id": "GDP"}, "unknown series"),
        ({"series_id": "WALCL", "limit": -1}, "limit must be"),
        ({"series_id": "WALCL", "limit": True}, "limit must be"),
        ({"series_id": "WALCL", "start": "17/10/2026"}, "start must be"),
        ({"series_id": "WALCL", "start": 20261017}, "start must be"),
    ],
)
async def test_fed_liquidity_series_validation(fetcher: Fetcher, kwargs: dict, message: str) -> None:
    out = await liquidity.fetch_fed_liquidity_series(fetcher, **kwargs)
    assert message in out["error"]


async def test_net_liquidity_validation(fetcher: Fetcher) -> None:
    assert "limit must be" in (await liquidity.fetch_net_liquidity(fetcher, limit="5"))["error"]
    assert "start must be" in (await liquidity.fetch_net_liquidity(fetcher, start="yesterday"))["error"]


# ---------------------------------------------------------------------------
# ECB SDMX-JSON
# ---------------------------------------------------------------------------


def test_parse_sdmx_money_units_measures_and_status() -> None:
    series = {s["key"]: s for s in liquidity.parse_sdmx_json(_BSI, "BSI")}
    m3 = series["BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.E"]
    assert m3["units"] == "EUR millions" and m3["scale"] == 1_000_000
    assert m3["measure"] == "amount_outstanding"
    assert m3["title"] == "Monetary aggregate M3, Stocks"
    assert m3["points"][-1] == {
        "observed_at": "2026-08",
        "period_end": "2026-08-31",
        "value": 17646463.0,
        "units": "EUR millions",
        "status": "Provisional value",
    }
    assert series["BSI.M.U2.Y.V.M30.X.4.U2.2300.Z01.E"]["measure"] == "flow"
    growth = series["BSI.M.U2.Y.V.M30.X.I.U2.2300.Z01.A"]
    assert growth["measure"] == "growth_rate" and growth["units"] == "percent" and growth["scale"] == 1
    assert series["BSI.M.U2.Y.V.M30.X.I.U2.2300.Z01.E"]["measure"] == "index"
    assert len(series) == 12


def test_parse_sdmx_eurosystem_weekly() -> None:
    (s,) = liquidity.parse_sdmx_json(_ILM, "ILM")
    assert s["measure"] == "amount_outstanding" and s["frequency"] == "W"
    assert [p["observed_at"] for p in s["points"]] == ["2026-W39", "2026-W40", "2026-W41"]
    assert s["points"][-1]["period_end"] == "2026-10-11"
    assert s["points"][-1]["value"] == 5943911.0


@respx.mock
async def test_fetch_ecb_series_growth_rate_change_is_points_not_percent(fetcher: Fetcher) -> None:
    key = "M.U2.Y.V.M10+M20+M30.X.1+4+I.U2.2300.Z01.E+A"
    respx.get(f"https://data-api.ecb.europa.eu/service/data/BSI/{key}").mock(
        return_value=httpx.Response(200, json=_BSI)
    )
    out = await liquidity.fetch_ecb_series(fetcher, dataset="bsi", key=key, limit=2)
    by_key = {s["key"]: s for s in out["series"]}
    growth = by_key["BSI.M.U2.Y.V.M10.X.I.U2.2300.Z01.A"]
    assert growth["change"]["pct"] is None
    assert growth["count"] == 2
    flow = by_key["BSI.M.U2.Y.V.M10.X.4.U2.2300.Z01.E"]
    assert flow["change"] is None  # differences of flows are not reported as changes
    stock = by_key["BSI.M.U2.Y.V.M10.X.1.U2.2300.Z01.E"]
    assert stock["change"]["absolute"] == 11293328.0 - 11291831.0
    assert respx.calls.last.request.url.params["lastNObservations"] == "3"


@respx.mock
async def test_ecb_404_and_outage_are_errors(fetcher: Fetcher) -> None:
    key = "M.U2.Y.V.M99.X.1.U2.2300.Z01.E"
    respx.get(f"https://data-api.ecb.europa.eu/service/data/BSI/{key}").mock(
        return_value=httpx.Response(404, text=_ECB_404, headers={"content-type": "application/problem+json"})
    )
    out = await liquidity.fetch_ecb_series(fetcher, key=key)
    assert "no series matches the key" in out["error"]
    assert "series" not in out


@respx.mock
async def test_ecb_unexpected_shape_is_an_error(fetcher: Fetcher) -> None:
    key = "W.U2.C.T000000.Z5.Z01"
    respx.get(f"https://data-api.ecb.europa.eu/service/data/ILM/{key}").mock(
        return_value=httpx.Response(200, json={"header": {}, "dataSets": []})
    )
    out = await liquidity.fetch_ecb_series(fetcher, dataset="ILM", key=key)
    assert "expected SDMX-JSON shape" in out["error"]


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"dataset": "EXR", "key": "D.USD.EUR.SP00.A"}, "dataset must be one of"),
        ({"key": "../../etc"}, "key must be an SDMX series key"),
        ({"key": "M.U2?x=1"}, "key must be an SDMX series key"),
        ({"key": "M.U2.Y.V.M30.X.1.U2.2300.Z01.E", "limit": -3}, "limit must be"),
    ],
)
async def test_ecb_series_validation(fetcher: Fetcher, kwargs: dict, message: str) -> None:
    out = await liquidity.fetch_ecb_series(fetcher, **kwargs)
    assert message in out["error"]


# ---------------------------------------------------------------------------
# Money supply
# ---------------------------------------------------------------------------


@respx.mock
async def test_money_supply_keeps_aggregates_separate_and_never_sums(fetcher: Fetcher) -> None:
    _mock_all()
    out = await liquidity.fetch_money_supply(fetcher, limit=2)
    assert "error" not in out
    ea = out["euro_area"]["aggregates"]
    assert set(ea) == {"M1", "M2", "M3"}
    for roles in ea.values():
        assert set(roles) == {"stock", "flow", "annual_growth_rate"}
    stocks = {name: roles["stock"]["latest"]["value"] for name, roles in ea.items()}
    assert stocks == {"M1": 11293328.0, "M2": 16447405.0, "M3": 17646463.0}
    assert stocks["M1"] < stocks["M2"] < stocks["M3"]  # nested, as published
    # No figure anywhere equals a sum of the aggregates.
    flat = json.dumps(out)
    assert str(sum(stocks.values())) not in flat
    assert not any(k in ea for k in ("total", "sum", "combined"))
    assert "never added together" in out["note"]
    assert ea["M3"]["flow"]["measure"] == "flow"
    assert ea["M3"]["annual_growth_rate"]["latest"]["units"] == "percent"
    us = out["us"]
    assert us["M2"]["latest"] == {"observed_at": "2026-08-01", "value": 23342.8, "units": "USD billions"}
    assert us["M2"]["count"] == 2
    assert us["M3"]["available"] is False and "discontinued" in us["M3"]["note"]


@respx.mock
async def test_money_supply_partial_outage_names_the_gap(fetcher: Fetcher) -> None:
    _mock_all(bsi=httpx.Response(503))
    out = await liquidity.fetch_money_supply(fetcher)
    assert "error" not in out  # US M2 still answered
    assert "error" in out["euro_area"] and "aggregates" not in out["euro_area"]
    assert any("ECB Data Portal returned no data" in g for g in out["data_gaps"])


@respx.mock
async def test_money_supply_total_outage_is_an_error(fetcher: Fetcher) -> None:
    _mock_all(fred_overrides={"M2SL": httpx.Response(500)}, bsi=httpx.Response(503))
    out = await liquidity.fetch_money_supply(fetcher)
    assert out["error"].startswith("money supply unavailable")
    us_only = await liquidity.fetch_money_supply(fetcher, region="us")
    assert "error" in us_only and "euro_area" not in us_only


async def test_money_supply_validation(fetcher: Fetcher) -> None:
    assert "region must be" in (await liquidity.fetch_money_supply(fetcher, region="uk"))["error"]
    assert "limit must be" in (await liquidity.fetch_money_supply(fetcher, limit=-1))["error"]


# ---------------------------------------------------------------------------
# Board
# ---------------------------------------------------------------------------


@respx.mock
async def test_board_rows_values_and_sorting(fetcher: Fetcher) -> None:
    _mock_all()
    out = await liquidity.fetch_liquidity_board(fetcher)
    assert out["unavailable"] == [] and "error" not in out
    rows = {r["indicator"]: r for r in out["indicators"]}
    assert set(rows) == {
        "fed_total_assets", "treasury_general_account", "reverse_repo", "net_liquidity",
        "us_m2", "euro_area_m1", "euro_area_m2", "euro_area_m3", "eurosystem_total_assets",
    }
    nl = rows["net_liquidity"]
    assert nl["latest_value"] == 5869.615 and nl["units"] == "USD billions"
    assert nl["previous_observed_at"] == "2026-10-07"
    assert rows["fed_total_assets"]["change"] == 6755639.0 - 6747560.0
    assert rows["eurosystem_total_assets"]["change"] == 5943911.0 - 5947826.0
    assert rows["eurosystem_total_assets"]["period_end"] == "2026-10-11"
    assert rows["us_m2"]["period_end"] == "2026-08-31"
    assert all(r["measure"] == "amount_outstanding" for r in rows.values())
    moves = [abs(r["change_pct"]) for r in out["indicators"]]
    assert moves == sorted(moves, reverse=True)
    assert out["indicators"][0]["indicator"] == "reverse_repo"

    newest = await liquidity.fetch_liquidity_board(fetcher, sort="newest")
    ends = [r["period_end"] for r in newest["indicators"]]
    assert ends == sorted(ends, reverse=True)
    assert newest["indicators"][0]["indicator"] == "reverse_repo"  # 2026-10-16


@respx.mock
async def test_board_component_outage_drops_net_liquidity_instead_of_zeroing(fetcher: Fetcher) -> None:
    _mock_all(fred_overrides={"WDTGAL": httpx.Response(503)}, ilm=httpx.Response(500))
    out = await liquidity.fetch_liquidity_board(fetcher)
    names = {r["indicator"] for r in out["indicators"]}
    assert "net_liquidity" not in names and "treasury_general_account" not in names
    assert "eurosystem_total_assets" not in names
    missing = {u["indicator"] for u in out["unavailable"]}
    assert {"treasury_general_account", "net_liquidity", "Eurosystem total assets"} <= missing
    assert any("never computed with a zero term" in u["error"] for u in out["unavailable"])
    assert "error" not in out  # the rest of the board is real


@respx.mock
async def test_board_total_outage_is_an_error(fetcher: Fetcher) -> None:
    respx.get(_FRED_URL).mock(return_value=httpx.Response(503))
    respx.get(_ECB_BSI_URL).mock(return_value=httpx.Response(503))
    respx.get(_ECB_ILM_URL).mock(return_value=httpx.Response(503))
    out = await liquidity.fetch_liquidity_board(fetcher)
    assert out["error"] == "liquidity board unavailable: every source failed"
    assert out["indicators"] == [] and len(out["unavailable"]) == 7


@respx.mock
async def test_board_flags_stale_rows(fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_all()
    monkeypatch.setattr(liquidity, "_today", lambda: date(2026, 11, 20))
    out = await liquidity.fetch_liquidity_board(fetcher, sort="newest")
    rows = {r["indicator"]: r for r in out["indicators"]}
    assert rows["fed_total_assets"]["freshness"]["stale"] is True
    assert rows["us_m2"]["freshness"]["stale"] is False  # monthly allowance
    assert any(g.startswith("fed_total_assets: newest observation 2026-10-14") for g in out["data_gaps"])


async def test_board_validation(fetcher: Fetcher) -> None:
    assert "sort must be" in (await liquidity.fetch_liquidity_board(fetcher, sort="loudest"))["error"]


# ---------------------------------------------------------------------------
# Request shape
# ---------------------------------------------------------------------------


@respx.mock
async def test_requests_send_descriptive_user_agent(fetcher: Fetcher) -> None:
    _mock_all()
    await liquidity.fetch_liquidity_board(fetcher)
    agents = {c.request.headers["user-agent"] for c in respx.calls}
    assert agents == {"world-intel-mcp (https://github.com/marc-shade/world-intel-mcp)"}
    ids = sorted(c.request.url.params["id"] for c in respx.calls if c.request.url.host == "fred.stlouisfed.org")
    assert ids == ["M2SL", "RRPONTSYD", "WALCL", "WDTGAL"]


def test_fred_rate_floor_matches_robots_crawl_delay() -> None:
    # Read at import time: the autouse fixture zeroes it for speed.
    assert _FRED_FLOOR_AT_IMPORT == 1.0


# ---------------------------------------------------------------------------
# Method statement and blocked sources (lead rulings 2026-10-17)
# ---------------------------------------------------------------------------


@respx.mock
async def test_net_liquidity_method_states_wednesday_levels_conversion_and_rrp_alignment(
    fetcher: Fetcher, tmp_path: Path
) -> None:
    respx.get(_FRED_URL).mock(side_effect=_fred_router())
    out = await liquidity.fetch_net_liquidity(fetcher, limit=1)
    method = out["method"]
    tga = method["components"]["WDTGAL"]
    assert (tga["observed"], tga["units"]) == ("Wednesday level", "USD millions")
    assert tga["conversion"] == "divided by 1,000 to USD billions"
    assert method["components"]["WALCL"]["observed"] == "Wednesday level"
    rrp = method["components"]["RRPONTSYD"]
    assert (rrp["observed"], rrp["units"]) == ("daily, business days", "USD billions")
    assert rrp["conversion"] == "none: already USD billions"
    assert "never a later day" in rrp["alignment"] and "within 5 days" in rrp["alignment"]
    # The statement matches the arithmetic: raw WDTGAL millions / 1,000 and
    # the raw same-Wednesday RRP billions, unconverted.
    point = out["points"][0]
    raw_tga = dict(_rows(_FRED["WDTGAL"], "WDTGAL"))[date(2026, 10, 14)]
    raw_rrp = dict(_rows(_FRED["RRPONTSYD"], "RRPONTSYD"))[date(2026, 10, 14)]
    assert point["treasury_general_account"] == raw_tga / 1000
    assert point["reverse_repo"] == raw_rrp
    assert point["reverse_repo_observed_at"] == point["observed_at"] == "2026-10-14"
    # Anyone tempted to "fix" the TGA term back to WTREGEN reads why not.
    assert "WTREGEN" in method["tga_choice"] and "week average" in method["tga_choice"]
    # The method travels with an error too, so a failed call still says what it would compute.
    # A fresh cache, so the 503 is not hidden behind the first call's cached CSVs.
    respx.get(_FRED_URL).mock(side_effect=_fred_router({"WDTGAL": httpx.Response(503)}))
    cold = Fetcher(cache=Cache(db_path=tmp_path / "cold.db"), breaker=CircuitBreaker(), max_retries=0)
    try:
        failed = await liquidity.fetch_net_liquidity(cold)
    finally:
        await cold.close()
        cold.cache.close()
    assert "error" in failed and failed["method"]["components"]["WDTGAL"]["units"] == "USD millions"


@respx.mock
async def test_board_names_blocked_sources_and_substitutes_no_proxy(fetcher: Fetcher) -> None:
    _mock_all()
    out = await liquidity.fetch_liquidity_board(fetcher)
    blocked = {(b["publisher"], b["measure"]) for b in out["blocked"]}
    assert blocked == {
        ("ICI", "money market fund assets, weekly"),
        ("ICI", "ETF net issuance, monthly actuals and weekly estimates"),
        ("FINRA", "customer margin balances (debit and free credit), monthly"),
        ("BLS", "BLS series (for example CPI-U, nonfarm payrolls)"),
    }
    assert all(b["status"] == "blocked" and b["measured"].startswith("2026-10-17: ") for b in out["blocked"])
    by_publisher = {b["publisher"]: b["measured"] for b in out["blocked"]}
    assert "403" in by_publisher["ICI"]
    assert "Restrictions d and e" in by_publisher["FINRA"]
    assert "Disallow: /" in by_publisher["BLS"]
    # No indicator row stands in for a blocked measure.
    labels = " ".join(f"{r['indicator']} {r['label']}" for r in out["indicators"]).lower()
    assert not any(word in labels for word in ("fund", "etf", "margin", "cpi", "payroll"))
    assert any("no proxy is substituted" in g for g in out["data_gaps"])
    # The response holds a copy: mutating it leaves the module constant intact.
    out["blocked"][0]["status"] = "mutated"
    assert liquidity.BLOCKED_SOURCES[0]["status"] == "blocked"
