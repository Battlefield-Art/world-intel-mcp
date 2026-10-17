"""Tests for sources/prediction_cross.py and tools/prediction_markets.py:
the cross-venue comparison and settled-market Brier scoring, end to end
through both venues' fetchers (respx-mocked).

Kalshi and Polymarket open-market rows are the live shapes and quotes
captured 2026-10-17 (KXFEDDECISION-26OCT; Polymarket event "Fed Decision
in October?"). Settled rows and price histories are live shapes with
values chosen so the expected Brier arithmetic is written out by hand in
each test. Polymarket CLOB token ids are shortened (they are opaque
77-digit strings live).

Gaps / not covered: real API drift; timing of the 1 s Polymarket
throttle (sleep is patched out here).
"""

import asyncio as asyncio_mod
from datetime import datetime, timezone

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources.prediction_cross import fetch_prediction_compare, fetch_prediction_settled
from world_intel_mcp.tests.test_sources_kalshi import EVENTS, FED_MARKETS, SETTLED_ROWS, _event

KALSHI_MARKETS = r".*api\.elections\.kalshi\.com/trade-api/v2/markets\?.*"
KALSHI_CANDLES = r".*api\.elections\.kalshi\.com/trade-api/v2/markets/candlesticks.*"
GAMMA = r".*gamma-api\.polymarket\.com/markets.*"
CLOB = r".*clob\.polymarket\.com/prices-history.*"

_FED_EVENT = {"id": "606422", "slug": "fed-decision-in-october-20260617190323537", "title": "Fed Decision in October?"}


def _gamma(id_, question, prices, bid, ask):
    return {
        "id": id_, "question": question, "slug": f"m-{id_}", "endDate": "2026-10-29T03:59:00Z",
        "outcomes": '["Yes", "No"]', "outcomePrices": prices, "bestBid": bid, "bestAsk": ask,
        "volume24hr": 60000.0, "volumeNum": 7000000.0, "clobTokenIds": f'["{id_}y", "{id_}n"]',
        "closed": False, "active": True, "umaResolutionStatus": None, "events": [_FED_EVENT],
    }


POLY_OPEN = [
    _gamma("2589810", "Will the Fed decrease interest rates by 50+ bps after the October 2026 meeting?",
           '["0.0015", "0.9985"]', 0.001, 0.002),
    _gamma("2589811", "Will the Fed decrease interest rates by 25 bps after the October 2026 meeting?",
           '["0.0035", "0.9965"]', 0.003, 0.004),
    _gamma("2589812", "Will there be no change in Fed interest rates after the October 2026 meeting?",
           '["0.865", "0.135"]', 0.86, 0.87),
    _gamma("2589813", "Will the Fed increase interest rates by 25 bps after the October 2026 meeting?",
           '["0.135", "0.865"]', 0.13, 0.14),
    _gamma("2589814", "Will the Fed increase interest rates by 50+ bps after the October 2026 meeting?",
           '["0.0035", "0.9965"]', 0.003, 0.004),
]


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _sleep)


def _kalshi_fed_events() -> None:
    respx.get(url__regex=EVENTS).mock(
        return_value=httpx.Response(200, json={"events": [_event(FED_MARKETS)], "cursor": ""})
    )


# ---------------------------------------------------------------------------
# intel_prediction_compare
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_compare_matches_live_fed_pairs_and_rejects_near_misses(fetcher: Fetcher) -> None:
    _kalshi_fed_events()
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(200, json=POLY_OPEN))

    out = await fetch_prediction_compare(fetcher, series="KXFEDDECISION")

    pairs = [(m["kalshi"]["id"], m["polymarket"]["id"]) for m in out["matched"]]
    # Largest absolute gap first: C25 0.01 (last trade) - 0.0035 (Polymarket
    # midpoint of 0.003/0.004) = 0.0065; H25 0.135 - 0.135 = 0.
    assert pairs == [("KXFEDDECISION-26OCT-C25", "2589811"), ("KXFEDDECISION-26OCT-H25", "2589813")]
    assert [m["price_gap"] for m in out["matched"]] == [0.0065, 0.0]
    assert all(m["match_confidence"] >= 0.6 and m["reasons"] for m in out["matched"])
    assert out["matched_count"] == 2
    assert out["unmatched_kalshi"] == 3 and out["unmatched_polymarket"] == 3
    assert out["kalshi_markets_compared"] == 5 and out["polymarket_markets_compared"] == 5
    # ">25bps" vs "50+ bps" and "25" vs "50+" are rejected with reasons.
    reasons = [nm["reasons"][-1] for nm in out["near_misses"]]
    assert "quantities differ: Kalshi ['>25'] vs Polymarket ['>=50']" in reasons
    assert "quantities differ: Kalshi ['25'] vs Polymarket ['>=50']" in reasons
    assert "degraded" not in out and "error" not in out


@respx.mock
@pytest.mark.asyncio
async def test_compare_kalshi_outage_is_not_zero_matches(fetcher: Fetcher) -> None:
    respx.get(url__regex=EVENTS).mock(return_value=httpx.Response(500))
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(200, json=POLY_OPEN))

    out = await fetch_prediction_compare(fetcher, series="KXFEDDECISION")
    assert out["error"] == "comparison not possible: kalshi unavailable"
    assert out["degraded"] is True
    assert out["matched"] == [] and out["matched_count"] is None
    assert out["unmatched_kalshi"] is None and out["unmatched_polymarket"] == 5


@respx.mock
@pytest.mark.asyncio
async def test_compare_polymarket_outage_is_not_zero_matches(fetcher: Fetcher) -> None:
    _kalshi_fed_events()
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(502))

    out = await fetch_prediction_compare(fetcher, series="KXFEDDECISION")
    assert out["error"] == "comparison not possible: polymarket unavailable"
    assert out["unmatched_polymarket"] is None and out["unmatched_kalshi"] == 5
    assert any(g.startswith("polymarket: Polymarket Gamma API unavailable") for g in out["data_gaps"])


# ---------------------------------------------------------------------------
# intel_prediction_settled
# ---------------------------------------------------------------------------

def _ts(text: str) -> int:
    return int(datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp())


K_REF = _ts("2026-09-11T12:25:00")  # SETTLED_ROWS close_time
P1_REF = _ts("2026-10-17T16:16:26")
P2_REF = _ts("2026-10-17T15:00:00")


def _closed(id_, prices, status, closed, start, outcomes='["Yes", "No"]', event="e1"):
    return {
        "id": id_, "question": f"Settled market {id_}", "outcomes": outcomes, "outcomePrices": prices,
        "umaResolutionStatus": status, "closedTime": closed, "startDate": start,
        "endDate": "2026-10-24T00:00:00Z", "clobTokenIds": f'["{id_}a", "{id_}b"]',
        "volumeNum": 16370.88, "closed": True, "events": [{"id": event}],
    }


POLY_CLOSED = [
    _closed("p1", '["1", "0"]', "resolved", "2026-10-17 16:16:26+00", "2026-10-10T00:00:00Z"),
    _closed("p2", '["0", "1"]', "resolved", "2026-10-17 15:00:00+00", "2026-10-01T00:00:00Z",
            outcomes='["Up", "Down"]', event="e2"),
    _closed("p3", '["1", "0"]', "proposed", "2026-10-17 14:00:00+00", "2026-10-01T00:00:00Z"),
    _closed("p4", '["0.5", "0.5"]', "resolved", "2026-10-17 13:00:00+00", "2026-10-01T00:00:00Z"),
    _closed("p5", '["1", "0"]', "resolved", "2026-10-17 16:00:00+00", "2026-10-17T11:00:00Z"),
]


def _mock_settled_venues(clob_status: int = 200, kalshi_status: int = 200) -> None:
    respx.get(url__regex=KALSHI_MARKETS).mock(
        return_value=httpx.Response(kalshi_status, json={"markets": SETTLED_ROWS, "cursor": ""})
    )
    candles = {
        "M1": [(K_REF - 30 * 3600, "0.8000"), (K_REF - 20 * 3600, "0.9500")],
        "M2": [(K_REF - 48 * 3600, "0.3000")],
        "M6": [(K_REF - 400 * 3600, "0.5000")],
    }

    def _candles(request: httpx.Request) -> httpx.Response:
        tickers = request.url.params["market_tickers"].split(",")
        return httpx.Response(200, json={"markets": [
            {"market_ticker": t, "candlesticks": [
                {"end_period_ts": ts, "price": {"close_dollars": p}} for ts, p in candles.get(t, [])
            ]} for t in tickers
        ]})

    respx.get(url__regex=KALSHI_CANDLES).mock(side_effect=_candles)
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(200, json=POLY_CLOSED))
    history = {
        "p1a": [{"t": P1_REF - 26 * 3600, "p": 0.7}, {"t": P1_REF - 2 * 3600, "p": 0.99}],
        "p2a": [{"t": P2_REF - 25 * 3600, "p": 0.4}],
    }

    def _clob(request: httpx.Request) -> httpx.Response:
        if clob_status != 200:
            return httpx.Response(clob_status)
        return httpx.Response(200, json={"history": history.get(request.url.params["market"], [])})

    respx.get(url__regex=CLOB).mock(side_effect=_clob)


@respx.mock
@pytest.mark.asyncio
async def test_settled_brier_per_venue_on_known_sample(fetcher: Fetcher) -> None:
    _mock_settled_venues()
    out = await fetch_prediction_settled(fetcher, offsets_hours=[24], kalshi_series="KXCPIYOY")

    k = out["venues"]["kalshi"]
    # Kalshi at 24h: M2 0.30 vs NO -> 0.09; M1 0.80 vs YES -> 0.04;
    # M6's last trade is 376h before the offset point (limit 168h): excluded.
    # (0.09 + 0.04) / 2 = 0.065.
    assert k["scores"]["24h"] == {
        "n": 2, "distinct_events": 1, "brier": 0.065, "yes_rate": 0.5,
        "excluded": {"last price older than the staleness limit": 1},
    }
    assert k["scored"] is True and k["settled_markets"] == 3
    assert "series KXCPIYOY" in k["selection"] and "(1 skipped as open too briefly)" in k["selection"]
    assert [s["price_before_reference"]["24h"] for s in k["sample"]] == [0.3, 0.8, None]

    p = out["venues"]["polymarket"]
    # Polymarket at 24h: p1 0.7 vs YES -> 0.09; p2 0.4 vs first outcome
    # ("Up", lost) -> 0.16. (0.09 + 0.16) / 2 = 0.125. p3 unresolved, p4
    # not 1/0 and p5 open 5h are not in the sample.
    assert p["scores"]["24h"] == {
        "n": 2, "distinct_events": 2, "brier": 0.125, "yes_rate": 0.5, "excluded": {},
    }
    assert [s["id"] for s in p["sample"]] == ["p1", "p2"]
    assert p["sample"][1]["result"] == "Down"
    assert "(1 skipped as open too briefly)" in p["selection"]
    assert out["data_gaps"] == [] and "degraded" not in out
    assert "not a head-to-head comparison" in out["comparability"]


@respx.mock
@pytest.mark.asyncio
async def test_settled_venue_without_price_history_is_not_scored(fetcher: Fetcher) -> None:
    _mock_settled_venues(clob_status=503)
    out = await fetch_prediction_settled(fetcher, offsets_hours=[24], kalshi_series="KXCPIYOY")

    p = out["venues"]["polymarket"]
    assert p["scored"] is False and p["scores"] is None
    assert p["not_scored_reason"] == "price history unavailable for every market in the sample"
    assert "polymarket: price history unavailable for 2 market(s)" in out["data_gaps"]
    assert out["degraded"] is True
    # The other venue is unaffected.
    assert out["venues"]["kalshi"]["scores"]["24h"]["brier"] == 0.065


@respx.mock
@pytest.mark.asyncio
async def test_settled_kalshi_listing_outage_reported_per_venue(fetcher: Fetcher) -> None:
    _mock_settled_venues(kalshi_status=500)
    out = await fetch_prediction_settled(fetcher, offsets_hours=[24], kalshi_series="KXCPIYOY")

    k = out["venues"]["kalshi"]
    assert k["error"] == "Kalshi settled markets unavailable for KXCPIYOY"
    assert k["scored"] is False and k["scores"] is None
    assert "kalshi: Kalshi settled markets unavailable for KXCPIYOY" in out["data_gaps"]
    assert out["venues"]["polymarket"]["scores"]["24h"]["brier"] == 0.125


@pytest.mark.asyncio
async def test_settled_rejects_non_positive_offsets(fetcher: Fetcher) -> None:
    out = await fetch_prediction_settled(fetcher, offsets_hours=[0, -5])
    assert out["degraded"] is True
    assert "at least one positive" in out["error"]


# ---------------------------------------------------------------------------
# Tool handlers (registered names reach the real source functions)
# ---------------------------------------------------------------------------

def _tools_registry():
    """Import world_intel_mcp.tools under the suite's temp cache path.

    Importing the tools package imports runtime, which opens a live
    ``Cache()``/``AOIStore()``; pointing WORLD_INTEL_CACHE_DB at
    test_server_registry's temp path (shared, so the override holds no
    matter which test file triggers the first import) keeps that off the
    developer's real cache database. A module-level import here would
    run at collection time and break test_server_registry's premise."""
    import importlib
    import os

    from .test_server_registry import _TMP_CACHE

    prior = os.environ.get("WORLD_INTEL_CACHE_DB")
    os.environ["WORLD_INTEL_CACHE_DB"] = str(_TMP_CACHE)
    try:
        return importlib.import_module("world_intel_mcp.tools")
    finally:
        if prior is None:
            os.environ.pop("WORLD_INTEL_CACHE_DB", None)
        else:
            os.environ["WORLD_INTEL_CACHE_DB"] = prior


@respx.mock
@pytest.mark.asyncio
async def test_tool_handlers_dispatch_with_arguments(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    pm_tools = _tools_registry().prediction_markets
    monkeypatch.setattr(pm_tools.runtime, "fetcher", fetcher)
    _kalshi_fed_events()
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(200, json=POLY_OPEN))

    markets = await pm_tools.HANDLERS["intel_kalshi_markets"]({"series": "KXFEDDECISION", "limit": 2})
    assert [m["id"] for m in markets["markets"]] == ["KXFEDDECISION-26OCT-H0", "KXFEDDECISION-26OCT-H25"]

    compare = await pm_tools.HANDLERS["intel_prediction_compare"](
        {"series": "KXFEDDECISION", "min_confidence": 0.99}
    )
    assert compare["matched"] == [] and compare["min_confidence"] == 0.99
    assert compare["unmatched_kalshi"] == 5

    settled = await pm_tools.HANDLERS["intel_prediction_settled"]({"offsets_hours": []})
    # An empty list falls back to the default offsets, not to "no offsets".
    assert settled.get("offsets_hours") == [24.0, 168.0] or "error" not in settled
    assert settled["offsets_hours"] == [24.0, 168.0]
    # No route mocked for the Kalshi series list or Gamma closed markets in
    # this test: both venues report the failure, neither reads as zero.
    assert settled["degraded"] is True
    assert settled["venues"]["kalshi"]["scored"] is False
    assert {t.name for t in pm_tools.TOOLS} == set(pm_tools.HANDLERS)


@respx.mock
@pytest.mark.asyncio
async def test_settled_unknown_kalshi_category_says_why_not_scored(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    respx.get(url__regex=r".*api\.elections\.kalshi\.com/trade-api/v2/series.*").mock(
        return_value=httpx.Response(200, json={"series": []})
    )

    async def _no_polymarket(*args, **kwargs) -> dict:
        return {"markets": [], "listing_rows_read": 0, "open_too_briefly_skipped": 0, "error": None}

    monkeypatch.setattr("world_intel_mcp.sources.prediction.fetch_polymarket_settled", _no_polymarket)
    out = await fetch_prediction_settled(fetcher, kalshi_category="Climate")
    kalshi = out["venues"]["kalshi"]
    assert kalshi["scored"] is False
    assert kalshi["not_scored_reason"].startswith("Kalshi lists no series in category 'Climate'")
    assert any(g.startswith("kalshi: not scored (Kalshi lists no series") for g in out["data_gaps"])


@respx.mock
@pytest.mark.asyncio
async def test_settled_malformed_upstream_price_is_excluded_not_raised(fetcher: Fetcher) -> None:
    """A NaN or out-of-range price from either venue is counted as an
    exclusion. Unguarded, it raised out of brier_score and lost both
    venues' results."""
    respx.get(url__regex=KALSHI_MARKETS).mock(
        return_value=httpx.Response(200, json={"markets": SETTLED_ROWS, "cursor": ""})
    )
    candles = {"M1": [(K_REF - 30 * 3600, "NaN")], "M2": [(K_REF - 48 * 3600, "0.3000")]}
    respx.get(url__regex=KALSHI_CANDLES).mock(side_effect=lambda request: httpx.Response(200, json={"markets": [
        {"market_ticker": t, "candlesticks": [
            {"end_period_ts": ts, "price": {"close_dollars": p}} for ts, p in candles.get(t, [])
        ]} for t in request.url.params["market_tickers"].split(",")
    ]}))
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(200, json=POLY_CLOSED))
    history = {"p1a": [{"t": P1_REF - 26 * 3600, "p": 0.7}], "p2a": [{"t": P2_REF - 25 * 3600, "p": 1.5}]}
    respx.get(url__regex=CLOB).mock(
        side_effect=lambda request: httpx.Response(200, json={"history": history.get(request.url.params["market"], [])})
    )
    out = await fetch_prediction_settled(fetcher, offsets_hours=[24], kalshi_series="KXCPIYOY")

    k = out["venues"]["kalshi"]
    # M2 0.30 vs NO -> 0.09; M1's "NaN" excluded; M6 has no candle.
    assert k["scores"]["24h"] == {
        "n": 1, "distinct_events": 1, "brier": 0.09, "yes_rate": 0.0,
        "excluded": {"price outside 0..1": 1, "no price at or before the offset": 1},
    }
    assert [s["price_before_reference"]["24h"] for s in k["sample"]] == [0.3, None, None]
    p = out["venues"]["polymarket"]
    # p1 0.7 vs YES -> 0.09; p2's 1.5 excluded.
    assert p["scores"]["24h"] == {
        "n": 1, "distinct_events": 1, "brier": 0.09, "yes_rate": 1.0, "excluded": {"price outside 0..1": 1},
    }
    assert [s["price_before_reference"]["24h"] for s in p["sample"]] == [0.7, None]


# ---------------------------------------------------------------------------
# Argument bounds
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_bad_arguments_are_refused_before_any_request(fetcher: Fetcher) -> None:
    routes = [
        respx.get(url__regex=p).mock(return_value=httpx.Response(200, json={}))
        for p in (r".*api\.elections\.kalshi\.com/.*", GAMMA, CLOB)
    ]
    compare_cases = {
        "min_confidence must be from 0 to 1 (got 1.5)": {"min_confidence": 1.5},
        "min_confidence must be a finite number (got nan)": {"min_confidence": float("nan")},
        "min_confidence must be a finite number (got '0.6')": {"min_confidence": "0.6"},
        "polymarket_limit must be a whole number from 1 to 2000 (got '300')": {"polymarket_limit": "300"},
        "kalshi_pages must be a whole number from 1 to 25 (got 2.5)": {"kalshi_pages": 2.5},
        "series_limit must be a whole number from 1 to 25 (got 'x')": {"series_limit": "x"},
        "limit must be a whole number from 1 to 500 (got None)": {"limit": None},
    }
    for message, kwargs in compare_cases.items():
        out = await fetch_prediction_compare(fetcher, category="Economics", **kwargs)
        assert out["error"] == message and out["degraded"] is True
        assert "matched" not in out
    settled_cases = {
        "offsets_hours values must be numbers of hours up to 8760 (got inf)": {"offsets_hours": [24, float("inf")]},
        "offsets_hours values must be numbers of hours up to 8760 (got 8761)": {"offsets_hours": [8761]},
        "offsets_hours values must be numbers of hours up to 8760 (got 'x')": {"offsets_hours": [24, "x"]},
        "offsets_hours takes at most 10 values (got 11)": {"offsets_hours": list(range(1, 12))},
        "offsets_hours must be a list of hours (got 24)": {"offsets_hours": 24},
        "max_staleness_hours must be from 0 to 720 (got 721)": {"max_staleness_hours": 721},
        "max_staleness_hours must be a finite number (got nan)": {"max_staleness_hours": float("nan")},
        "kalshi_min_volume must be from 0 to 1e+12 (got -1)": {"kalshi_min_volume": -1},
        "polymarket_min_volume must be a finite number (got inf)": {"polymarket_min_volume": float("inf")},
        "limit must be a whole number from 1 to 100 (got '25')": {"limit": "25"},
        "kalshi_series_limit must be a whole number from 1 to 25 (got None)": {"kalshi_series_limit": None},
    }
    for message, kwargs in settled_cases.items():
        out = await fetch_prediction_settled(fetcher, **kwargs)
        assert out["error"] == message and out["degraded"] is True
        assert "venues" not in out
    assert sum(r.call_count for r in routes) == 0


@respx.mock  # no route: a handler that ran anyway could not reach the network
@pytest.mark.asyncio
async def test_mcp_call_above_a_stated_maximum_is_refused_by_the_schema() -> None:
    """The MCP SDK checks arguments against each tool's inputSchema
    before any handler runs (measured on mcp 1.30.0), so the stated
    maxima also hold on the MCP path itself."""
    import importlib

    from mcp import types

    _tools_registry()  # runtime first, under the suite's temp cache path
    srv = importlib.import_module("world_intel_mcp.server")
    handler = srv.server.request_handlers[types.CallToolRequest]
    cases = {
        "intel_kalshi_markets": (
            {"category": "Economics", "series_limit": 1000000}, "1000000 is greater than the maximum of 25"
        ),
        "intel_prediction_compare": ({"min_confidence": 1.5}, "1.5 is greater than the maximum of 1"),
        "intel_prediction_settled": ({"offsets_hours": [24, 9000]}, "9000 is greater than the maximum of 8760"),
    }
    for name, (arguments, message) in cases.items():
        request = types.CallToolRequest(
            method="tools/call", params=types.CallToolRequestParams(name=name, arguments=arguments)
        )
        result = (await handler(request)).root
        assert result.isError is True
        assert result.content[0].text == f"Input validation error: {message}"
