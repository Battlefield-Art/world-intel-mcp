"""Cross-venue compare and settled scoring: expired-cache marking, price
basis and ranking, reference times and offset checks (review round,
2026-10-17).

Same fixtures as test_prediction_cross.py. respx keeps one route per
pattern, and a later ``.mock()`` on the same pattern replaces the
earlier response, so each test sets up the shared venues first and
then overrides the one endpoint it fails.

Gaps / not covered: a real outage on either venue; ordering of the live
trades endpoint. The lookup asks for one trade (``limit=1``), so it
relies on the endpoint listing newest first, which was observed live on
2026-10-17 (two samples) and is not pinned by any test here.
"""

import asyncio as asyncio_mod
from datetime import datetime, timezone

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources.prediction_cross import fetch_prediction_compare, fetch_prediction_settled
from world_intel_mcp.tests.test_prediction_cross import (
    CLOB,
    GAMMA,
    K_REF,
    KALSHI_CANDLES,
    P1_REF,
    P2_REF,
    POLY_CLOSED,
    POLY_OPEN,
    _mock_settled_venues,
)
from world_intel_mcp.tests.test_sources_kalshi import EVENTS, FED_MARKETS, SETTLED_ROWS, _event
from world_intel_mcp.tests.test_sources_kalshi_stale import TEN_DAYS, TRADES, expire

STALE_SUFFIX = "served from expired cache after a failed live fetch, the oldest 240.0 h old"
LAST_TRADE = "2026-10-17T17:43:15.314853Z"  # live shape and value, KXFEDDECISION-26OCT-C25


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _sleep)


def _fed_events(status: int = 200) -> None:
    respx.get(url__regex=EVENTS).mock(
        return_value=httpx.Response(status, json={"events": [_event(FED_MARKETS)], "cursor": ""})
    )


def _trades(status: int = 200):
    return respx.get(url__regex=TRADES).mock(return_value=httpx.Response(status, json={"trades": [
        {"created_time": LAST_TRADE, "yes_price_dollars": "0.0100", "count_fp": "935.00"},
    ], "cursor": ""}))


# ---------------------------------------------------------------------------
# intel_prediction_compare
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_compare_with_kalshi_from_expired_cache_is_degraded_and_says_so(fetcher: Fetcher) -> None:
    expire(fetcher, "kalshi:events:series:KXFEDDECISION:first", {"events": [_event(FED_MARKETS)], "cursor": ""})
    _fed_events(500)
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(200, json=POLY_OPEN))
    _trades()

    out = await fetch_prediction_compare(fetcher, series="KXFEDDECISION")

    assert out["matched_count"] == 2 and "error" not in out  # still compared, and marked
    assert out["degraded"] is True
    assert out["kalshi_scan"]["stale_responses"] == 1
    assert TEN_DAYS <= out["kalshi_scan"]["stale_max_age_seconds"] < TEN_DAYS + 60
    assert f"kalshi: Kalshi open markets: 1 response(s) {STALE_SUFFIX}" in out["data_gaps"]


@respx.mock
@pytest.mark.asyncio
async def test_compare_with_polymarket_from_expired_cache_is_degraded(fetcher: Fetcher) -> None:
    _fed_events()
    expire(fetcher, "prediction:polymarket:open:5:0", POLY_OPEN)
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(500))
    _trades()

    out = await fetch_prediction_compare(fetcher, series="KXFEDDECISION", polymarket_limit=5)

    assert out["matched_count"] == 2 and "error" not in out
    assert out["degraded"] is True
    assert "polymarket: served from expired cache after a failed live fetch" in out["data_gaps"]


@respx.mock
@pytest.mark.asyncio
async def test_compare_ranks_comparable_pairs_before_flagged_ones(fetcher: Fetcher) -> None:
    """C25's Kalshi price is a last trade on a one-sided book (bid 0): its
    0.0065 gap is not a like-for-like difference, so the live H25 pair
    (both sides two-sided, spread 0.01) ranks first despite a 0 gap."""
    _fed_events()
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(200, json=POLY_OPEN))
    trades = _trades()

    out = await fetch_prediction_compare(fetcher, series="KXFEDDECISION")

    h25, c25 = out["matched"]
    assert (h25["kalshi"]["id"], c25["kalshi"]["id"]) == ("KXFEDDECISION-26OCT-H25", "KXFEDDECISION-26OCT-C25")
    assert h25["comparable"] is True and h25["flags"] == []
    assert (h25["kalshi"]["price_basis"], h25["kalshi"]["spread"]) == ("bid_ask_midpoint", 0.01)
    assert (h25["polymarket"]["price_basis"], h25["polymarket"]["spread"]) == ("bid_ask_midpoint", 0.01)
    assert c25["comparable"] is False and c25["price_gap"] == 0.0065
    assert c25["flags"] == ["kalshi price is not a two-sided quote midpoint (basis: last_trade)"]
    assert (c25["kalshi"]["price_basis"], c25["kalshi"]["spread"]) == ("last_trade", None)
    assert c25["kalshi"]["last_trade_time"] == LAST_TRADE
    assert h25["kalshi"]["last_trade_time"] is None and c25["polymarket"]["last_trade_time"] is None
    # Only the last-trade pair is looked up, one trade per market.
    assert [c.request.url.params["ticker"] for c in trades.calls] == ["KXFEDDECISION-26OCT-C25"]
    assert [c.request.url.params["limit"] for c in trades.calls] == ["1"]
    assert out["comparable_count"] == 1 and "degraded" not in out
    assert out["ranking"].startswith("Pairs with comparable: true")
    assert "not a probability" in out["method"]


@respx.mock
@pytest.mark.asyncio
async def test_compare_failed_last_trade_lookup_is_a_gap_not_an_outage(fetcher: Fetcher) -> None:
    _fed_events()
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(200, json=POLY_OPEN))
    _trades(500)

    out = await fetch_prediction_compare(fetcher, series="KXFEDDECISION")

    assert [p["kalshi"]["last_trade_time"] for p in out["matched"]] == [None, None]
    assert "kalshi: Kalshi last-trade time unavailable for 1 market(s)" in out["data_gaps"]
    assert "degraded" not in out and out["matched_count"] == 2


# ---------------------------------------------------------------------------
# intel_prediction_settled: expired-cache serves
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_settled_kalshi_candles_from_expired_cache_are_not_scored(fetcher: Fetcher) -> None:
    expire(fetcher, f"kalshi:candles:M2,M1,M6:{K_REF - 192 * 3600}:{K_REF - 24 * 3600}", {"markets": [
        {"market_ticker": "M1", "candlesticks": [{"end_period_ts": K_REF - 30 * 3600, "price": {"close_dollars": "0.8000"}}]},
        {"market_ticker": "M2", "candlesticks": [{"end_period_ts": K_REF - 48 * 3600, "price": {"close_dollars": "0.3000"}}]},
        {"market_ticker": "M6", "candlesticks": []},
    ]})
    _mock_settled_venues()
    respx.get(url__regex=KALSHI_CANDLES).mock(return_value=httpx.Response(500))

    out = await fetch_prediction_settled(fetcher, offsets_hours=[24], kalshi_series="KXCPIYOY")

    k = out["venues"]["kalshi"]
    assert k["scored"] is False and k["scores"] is None
    assert k["not_scored_reason"] == "price history served from expired cache for every market in the sample"
    assert f"kalshi: candlesticks for 3 market(s) {STALE_SUFFIX}" in out["data_gaps"]
    assert out["degraded"] is True
    assert out["venues"]["polymarket"]["scores"]["24h"]["brier"] == 0.125  # unaffected


@respx.mock
@pytest.mark.asyncio
async def test_settled_polymarket_history_from_expired_cache_is_excluded(fetcher: Fetcher) -> None:
    expire(fetcher, f"prediction:polymarket:history:p1a:{P1_REF - 192 * 3600}:{P1_REF - 24 * 3600}",
           {"history": [{"t": P1_REF - 26 * 3600, "p": 0.7}]})
    _mock_settled_venues()

    def _clob(request: httpx.Request) -> httpx.Response:
        if request.url.params["market"] == "p1a":
            return httpx.Response(500)
        return httpx.Response(200, json={"history": [{"t": P2_REF - 25 * 3600, "p": 0.4}]})

    respx.get(url__regex=CLOB).mock(side_effect=_clob)

    out = await fetch_prediction_settled(fetcher, offsets_hours=[24], kalshi_series="KXCPIYOY")

    p = out["venues"]["polymarket"]
    # p2 only: 0.4 vs its first outcome ("Up", lost) -> 0.16. p1's
    # 10-day-old copy is not scored as if it were current.
    assert p["scores"]["24h"] == {
        "n": 1, "distinct_events": 1, "brier": 0.16, "yes_rate": 0.0,
        "excluded": {"price history served from expired cache": 1},
    }
    assert [s["price_before_reference"]["24h"] for s in p["sample"]] == [None, 0.4]
    assert (
        f"polymarket: price history for 1 market(s) {STALE_SUFFIX}; not scored" in out["data_gaps"]
    )
    assert out["degraded"] is True


@respx.mock
@pytest.mark.asyncio
async def test_settled_kalshi_listing_from_expired_cache_is_scored_and_marked(fetcher: Fetcher) -> None:
    expire(fetcher, "kalshi:settled:KXCPIYOY:first", {"markets": SETTLED_ROWS, "cursor": ""})
    _mock_settled_venues(kalshi_status=500)

    out = await fetch_prediction_settled(fetcher, offsets_hours=[24], kalshi_series="KXCPIYOY")

    k = out["venues"]["kalshi"]
    assert k["scores"]["24h"]["brier"] == 0.065  # the listing is old; the prices are live
    assert k["selection"].endswith("; 1 listing response(s) served from expired cache")
    assert out["data_gaps"] == [f"kalshi: Kalshi settled listing: 1 response(s) {STALE_SUFFIX}"]
    assert out["degraded"] is True


@respx.mock
@pytest.mark.asyncio
async def test_settled_polymarket_listing_from_expired_cache_is_scored_and_marked(fetcher: Fetcher) -> None:
    expire(fetcher, "prediction:polymarket:settled:100:10000.0", POLY_CLOSED)
    _mock_settled_venues()
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(500))

    out = await fetch_prediction_settled(fetcher, offsets_hours=[24], kalshi_series="KXCPIYOY")

    p = out["venues"]["polymarket"]
    assert p["scores"]["24h"]["brier"] == 0.125
    assert p["selection"].endswith("; the listing was served from expired cache")
    assert out["data_gaps"] == [
        "polymarket: closed-markets listing served from expired cache after a failed live fetch"
    ]
    assert out["degraded"] is True


# ---------------------------------------------------------------------------
# intel_prediction_settled: reference times, windows and offsets
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_settled_history_window_ends_at_the_smallest_offset_before_the_reference(fetcher: Fetcher) -> None:
    _mock_settled_venues()
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(200, json=[
        dict(POLY_CLOSED[0], endDate="2026-10-17T12:00:00Z"),
    ] + POLY_CLOSED[1:]))
    clob = respx.get(url__regex=CLOB).mock(return_value=httpx.Response(200, json={"history": []}))

    out = await fetch_prediction_settled(fetcher, offsets_hours=[24, 72], kalshi_series="KXCPIYOY")

    windows = {c.request.url.params["market"]: (int(c.request.url.params["startTs"]),
                                                int(c.request.url.params["endTs"])) for c in clob.calls}
    p1_ref = int(datetime(2026, 10, 17, 12, tzinfo=timezone.utc).timestamp())  # endDate, before closedTime
    # Window: [reference - (72 + 168) h, reference - 24 h].
    assert windows == {
        "p1a": (p1_ref - 240 * 3600, p1_ref - 24 * 3600),
        "p2a": (P2_REF - 240 * 3600, P2_REF - 24 * 3600),
    }
    assert [s["reference"] for s in out["venues"]["polymarket"]["sample"]] == [
        "endDate (scheduled end; the market closed later, after resolution)",
        "closedTime (closed at or before its scheduled endDate)",
    ]


@respx.mock
@pytest.mark.asyncio
async def test_settled_refuses_any_offset_at_or_below_zero(fetcher: Fetcher) -> None:
    """A bad offset next to a good one is refused, not dropped."""
    routes = [
        respx.get(url__regex=p).mock(return_value=httpx.Response(200, json={}))
        for p in (r".*api\.elections\.kalshi\.com/.*", GAMMA, CLOB)
    ]
    for offsets, message in (
        ([-5, 24], "offsets_hours values must be above 0 hours (got -5)"),
        ([24, 0], "offsets_hours values must be above 0 hours (got 0)"),
    ):
        out = await fetch_prediction_settled(fetcher, offsets_hours=offsets)
        assert out["error"] == message and out["degraded"] is True and "venues" not in out
    assert sum(r.call_count for r in routes) == 0


@respx.mock
@pytest.mark.asyncio
async def test_settled_states_reference_times_and_per_offset_samples(fetcher: Fetcher) -> None:
    _mock_settled_venues()

    out = await fetch_prediction_settled(fetcher, offsets_hours=[24], kalshi_series="KXCPIYOY")

    assert "Reference times differ by venue" in out["comparability"]
    assert "samples differ by offset" in out["comparability"]
    k, p = out["venues"]["kalshi"], out["venues"]["polymarket"]
    assert k["reference_time"].startswith("close_time (when trading ended")
    assert p["reference_time"].startswith("the earlier of endDate")
    assert [s["reference"] for s in k["sample"]] == ["close_time (trading end)"] * 3
    assert [s["reference"] for s in p["sample"]] == ["closedTime (closed at or before its scheduled endDate)"] * 2
