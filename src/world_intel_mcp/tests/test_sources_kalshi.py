"""Tests for sources/kalshi.py, respx-mocked.

Fixtures are the live response shapes captured 2026-10-17 from
``api.elections.kalshi.com/trade-api/v2`` (``/events?series_ticker=
KXFEDDECISION&with_nested_markets=true``, ``/series?category=Economics
&include_volume=true``, ``/markets?status=settled``, ``/markets/
candlesticks``), trimmed to the fields the module reads. Market values
for the KXFEDDECISION-26OCT event are the live quotes from that capture;
the KXFEDDECISION series volume is scaled down tenfold (a 9-digit run
trips the repo's privacy hook) without changing the series order.

Gaps / not covered: live API drift; the 10,000-candle cap is exercised
with a lowered budget, not against the real limit.
"""

import asyncio as asyncio_mod

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import kalshi
from world_intel_mcp.sources.kalshi import (
    fetch_kalshi_markets,
    fetch_kalshi_price_history,
    fetch_kalshi_settled,
    implied_probability,
    normalize_market,
)

EVENTS = r".*api\.elections\.kalshi\.com/trade-api/v2/events.*"
SERIES = r".*api\.elections\.kalshi\.com/trade-api/v2/series.*"
MARKETS = r".*api\.elections\.kalshi\.com/trade-api/v2/markets\?.*"
CANDLES = r".*api\.elections\.kalshi\.com/trade-api/v2/markets/candlesticks.*"


def _m(ticker, title, yes_sub, bid, ask, last, vol, vol24):
    return {
        "ticker": ticker, "event_ticker": "KXFEDDECISION-26OCT", "title": title,
        "yes_sub_title": yes_sub, "status": "active", "yes_bid_dollars": bid,
        "yes_ask_dollars": ask, "last_price_dollars": last, "volume_fp": vol,
        "volume_24h_fp": vol24, "open_interest_fp": "1.00", "close_time": "2026-10-28T17:59:00Z",
        "expected_expiration_time": "2026-10-28T18:05:00Z", "open_time": "2025-09-29T14:00:00Z",
        "result": "", "market_type": "binary",
    }


FED_MARKETS = [
    _m("KXFEDDECISION-26OCT-C26", "Will the Federal Reserve Cut rates by >25bps at their October 2026 meeting?",
       "Cut >25bps", "0.0000", "0.0100", "0.0100", "236878.39", "0.00"),
    _m("KXFEDDECISION-26OCT-C25", "Will the Federal Reserve Cut rates by 25bps at their October 2026 meeting?",
       "Cut 25bps", "0.0000", "0.0100", "0.0100", "1406426.34", "32484.06"),
    _m("KXFEDDECISION-26OCT-H0", "Will the Federal Reserve Hike rates by 0bps at their October 2026 meeting?",
       "Fed maintains rate", "0.8600", "0.8700", "0.8600", "5635368.81", "64959.15"),
    _m("KXFEDDECISION-26OCT-H25", "Will the Federal Reserve Hike rates by 25bps at their October 2026 meeting?",
       "Hike 25bps", "0.1300", "0.1400", "0.1400", "4066283.05", "61814.96"),
    _m("KXFEDDECISION-26OCT-H26", "Will the Federal Reserve Hike rates by >25bps at their October 2026 meeting?",
       "Hike >25bps", "0.0000", "0.0100", "0.0100", "983866.31", "3882.22"),
]


def _event(markets, ticker="KXFEDDECISION-26OCT", series="KXFEDDECISION", title="Fed decision in Oct 2026?",
           category="Economics"):
    return {
        "event_ticker": ticker, "series_ticker": series, "title": title,
        "sub_title": "On Oct 28, 2026", "category": category, "mutually_exclusive": True,
        "markets": markets,
    }


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _sleep)


# ---------------------------------------------------------------------------
# Pricing and normalization
# ---------------------------------------------------------------------------

def test_implied_probability_basis() -> None:
    assert implied_probability({"yes_bid_dollars": "0.8600", "yes_ask_dollars": "0.8700"}) == (0.865, "bid_ask_midpoint")
    # One-sided book (bid 0) falls back to the last trade.
    assert implied_probability(
        {"yes_bid_dollars": "0.0000", "yes_ask_dollars": "0.0100", "last_price_dollars": "0.0100"}
    ) == (0.01, "last_trade")
    assert implied_probability({"yes_bid_dollars": "0.0000", "last_price_dollars": "0.0000"}) == (None, "no_price")
    # Crossed book is not a midpoint.
    assert implied_probability(
        {"yes_bid_dollars": "0.6", "yes_ask_dollars": "0.5", "last_price_dollars": "0.55"}
    ) == (0.55, "last_trade")


def test_normalize_market_fields() -> None:
    n = normalize_market(FED_MARKETS[2], _event(FED_MARKETS))
    assert n["id"] == "KXFEDDECISION-26OCT-H0"
    assert n["yes_side"] == "Fed maintains rate"
    assert n["context"] == "Fed decision in Oct 2026?"
    assert n["end_time"] == "2026-10-28T18:05:00Z"
    assert n["volume"] == 5635368.81 and n["volume_24h"] == 64959.15
    assert n["series_ticker"] == "KXFEDDECISION"
    assert n["url"] == "https://kalshi.com/markets/kxfeddecision"
    # yes_sub_title equal to the title adds nothing: blanked.
    same = dict(FED_MARKETS[0], yes_sub_title=FED_MARKETS[0]["title"])
    assert normalize_market(same, None)["yes_side"] == ""
    # Series falls back to the event-ticker prefix when no event is given.
    assert normalize_market(FED_MARKETS[0], None)["series_ticker"] == "KXFEDDECISION"


# ---------------------------------------------------------------------------
# Open markets
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_series_scan_ranks_by_24h_volume(fetcher: Fetcher) -> None:
    route = respx.get(url__regex=EVENTS).mock(
        return_value=httpx.Response(200, json={"events": [_event(FED_MARKETS)], "cursor": ""})
    )

    out = await fetch_kalshi_markets(fetcher, series="KXFEDDECISION", limit=3)

    assert [m["id"] for m in out["markets"]] == [
        "KXFEDDECISION-26OCT-H0", "KXFEDDECISION-26OCT-H25", "KXFEDDECISION-26OCT-C25",
    ]
    assert [m["probability"] for m in out["markets"]] == [0.865, 0.135, 0.01]
    assert [m["price_basis"] for m in out["markets"]] == ["bid_ask_midpoint", "bid_ask_midpoint", "last_trade"]
    assert out["count"] == 3 and out["markets_considered"] == 5
    assert out["scan"] == {
        "mode": "series", "series": "KXFEDDECISION", "pages_fetched": 1,
        "exhaustive": True, "events_scanned": 1,
    }
    assert out["data_gaps"] == [] and "degraded" not in out and "error" not in out
    assert "dollars per $1 payout" in out["units"]["price"]
    params = route.calls.last.request.url.params
    assert params["series_ticker"] == "KXFEDDECISION"
    assert params["status"] == "open" and params["with_nested_markets"] == "true"


@respx.mock
@pytest.mark.asyncio
async def test_untraded_markets_excluded_and_counted(fetcher: Fetcher) -> None:
    untraded = _m("KXFEDDECISION-26OCT-X", "Untraded", "X", "0.0000", "0.0000", "0.0000", "0.00", "0.00")
    respx.get(url__regex=EVENTS).mock(
        return_value=httpx.Response(200, json={"events": [_event(FED_MARKETS + [untraded])], "cursor": ""})
    )
    out = await fetch_kalshi_markets(fetcher, series="KXFEDDECISION")
    assert out["markets_considered"] == 5 and out["untraded_excluded"] == 1

    kept = await fetch_kalshi_markets(fetcher, series="KXFEDDECISION", include_untraded=True)
    assert kept["markets_considered"] == 6 and kept["untraded_excluded"] == 0


@respx.mock
@pytest.mark.asyncio
async def test_outage_is_an_error_not_an_empty_market_list(fetcher: Fetcher) -> None:
    respx.get(url__regex=EVENTS).mock(return_value=httpx.Response(500))
    out = await fetch_kalshi_markets(fetcher)
    assert out["markets"] == [] and out["count"] == 0
    assert out["degraded"] is True
    assert out["error"] == "Kalshi /events page 1 unavailable (no live or cached data)"


@respx.mock
@pytest.mark.asyncio
async def test_partial_page_failure_is_degraded_with_gap(fetcher: Fetcher) -> None:
    def _pages(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("cursor") == "c2":
            return httpx.Response(500)
        return httpx.Response(200, json={"events": [_event(FED_MARKETS)], "cursor": "c2"})

    respx.get(url__regex=EVENTS).mock(side_effect=_pages)
    out = await fetch_kalshi_markets(fetcher, pages=3)
    assert out["degraded"] is True
    assert "error" not in out  # page 1 was read: partial, not an outage
    assert out["data_gaps"] == ["Kalshi /events page 2 unavailable (no live or cached data)"]
    assert out["scan"]["pages_fetched"] == 1 and out["scan"]["exhaustive"] is False
    assert out["count"] == 5


@respx.mock
@pytest.mark.asyncio
async def test_unfiltered_partial_scan_says_so_without_degrading(fetcher: Fetcher) -> None:
    respx.get(url__regex=EVENTS).mock(
        return_value=httpx.Response(200, json={"events": [_event(FED_MARKETS)], "cursor": "more"})
    )
    out = await fetch_kalshi_markets(fetcher, pages=1)
    assert out["scan"]["exhaustive"] is False
    assert out["scan"]["api_order"] == "events by close time, furthest out first"
    assert out["data_gaps"] == [
        "scanned 1 page(s) of open events; more exist and were not read, "
        "so the ranking covers only the scanned events"
    ]
    assert "degraded" not in out


@respx.mock
@pytest.mark.asyncio
async def test_category_scan_reads_top_series_by_lifetime_volume(fetcher: Fetcher) -> None:
    respx.get(url__regex=SERIES).mock(
        return_value=httpx.Response(200, json={"series": [
            {"ticker": "KXFEDDECISION", "category": "Economics", "volume_fp": "62372208.20"},
            {"ticker": "KXCPIYOY", "category": "Economics", "volume_fp": "38232786.00"},
            {"ticker": "KXAAAGASM", "category": "Economics", "volume_fp": "61728678.00"},
        ]})
    )
    gas = {
        "ticker": "KXAAAGASM-26OCT31-4.45", "event_ticker": "KXAAAGASM-26OCT31",
        "title": "Will average gas prices be above $4.45?", "yes_sub_title": "Above $4.45",
        "status": "active", "yes_bid_dollars": "0.2000", "yes_ask_dollars": "0.2400",
        "last_price_dollars": "0.2200", "volume_fp": "4225.68", "volume_24h_fp": "100000.00",
        "close_time": "2026-10-31T14:00:00Z",
    }
    off_category = dict(gas, ticker="KXAAAGASM-OTHER", event_ticker="KXAAAGASM-OTHER")

    def _events(request: httpx.Request) -> httpx.Response:
        series = request.url.params.get("series_ticker")
        if series == "KXFEDDECISION":
            return httpx.Response(200, json={"events": [_event(FED_MARKETS)], "cursor": ""})
        if series == "KXAAAGASM":
            return httpx.Response(200, json={"events": [
                _event([gas], "KXAAAGASM-26OCT31", "KXAAAGASM", "US gas price on Oct 31?"),
                _event([off_category], "KXAAAGASM-OTHER", "KXAAAGASM", "Mislabelled", category="Sports"),
            ], "cursor": ""})
        return httpx.Response(404)

    route = respx.get(url__regex=EVENTS).mock(side_effect=_events)
    out = await fetch_kalshi_markets(fetcher, category="Economics", series_limit=2, limit=2)

    assert out["scan"]["mode"] == "category_top_series"
    assert out["scan"]["series_scanned"] == ["KXFEDDECISION", "KXAAAGASM"]
    assert route.call_count == 2  # KXCPIYOY (third by volume) never read
    assert out["markets_considered"] == 6  # 5 Fed + 1 gas; the Sports-labelled event dropped
    assert [m["id"] for m in out["markets"]] == ["KXAAAGASM-26OCT31-4.45", "KXFEDDECISION-26OCT-H0"]
    assert out["markets"][0]["probability"] == 0.22


@respx.mock
@pytest.mark.asyncio
async def test_category_series_listing_outage_is_an_error(fetcher: Fetcher) -> None:
    respx.get(url__regex=SERIES).mock(return_value=httpx.Response(503))
    out = await fetch_kalshi_markets(fetcher, category="Economics")
    assert out["degraded"] is True
    assert out["error"] == "Kalshi /series?category=Economics unavailable (no live or cached data)"
    assert out["markets"] == []


# ---------------------------------------------------------------------------
# Settled markets and price history
# ---------------------------------------------------------------------------

def _settled(ticker, result, volume, open_time, settled_at, event="KXCPIYOY-26AUG"):
    return {
        "ticker": ticker, "event_ticker": event, "title": f"CPI market {ticker}",
        "yes_sub_title": ticker, "status": "finalized", "result": result, "volume_fp": volume,
        "open_time": open_time, "close_time": "2026-09-11T12:25:00Z", "settlement_ts": settled_at,
    }


SETTLED_ROWS = [
    _settled("M1", "yes", "32791.90", "2026-07-20T14:00:00Z", "2026-09-11T12:30:00Z"),
    _settled("M2", "no", "42347.50", "2026-07-20T14:00:00Z", "2026-09-11T12:31:00Z"),
    _settled("M3", "no", "231.66", "2026-07-20T14:00:00Z", "2026-09-11T12:32:00Z"),   # volume floor
    _settled("M4", "", "9000.00", "2026-07-20T14:00:00Z", "2026-09-11T12:33:00Z"),     # no yes/no result
    _settled("M5", "yes", "5000.00", "2026-09-11T02:25:00Z", "2026-09-11T12:34:00Z"),  # open 10h only
    _settled("M6", "yes", "2000.00", "2026-01-01T00:00:00Z", "2026-09-10T00:00:00Z", event="KXCPIYOY-26JUL"),
]


@respx.mock
@pytest.mark.asyncio
async def test_settled_selection_filters_and_orders(fetcher: Fetcher) -> None:
    route = respx.get(url__regex=MARKETS).mock(
        return_value=httpx.Response(200, json={"markets": SETTLED_ROWS, "cursor": ""})
    )
    out = await fetch_kalshi_settled(fetcher, series="KXCPIYOY", min_volume=1000, min_open_hours=24)

    assert [m["id"] for m in out["markets"]] == ["M2", "M1", "M6"]  # newest settlement first
    assert [m["outcome"] for m in out["markets"]] == [0, 1, 1]
    assert out["skipped"] == {"result": 1, "volume": 1, "too_short": 1}
    assert out["listing_rows_read"] == 6 and out["error"] is None
    params = route.calls.last.request.url.params
    assert params["status"] == "settled" and params["mve_filter"] == "exclude"
    assert params["series_ticker"] == "KXCPIYOY"


@respx.mock
@pytest.mark.asyncio
async def test_settled_listing_outage_is_an_error(fetcher: Fetcher) -> None:
    respx.get(url__regex=MARKETS).mock(return_value=httpx.Response(500))
    out = await fetch_kalshi_settled(fetcher, series="KXCPIYOY")
    assert out["markets"] == []
    assert out["error"] == "Kalshi settled markets unavailable for KXCPIYOY"


@respx.mock
@pytest.mark.asyncio
async def test_price_history_parses_and_splits_under_candle_budget(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    ref = 1792252800
    monkeypatch.setattr(kalshi, "_CANDLE_BUDGET", 300)

    def _candles(request: httpx.Request) -> httpx.Response:
        tickers = request.url.params["market_tickers"].split(",")
        return httpx.Response(200, json={"markets": [
            {"market_ticker": t, "candlesticks": [
                {"end_period_ts": ref - 30 * 3600, "price": {"close_dollars": "0.4200"}},
                {"end_period_ts": ref - 25 * 3600, "price": {"close_dollars": None}},
            ]} for t in tickers
        ]})

    route = respx.get(url__regex=CANDLES).mock(side_effect=_candles)
    markets = [
        {"id": "A", "reference_ts": ref, "open_ts": None},
        {"id": "B", "reference_ts": ref, "open_ts": None},
        {"id": "C", "reference_ts": ref, "open_ts": ref - 10 * 3600},  # opened after the window
    ]
    history, errors = await fetch_kalshi_price_history(fetcher, markets, 200, 24)

    # Each window is 176h = 177 hourly candles; two would be 354 > 300.
    assert route.call_count == 2
    assert [c.request.url.params["market_tickers"] for c in route.calls] == ["A", "B"]
    assert history["A"] == [(ref - 30 * 3600, 0.42)]  # null-price hour skipped
    assert history["B"] == [(ref - 30 * 3600, 0.42)]
    assert history["C"] == []  # no request: "no price", not "unavailable"
    assert errors == []
    p = route.calls[0].request.url.params
    assert p["period_interval"] == "60"
    assert int(p["start_ts"]) == ref - 200 * 3600 and int(p["end_ts"]) == ref - 24 * 3600


@respx.mock
@pytest.mark.asyncio
async def test_price_history_outage_reported(fetcher: Fetcher) -> None:
    respx.get(url__regex=CANDLES).mock(return_value=httpx.Response(500))
    history, errors = await fetch_kalshi_price_history(
        fetcher, [{"id": "A", "reference_ts": 1792252800, "open_ts": None}], 48, 24
    )
    assert "A" not in history
    assert errors == ["candlesticks unavailable for 1 market(s)"]


# ---------------------------------------------------------------------------
# Request pacing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_requests_are_spaced_by_min_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two collector sources scanning Kalshi at once drew HTTP 429 live
    (2026-10-17); each request now waits until _MIN_INTERVAL after the
    previous one. Sleeps are recorded, not slept."""
    sleeps: list[float] = []

    async def _record(seconds, *args, **kwargs) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio_mod, "sleep", _record)
    monkeypatch.setitem(kalshi._pace_state, "last", 0.0)
    await kalshi._pace()  # nothing before it: no wait
    await kalshi._pace()  # straight after: waits about the whole interval
    assert len(sleeps) == 1
    assert kalshi._MIN_INTERVAL - 0.05 < sleeps[0] <= kalshi._MIN_INTERVAL
    monkeypatch.setitem(kalshi._pace_state, "last", -1e9)  # long ago
    await kalshi._pace()
    assert len(sleeps) == 1


@respx.mock
@pytest.mark.asyncio
async def test_every_request_goes_through_the_pacer(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    paced: list[int] = []

    async def _count() -> None:
        paced.append(1)

    monkeypatch.setattr(kalshi, "_pace", _count)
    series_route = respx.get(url__regex=SERIES).mock(return_value=httpx.Response(200, json={"series": [
        {"ticker": "KXFEDDECISION", "category": "Economics", "volume_fp": "9.00"},
    ]}))
    events_route = respx.get(url__regex=EVENTS).mock(
        return_value=httpx.Response(200, json={"events": [_event(FED_MARKETS)], "cursor": ""})
    )
    out = await fetch_kalshi_markets(fetcher, category="Economics", limit=2)
    assert out["count"] == 2
    assert series_route.call_count == 1 and events_route.call_count == 1
    assert len(paced) == 2


# ---------------------------------------------------------------------------
# Category resolution
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_category_scan_drops_series_outside_the_category(fetcher: Fetcher) -> None:
    """/series?category= is loose (live: Economics returned 7 series of
    other categories among 869); only exact matches are scanned."""
    respx.get(url__regex=SERIES).mock(return_value=httpx.Response(200, json={"series": [
        {"ticker": "KXPRESPARTY", "category": "Politics", "volume_fp": "75000000.00"},
        {"ticker": "KXFEDDECISION", "category": "Economics", "volume_fp": "62372208.20"},
    ]}))
    events_route = respx.get(url__regex=EVENTS).mock(
        return_value=httpx.Response(200, json={"events": [_event(FED_MARKETS)], "cursor": ""})
    )
    out = await fetch_kalshi_markets(fetcher, category="Economics", series_limit=2)
    assert out["scan"]["series_scanned"] == ["KXFEDDECISION"]
    assert [c.request.url.params["series_ticker"] for c in events_route.calls] == ["KXFEDDECISION"]


@respx.mock
@pytest.mark.asyncio
async def test_unknown_category_is_a_named_gap_not_an_outage(fetcher: Fetcher) -> None:
    """Live: category=Climate answers {"series": null} with HTTP 200
    ('Climate and Weather' is the name). That must not read as an empty
    market or as an outage."""
    respx.get(url__regex=SERIES).mock(return_value=httpx.Response(200, json={"series": None}))
    out = await fetch_kalshi_markets(fetcher, category="Climate")
    assert out["markets"] == [] and out["count"] == 0
    assert "error" not in out and "degraded" not in out
    assert any("no series in category 'Climate'" in g for g in out["data_gaps"])

    settled = await fetch_kalshi_settled(fetcher, category="Climate")
    assert settled["markets"] == [] and settled["error"] is None
    assert any("no series in category 'Climate'" in g for g in settled["data_gaps"])


# ---------------------------------------------------------------------------
# Argument bounds: a caller's count never multiplies upstream requests
# ---------------------------------------------------------------------------

# 30 series in one category (live /series row shape); volumes rise with
# the index, so the 25 highest are KXS29 down to KXS05.
MANY_SERIES = [
    {"ticker": f"KXS{i:02d}", "category": "Economics", "volume_fp": f"{1000 + i}.00"} for i in range(30)
]
TOP_25 = [f"KXS{i:02d}" for i in range(29, 4, -1)]


def test_count_arg_clamps_whole_numbers_and_refuses_the_rest() -> None:
    assert [kalshi.count_arg(v, "pages", 25) for v in (10**6, 25, 3, 2.0, 0, -3)] == [25, 25, 3, 2, 1, 1]
    for bad in ("3", 2.5, None, True, float("nan"), float("inf"), [3]):
        with pytest.raises(ValueError, match=r"^pages must be a whole number from 1 to 25 \(got "):
            kalshi.count_arg(bad, "pages", 25)


@respx.mock
@pytest.mark.asyncio
async def test_huge_series_limit_scans_at_most_25_series(fetcher: Fetcher) -> None:
    series_route = respx.get(url__regex=SERIES).mock(
        return_value=httpx.Response(200, json={"series": MANY_SERIES})
    )
    events_route = respx.get(url__regex=EVENTS).mock(
        return_value=httpx.Response(200, json={"events": [], "cursor": ""})
    )
    out = await fetch_kalshi_markets(fetcher, category="Economics", series_limit=10**6)
    assert series_route.call_count == 1
    assert events_route.call_count == 25
    assert [c.request.url.params["series_ticker"] for c in events_route.calls] == TOP_25
    assert out["scan"]["series_scanned"] == TOP_25


@respx.mock
@pytest.mark.asyncio
async def test_huge_series_limit_reads_at_most_25_settled_series(fetcher: Fetcher) -> None:
    respx.get(url__regex=SERIES).mock(return_value=httpx.Response(200, json={"series": MANY_SERIES}))
    markets_route = respx.get(url__regex=MARKETS).mock(
        return_value=httpx.Response(200, json={"markets": [], "cursor": ""})
    )
    out = await fetch_kalshi_settled(fetcher, category="Economics", series_limit=10**6)
    assert markets_route.call_count == 25
    assert [c.request.url.params["series_ticker"] for c in markets_route.calls] == TOP_25
    assert out["scope"]["series_read"] == TOP_25


@respx.mock
@pytest.mark.asyncio
async def test_counts_that_are_not_whole_numbers_are_refused_before_any_request(fetcher: Fetcher) -> None:
    routes = [
        respx.get(url__regex=p).mock(return_value=httpx.Response(200, json={"series": MANY_SERIES}))
        for p in (SERIES, EVENTS)
    ]
    for bad in ("ten", 2.5, None, True, float("nan"), float("inf")):
        out = await fetch_kalshi_markets(fetcher, category="Economics", series_limit=bad)
        assert out["error"] == f"series_limit must be a whole number from 1 to 25 (got {bad!r})"
        assert out["degraded"] is True and "markets" not in out
    out = await fetch_kalshi_markets(fetcher, limit="5")
    assert out["error"] == "limit must be a whole number from 1 to 500 (got '5')"
    assert sum(r.call_count for r in routes) == 0


@respx.mock
@pytest.mark.asyncio
async def test_open_markets_scan_clamps_series_limit_itself(fetcher: Fetcher) -> None:
    """The cross-venue compare and any direct caller reach this function
    without fetch_kalshi_markets' checks, so it clamps on its own."""
    respx.get(url__regex=SERIES).mock(return_value=httpx.Response(200, json={"series": MANY_SERIES}))
    events_route = respx.get(url__regex=EVENTS).mock(
        return_value=httpx.Response(200, json={"events": [], "cursor": ""})
    )
    out = await kalshi.fetch_kalshi_open_markets(fetcher, category="Economics", series_limit=10**6)
    assert events_route.call_count == 25
    assert out["scan"]["series_scanned"] == TOP_25
