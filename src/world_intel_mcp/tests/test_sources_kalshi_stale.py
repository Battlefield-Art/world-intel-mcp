"""Expired-cache marking, pacing, value checks and request bounds for
sources/kalshi.py (review round, 2026-10-17).

An expired-cache serve is produced the way it happens live: an entry
exists under the request's cache key but has expired, the upstream
answers 500, and the fetcher falls back to the expired entry, marking
the dict ``_stale`` with its age. ``expire`` backdates the entry by 10
days (864,000 s, 240.0 h).

Gaps / not covered: a real Kalshi outage; two processes sharing the
upstream rate limit (pacing is per process).
"""

import asyncio as asyncio_mod
import json
from collections import Counter

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import kalshi
from world_intel_mcp.tests.test_sources_kalshi import (
    CANDLES,
    EVENTS,
    FED_MARKETS,
    MANY_SERIES,
    MARKETS,
    SERIES,
    SETTLED_ROWS,
    _event,
    _m,
    _settled,
)

TRADES = r".*api\.elections\.kalshi\.com/trade-api/v2/markets/trades.*"
TEN_DAYS = 864000
STALE_SUFFIX = "served from expired cache after a failed live fetch, the oldest 240.0 h old"


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _sleep)


def expire(fetcher: Fetcher, key: str, value, age_seconds: int = TEN_DAYS) -> None:
    """Store ``value`` under ``key`` as an expired entry written ``age_seconds`` ago."""
    fetcher.cache.set(key, value, -10)
    conn = fetcher.cache._get_conn()
    conn.execute("UPDATE cache SET created_at = created_at - ? WHERE key = ?", (age_seconds, key))
    conn.commit()


# ---------------------------------------------------------------------------
# Expired-cache serves are marked
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_open_markets_from_expired_cache_are_marked_stale_and_degraded(fetcher: Fetcher) -> None:
    expire(fetcher, "kalshi:events:series:KXFEDDECISION:first", {"events": [_event(FED_MARKETS)], "cursor": ""})
    respx.get(url__regex=EVENTS).mock(return_value=httpx.Response(500))

    out = await kalshi.fetch_kalshi_markets(fetcher, series="KXFEDDECISION", limit=3)

    assert out["count"] == 3  # the old data is still shown, and marked
    assert out["stale"] is True and out["degraded"] is True and "error" not in out
    assert out["scan"]["stale_responses"] == 1
    assert TEN_DAYS <= out["scan"]["stale_max_age_seconds"] < TEN_DAYS + 60
    assert out["data_gaps"] == [f"Kalshi open markets: 1 response(s) {STALE_SUFFIX}"]


@respx.mock
@pytest.mark.asyncio
async def test_category_series_listing_from_expired_cache_is_counted(fetcher: Fetcher) -> None:
    expire(fetcher, "kalshi:series:Economics", {"series": [
        {"ticker": "KXFEDDECISION", "category": "Economics", "volume_fp": "9.00"},
    ]})
    respx.get(url__regex=SERIES).mock(return_value=httpx.Response(503))
    respx.get(url__regex=EVENTS).mock(
        return_value=httpx.Response(200, json={"events": [_event(FED_MARKETS)], "cursor": ""})
    )

    out = await kalshi.fetch_kalshi_markets(fetcher, category="Economics", limit=2)

    assert out["count"] == 2 and out["scan"]["series_scanned"] == ["KXFEDDECISION"]
    assert out["stale"] is True and out["degraded"] is True
    assert out["scan"]["stale_responses"] == 1


@respx.mock
@pytest.mark.asyncio
async def test_settled_listing_from_expired_cache_is_counted(fetcher: Fetcher) -> None:
    expire(fetcher, "kalshi:settled:KXCPIYOY:first", {"markets": SETTLED_ROWS, "cursor": ""})
    respx.get(url__regex=MARKETS).mock(return_value=httpx.Response(500))

    out = await kalshi.fetch_kalshi_settled(fetcher, series="KXCPIYOY", min_volume=1000, min_open_hours=24)

    assert [m["id"] for m in out["markets"]] == ["M2", "M1", "M6"]
    assert out["error"] is None and out["stale_responses"] == 1
    assert TEN_DAYS <= out["stale_max_age_seconds"] < TEN_DAYS + 60
    assert out["stale_gap"] == f"Kalshi settled listing: 1 response(s) {STALE_SUFFIX}"


@respx.mock
@pytest.mark.asyncio
async def test_fresh_settled_listing_reports_no_stale_responses(fetcher: Fetcher) -> None:
    respx.get(url__regex=MARKETS).mock(return_value=httpx.Response(200, json={"markets": SETTLED_ROWS, "cursor": ""}))
    out = await kalshi.fetch_kalshi_settled(fetcher, series="KXCPIYOY")
    assert out["stale_responses"] == 0 and out["stale_gap"] is None and out["stale_max_age_seconds"] is None


@respx.mock
@pytest.mark.asyncio
async def test_candles_from_expired_cache_are_named_per_ticker(fetcher: Fetcher) -> None:
    ref = 1792252800
    start, end = ref - 48 * 3600, ref - 24 * 3600
    expire(fetcher, f"kalshi:candles:A:{start}:{end}", {"markets": [
        {"market_ticker": "A", "candlesticks": [{"end_period_ts": ref - 30 * 3600, "price": {"close_dollars": "0.4200"}}]},
    ]})
    respx.get(url__regex=CANDLES).mock(return_value=httpx.Response(500))
    stale: dict = {}

    history, errors = await kalshi.fetch_kalshi_price_history(
        fetcher, [{"id": "A", "reference_ts": ref, "open_ts": None}], 48, 24, stale=stale
    )

    assert history == {"A": [(ref - 30 * 3600, 0.42)]}
    assert list(stale) == ["A"] and TEN_DAYS <= stale["A"] < TEN_DAYS + 60
    assert errors == [f"candlesticks for 1 market(s) {STALE_SUFFIX}"]


# ---------------------------------------------------------------------------
# Pacing: only a cache miss waits
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_cache_hits_are_not_paced(fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    paced: list[int] = []

    async def _count() -> None:
        paced.append(1)

    monkeypatch.setattr(kalshi, "_pace", _count)
    route = respx.get(url__regex=EVENTS).mock(
        return_value=httpx.Response(200, json={"events": [_event(FED_MARKETS)], "cursor": ""})
    )
    first = await kalshi.fetch_kalshi_markets(fetcher, series="KXFEDDECISION")
    second = await kalshi.fetch_kalshi_markets(fetcher, series="KXFEDDECISION")

    assert route.call_count == 1  # the second scan is a cache hit...
    assert len(paced) == 1  # ...and waits for nothing
    assert second["markets"] == first["markets"]


# ---------------------------------------------------------------------------
# Request bounds: 5 open-event pages and 3 settled pages per series
# ---------------------------------------------------------------------------

def _endless(payload_key: str):
    """Every page names a next page; cursors are distinct, so none is a cache hit."""

    def _page(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        page = int(params["cursor"].rsplit(":", 1)[1]) + 1 if "cursor" in params else 1
        return httpx.Response(200, json={payload_key: [], "cursor": f"{params['series_ticker']}:{page}"})

    return _page


@respx.mock
@pytest.mark.asyncio
async def test_category_scan_makes_at_most_126_requests(fetcher: Fetcher) -> None:
    series_route = respx.get(url__regex=SERIES).mock(return_value=httpx.Response(200, json={"series": MANY_SERIES}))
    events_route = respx.get(url__regex=EVENTS).mock(side_effect=_endless("events"))

    await kalshi.fetch_kalshi_markets(fetcher, category="Economics", series_limit=10**6, pages=10**6)

    per_series = Counter(c.request.url.params["series_ticker"] for c in events_route.calls)
    assert len(per_series) == 25 and set(per_series.values()) == {5}
    assert series_route.call_count + events_route.call_count == 126


@respx.mock
@pytest.mark.asyncio
async def test_settled_category_listing_makes_at_most_76_requests(fetcher: Fetcher) -> None:
    series_route = respx.get(url__regex=SERIES).mock(return_value=httpx.Response(200, json={"series": MANY_SERIES}))
    markets_route = respx.get(url__regex=MARKETS).mock(side_effect=_endless("markets"))

    await kalshi.fetch_kalshi_settled(fetcher, category="Economics", series_limit=10**6, pages=10**6)

    per_series = Counter(c.request.url.params["series_ticker"] for c in markets_route.calls)
    assert len(per_series) == 25 and set(per_series.values()) == {3}
    assert series_route.call_count + markets_route.call_count == 76


# ---------------------------------------------------------------------------
# Values: finite prices in 0..1, readable volumes
# ---------------------------------------------------------------------------

def test_prices_must_be_finite_and_within_0_to_1() -> None:
    assert kalshi.implied_probability({"last_price_dollars": "inf"}) == (None, "no_price")
    assert kalshi.implied_probability({"last_price_dollars": "1.5"}) == (None, "no_price")
    assert kalshi.implied_probability({"last_price_dollars": "1.0000"}) == (1.0, "last_trade")
    assert kalshi.implied_probability(
        {"yes_bid_dollars": "nan", "yes_ask_dollars": "0.6", "last_price_dollars": "0.55"}
    ) == (0.55, "last_trade")
    bad = dict(FED_MARKETS[0], yes_bid_dollars="nan", last_price_dollars="inf", open_interest_fp="inf")
    n = kalshi.normalize_market(bad, None)
    assert n["probability"] is None and n["price_basis"] == "no_price"
    assert n["yes_bid"] is None and n["last_price"] is None and n["open_interest"] is None
    json.dumps(n, allow_nan=False)  # strict JSON: no NaN or Infinity anywhere


def test_spread_is_set_only_for_a_two_sided_book() -> None:
    assert kalshi.normalize_market(FED_MARKETS[2], None)["spread"] == 0.01  # 0.86 / 0.87
    assert kalshi.normalize_market(FED_MARKETS[1], None)["spread"] is None  # bid 0: one-sided
    assert kalshi.normalize_market(FED_MARKETS[1], None)["last_trade_time"] is None


@respx.mock
@pytest.mark.asyncio
async def test_open_market_with_unreadable_volume_is_excluded_and_counted(fetcher: Fetcher) -> None:
    nan_row = _m("KXFEDDECISION-26OCT-NAN", "NaN volume", "X", "0.4000", "0.5000", "0.4500", "NaN", "10.00")
    respx.get(url__regex=EVENTS).mock(
        return_value=httpx.Response(200, json={"events": [_event(FED_MARKETS + [nan_row])], "cursor": ""})
    )

    out = await kalshi.fetch_kalshi_markets(fetcher, series="KXFEDDECISION")

    assert out["markets_considered"] == 5 and out["untraded_excluded"] == 0
    assert out["volume_unreadable_excluded"] == 1
    assert out["data_gaps"] == ["1 market(s) left out: volume unreadable (not a finite contract count)"]
    assert "KXFEDDECISION-26OCT-NAN" not in [m["id"] for m in out["markets"]]


@respx.mock
@pytest.mark.asyncio
async def test_settled_rows_with_unreadable_volume_are_skipped_not_kept(fetcher: Fetcher) -> None:
    rows = [
        _settled("MN", "yes", "nan", "2026-07-20T14:00:00Z", "2026-09-11T12:35:00Z"),
        _settled("MI", "no", "inf", "2026-07-20T14:00:00Z", "2026-09-11T12:36:00Z"),
    ] + SETTLED_ROWS
    respx.get(url__regex=MARKETS).mock(return_value=httpx.Response(200, json={"markets": rows, "cursor": ""}))

    out = await kalshi.fetch_kalshi_settled(fetcher, series="KXCPIYOY", min_volume=1000, min_open_hours=24)

    assert [m["id"] for m in out["markets"]] == ["M2", "M1", "M6"]
    assert out["skipped"] == {"result": 1, "volume": 3, "too_short": 1}


# ---------------------------------------------------------------------------
# Identifiers and text
# ---------------------------------------------------------------------------

def test_identifiers_that_are_not_tickers_build_no_url() -> None:
    evil = dict(FED_MARKETS[0], event_ticker="../EVIL x-1", ticker="../EVIL x-1-A")
    assert kalshi.normalize_market(evil, None)["url"] == ""
    assert kalshi.normalize_market(FED_MARKETS[0], {"series_ticker": "KX FED"})["url"] == ""
    assert kalshi.normalize_market(FED_MARKETS[0], None)["url"] == "https://kalshi.com/markets/kxfeddecision"


def test_long_titles_are_capped() -> None:
    long_title = dict(FED_MARKETS[0], title="x" * 600, yes_sub_title="y" * 600)
    n = kalshi.normalize_market(long_title, {"title": "z" * 600})
    assert len(n["question"]) == len(n["context"]) == len(n["yes_side"]) == 500
    assert n["question"].endswith("...")


@respx.mock
@pytest.mark.asyncio
async def test_settled_rows_without_a_valid_ticker_are_skipped_and_counted(fetcher: Fetcher) -> None:
    """Tickers are joined into the candlestick request, so a comma or a
    blank in one would corrupt it."""
    bad = [dict(SETTLED_ROWS[0], ticker="M1,M9"), dict(SETTLED_ROWS[1], ticker="")]
    respx.get(url__regex=MARKETS).mock(
        return_value=httpx.Response(200, json={"markets": bad + SETTLED_ROWS, "cursor": ""})
    )

    out = await kalshi.fetch_kalshi_settled(fetcher, series="KXCPIYOY", min_volume=1000, min_open_hours=24)

    assert out["invalid_ticker_skipped"] == 2 and out["listing_rows_read"] == 8
    assert [m["id"] for m in out["markets"]] == ["M2", "M1", "M6"]


# ---------------------------------------------------------------------------
# Last-trade time
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_last_trade_times_come_from_the_trades_endpoint(fetcher: Fetcher) -> None:
    def _trades(request: httpx.Request) -> httpx.Response:
        ticker = request.url.params["ticker"]
        if ticker == "KXFEDDECISION-26OCT-C25":  # live shape, 2026-10-17
            return httpx.Response(200, json={"trades": [
                {"created_time": "2026-10-17T17:43:15.314853Z", "yes_price_dollars": "0.0100", "count_fp": "935.00"},
            ], "cursor": ""})
        if ticker == "KXFEDDECISION-26OCT-X":
            return httpx.Response(200, json={"trades": [], "cursor": ""})
        return httpx.Response(500)

    route = respx.get(url__regex=TRADES).mock(side_effect=_trades)

    times, gaps = await kalshi.fetch_kalshi_last_trade_times(
        fetcher, ["KXFEDDECISION-26OCT-C25", "KXFEDDECISION-26OCT-X", "KXFEDDECISION-26OCT-DOWN", "not a ticker"]
    )

    assert times == {"KXFEDDECISION-26OCT-C25": "2026-10-17T17:43:15.314853Z", "KXFEDDECISION-26OCT-X": None}
    assert gaps == ["Kalshi last-trade time unavailable for 2 market(s)"]
    assert {c.request.url.params["ticker"] for c in route.calls} == {
        "KXFEDDECISION-26OCT-C25", "KXFEDDECISION-26OCT-X", "KXFEDDECISION-26OCT-DOWN",
    }  # the malformed ticker is never requested
    assert {c.request.url.params["limit"] for c in route.calls} == {"1"}


@respx.mock
@pytest.mark.asyncio
async def test_an_unreadable_trade_time_is_a_gap_not_no_trade(fetcher: Fetcher) -> None:
    respx.get(url__regex=TRADES).mock(
        return_value=httpx.Response(200, json={"trades": [{"created_time": "yesterday"}], "cursor": ""})
    )
    times, gaps = await kalshi.fetch_kalshi_last_trade_times(fetcher, ["KXFEDDECISION-26OCT-C25"])
    assert times == {}
    assert gaps == ["Kalshi last-trade time unavailable for 1 market(s)"]


@respx.mock
@pytest.mark.asyncio
async def test_failed_trade_lookups_do_not_open_the_market_data_breaker(fetcher: Fetcher) -> None:
    """Three failed lookups trip a breaker (threshold 3); it must be the
    lookups' own, or the next market scan would be refused for 300 s."""
    respx.get(url__regex=TRADES).mock(return_value=httpx.Response(500))
    events = respx.get(url__regex=EVENTS).mock(
        return_value=httpx.Response(200, json={"events": [_event(FED_MARKETS)], "cursor": ""})
    )

    _, gaps = await kalshi.fetch_kalshi_last_trade_times(
        fetcher, ["KXFEDDECISION-26OCT-C25", "KXFEDDECISION-26OCT-C26", "KXFEDDECISION-26OCT-H26"]
    )
    out = await kalshi.fetch_kalshi_markets(fetcher, series="KXFEDDECISION")

    assert gaps == ["Kalshi last-trade time unavailable for 3 market(s)"]
    assert fetcher.breaker.is_available("kalshi") is True
    assert events.call_count == 1 and out["count"] == 5 and "degraded" not in out
    assert fetcher.breaker.is_available("kalshi_trades") is False
