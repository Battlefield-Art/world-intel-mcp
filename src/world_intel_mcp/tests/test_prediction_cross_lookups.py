"""Cross-venue compare: per-pair staleness, the last-trade lookup's
bounds and statuses, quotes on the outcome the Kalshi YES pays on,
Kalshi quote range checks, and matching off the event loop (re-review
round, 2026-10-17).

Same respx fixtures as test_prediction_cross.py and
test_prediction_cross_review.py. respx keeps one route per pattern, and
a later ``.mock()`` on the same pattern replaces the earlier response.

Gaps / not covered: a real outage on either venue; the 10 s deadline
itself (the deadline test shortens it); the collector's 45 s budget is
not exercised end to end here.
"""

import asyncio as asyncio_mod
import threading
import time

import httpx
import pytest
import respx

from world_intel_mcp.analysis import prediction_matching as pm
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import kalshi, prediction_cross
from world_intel_mcp.sources.prediction_cross import fetch_prediction_compare
from world_intel_mcp.tests.test_prediction_cross import GAMMA, POLY_OPEN, _gamma
from world_intel_mcp.tests.test_sources_kalshi import EVENTS, FED_MARKETS, _event, _m
from world_intel_mcp.tests.test_sources_kalshi_stale import TEN_DAYS, TRADES, expire

C25 = "KXFEDDECISION-26OCT-C25"
H25 = "KXFEDDECISION-26OCT-H25"
LAST_TRADE = "2026-10-17T17:43:15.314853Z"
STALE_FLAG = "price from a copy 240.0 h old, served from expired cache after a failed live fetch"


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _sleep)


def _fed_events(status: int = 200) -> None:
    respx.get(url__regex=EVENTS).mock(
        return_value=httpx.Response(status, json={"events": [_event(FED_MARKETS)], "cursor": ""})
    )


def _gamma_open(status: int = 200, rows=POLY_OPEN) -> None:
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(status, json=rows))


def _trades(price: str = "0.0100", status: int = 200, trades=None):
    rows = [{"created_time": LAST_TRADE, "yes_price_dollars": price, "count_fp": "935.00"}] if trades is None else trades
    return respx.get(url__regex=TRADES).mock(return_value=httpx.Response(status, json={"trades": rows, "cursor": ""}))


def _by_kalshi(out: dict) -> dict:
    return {p["kalshi"]["id"]: p for p in out["matched"]}


# ---------------------------------------------------------------------------
# Staleness per pair
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_a_pair_priced_from_an_expired_kalshi_copy_is_flagged_not_comparable(fetcher: Fetcher) -> None:
    expire(fetcher, "kalshi:events:series:KXFEDDECISION:first", {"events": [_event(FED_MARKETS)], "cursor": ""})
    _fed_events(500)
    _gamma_open()
    trades = _trades()

    out = await fetch_prediction_compare(fetcher, series="KXFEDDECISION")

    h25 = _by_kalshi(out)[H25]
    assert h25["kalshi"]["stale"] is True
    assert TEN_DAYS <= h25["kalshi"]["stale_age_seconds"] < TEN_DAYS + 60
    assert h25["flags"] == [f"kalshi {STALE_FLAG}"] and h25["comparable"] is False
    assert out["comparable_count"] == 0
    # The trade lookup is skipped for a side priced from an expired listing:
    # a live trade time beside an old price would read as a fresh price.
    c25 = _by_kalshi(out)[C25]
    assert c25["kalshi"]["last_trade_lookup"] == "not_looked_up_stale_listing"
    assert c25["kalshi"]["last_trade_time"] is None
    assert trades.calls.call_count == 0
    assert "kalshi: last-trade time not looked up for 1 pair(s) priced from an expired listing copy" in out["data_gaps"]


@respx.mock
@pytest.mark.asyncio
async def test_a_pair_priced_from_an_expired_polymarket_copy_is_flagged_and_ranked_after_live(fetcher: Fetcher) -> None:
    _fed_events()
    expire(fetcher, "prediction:polymarket:open:5:0", POLY_OPEN)
    _gamma_open(500)
    _trades()

    out = await fetch_prediction_compare(fetcher, series="KXFEDDECISION", polymarket_limit=5)

    h25 = _by_kalshi(out)[H25]
    assert h25["polymarket"]["stale"] is True
    assert h25["flags"] == [f"polymarket {STALE_FLAG}"] and h25["comparable"] is False
    assert out["comparable_count"] == 0
    assert "polymarket: the oldest open-markets copy is 240.0 h old" in out["data_gaps"]


# ---------------------------------------------------------------------------
# Last-trade lookups: statuses, trade price, cap and deadline
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_a_market_with_no_trade_says_no_trade(fetcher: Fetcher) -> None:
    _fed_events()
    _gamma_open()
    _trades(trades=[])

    out = await fetch_prediction_compare(fetcher, series="KXFEDDECISION")

    side = _by_kalshi(out)[C25]["kalshi"]
    assert (side["last_trade_lookup"], side["last_trade_time"]) == ("no_trade", None)
    assert out["last_trade_lookup"]["no_trade"]  # the statuses are explained in the response
    assert "last_trade_lookup" not in _by_kalshi(out)[H25]["kalshi"]  # a midpoint needs no lookup


@respx.mock
@pytest.mark.asyncio
async def test_a_failed_lookup_says_lookup_failed(fetcher: Fetcher) -> None:
    _fed_events()
    _gamma_open()
    _trades(status=500)

    out = await fetch_prediction_compare(fetcher, series="KXFEDDECISION")

    side = _by_kalshi(out)[C25]["kalshi"]
    assert (side["last_trade_lookup"], side["last_trade_time"]) == ("lookup_failed", None)


@respx.mock
@pytest.mark.asyncio
async def test_a_trade_price_that_differs_from_the_listing_is_flagged(fetcher: Fetcher) -> None:
    _fed_events()
    _gamma_open()
    _trades(price="0.0300")

    out = await fetch_prediction_compare(fetcher, series="KXFEDDECISION")

    c25 = _by_kalshi(out)[C25]
    assert (c25["kalshi"]["last_trade_time"], c25["kalshi"]["last_trade_price"]) == (LAST_TRADE, 0.03)
    assert c25["kalshi"]["last_trade_lookup"] == "found"
    assert "kalshi last trade at 0.03 (looked up separately) differs from the listing's last price 0.01" in c25["flags"]
    assert c25["comparable"] is False


@respx.mock
@pytest.mark.asyncio
async def test_a_trade_from_an_expired_copy_says_so_with_its_age(fetcher: Fetcher) -> None:
    _fed_events()
    _gamma_open()
    expire(fetcher, f"kalshi:trades:last:{C25}", {"trades": [
        {"created_time": LAST_TRADE, "yes_price_dollars": "0.0100", "count_fp": "935.00"},
    ], "cursor": ""})
    _trades(status=500)

    out = await fetch_prediction_compare(fetcher, series="KXFEDDECISION")

    side = _by_kalshi(out)[C25]["kalshi"]
    assert (side["last_trade_lookup"], side["last_trade_time"]) == ("found_in_expired_copy", LAST_TRADE)
    assert TEN_DAYS <= side["last_trade_age_seconds"] < TEN_DAYS + 60


def _ladder(n: int):
    """``n`` same-event pairs, each Kalshi side priced from a last trade."""
    markets, rows = [], []
    for i in range(n):
        level = f"{100 + i},000"
        k = _m(f"KXBTC-26OCT-B{100 + i}", f"Will Bitcoin be above ${level} at the end of October 2026?", "",
               "0.0000", "0.0300", "0.0200", "1000.00", str(100 - i))
        k["event_ticker"] = "KXBTC-26OCT"
        markets.append(k)
        rows.append(_gamma(f"77{i:02d}", f"Will Bitcoin be above ${level} at the end of October 2026?",
                           '["0.02", "0.98"]', 0.01, 0.03))
    event = _event(markets, ticker="KXBTC-26OCT", series="KXBTC", title="Bitcoin at the end of October 2026?")
    return event, rows


@respx.mock
@pytest.mark.asyncio
async def test_last_trade_lookups_stop_at_the_cap_and_say_how_many_were_skipped(fetcher: Fetcher) -> None:
    event, rows = _ladder(12)
    respx.get(url__regex=EVENTS).mock(return_value=httpx.Response(200, json={"events": [event], "cursor": ""}))
    _gamma_open(rows=rows)
    trades = _trades(price="0.0200")

    out = await fetch_prediction_compare(fetcher, series="KXBTC")

    assert out["matched_count"] == 12
    assert trades.calls.call_count == prediction_cross.MAX_TRADE_LOOKUPS == 10
    statuses = [p["kalshi"]["last_trade_lookup"] for p in out["matched"]]
    assert statuses == ["found"] * 10 + ["not_looked_up_cap"] * 2
    assert "kalshi: last-trade time looked up for the first 10 of 12 last-trade pairs shown; 2 not looked up (cap 10)" in (
        out["data_gaps"]
    )


@respx.mock
@pytest.mark.asyncio
async def test_a_trades_endpoint_that_never_answers_is_cut_at_the_deadline(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(prediction_cross, "TRADE_LOOKUP_DEADLINE_SECONDS", 0.2)
    _fed_events()
    _gamma_open()

    async def _hang(request):
        await asyncio_mod.Event().wait()  # asyncio.sleep is patched out here

    respx.get(url__regex=TRADES).mock(side_effect=_hang)

    started = time.monotonic()
    # The outer bound turns a missing deadline into a failure, not a hung run.
    out = await asyncio_mod.wait_for(fetch_prediction_compare(fetcher, series="KXFEDDECISION"), 5)
    elapsed = time.monotonic() - started

    assert elapsed < 5
    side = _by_kalshi(out)[C25]["kalshi"]
    assert (side["last_trade_lookup"], side["last_trade_time"]) == ("not_looked_up_deadline", None)
    assert "kalshi: last-trade lookups stopped at the 0.2 s deadline; 1 pair(s) not looked up" in out["data_gaps"]
    assert out["matched_count"] == 2 and "degraded" not in out  # supplementary, the comparison is complete


# ---------------------------------------------------------------------------
# Matching runs off the event loop
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_matching_runs_in_a_worker_thread(fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    _fed_events()
    _gamma_open()
    _trades()
    seen: list[int] = []
    real = prediction_cross.match_markets

    def _spy(*args, **kwargs):
        seen.append(threading.get_ident())
        return real(*args, **kwargs)

    monkeypatch.setattr(prediction_cross, "match_markets", _spy)

    out = await fetch_prediction_compare(fetcher, series="KXFEDDECISION")

    assert out["matched_count"] == 2
    assert len(seen) == 1 and seen[0] != threading.get_ident()


# ---------------------------------------------------------------------------
# Pair briefs: the quoted outcome, quote ranges, no-price flags
# ---------------------------------------------------------------------------

def test_an_inverted_pair_quotes_the_outcome_the_kalshi_yes_pays_on() -> None:
    from world_intel_mcp.tests.test_prediction_matching import NOW, _k, _p

    k = _k("Will Zhejiang Lions win the Zhejiang Lions vs Shanxi Loongs game?", "Shanxi Loongs",
           end="2026-10-17T13:00:00Z", prob=0.40, context="Zhejiang Lions vs Shanxi Loongs")
    p = _p("Zhejiang Lions vs. Shanxi Loongs", outcomes=("Zhejiang Lions", "Shanxi Loongs"),
           end="2026-10-17T14:00:00Z", prob=0.62)
    p.update(yes_bid=0.61, yes_ask=0.63, last_price=0.6)

    (pair,) = pm.match_markets([k], [p], min_confidence=0.5, now=NOW)["matched"]

    assert pair["side"] == "inverted"
    brief = pair["polymarket"]
    assert brief["quoted_outcome"] == "Shanxi Loongs"
    assert (brief["probability"], brief["yes_bid"], brief["yes_ask"], brief["last_price"]) == (0.38, 0.37, 0.39, 0.4)


def test_kalshi_quotes_outside_0_to_1_are_left_null_and_the_pair_says_why() -> None:
    raw = _m("KXFEDDECISION-26OCT-X", "Will the Federal Reserve Hike rates by 25bps at their October 2026 meeting?",
             "Hike 25bps", "45", "46", "45", "10.00", "1.00")
    market = kalshi.normalize_market(raw, _event([raw]))
    assert (market["yes_bid"], market["yes_ask"], market["last_price"]) == (None, None, None)
    assert (market["probability"], market["price_basis"]) == (None, "no_price")
    assert kalshi.bad_quote_fields(raw) == ["yes_bid_dollars", "yes_ask_dollars", "last_price_dollars"]
    assert kalshi.bad_quote_fields(FED_MARKETS[0]) == []

    flags = pm._quote_flags("kalshi", market)
    assert flags == ["kalshi price unavailable (basis: no_price)"]


@respx.mock
@pytest.mark.asyncio
async def test_kalshi_listing_counts_quotes_that_are_not_prices(fetcher: Fetcher) -> None:
    bad = _m("KXFEDDECISION-26OCT-X", "Will the Federal Reserve Hike rates by 75bps at their October 2026 meeting?",
             "Hike 75bps", "45", "46", "45", "10.00", "1.00")
    respx.get(url__regex=EVENTS).mock(
        return_value=httpx.Response(200, json={"events": [_event(FED_MARKETS + [bad])], "cursor": ""})
    )

    out = await kalshi.fetch_kalshi_open_markets(fetcher, series="KXFEDDECISION")

    assert ("1 market(s) with a quote field that is not a price from 0 to 1 (left null; the price falls back or "
            "is missing)") in out["data_gaps"]
