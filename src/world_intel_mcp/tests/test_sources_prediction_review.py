"""Expired-cache marking, value checks and request bounds for
sources/prediction.py (review round, 2026-10-17).

Gamma serves lists, which cannot carry the fetcher's inline ``_stale``
marker, so a stale settled listing is detected through the breaker's
stale counter, as for the open listing; CLOB histories are dicts and
carry the marker with its age.

Gaps / not covered: a real Gamma or CLOB outage; the shared polymarket
breaker opening part-way through scoring (documented, not changed).
"""

import asyncio as asyncio_mod
import json
from datetime import datetime, timezone

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import prediction
from world_intel_mcp.tests.test_prediction_cross import CLOB, GAMMA, P1_REF, P2_REF, POLY_CLOSED, _gamma
from world_intel_mcp.tests.test_sources_kalshi_stale import TEN_DAYS, expire
from world_intel_mcp.tests.test_sources_prediction import _MARKETS

LISTING_STALE = "closed-markets listing served from expired cache after a failed live fetch"


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _sleep)


# ---------------------------------------------------------------------------
# Expired-cache serves are marked
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_settled_listing_from_expired_cache_is_marked_stale(fetcher: Fetcher) -> None:
    expire(fetcher, "prediction:polymarket:settled:100:10000.0", POLY_CLOSED)
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(500))

    out = await prediction.fetch_polymarket_settled(fetcher, min_open_hours=24)

    assert [m["id"] for m in out["markets"]] == ["p1", "p2"]  # still usable, and marked
    assert out["error"] is None and out["stale"] is True
    assert out["data_gaps"] == [LISTING_STALE]


@respx.mock
@pytest.mark.asyncio
async def test_fresh_settled_listing_is_not_marked_stale(fetcher: Fetcher) -> None:
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(200, json=POLY_CLOSED))
    out = await prediction.fetch_polymarket_settled(fetcher, min_open_hours=24)
    assert [m["id"] for m in out["markets"]] == ["p1", "p2"]
    assert "stale" not in out and "data_gaps" not in out


@respx.mock
@pytest.mark.asyncio
async def test_price_history_from_expired_cache_reports_its_age(fetcher: Fetcher) -> None:
    expire(fetcher, "prediction:polymarket:history:tok:1000:5000", {"history": [
        {"t": 2000, "p": 0.4}, {"t": 3000, "p": "x"},
    ]})
    route = respx.get(url__regex=CLOB).mock(return_value=httpx.Response(500))
    ages: list = []

    points = await prediction.fetch_polymarket_price_history(fetcher, "tok", 1000, 5000, stale=ages)

    assert points == [(2000, 0.4)]
    assert len(ages) == 1 and TEN_DAYS <= ages[0] < TEN_DAYS + 60

    route.mock(return_value=httpx.Response(200, json={"history": [{"t": 7000, "p": 0.5}]}))
    fresh: list = []
    assert await prediction.fetch_polymarket_price_history(fetcher, "tok", 6000, 9000, stale=fresh) == [(7000, 0.5)]
    assert fresh == []


# ---------------------------------------------------------------------------
# Values: finite prices in 0..1, readable volumes
# ---------------------------------------------------------------------------

def test_outcome_prices_must_be_finite_and_within_0_to_1() -> None:
    parse = prediction._parse_outcome_prices
    assert parse("[1.5, -0.5]") is None
    assert parse('["NaN", "0.5"]') is None
    assert parse('["inf", "0"]') is None
    assert parse('["1", "0"]') == 1.0
    bad = dict(_gamma("9", "Q?", '["inf", "0"]', "nan", 1.5), lastTradePrice="Infinity")
    n = prediction.normalize_gamma_market(bad)
    assert n["probability"] is None and n["price_basis"] == "no_price"
    assert n["outcome_prices"] == [None, 0.0]
    assert n["yes_bid"] is None and n["yes_ask"] is None and n["last_price"] is None
    json.dumps(n, allow_nan=False)  # strict JSON: no NaN or Infinity anywhere


def test_spread_is_set_only_for_a_two_sided_book() -> None:
    two_sided = prediction.normalize_gamma_market(_gamma("8", "Q?", '["0.4", "0.6"]', 0.39, 0.41))
    assert (two_sided["probability"], two_sided["price_basis"], two_sided["spread"]) == (0.4, "bid_ask_midpoint", 0.02)
    assert two_sided["last_trade_time"] is None  # Gamma publishes none
    one_sided = prediction.normalize_gamma_market(_gamma("7", "Q?", '["0.4", "0.6"]', 0, 0.41))
    assert (one_sided["price_basis"], one_sided["spread"]) == ("gamma_outcome_price", None)


@respx.mock
@pytest.mark.asyncio
async def test_open_listing_counts_markets_left_out_for_their_price(fetcher: Fetcher) -> None:
    rows = _MARKETS + [{"question": "Impossible", "outcomePrices": "[1.5, -0.5]", "volume24hr": 5}]
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(200, json=rows))

    out = await prediction.fetch_prediction_markets(fetcher)

    assert out["count"] == 3  # the impossible price is not shown as a probability
    assert out["data_gaps"] == ["2 market(s) left out: outcome price missing, unreadable or outside 0..1"]
    assert "degraded" not in out


def test_unreadable_volume_is_none_not_zero() -> None:
    ok = _gamma("8", "Q?", '["0.4", "0.6"]', 0.39, 0.41)
    assert prediction.normalize_gamma_market(dict(ok, volumeNum="NaN"))["volume"] is None
    unreadable = prediction.normalize_gamma_market(dict(ok, volumeNum="x", volume24hr="inf"))
    assert unreadable["volume"] is None and unreadable["volume_24h"] is None
    assert prediction.normalize_gamma_market(dict(ok, volumeNum=None, volume=None))["volume"] == 0.0


@respx.mock
@pytest.mark.asyncio
async def test_settled_volume_floor_is_checked_here_and_counted(fetcher: Fetcher) -> None:
    """Gamma's volume_num_min filter is not trusted alone: a NaN volume
    or one below the floor is skipped and counted."""
    rows = [dict(POLY_CLOSED[0], volumeNum="NaN"), dict(POLY_CLOSED[1], volumeNum=500)] + POLY_CLOSED[2:]
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(200, json=rows))

    out = await prediction.fetch_polymarket_settled(fetcher, min_open_hours=24)

    assert out["markets"] == []
    assert out["volume_skipped"] == 2 and out["open_too_briefly_skipped"] == 1


# ---------------------------------------------------------------------------
# Identifiers and text
# ---------------------------------------------------------------------------

def test_slugs_that_are_not_slugs_build_no_url() -> None:
    assert prediction.event_url("../../x?y=1#z) [click](https://evil.example)") == ""
    assert prediction.event_url("Will-X") == "" and prediction.event_url(None) == ""
    assert prediction.event_url("will-x-2026") == "https://polymarket.com/event/will-x-2026"
    evil = dict(_gamma("6", "x" * 600, '["0.4", "0.6"]', 0.39, 0.41), slug="../../x?y=1#z")
    evil["events"] = [{"id": "e", "slug": "../../x?y=1#z) [click](https://evil.example)", "title": "t" * 600}]
    n = prediction.normalize_gamma_market(evil)
    assert n["url"] == ""
    assert len(n["question"]) == len(n["context"]) == 500 and n["question"].endswith("...")


# ---------------------------------------------------------------------------
# Reference time and request bounds
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_settled_reference_is_the_earlier_of_end_date_and_closed_time(fetcher: Fetcher) -> None:
    rows = [dict(POLY_CLOSED[0], endDate="2026-10-17T12:00:00Z"), dict(POLY_CLOSED[1], endDate=None)] + POLY_CLOSED[2:]
    respx.get(url__regex=GAMMA).mock(return_value=httpx.Response(200, json=rows))

    out = await prediction.fetch_polymarket_settled(fetcher, min_open_hours=24)

    p1, p2 = out["markets"]
    # p1 closed 16:16:26, after its scheduled end at 12:00: the end counts.
    assert p1["reference_ts"] == int(datetime(2026, 10, 17, 12, tzinfo=timezone.utc).timestamp()) < P1_REF
    assert p1["reference"] == "endDate (scheduled end; the market closed later, after resolution)"
    assert (p2["reference_ts"], p2["reference"]) == (P2_REF, "closedTime (no endDate published)")


@respx.mock
@pytest.mark.asyncio
async def test_open_listing_reads_at_most_2000_markets(fetcher: Fetcher) -> None:
    def _page(request: httpx.Request) -> httpx.Response:
        offset, size = int(request.url.params["offset"]), int(request.url.params["limit"])
        return httpx.Response(200, json=[
            _gamma(str(offset + i), f"Q{offset + i}?", '["0.4", "0.6"]', 0.39, 0.41) for i in range(size)
        ])

    route = respx.get(url__regex=GAMMA).mock(side_effect=_page)

    out = await prediction.fetch_polymarket_open(fetcher, limit=10**6)

    assert route.call_count == 20
    assert [int(c.request.url.params["offset"]) for c in route.calls] == list(range(0, 2000, 100))
    assert out["requested"] == 2000 and out["rows_read"] == 2000
