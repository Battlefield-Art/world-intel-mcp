"""Tests for sources/prediction.py — respx-mocked Polymarket Gamma API.

Gaps / not covered: real Gamma API schema drift; the module's documented
"empty results on upstream failure" contract carries no degraded marker,
which is asserted as-is (see the api-down test). Superseded 2026-10-17:
the outage markers (error/degraded, stale on an expired-cache serve) are
now pinned by the Phase 24.5 regression tests at the end of this file.
"""

import asyncio as asyncio_mod

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources.prediction import (
    _classify_sentiment,
    _parse_outcome_prices,
    _safe_float,
    fetch_prediction_markets,
)

_MARKETS = [
    {
        "question": "Will X happen by 2027?",
        "outcomePrices": "[0.9, 0.1]",
        "volume24hr": "1000.5",
        "volume": "50000",
        "liquidity": "200",
        "category": "politics",
        "slug": "will-x",
    },
    {
        "question": "Coin flip?",
        "outcomePrices": "[0.5, 0.5]",
        "volume24hr": 2000,
        "volume": 9000,
        "liquidity": 100,
        "category": "fun",
        "slug": "",
    },
    # Unparseable outcomePrices -> skipped entirely.
    {"question": "Broken", "outcomePrices": "notjson", "volume24hr": 99999},
    {
        "question": "Will Y collapse?",
        "outcomePrices": "[0.05, 0.95]",
        "volume24hr": 1,
        "volume": 10,
        "liquidity": 5,
        "category": "econ",
        "slug": "will-y",
    },
    "garbage-non-dict-entry",
]


@respx.mock
@pytest.mark.asyncio
async def test_fetch_prediction_markets_parses_and_sorts(fetcher: Fetcher) -> None:
    route = respx.get(url__regex=r".*gamma-api\.polymarket\.com/markets.*").mock(
        return_value=httpx.Response(200, json=_MARKETS)
    )

    result = await fetch_prediction_markets(fetcher, limit=20)

    assert result["source"] == "polymarket"
    # Broken and non-dict entries skipped.
    assert result["count"] == 3
    # Sorted by 24h volume descending: 2000, 1000.5, 1.
    assert [m["volume_24h"] for m in result["markets"]] == [2000.0, 1000.5, 1.0]

    top = result["markets"][0]
    assert top["question"] == "Coin flip?"
    assert top["yes_probability"] == 0.5
    assert top["sentiment"] == "uncertain"
    assert top["url"] == ""  # empty slug -> no URL fabricated

    second = result["markets"][1]
    assert second["yes_probability"] == 0.9
    assert second["sentiment"] == "strong_yes"
    assert second["url"] == "https://polymarket.com/event/will-x"
    assert second["total_volume"] == 50000.0
    assert second["liquidity"] == 200.0
    assert second["category"] == "politics"

    assert result["markets"][2]["sentiment"] == "strong_no"

    # Query contract sent to the API.
    params = route.calls.last.request.url.params
    assert params["active"] == "true"
    assert params["closed"] == "false"
    assert params["limit"] == "20"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_prediction_markets_api_down(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Upstream failure keeps the documented empty shape. It now also
    carries error/degraded markers (fixed 2026-10-17, ROADMAP Phase 24.5;
    see the regression tests at the end of this file)."""

    async def _no_sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)

    respx.get(url__regex=r".*gamma-api\.polymarket\.com.*").mock(
        return_value=httpx.Response(500)
    )

    result = await fetch_prediction_markets(fetcher)
    assert result["markets"] == []
    assert result["count"] == 0
    assert result["source"] == "polymarket"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_prediction_markets_non_list_response(fetcher: Fetcher) -> None:
    respx.get(url__regex=r".*gamma-api\.polymarket\.com.*").mock(
        return_value=httpx.Response(200, json={"unexpected": "dict"})
    )

    result = await fetch_prediction_markets(fetcher)
    assert result["markets"] == []
    assert result["count"] == 0


# ---------------------------------------------------------------------------
# ROADMAP Phase 24.5: an outage must be distinguishable from "no markets".
# Written failing-first against the pre-fix module (which returned the
# bare empty shape above with no marker).
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_outage_carries_error_and_degraded_not_bare_empty(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _no_sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    respx.get(url__regex=r".*gamma-api\.polymarket\.com.*").mock(
        return_value=httpx.Response(503)
    )

    result = await fetch_prediction_markets(fetcher)

    # Existing keys kept (additive fix), outage marked.
    assert result["markets"] == [] and result["count"] == 0
    assert result["source"] == "polymarket"
    assert result["degraded"] is True
    assert "unavailable" in result["error"]


@respx.mock
@pytest.mark.asyncio
async def test_non_list_response_is_marked_degraded(fetcher: Fetcher) -> None:
    respx.get(url__regex=r".*gamma-api\.polymarket\.com.*").mock(
        return_value=httpx.Response(200, json={"unexpected": "dict"})
    )

    result = await fetch_prediction_markets(fetcher)
    assert result["degraded"] is True
    assert "expected a list" in result["error"]


@respx.mock
@pytest.mark.asyncio
async def test_quiet_market_is_not_marked_as_outage(fetcher: Fetcher) -> None:
    """The other half of the distinction: a live empty list is a real
    answer and must not carry the outage markers."""
    respx.get(url__regex=r".*gamma-api\.polymarket\.com.*").mock(
        return_value=httpx.Response(200, json=[])
    )

    result = await fetch_prediction_markets(fetcher)
    assert result["count"] == 0
    assert "error" not in result
    assert "degraded" not in result
    assert "stale" not in result


@respx.mock
@pytest.mark.asyncio
async def test_stale_list_serve_is_marked_stale(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Gamma returns a list, which cannot carry the fetcher's inline
    _stale marker; the breaker's stale counter is the signal."""

    async def _no_sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    fetcher.cache.set("prediction:polymarket:20", _MARKETS[:2], -10)  # expired
    respx.get(url__regex=r".*gamma-api\.polymarket\.com.*").mock(
        return_value=httpx.Response(500)
    )

    result = await fetch_prediction_markets(fetcher)
    assert result["count"] == 2
    assert result["stale"] is True
    assert result["data_gaps"] == ["served from expired cache after a failed live fetch"]


def test_parse_outcome_prices() -> None:
    assert _parse_outcome_prices(None) is None
    assert _parse_outcome_prices("notjson") is None
    assert _parse_outcome_prices("[]") is None
    assert _parse_outcome_prices("{}") is None
    assert _parse_outcome_prices("[0.7, 0.3]") == 0.7
    assert _parse_outcome_prices('["0.25", "0.75"]') == 0.25


def test_classify_sentiment_boundaries() -> None:
    assert _classify_sentiment(0.86) == "strong_yes"
    assert _classify_sentiment(0.85) == "leaning_yes"  # boundary is strict >
    assert _classify_sentiment(0.66) == "leaning_yes"
    assert _classify_sentiment(0.5) == "uncertain"
    assert _classify_sentiment(0.34) == "leaning_no"
    assert _classify_sentiment(0.14) == "strong_no"


def test_safe_float() -> None:
    assert _safe_float(None) == 0.0
    assert _safe_float("1.5") == 1.5
    assert _safe_float("x") == 0.0
    assert _safe_float(3) == 3.0
