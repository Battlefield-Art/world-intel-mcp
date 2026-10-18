"""CLI marking of expired copies and last-trade lookups (re-review round,
2026-10-17): ``predictions`` titles an expired Polymarket list as such
and prints its data gaps; ``kalshi`` notes a market priced from an
expired copy; ``prediction-compare`` says what each Kalshi last-trade
lookup found.

The ``predictions`` test runs the real source against a respx-mocked
Gamma outage with an expired cached list; the others use fakes
returning the documented shapes (pinned in test_prediction_cross_lookups.py).

Gaps / not covered: terminal widths below 200 columns.
"""

import asyncio as asyncio_mod

import httpx
import pytest
import respx
from click.testing import CliRunner

from world_intel_mcp import cli
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources.prediction import fetch_prediction_markets
from world_intel_mcp.tests.test_sources_kalshi_stale import TEN_DAYS, expire
from world_intel_mcp.tests.test_sources_prediction import _MARKETS

GAMMA_ANY = r".*gamma-api\.polymarket\.com.*"
STALE_GAP = "served from expired cache after a failed live fetch"


@pytest.fixture(autouse=True)
def _cli_isolation(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "cli-cache.db"))
    monkeypatch.setenv("COLUMNS", "200")
    # rich pins the width when COLUMNS is set at import; None reads it per call.
    monkeypatch.setattr(cli.console, "_width", None)
    monkeypatch.setattr(cli, "_fetcher", None)

    async def _sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _sleep)


def _invoke(*args: str):
    return CliRunner().invoke(cli.main, list(args), catch_exceptions=False)


def _fake(payload):
    async def _f(*args, **kwargs):
        return payload

    return _f


# ---------------------------------------------------------------------------
# predictions (legacy Polymarket list)
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_legacy_list_from_expired_cache_is_stale_and_degraded(fetcher: Fetcher) -> None:
    expire(fetcher, "prediction:polymarket:20", _MARKETS[:2])
    respx.get(url__regex=GAMMA_ANY).mock(return_value=httpx.Response(500))

    result = await fetch_prediction_markets(fetcher)

    assert result["count"] == 2
    assert result["stale"] is True and result["degraded"] is True
    assert result["data_gaps"] == [STALE_GAP]


@respx.mock
def test_predictions_titles_an_expired_list_and_prints_its_gaps(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    expire(fetcher, "prediction:polymarket:20", _MARKETS[:2])
    respx.get(url__regex=GAMMA_ANY).mock(return_value=httpx.Response(500))
    monkeypatch.setattr(cli, "_fetcher", fetcher)

    result = _invoke("predictions")

    assert result.exit_code == 0
    assert f"Gap: {STALE_GAP}" in result.output
    assert "Prediction Markets (expired cached copy)" in result.output


@respx.mock
def test_predictions_live_list_has_the_plain_title_and_no_gap(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    respx.get(url__regex=GAMMA_ANY).mock(return_value=httpx.Response(200, json=_MARKETS[:2]))
    monkeypatch.setattr(cli, "_fetcher", fetcher)

    result = _invoke("predictions")

    assert result.exit_code == 0
    assert "Gap:" not in result.output and "expired" not in result.output
    assert "Prediction Markets" in result.output


# ---------------------------------------------------------------------------
# kalshi and prediction-compare
# ---------------------------------------------------------------------------

def test_kalshi_notes_a_market_priced_from_an_expired_copy(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "markets": [
            {"question": "Will the Federal Reserve Hike rates by 25bps at their October 2026 meeting?",
             "yes_side": "Hike 25bps", "probability": 0.135, "price_basis": "bid_ask_midpoint",
             "volume_24h": 61814.96, "volume": 4066283.05, "stale": True, "stale_age_seconds": TEN_DAYS},
            {"question": "Will the Federal Reserve Hike rates by 0bps at their October 2026 meeting?",
             "yes_side": "Fed maintains rate", "probability": 0.865, "price_basis": "bid_ask_midpoint",
             "volume_24h": 64959.15, "volume": 5635368.81, "stale": False, "stale_age_seconds": None},
        ],
        "count": 2, "scan": {}, "units": {}, "data_gaps": [],
    }
    monkeypatch.setattr(cli.kalshi_src, "fetch_kalshi_markets", _fake(payload))

    result = _invoke("kalshi", "--series", "KXFEDDECISION")

    assert result.exit_code == 0
    assert result.output.count("(expired copy, 240.0 h old)") == 1


def _pair(kalshi_side: dict) -> dict:
    return {
        "match_confidence": 0.724, "flags": [], "comparable": False,
        "kalshi": dict({"question": "Will the Federal Reserve Cut rates by 25bps at their October 2026 meeting?"},
                       **kalshi_side),
        "polymarket": {"question": "Will the Fed decrease interest rates by 25 bps after the October 2026 meeting?"},
        "kalshi_yes_probability": 0.01, "polymarket_yes_probability": 0.0035, "price_gap": 0.0065,
    }


def test_compare_flags_say_what_each_last_trade_lookup_found(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "matched": [
            _pair({"last_trade_lookup": "found", "last_trade_time": "2026-10-17T17:43:15Z", "last_trade_price": 0.03}),
            _pair({"last_trade_lookup": "found_in_expired_copy", "last_trade_time": "2026-10-07T21:54:56Z",
                   "last_trade_price": 0.01}),
            _pair({"last_trade_lookup": "not_looked_up_cap", "last_trade_time": None}),
        ],
        "matched_count": 3, "kalshi_markets_compared": 3, "polymarket_markets_compared": 3,
        "unmatched_kalshi": 0, "unmatched_polymarket": 0, "min_confidence": 0.6, "data_gaps": [],
    }
    monkeypatch.setattr(cli.prediction_cross, "fetch_prediction_compare", _fake(payload))

    result = _invoke("prediction-compare")

    assert result.exit_code == 0
    # The Flags column wraps (max 40 wide), so the full notes are checked
    # on the helper and their pieces in the rendered table.
    assert [cli._last_trade_note(p["kalshi"]) for p in payload["matched"]] == [
        "kalshi last trade 2026-10-17T17:43:15Z at 3.0%",
        "kalshi last trade 2026-10-07T21:54:56Z at 1.0% (expired copy)",
        "kalshi last trade: not looked up cap",
    ]
    for piece in ("2026-10-17T17:43:15Z", "at 3.0%", "2026-10-07T21:54:56Z", "(expired copy)",
                  "kalshi last trade: not looked up cap"):
        assert piece in result.output
    assert cli._last_trade_note({"question": "no lookup"}) is None
