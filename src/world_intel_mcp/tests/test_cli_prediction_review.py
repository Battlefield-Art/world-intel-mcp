"""CLI tests for the review round (2026-10-17): ``prediction-settled``
prints one Brier table per venue with its reference time, and
``prediction-compare`` shows each pair's flags, the Kalshi last-trade
time and what the score is.

Fetch functions are replaced with fakes returning the documented shapes
(pinned against respx-mocked upstreams in test_prediction_cross_review.py).

Gaps / not covered: terminal widths below 200 columns.
"""

import pytest

from world_intel_mcp import cli
from world_intel_mcp.tests.test_cli_prediction_markets import COMPARE_PAYLOAD, _fake, _invoke

LAST_TRADE = "2026-10-17T17:43:15.314853Z"


@pytest.fixture(autouse=True)
def _cli_isolation(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "cli-cache.db"))
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.setattr(cli, "_fetcher", None)


def _scores(brier: float, n: int) -> dict:
    return {"24h": {"n": n, "distinct_events": n, "brier": brier, "yes_rate": 0.5, "excluded": {}}}


SETTLED_BOTH = {
    "venues": {
        "kalshi": {"scored": True, "scores": _scores(0.065, 2),
                   "reference_time": "close_time (when trading ended; the actual close when a market closes early)"},
        "polymarket": {"scored": True, "scores": _scores(0.125, 2),
                       "reference_time": "the earlier of endDate (scheduled end; a game's start) and closedTime"},
    },
    "comparability": "Reference times differ by venue.",
    "data_gaps": [],
}


def test_prediction_settled_prints_one_table_per_venue(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli.prediction_cross, "fetch_prediction_settled", _fake(SETTLED_BOTH))
    out = _invoke("prediction-settled").output
    kalshi_at = out.index("Brier scores, kalshi sample only")
    polymarket_at = out.index("Brier scores, polymarket sample only")
    assert kalshi_at < out.index("0.0650") < polymarket_at < out.index("0.1250")
    assert "kalshi reference time: close_time (when trading ended" in out
    assert "polymarket reference time: the earlier of endDate" in out
    assert "Reference times differ by venue." in out


def test_prediction_settled_unscored_venue_prints_no_table(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = dict(SETTLED_BOTH, venues={
        "kalshi": SETTLED_BOTH["venues"]["kalshi"],
        "polymarket": {"scored": False, "scores": None,
                       "not_scored_reason": "price history served from expired cache for every market in the sample"},
    })
    monkeypatch.setattr(cli.prediction_cross, "fetch_prediction_settled", _fake(payload))
    out = _invoke("prediction-settled").output
    assert "Brier scores, polymarket sample only" not in out
    assert "polymarket: not scored (price history served from expired cache for every market in the sample)" in out


def _pair(kalshi_id: str, gap: float, flags: list, last_trade: str | None) -> dict:
    base = COMPARE_PAYLOAD["matched"][0]
    return dict(
        base, price_gap=gap, flags=flags, comparable=not flags,
        kalshi=dict(base["kalshi"], id=kalshi_id, last_trade_time=last_trade),
    )


def test_prediction_compare_shows_flags_last_trade_and_score_meaning(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = dict(
        COMPARE_PAYLOAD,
        matched=[
            _pair("H25", 0.0, [], None),
            _pair("C25", 0.0065, ["kalshi price is not a two-sided quote midpoint (basis: last_trade)"], LAST_TRADE),
        ],
        matched_count=2, comparable_count=1,
    )
    monkeypatch.setattr(cli.prediction_cross, "fetch_prediction_compare", _fake(payload))
    out = _invoke("prediction-compare").output
    assert "2 matched" in out and "; 1 comparable (both prices live two-sided quotes)" in out
    assert "Flags" in out and "Score" in out
    assert "last_trade" in out and LAST_TRADE in out
    assert "Score: ordinal match score for ranking pairs, not a probability." in out
    assert "Flagged pairs are listed after comparable ones" in out


def test_prediction_compare_without_flags_prints_no_flag_note(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = dict(COMPARE_PAYLOAD, matched=[_pair("H25", 0.0, [], None)], comparable_count=1)
    monkeypatch.setattr(cli.prediction_cross, "fetch_prediction_compare", _fake(payload))
    out = _invoke("prediction-compare").output
    assert "Flagged pairs are listed" not in out
    assert "Score: ordinal match score for ranking pairs, not a probability." in out
