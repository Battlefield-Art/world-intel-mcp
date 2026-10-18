"""CLI tests for the Phase 29 prediction-market commands: ``kalshi``,
``prediction-compare`` and ``prediction-settled``.

Fetch functions are replaced with fakes returning the documented
response shapes (the shapes themselves are pinned against respx-mocked
upstreams in test_sources_kalshi.py and test_prediction_cross.py).

Gaps / not covered: terminal widths below 200 columns.
"""

import json

import pytest
from click.testing import CliRunner

from world_intel_mcp import cli


@pytest.fixture(autouse=True)
def _cli_isolation(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "cli-cache.db"))
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.setattr(cli, "_fetcher", None)


def _invoke(*args: str):
    return CliRunner().invoke(cli.main, list(args), catch_exceptions=False)


def _fake(payload, calls: list | None = None):
    async def _f(*args, **kwargs):
        if calls is not None:
            calls.append(kwargs)
        return payload

    return _f


KALSHI_PAYLOAD = {
    "markets": [
        {"question": "Will the Federal Reserve Hike rates by 0bps at their October 2026 meeting?",
         "yes_side": "Fed maintains rate", "probability": 0.865, "price_basis": "bid_ask_midpoint",
         "volume_24h": 64959.15, "volume": 5635368.81},
        {"question": "Lions vs [red]Loongs[/red]", "yes_side": "", "probability": None,
         "price_basis": "no_price", "volume_24h": 0.0, "volume": 12.0},
    ],
    "count": 2,
    "scan": {"mode": "series", "events_scanned": 1},
    "units": {"price": "Kalshi quotes YES contracts in US dollars per $1 payout"},
    "data_gaps": ["scanned 1 page(s) of open events; more exist and were not read"],
}


def test_kalshi_renders_probabilities_gaps_and_units(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list = []
    monkeypatch.setattr(cli.kalshi_src, "fetch_kalshi_markets", _fake(KALSHI_PAYLOAD, calls))
    result = _invoke("kalshi", "--series", "KXFEDDECISION", "-n", "5")
    assert result.exit_code == 0
    assert calls == [{"category": None, "series": "KXFEDDECISION", "limit": 5, "pages": 3}]
    assert "86.5%" in result.output
    assert "Fed maintains rate" in result.output
    assert "64,959" in result.output
    assert "[red]Loongs[/red]" in result.output  # remote text rendered literally
    assert "Gap:" in result.output and "more exist" in result.output
    assert "dollars per $1 payout" in result.output


def test_kalshi_error_is_printed_not_an_empty_table(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"markets": [], "count": 0, "error": "Kalshi /events page 1 unavailable", "degraded": True}
    monkeypatch.setattr(cli.kalshi_src, "fetch_kalshi_markets", _fake(payload))
    result = _invoke("kalshi")
    assert "Error:" in result.output and "page 1 unavailable" in result.output
    assert "No traded Kalshi markets" not in result.output


def test_kalshi_empty_scan_names_what_was_scanned(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"markets": [], "count": 0, "scan": {"events_scanned": 7}, "data_gaps": []}
    monkeypatch.setattr(cli.kalshi_src, "fetch_kalshi_markets", _fake(payload))
    result = _invoke("kalshi")
    assert "No traded Kalshi markets in the scanned set (7 events)" in result.output


def test_kalshi_json_output_is_the_raw_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli.kalshi_src, "fetch_kalshi_markets", _fake(KALSHI_PAYLOAD))
    result = _invoke("--json-output", "kalshi")
    assert json.loads(result.output)["markets"][0]["probability"] == 0.865


COMPARE_PAYLOAD = {
    "matched": [{
        "match_confidence": 0.724,
        "kalshi": {"question": "Will the Federal Reserve Cut rates by 25bps at their October 2026 meeting?"},
        "polymarket": {"question": "Will the Fed decrease interest rates by 25 bps after the October 2026 meeting?"},
        "kalshi_yes_probability": 0.01, "polymarket_yes_probability": 0.0035, "price_gap": 0.0065,
    }],
    "matched_count": 1, "kalshi_markets_compared": 5, "polymarket_markets_compared": 5,
    "unmatched_kalshi": 4, "unmatched_polymarket": 4, "min_confidence": 0.6, "data_gaps": [],
}


def test_prediction_compare_renders_pairs_and_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list = []
    monkeypatch.setattr(cli.prediction_cross, "fetch_prediction_compare", _fake(COMPARE_PAYLOAD, calls))
    result = _invoke("prediction-compare", "-c", "Economics", "--min-confidence", "0.7")
    assert result.exit_code == 0
    assert calls[0]["category"] == "Economics" and calls[0]["min_confidence"] == 0.7
    assert "Compared 5 Kalshi / 5 Polymarket markets: 1 matched" in result.output
    assert "unmatched Kalshi 4, unmatched Polymarket 4" in result.output
    assert "0.72" in result.output and "+0.7pp" in result.output
    # 0.0035 * 100 is 0.35000000000000003 in floating point: .1f gives 0.4.
    assert "1.0%" in result.output and "0.4%" in result.output


def test_prediction_compare_no_pairs_says_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = dict(COMPARE_PAYLOAD, matched=[], matched_count=0)
    monkeypatch.setattr(cli.prediction_cross, "fetch_prediction_compare", _fake(payload))
    result = _invoke("prediction-compare")
    assert "No pair reached match confidence 0.6" in result.output


def test_prediction_compare_outage_prints_error(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"error": "comparison not possible: kalshi unavailable", "degraded": True, "matched": []}
    monkeypatch.setattr(cli.prediction_cross, "fetch_prediction_compare", _fake(payload))
    result = _invoke("prediction-compare")
    assert "Error:" in result.output and "kalshi unavailable" in result.output


SETTLED_PAYLOAD = {
    "venues": {
        "kalshi": {"scores": {"24h": {"n": 2, "distinct_events": 1, "brier": 0.065, "yes_rate": 0.5,
                                      "excluded": {"last price older than the staleness limit": 1}}}},
        "polymarket": {"scored": False, "scores": None,
                       "not_scored_reason": "price history unavailable for every market in the sample"},
    },
    "comparability": "Each venue is scored on its own sample of different markets.",
    "data_gaps": ["polymarket: price history unavailable for 2 market(s)"],
}


def test_prediction_settled_renders_scores_and_unscored_venue(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list = []
    monkeypatch.setattr(cli.prediction_cross, "fetch_prediction_settled", _fake(SETTLED_PAYLOAD, calls))
    result = _invoke("prediction-settled", "--offset", "24", "--offset", "72", "--kalshi-series", "KXCPIYOY")
    assert result.exit_code == 0
    assert calls[0]["offsets_hours"] == [24.0, 72.0]
    assert calls[0]["kalshi_series"] == "KXCPIYOY"
    assert "0.0650" in result.output and "50.0%" in result.output
    assert "last price older than the staleness limit: 1" in result.output
    assert "not scored" in result.output
    assert "price history unavailable for every market" in result.output
    assert "Gap:" in result.output
    assert "own sample of different markets" in result.output


def test_prediction_settled_default_offsets_pass_none(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list = []
    monkeypatch.setattr(cli.prediction_cross, "fetch_prediction_settled", _fake(SETTLED_PAYLOAD, calls))
    _invoke("prediction-settled")
    assert calls[0]["offsets_hours"] is None
    assert calls[0]["kalshi_category"] == "Economics"


def test_kalshi_shows_whole_questions(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ladder rows differ only at the end of the question; slicing it to
    the column width made the October and December rows read the same."""
    october = KALSHI_PAYLOAD["markets"][0]
    december = dict(october, question=october["question"].replace("October", "December"))
    payload = dict(KALSHI_PAYLOAD, markets=[october, december], count=2)
    monkeypatch.setattr(cli.kalshi_src, "fetch_kalshi_markets", _fake(payload))
    out = _invoke("kalshi").output
    assert out.count("October") == 1 and out.count("December") == 1


def test_prediction_compare_shows_whole_questions(monkeypatch: pytest.MonkeyPatch) -> None:
    october = COMPARE_PAYLOAD["matched"][0]
    december = dict(
        october,
        kalshi={"question": october["kalshi"]["question"].replace("October", "December")},
        polymarket={"question": october["polymarket"]["question"].replace("October", "December")},
    )
    payload = dict(COMPARE_PAYLOAD, matched=[october, december], matched_count=2)
    monkeypatch.setattr(cli.prediction_cross, "fetch_prediction_compare", _fake(payload))
    out = _invoke("prediction-compare").output
    assert out.count("October") == 2 and out.count("December") == 2
