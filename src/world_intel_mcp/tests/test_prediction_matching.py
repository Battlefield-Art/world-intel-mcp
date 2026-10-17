"""Tests for analysis/prediction_matching.py: cross-venue matching and
forecast scoring.

Titles, sides and dates are the live shapes captured 2026-10-17 from
Kalshi ``/events?series_ticker=KXFEDDECISION`` and Polymarket Gamma
``/events?slug=fed-decision-in-october-...``; prices are the live
quotes from the same capture. A wrong match is worse than no match, so
most cases here are near misses that must NOT match.

Gaps / not covered: synonym coverage beyond the module's table; sports
team aliases (documented as unmatched by design).
"""

from datetime import datetime, timezone

import pytest

from world_intel_mcp.analysis.prediction_matching import (
    brier_score,
    match_markets,
    price_at_offset,
    score_pair,
    text_features,
    tokenize,
)

NOW = datetime(2026, 10, 17, 16, 0, tzinfo=timezone.utc)


def _k(question, yes_side, end="2026-10-28T18:05:00Z", prob=0.135, context="Fed decision in Oct 2026?", id_="K"):
    return {
        "venue": "kalshi", "id": id_, "question": question, "context": context,
        "yes_side": yes_side, "outcomes": ["Yes", "No"], "end_time": end, "probability": prob,
    }


def _p(question, outcomes=("Yes", "No"), end="2026-10-29T03:59:00Z", prob=0.135, id_="P"):
    return {
        "venue": "polymarket", "id": id_, "question": question, "context": "Fed Decision in October?",
        "yes_side": "", "outcomes": list(outcomes), "end_time": end, "probability": prob,
    }


K_HIKE_25_OCT = _k("Will the Federal Reserve Hike rates by 25bps at their October 2026 meeting?", "Hike 25bps", id_="KXFEDDECISION-26OCT-H25")
K_CUT_25_OCT = _k("Will the Federal Reserve Cut rates by 25bps at their October 2026 meeting?", "Cut 25bps", prob=0.005, id_="KXFEDDECISION-26OCT-C25")
K_CUT_GT25_OCT = _k("Will the Federal Reserve Cut rates by >25bps at their October 2026 meeting?", "Cut >25bps", prob=0.005, id_="KXFEDDECISION-26OCT-C26")
P_HIKE_25_OCT = _p("Will the Fed increase interest rates by 25 bps after the October 2026 meeting?", id_="2589813")
P_CUT_25_OCT = _p("Will the Fed decrease interest rates by 25 bps after the October 2026 meeting?", prob=0.0035, id_="2589811")
P_CUT_50_OCT = _p("Will the Fed decrease interest rates by 50+ bps after the October 2026 meeting?", prob=0.0015, id_="2589810")
P_NO_CHANGE = _p("Will there be no change in Fed interest rates after the October 2026 meeting?", prob=0.865, id_="2589812")


# ---------------------------------------------------------------------------
# Tokenization
# ---------------------------------------------------------------------------

def test_tokenize_folds_units_numbers_and_synonyms() -> None:
    assert tokenize("Cut 25bps") == ["cut", "25", "bps"]
    assert tokenize("Fed’s $100k") == ["fed", "100000"]
    assert tokenize("Oct decrease") == ["october", "cut"]


def test_comparators_stay_attached_to_quantities() -> None:
    assert text_features("Cut >25bps")["quantities"] == {">25"}
    assert text_features("by 50+ bps")["quantities"] == {">=50"}
    assert text_features("more than 3.5 goals")["quantities"] == {">3.5"}
    assert text_features("at least 3 seats")["quantities"] == {">=3"}
    assert text_features("under 2.5")["quantities"] == {"<2.5"}
    assert text_features("3 or more")["quantities"] == {">=3"}
    assert text_features("exactly 25 bps")["quantities"] == {"25"}


def test_day_of_month_and_year_are_not_quantities() -> None:
    f = text_features("above 2.75% following the Fed's Oct 28, 2026 meeting?")
    assert f["quantities"] == {">2.75"}
    assert f["days"] == {"28"}
    assert f["years"] == {"2026"}


# ---------------------------------------------------------------------------
# True matches (live pairs)
# ---------------------------------------------------------------------------

def test_live_fed_hike_pair_matches_with_reasons() -> None:
    r = score_pair(K_HIKE_25_OCT, P_HIKE_25_OCT, now=NOW)
    assert r["rejected"] is False
    assert r["side"] == "aligned"
    # dice 0.75 (9 shared words of 13 / 11) x date factor for 0.4 days of
    # a 3-day tolerance (1 - 0.5 * 0.4 / 3 = 0.9338) = 0.700.
    assert r["confidence"] == pytest.approx(0.700, abs=0.002)
    assert any("quantities agree ['25']" in x for x in r["reasons"])
    assert any("end dates 0.4 days apart" in x for x in r["reasons"])


def test_live_fed_cut_pair_matches() -> None:
    r = score_pair(K_CUT_25_OCT, P_CUT_25_OCT, now=NOW)
    assert r["rejected"] is False and r["confidence"] >= 0.6


# ---------------------------------------------------------------------------
# Near misses that must NOT match
# ---------------------------------------------------------------------------

def test_near_miss_25_vs_50_plus_bps_rejected() -> None:
    r = score_pair(K_CUT_25_OCT, P_CUT_50_OCT, now=NOW)
    assert r["rejected"] is True and r["confidence"] == 0.0
    assert r["reasons"][-1] == "quantities differ: Kalshi ['25'] vs Polymarket ['>=50']"


def test_near_miss_more_than_25_vs_exactly_25_rejected() -> None:
    """The first live run (before comparators) matched these; it was wrong."""
    r = score_pair(K_CUT_GT25_OCT, P_CUT_25_OCT, now=NOW)
    assert r["rejected"] is True
    assert "quantities differ" in r["reasons"][-1]


def test_near_miss_other_year_rejected() -> None:
    k2027 = _k("Will the Federal Reserve Cut rates by 25bps at their October 2027 meeting?", "Cut 25bps",
               end="2027-10-27T18:05:00Z", context="Fed decision in Oct 2027?")
    r = score_pair(k2027, P_CUT_25_OCT, now=NOW)
    assert r["rejected"] is True
    assert r["reasons"][-1] == "years differ: Kalshi ['2027'] vs Polymarket ['2026']"


def test_near_miss_negation_rejected() -> None:
    """Same event (Fed holds), different framing: rejected, not guessed."""
    k_hold = _k("Will the Federal Reserve Hike rates by 0bps at their October 2026 meeting?", "Fed maintains rate", prob=0.865)
    r = score_pair(k_hold, P_NO_CHANGE, now=NOW)
    assert r["rejected"] is True


def test_near_miss_end_dates_too_far_apart_rejected() -> None:
    p_dec = _p("Will the Fed increase interest rates by 25 bps after the October 2026 meeting?", end="2026-12-10T00:00:00Z")
    r = score_pair(K_HIKE_25_OCT, p_dec, now=NOW)
    assert r["rejected"] is True
    assert r["reasons"][-1].startswith("end dates 42.")


def test_near_miss_yes_side_not_named_rejected() -> None:
    k = _k("Who will win the 2028 Democratic presidential nomination?", "Gavin Newsom",
           end="2028-08-20T00:00:00Z", context="Democratic nominee 2028")
    p = _p("Will Josh Shapiro win the 2028 Democratic presidential nomination?", end="2028-08-25T00:00:00Z")
    r = score_pair(k, p, now=NOW)
    assert r["rejected"] is True
    assert "not named in Polymarket question" in r["reasons"][-1]


def test_named_outcomes_second_side_inverts_price() -> None:
    k = _k("Will Zhejiang Lions win the Zhejiang Lions vs Shanxi Loongs game?", "Shanxi Loongs",
           end="2026-10-17T13:00:00Z", prob=0.40, context="Zhejiang Lions vs Shanxi Loongs")
    p = _p("Zhejiang Lions vs. Shanxi Loongs", outcomes=("Zhejiang Lions", "Shanxi Loongs"),
           end="2026-10-17T14:00:00Z", prob=0.62)
    out = match_markets([k], [p], min_confidence=0.5, now=NOW)
    assert len(out["matched"]) == 1
    pair = out["matched"][0]
    assert pair["side"] == "inverted"
    assert pair["polymarket_yes_probability"] == pytest.approx(0.38)
    assert pair["price_gap"] == pytest.approx(0.02)


def test_yes_side_matching_both_outcomes_rejected() -> None:
    k = _k("Lions vs Loongs", "Lions", end="2026-10-17T13:00:00Z", context="")
    p = _p("Lions vs Loongs", outcomes=("Lions", "Lions B"), end="2026-10-17T13:00:00Z")
    r = score_pair(k, p, now=NOW)
    assert r["rejected"] is True
    assert "matches both Polymarket outcomes" in r["reasons"][-1]


def test_missing_end_date_rejected() -> None:
    p = dict(P_HIKE_25_OCT, end_time=None)
    assert score_pair(K_HIKE_25_OCT, p, now=NOW)["rejected"] is True


# ---------------------------------------------------------------------------
# match_markets
# ---------------------------------------------------------------------------

def test_match_markets_one_to_one_with_counts_and_near_misses() -> None:
    kalshi = [K_HIKE_25_OCT, K_CUT_25_OCT, K_CUT_GT25_OCT]
    poly = [P_HIKE_25_OCT, P_CUT_25_OCT, P_CUT_50_OCT, P_NO_CHANGE]
    out = match_markets(kalshi, poly, now=NOW)
    pairs = {(m["kalshi"]["id"], m["polymarket"]["id"]) for m in out["matched"]}
    assert pairs == {
        ("KXFEDDECISION-26OCT-H25", "2589813"),
        ("KXFEDDECISION-26OCT-C25", "2589811"),
    }
    assert out["unmatched_kalshi"] == 1
    assert out["unmatched_polymarket"] == 2
    gaps = {m["kalshi"]["id"]: m["price_gap"] for m in out["matched"]}
    assert gaps["KXFEDDECISION-26OCT-H25"] == pytest.approx(0.0)
    assert gaps["KXFEDDECISION-26OCT-C25"] == pytest.approx(0.0015)
    assert all(nm["reasons"] for nm in out["near_misses"])
    assert any("quantities differ" in nm["reasons"][-1] for nm in out["near_misses"])


def test_below_threshold_is_not_a_match() -> None:
    out = match_markets([K_HIKE_25_OCT], [P_HIKE_25_OCT], min_confidence=0.95, now=NOW)
    assert out["matched"] == []
    assert out["unmatched_kalshi"] == 1
    assert "below threshold 0.95" in out["near_misses"][0]["reasons"][-1]


def test_missing_price_leaves_gap_none() -> None:
    k = dict(K_HIKE_25_OCT, probability=None)
    out = match_markets([k], [P_HIKE_25_OCT], now=NOW)
    assert out["matched"][0]["price_gap"] is None


# ---------------------------------------------------------------------------
# Forecast scoring
# ---------------------------------------------------------------------------

def test_brier_known_sample() -> None:
    # (0.9-1)^2 + (0.2-0)^2 + (0.6-0)^2 = 0.01 + 0.04 + 0.36 = 0.41; / 3
    assert brier_score([(0.9, 1), (0.2, 0), (0.6, 0)]) == pytest.approx(0.41 / 3)
    assert brier_score([(0.5, 1), (0.5, 0)]) == pytest.approx(0.25)
    assert brier_score([(1.0, 1), (0.0, 0)]) == 0.0


def test_brier_empty_is_none_not_zero() -> None:
    assert brier_score([]) is None


def test_brier_rejects_bad_inputs() -> None:
    with pytest.raises(ValueError):
        brier_score([(0.5, 2)])
    with pytest.raises(ValueError):
        brier_score([(1.5, 1)])


def test_price_at_offset_picks_last_point_before_target() -> None:
    ref = 1_000_000
    hist = [(ref - 30 * 3600, 0.40), (ref - 25 * 3600, 0.55), (ref - 23 * 3600, 0.90), (ref - 1, 0.99)]
    assert price_at_offset(hist, ref, 24, 168) == (0.55, "ok")
    assert price_at_offset(hist, ref, 48, 168) == (None, "no price at or before the offset")
    price, reason = price_at_offset(hist, ref, 24, 0.5)
    assert price is None and reason.startswith("last price 1.0h before the offset")
