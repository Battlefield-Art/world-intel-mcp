"""Matching gates and candidate completeness for
analysis/prediction_matching.py (review round, 2026-10-17).

Pairs that name different events must be rejected, each with the gate
that caught it: rate actions, directions, signed numbers, a dollar
amount that looks like a year, ordinals. Same-event pairs that the old
blocking never scored ("Trump pardon Bannon?" among 30 "trump pardon"
titles), "U.S." against "US", "$10m" against "$10 million" and a
fractional threshold worded strict on one venue and inclusive on the
other must match. The negation gate, the date factor and one-to-one
matching each get a case that fails if the rule is removed.

Every expected reason and score below was computed with the module
before it was written here (scratch probes); "now" is pinned so the
date tolerance is fixed.

Gaps / not covered: wording beyond these cases; the candidate-pair
completeness check is randomized over a small vocabulary, not
exhaustive, and the prefix bound is not machine-proved (the total order
it relies on is tested in test_prediction_matching_late_gates.py).
"""

import random
from datetime import datetime, timezone

import pytest

from world_intel_mcp.analysis import prediction_matching as pm

NOW = datetime(2026, 10, 17, 16, 0, tzinfo=timezone.utc)
END = "2026-12-10T19:00:00Z"


def _k(question, yes="", end=END, id_="K", prob=0.5, **quote):
    return dict({"venue": "kalshi", "id": id_, "question": question, "context": "", "yes_side": yes,
                 "outcomes": ["Yes", "No"], "end_time": end, "probability": prob}, **quote)


def _p(question, end=END, id_="P", prob=0.5, **quote):
    return dict({"venue": "polymarket", "id": id_, "question": question, "context": "", "yes_side": "",
                 "outcomes": ["Yes", "No"], "end_time": end, "probability": prob}, **quote)


# ---------------------------------------------------------------------------
# Different events: rejected, with the gate that caught them
# ---------------------------------------------------------------------------

REJECTED = {
    "cut vs raise": (
        "Will the Fed cut rates in December 2026?", "Will the Fed raise rates in December 2026?",
        "actions differ: Kalshi ['cut'] vs Polymarket ['raise']",
    ),
    "cut vs hold": (
        "Will the Fed cut rates in December 2026?", "Will the Fed hold rates in December 2026?",
        "actions differ: Kalshi ['cut'] vs Polymarket ['hold']",
    ),
    "above vs below a dollar amount that looks like a year": (
        "Will Ethereum be above $2,000 on Dec 10, 2026?", "Will Ethereum be below $2,000 on December 10, 2026?",
        "quantities differ: Kalshi ['>2000'] vs Polymarket ['<2000']",
    ),
    "a minus sign": (
        "Will Q4 GDP growth be below -0.5%?", "Will Q4 GDP growth be below 0.5%?",
        "quantities differ: Kalshi ['4', '<-0.5'] vs Polymarket ['4', '<0.5']",
    ),
    "ordinals": (
        "Will Smith finish 1st in the 2026 Boston Marathon?", "Will Smith finish 2nd in the 2026 Boston Marathon?",
        "quantities differ: Kalshi ['1st'] vs Polymarket ['2nd']",
    ),
    "directions without numbers": (
        "Will gas prices end the year above last year's level in 2026?",
        "Will gas prices end the year below last year's level in 2026?",
        "directions differ: Kalshi ['above'] vs Polymarket ['below']",
    ),
    "an integer threshold strict on one venue and inclusive on the other": (
        "Will the Fed cut rates by more than 50 bps in December 2026?",
        "Will the Fed cut rates by 50+ bps in December 2026?",
        "quantities differ: Kalshi ['>50'] vs Polymarket ['>=50']",
    ),
}


@pytest.mark.parametrize("case", sorted(REJECTED))
def test_pairs_naming_different_events_are_rejected(case: str) -> None:
    kalshi_q, poly_q, reason = REJECTED[case]
    result = pm.score_pair(_k(kalshi_q), _p(poly_q), now=NOW)
    assert result["rejected"] is True and result["confidence"] == 0.0
    assert result["reasons"][-1] == reason


def test_a_rejected_pair_is_never_matched() -> None:
    out = pm.match_markets(
        [_k("Will the Fed cut rates in December 2026?")], [_p("Will the Fed raise rates in December 2026?")], now=NOW
    )
    assert out["matched"] == [] and out["unmatched_kalshi"] == 1 and out["unmatched_polymarket"] == 1
    assert [nm["reasons"][-1] for nm in out["near_misses"]] == ["actions differ: Kalshi ['cut'] vs Polymarket ['raise']"]


def test_negation_gate_rejects_an_otherwise_identical_pair() -> None:
    same = pm.score_pair(_k("Will Bitcoin hit $150k in 2026?"), _p("Will Bitcoin hit $150k in 2026?"), now=NOW)
    assert same["rejected"] is False and same["confidence"] == 1.0
    negated = pm.score_pair(_k("Will Bitcoin hit $150k in 2026?"), _p("Will Bitcoin not hit $150k in 2026?"), now=NOW)
    assert negated["rejected"] is True
    assert negated["reasons"][-1] == "negation differs (one title says not/no/never/without, the other does not)"


def test_no_change_names_the_hold_outcome_not_a_negation() -> None:
    result = pm.score_pair(
        _k("Will the Fed hold rates in December 2026?"), _p("Will there be no change in Fed rates in December 2026?"), now=NOW
    )
    assert result["rejected"] is False and result["confidence"] == 1.0


# ---------------------------------------------------------------------------
# Same events worded differently: matched
# ---------------------------------------------------------------------------

def test_us_with_dots_matches_us() -> None:
    result = pm.score_pair(
        _k("Will the U.S. enter a recession in 2026?"), _p("Will the US enter a recession in 2026?"), now=NOW
    )
    assert result["rejected"] is False and result["confidence"] == 1.0


def test_money_scale_words_and_suffixes_are_the_same_amount() -> None:
    assert pm.tokenize("$10m") == pm.tokenize("$10 million") == ["10000000"]
    result = pm.score_pair(
        _k("Will the Ethereum Foundation sell $10m of ETH in 2026?"),
        _p("Will the Ethereum Foundation sell $10 million of ETH in 2026?"),
        now=NOW,
    )
    assert result["rejected"] is False and result["confidence"] == 1.0


def test_fractional_threshold_strict_vs_inclusive_matches_with_a_flag() -> None:
    kalshi = _k("Will the unemployment rate be 4.5% or higher in November 2026?",
                price_basis="bid_ask_midpoint", spread=0.02)
    poly = _p("Will the unemployment rate be above 4.5% in November 2026?",
              price_basis="bid_ask_midpoint", spread=0.02)
    result = pm.score_pair(kalshi, poly, now=NOW)
    assert result["rejected"] is False and result["boundary_differs"] is True
    (pair,) = pm.match_markets([kalshi], [poly], now=NOW)["matched"]
    assert pair["match_confidence"] == 1.0
    assert pair["flags"] == ["thresholds differ at the boundary (strict on one venue, inclusive on the other)"]
    assert pair["comparable"] is False


def test_rare_word_pair_among_many_shared_word_titles_is_scored_and_matched() -> None:
    """Old blocking needed two shared words each in under 20% of the
    Polymarket titles, so this pair was never scored (pairs_scored 0)."""
    names = ["Bannon", "Giuliani", "Hunter", "Navarro", "Flynn", "Stone", "Manafort", "Meadows", "Eastman", "Clark",
             "Chesebro", "Ellis", "Powell", "Kerik", "Epshteyn", "Roman", "Trusty", "Smith", "Jones", "Brown",
             "Taylor", "Wilson", "Moore", "Martin", "Lee", "Walker", "Hall", "Allen", "Young", "King"]
    poly = [_p(f"Will Trump pardon {n}?", end="2026-12-31T00:00:00Z", id_=f"P{i}") for i, n in enumerate(names)]
    out = pm.match_markets([_k("Trump pardon Bannon?", end="2026-12-31T00:00:00Z", id_="KB")], poly, now=NOW)
    assert [(m["kalshi"]["id"], m["polymarket"]["id"], m["match_confidence"]) for m in out["matched"]] == [
        ("KB", "P0", 1.0)
    ]
    assert out["pairs_scored"] == 30


def test_candidate_pairs_include_every_pair_a_brute_force_scan_finds() -> None:
    vocabulary = "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima".split()
    rng = random.Random(20261017)

    def bag() -> str:
        return " ".join(rng.sample(vocabulary, rng.randint(1, 6)))

    k_feats = [pm._market_features(_k(bag()), "kalshi") for _ in range(40)]
    p_feats = [pm._market_features(_p(bag()), "polymarket") for _ in range(40)]
    for threshold in (0.3, 0.499, 0.6, 0.75, 1.0):
        brute = set()
        for ki, kf in enumerate(k_feats):
            for pi, pf in enumerate(p_feats):
                a, b = kf["bag"]["words"], pf["bag"]["words"]
                if 2 * len(a & b) / (len(a) + len(b)) >= threshold:
                    brute.add((ki, pi))
        candidates = set(pm._candidate_pairs(k_feats, p_feats, threshold))
        assert brute and brute <= candidates, threshold


# ---------------------------------------------------------------------------
# Score: date factor, one-to-one, ordinal statement, quote flags
# ---------------------------------------------------------------------------

def test_date_factor_lowers_the_score_with_the_end_date_gap() -> None:
    question = "Will Bitcoin hit $150k by Oct 20, 2026?"
    same_day = pm.score_pair(_k(question, end="2026-10-20T00:00:00Z"), _p(question, end="2026-10-20T00:00:00Z"), now=NOW)
    apart = pm.score_pair(_k(question, end="2026-10-20T00:00:00Z"), _p(question, end="2026-10-21T12:00:00Z"), now=NOW)
    assert same_day["confidence"] == 1.0
    # 1.5 days apart, tolerance 3 days: 1 - 0.5 * 1.5 / 3 = 0.75.
    assert apart["confidence"] == 0.75
    assert "end dates 1.5 days apart (tolerance 3.0 days)" in apart["reasons"]


def test_matching_is_one_to_one_on_both_venues() -> None:
    question = "Will Bitcoin hit $150k in 2026?"
    later = "2026-12-12T07:00:00Z"  # 1.5 days after END: scores 0.91, above the threshold
    two_kalshi = pm.match_markets([_k(question, id_="K1"), _k(question, id_="K2", end=later)], [_p(question, id_="P1")], now=NOW)
    assert [(m["kalshi"]["id"], m["polymarket"]["id"]) for m in two_kalshi["matched"]] == [("K1", "P1")]
    assert two_kalshi["unmatched_kalshi"] == 1 and two_kalshi["unmatched_polymarket"] == 0
    two_poly = pm.match_markets([_k(question, id_="K1")], [_p(question, id_="P1", end=later), _p(question, id_="P2")], now=NOW)
    assert [(m["kalshi"]["id"], m["polymarket"]["id"]) for m in two_poly["matched"]] == [("K1", "P2")]
    assert two_poly["unmatched_kalshi"] == 0 and two_poly["unmatched_polymarket"] == 1


def test_score_is_documented_as_ordinal_not_a_probability() -> None:
    assert "not a calibrated probability" in pm.__doc__
    assert "ordinal score, not a probability" in pm.score_pair.__doc__


def test_pairs_carry_quotes_and_flag_prices_that_are_not_live_two_sided_quotes() -> None:
    question = "Will Bitcoin hit $150k in 2026?"
    last_trade = dict(price_basis="last_trade", yes_bid=0.0, yes_ask=0.01, spread=None, last_price=0.01,
                      last_trade_time=None)
    wide = dict(price_basis="bid_ask_midpoint", yes_bid=0.30, yes_ask=0.50, spread=0.2, last_price=0.4,
                last_trade_time=None)
    (flagged,) = pm.match_markets([_k(question, prob=0.01, **last_trade)], [_p(question, prob=0.4, **wide)], now=NOW)["matched"]
    assert flagged["flags"] == [
        "kalshi price is not a two-sided quote midpoint (basis: last_trade)",
        "polymarket bid/ask spread 0.20 is wider than 0.10",
    ]
    assert flagged["comparable"] is False and flagged["price_gap"] == -0.39
    assert {k: flagged["kalshi"][k] for k in ("price_basis", "yes_bid", "yes_ask", "spread", "last_price")} == {
        "price_basis": "last_trade", "yes_bid": 0.0, "yes_ask": 0.01, "spread": None, "last_price": 0.01,
    }
    assert "last_trade_time" in flagged["kalshi"] and "last_trade_time" in flagged["polymarket"]
    live_k = dict(price_basis="bid_ask_midpoint", yes_bid=0.39, yes_ask=0.41, spread=0.02)
    live_p = dict(price_basis="bid_ask_midpoint", yes_bid=0.38, yes_ask=0.40, spread=0.02)
    (clean,) = pm.match_markets([_k(question, prob=0.4, **live_k)], [_p(question, prob=0.39, **live_p)], now=NOW)["matched"]
    assert clean["flags"] == [] and clean["comparable"] is True and clean["price_gap"] == 0.01


def test_upstream_text_is_capped() -> None:
    clipped = pm.clip_text("x" * 600)
    assert len(clipped) == pm.MAX_TEXT_CHARS == 500 and clipped.endswith("...")
    assert pm.clip_text(None) == "" and pm.clip_text(12) == "12" and pm.clip_text("short") == "short"
