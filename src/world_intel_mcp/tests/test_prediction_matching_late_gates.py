"""Gates that reject a pair from the two titles alone (re-review round,
2026-10-17): days of the same month, months, clock times, quantity
units and kinds, quantity order, opposite wording, rate actions worded
as verbs, listed and distinguishing names, negation words, dash minus
signs, number words, and a plain four-digit amount after a comparator.

One rejection test per class, each with the reason the gate gives. The
same-event cases each class must keep matching sit beside them: a day
on one venue only, a clock time on one venue only, 25 bps against
0.25%, "raised" against "increased", "hold" outside a rate question,
"between" against at least/at most, and "Fed" against "Federal
Reserve" (the live pairs keep matching in test_prediction_cross.py).

Every expected reason and score below was computed with the module
before it was written here (scratch probes); "now" is pinned so the
date tolerance is fixed.

Gaps / not covered: wording beyond these cases. Known false negatives
(documented in the module): "before Jan 1, 2027" against "in 2026",
"first" against "1st", "keep rates" against "hold rates". Names are
read from capitalization and short lists, so an unlisted lower-case
qualifier still costs only Dice.
"""

from datetime import datetime, timezone

import pytest

from world_intel_mcp.analysis import prediction_matching as pm

NOW = datetime(2026, 10, 17, 16, 0, tzinfo=timezone.utc)
END = "2026-12-10T19:00:00Z"


def _k(question, yes="", end=END):
    return {"venue": "kalshi", "id": "K", "question": question, "context": "", "yes_side": yes,
            "outcomes": ["Yes", "No"], "end_time": end, "probability": 0.5}


def _p(question, end=END):
    return {"venue": "polymarket", "id": "P", "question": question, "context": "", "yes_side": "",
            "outcomes": ["Yes", "No"], "end_time": end, "probability": 0.5}


# ---------------------------------------------------------------------------
# Different events: rejected, with the gate that caught them
# ---------------------------------------------------------------------------

# case: (Kalshi question, Kalshi YES side, Kalshi end, Polymarket question, Polymarket end, reason)
REJECTED = {
    "days of the same month": (
        "Will Bitcoin be above $100,000 on Oct 20, 2026?", "", "2026-10-20T21:00:00Z",
        "Will Bitcoin be above $100,000 on October 21, 2026?", "2026-10-21T16:00:00Z",
        "days differ in october: Kalshi ['20'] vs Polymarket ['21']",
    ),
    "a deadline ladder": (
        "Will the government shutdown end before Jan 1, 2027?", "", "2027-01-01T00:00:00Z",
        "Will the government shutdown end before January 8, 2027?", "2027-01-08T00:00:00Z",
        "days differ in january: Kalshi ['1'] vs Polymarket ['8']",
    ),
    "a deadline ladder with the day on the Kalshi YES side": (
        "When will the government shutdown end?", "Before Jan 1, 2027", "2027-01-01T00:00:00Z",
        "Will the government shutdown end before January 8, 2027?", "2027-01-08T00:00:00Z",
        "days differ in january: Kalshi ['1'] vs Polymarket ['8']",
    ),
    "months": (
        "Will the US enter a recession by March 2028?", "", "2028-03-31T00:00:00Z",
        "Will the US enter a recession by April 2028?", "2028-04-30T00:00:00Z",
        "months differ: Kalshi ['march'] vs Polymarket ['april']",
    ),
    "clock times": (
        "Will Bitcoin be above $100,000 at 5pm ET on Oct 20, 2026?", "", "2026-10-20T21:00:00Z",
        "Will Bitcoin be above $100,000 at 12pm ET on Oct 20, 2026?", "2026-10-20T16:00:00Z",
        "clock times differ: Kalshi ['17:00'] vs Polymarket ['12:00']",
    ),
    "dollars vs percent": (
        "Will US GDP grow by $500 in 2026?", "", END, "Will US GDP grow by 500% in 2026?", END,
        "quantity units differ for 500: Kalshi ['$'] vs Polymarket ['%']",
    ),
    "basis points vs percent of the same number": (
        "Will the Fed cut rates by 50 bps in December 2026?", "", END,
        "Will the Fed cut rates by 50% in December 2026?", END,
        "quantity units differ for 50: Kalshi ['bps'] vs Polymarket ['%']",
    ),
    "to a level vs by a change": (
        "Will the Fed cut rates to 3.5% in December 2026?", "", END,
        "Will the Fed cut rates by 3.5% in December 2026?", END,
        "quantity kinds ('to' a level vs 'by' a change) differ for 3.5: Kalshi ['to'] vs Polymarket ['by']",
    ),
    "quantity order": (
        "Will Brazil beat Argentina 2-1 in the 2026 final?", "", END,
        "Will Brazil beat Argentina 1-2 in the 2026 final?", END,
        "quantities in a different order: Kalshi ['2', '1'] vs Polymarket ['1', '2']",
    ),
    "rise vs fall": (
        "Will oil prices rise in December 2026?", "", END, "Will oil prices fall in December 2026?", END,
        "opposite wording: Kalshi ['up'] vs Polymarket ['down']",
    ),
    "positive vs negative": (
        "Will Q4 GDP growth be positive in 2026?", "", END, "Will Q4 GDP growth be negative in 2026?", END,
        "opposite wording: Kalshi ['positive'] vs Polymarket ['negative']",
    ),
    "before vs after": (
        "Will Musk leave Tesla before the 2026 election?", "", END,
        "Will Musk leave Tesla after the 2026 election?", END,
        "opposite wording: Kalshi ['before'] vs Polymarket ['after']",
    ),
    "tighten vs ease": (
        "Will the ECB tighten policy in December 2026?", "", END, "Will the ECB ease policy in December 2026?", END,
        "actions differ: Kalshi ['raise'] vs Polymarket ['cut']",
    ),
    "boost vs reduce": (
        "Will the BoE boost rates in December 2026?", "", END, "Will the BoE reduce rates in December 2026?", END,
        "actions differ: Kalshi ['raise'] vs Polymarket ['cut']",
    ),
    "increase vs decrease": (
        "Will the Fed increase rates in December 2026?", "", END,
        "Will the Fed decrease rates in December 2026?", END,
        "actions differ: Kalshi ['raise'] vs Polymarket ['cut']",
    ),
    "chambers": (
        "Will Democrats win the House in 2026?", "", END, "Will Democrats win the Senate in 2026?", END,
        "a listed name on one venue only: Kalshi ['house'] vs Polymarket ['senate']",
    ),
    "countries": (
        "Will the US impose tariffs on China in 2026?", "", END,
        "Will the US impose tariffs on Canada in 2026?", END,
        "a listed name on one venue only: Kalshi ['china'] vs Polymarket ['canada']",
    ),
    "central banks named in full": (
        "Will the Bank of England cut rates in December 2026?", "", END,
        "Will the Bank of Japan cut rates in December 2026?", END,
        "distinguishing names differ: Kalshi ['boe'] vs Polymarket ['boj']",
    ),
    "people": (
        "Will Trump pardon Navarro in 2026?", "", END, "Will Trump pardon Bannon in 2026?", END,
        "distinguishing names differ: Kalshi ['navarro'] vs Polymarket ['bannon']",
    ),
    "Bitcoin vs Bitcoin Cash": (
        "Will Bitcoin hit $150k in 2026?", "", END, "Will Bitcoin Cash hit $150k in 2026?", END,
        "a listed name on one venue only: Kalshi [] vs Polymarket ['cash']",
    ),
    "headline vs core": (
        "Will CPI be above 3% in November 2026?", "", END, "Will core CPI be above 3% in November 2026?", END,
        "a listed name on one venue only: Kalshi [] vs Polymarket ['core']",
    ),
    "won't": (
        "Will Bitcoin hit $150k in 2026?", "", END, "Bitcoin won't hit $150k in 2026?", END,
        "negation differs (one title says not/no/never/without, the other does not)",
    ),
    "fail to": (
        "Will Congress pass the budget in 2026?", "", END, "Will Congress fail to pass the budget in 2026?", END,
        "negation differs (one title says not/no/never/without, the other does not)",
    ),
    "an en dash minus sign": (
        "Will Q4 GDP growth be below –0.5%?", "", END, "Will Q4 GDP growth be below 0.5%?", END,
        "quantities differ: Kalshi ['4', '<-0.5'] vs Polymarket ['4', '<0.5']",
    ),
    "number words": (
        "Will the Fed cut rates three times in 2026?", "", END, "Will the Fed cut rates four times in 2026?", END,
        "quantities differ: Kalshi ['3'] vs Polymarket ['4']",
    ),
    "a plain four-digit amount after a comparator (above)": (
        "Will the S&P 500 be above 2050 in 2026?", "", END, "Will the S&P 500 be above 2100 in 2026?", END,
        "quantities differ: Kalshi ['500', '>2050'] vs Polymarket ['500', '>2100']",
    ),
    "a plain four-digit amount after a comparator (at least)": (
        "Will gold be at least 2000 in 2026?", "", END, "Will gold be at least 1950 in 2026?", END,
        "quantities differ: Kalshi ['>=2000'] vs Polymarket ['>=1950']",
    ),
}


@pytest.mark.parametrize("case", sorted(REJECTED))
def test_titles_naming_different_events_are_rejected(case: str) -> None:
    kalshi_q, yes, kalshi_end, poly_q, poly_end, reason = REJECTED[case]
    result = pm.score_pair(_k(kalshi_q, yes, kalshi_end), _p(poly_q, poly_end), now=NOW)
    assert result["rejected"] is True and result["confidence"] == 0.0
    assert result["reasons"][-1] == reason


def test_a_day_rejection_also_keeps_the_pair_out_of_matches() -> None:
    out = pm.match_markets(
        [_k("Will Bitcoin be above $100,000 on Oct 20, 2026?", end="2026-10-20T21:00:00Z")],
        [_p("Will Bitcoin be above $100,000 on October 21, 2026?", end="2026-10-21T16:00:00Z")],
        now=NOW,
    )
    assert out["matched"] == [] and out["unmatched_kalshi"] == 1 and out["unmatched_polymarket"] == 1


# ---------------------------------------------------------------------------
# Same events: still matched
# ---------------------------------------------------------------------------

# case: (Kalshi question, Kalshi end, Polymarket question, Polymarket end, confidence, a reason it carries)
MATCHED = {
    "a day on one venue only": (
        "Will the Fed cut rates on Oct 28, 2026?", "2026-10-28T18:00:00Z",
        "Will the Fed cut rates in October 2026?", "2026-10-28T18:00:00Z",
        1.0, "actions agree ['cut']",
    ),
    "a clock time on one venue only": (
        "Will Bitcoin be above $100,000 at 5pm ET on Oct 20, 2026?", "2026-10-20T21:00:00Z",
        "Will Bitcoin be above $100,000 on Oct 20, 2026?", "2026-10-20T21:00:00Z",
        1.0, "quantities agree ['>100000']",
    ),
    "25 bps vs 0.25%": (
        "Will the Fed cut rates by 25 bps in December 2026?", END,
        "Will the Fed cut rates by 0.25% in December 2026?", END,
        0.889, "quantities agree once bps are read as percent: Kalshi ['25'] vs Polymarket ['0.25']",
    ),
    "raised vs increased": (
        "Will the Fed have raised rates in December 2026?", END,
        "Will the Fed have increased rates in December 2026?", END,
        1.0, "actions agree ['raise']",
    ),
    "hold outside a rate question is not an action": (
        "Will Iran hold a presidential election in 2026?", END,
        "Will Iran have a presidential election in 2026?", END,
        0.857, "both venues price YES on the question as worded",
    ),
    "between vs at least and at most": (
        "Will CPI be between 3% and 4% in November 2026?", END,
        "Will CPI be at least 3% and at most 4% in November 2026?", END,
        1.0, "both venues price YES on the question as worded",
    ),
    "Fed vs Federal Reserve": (
        "Will the Fed hike rates in December 2026?", END,
        "Will the Federal Reserve hike rates in December 2026?", END,
        0.667, "actions agree ['raise']",
    ),
}


@pytest.mark.parametrize("case", sorted(MATCHED))
def test_same_events_still_match(case: str) -> None:
    kalshi_q, kalshi_end, poly_q, poly_end, confidence, reason = MATCHED[case]
    result = pm.score_pair(_k(kalshi_q, end=kalshi_end), _p(poly_q, end=poly_end), now=NOW)
    assert result["rejected"] is False and result["confidence"] == confidence
    assert confidence >= pm.DEFAULT_MIN_CONFIDENCE
    assert reason in result["reasons"]


def test_hold_in_a_rate_question_is_still_an_action() -> None:
    result = pm.score_pair(
        _k("Will the Fed hold rates in December 2026?"), _p("Will the Fed raise rates in December 2026?"), now=NOW
    )
    assert result["reasons"][-1] == "actions differ: Kalshi ['hold'] vs Polymarket ['raise']"


def test_clock_times_read_twelve_and_twenty_four_hour_forms() -> None:
    assert pm.clock_times("5pm ET on Oct 20") == {"17:00"}
    assert pm.clock_times("at 17:00 UTC") == {"17:00"}
    assert pm.clock_times("12:30 pm") == {"12:30"}
    assert pm.clock_times("noon") == set()


# ---------------------------------------------------------------------------
# Candidate blocking: the total order every market shares
# ---------------------------------------------------------------------------

def test_prefix_breaks_rarity_ties_on_the_word_itself() -> None:
    """Prefix filtering finds every pair only if all markets order their
    words the same way; with equal document frequencies that order is
    alphabetical, never the set's iteration order."""
    words = set("alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima".split())
    tied = {w: 3 for w in words}
    # 12 words at 0.75: need = ceil(0.75 * 12 / 1.25) = 8, so the prefix keeps 12 - 8 + 1 = 5.
    assert pm._prefix(words, tied, 0.75) == ["alpha", "bravo", "charlie", "delta", "echo"]
    rarer = dict(tied, lima=1, alpha=5)
    assert pm._prefix(words, rarer, 0.75) == ["lima", "bravo", "charlie", "delta", "echo"]
