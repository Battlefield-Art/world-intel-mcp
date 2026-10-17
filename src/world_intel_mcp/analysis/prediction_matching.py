"""Cross-venue prediction-market matching and forecast scoring.

Pure functions, no I/O. Used by ``tools/prediction_markets.py`` to put
the same event on Kalshi and Polymarket side by side and to score
settled markets against their pre-settlement prices.

Matching is deterministic: normalized title tokens, quantities, years,
end dates, negation and the outcome side must all agree before a pair
is scored, and a pair below ``DEFAULT_MIN_CONFIDENCE`` is not a match.
A wrong match is worse than no match, so every gate is a hard reject
and every reject carries its reason.

Normalized market shape (both venues) consumed here::

    {"venue": "kalshi" | "polymarket", "id": str, "question": str,
     "context": str,            # event title, folded into the token bag
     "yes_side": str,           # what YES pays out on ("" = the question itself)
     "outcomes": [str, str],    # Polymarket labels; Kalshi ["Yes", "No"]
     "end_time": iso8601 | None,
     "probability": float | None}   # YES implied probability, 0..1

Gaps / not covered:
- No synonym coverage beyond the small table below (month names, rate
  verbs, above/below). Pairs that word the same event differently
  ("Fed cuts" vs "FOMC lowers the target range") fall below the
  threshold and are reported unmatched, not matched.
- Team, ticker and person aliases ("KC" vs "Kansas City Chiefs") are
  not resolved, so most sports pairs stay unmatched.
- Resolution-rule equivalence is not checked: two markets can share a
  title and still settle on different sources or times. The reasons
  list says what was compared; it does not claim more.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone

DEFAULT_MIN_CONFIDENCE = 0.6

# Date tolerance: the larger of a floor and a fraction of the horizon,
# so a 2028 election may differ by weeks but a weekly market by days.
_DATE_TOLERANCE_FLOOR_DAYS = 3.0
_DATE_TOLERANCE_HORIZON_FRACTION = 0.15

_STOPWORDS = frozenset(
    """
    a an the will be is are was were to of in on at by for from with and or
    as it its this that these those than then there their his her who what
    which when where how does do did has have had any each after before
    following during until between over under next end this
    """.split()
) - {"over", "under", "after", "before"}

# Folded before comparison. Kept small on purpose: each entry is a
# claim that the two words mean the same thing in a market title.
_SYNONYMS = {
    "jan": "january", "feb": "february", "mar": "march", "apr": "april",
    "jun": "june", "jul": "july", "aug": "august", "sep": "september",
    "sept": "september", "oct": "october", "nov": "november", "dec": "december",
    "cuts": "cut", "decrease": "cut", "decreases": "cut", "lower": "cut",
    "lowers": "cut", "hike": "raise", "hikes": "raise", "increase": "raise",
    "increases": "raise", "raises": "raise", "over": "above", "more": "above",
    "exceed": "above", "exceeds": "above", "under": "below", "fewer": "below",
    "less": "below", "wins": "win", "won": "win", "winner": "win",
    "usa": "us", "u.s.": "us", "bp": "bps", "basis": "bps",
}

_MONTHS = frozenset(
    "january february march april may june july august september october "
    "november december".split()
)
_NEGATIONS = frozenset({"not", "no", "never", "without"})
_YES_NO = frozenset({"yes", "no"})

_NUM_RE = re.compile(r"^\$?(\d+(?:\.\d+)?)([kmb]?)$")
_TOKEN_RE = re.compile(r"\$?\d[\d,]*(?:\.\d+)?[kmb]?(?![a-z])|[a-z][a-z.&']*")


# ---------------------------------------------------------------------------
# Tokenization
# ---------------------------------------------------------------------------

def _canonical_number(raw: str) -> str | None:
    m = _NUM_RE.match(raw.replace(",", ""))
    if not m:
        return None
    value = float(m.group(1)) * {"": 1, "k": 1e3, "m": 1e6, "b": 1e9}[m.group(2)]
    return f"{value:g}" if value != int(value) else str(int(value))


def tokenize(text: str) -> list[str]:
    """Lowercase, fold synonyms, canonicalize numbers; stopwords kept.

    ``"25bps"`` -> ``["25", "bps"]``; ``"$100k"`` -> ``["100000"]``;
    ``"2.75%"`` -> ``["2.75"]``; ``"Fed's"`` -> ``["fed"]``.
    """
    text = (text or "").replace("’", "'").replace("≥", ">=").replace("≤", "<=")
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    # Comparators become words so a quantity keeps its direction:
    # ">25bps" and "25 bps" are different contracts.
    text = text.replace(">=", " atleast ").replace("<=", " atmost ")
    text = text.replace(">", " above ").replace("<", " below ")
    text = re.sub(r"(\d)\s*\+", r"\1 ormore ", text)
    text = re.sub(r"\bat least\b", " atleast ", text)
    text = re.sub(r"\bat most\b", " atmost ", text)
    text = re.sub(r"(\d)(bps|bp)\b", r"\1 \2", text)
    out: list[str] = []
    for raw in _TOKEN_RE.findall(text):
        if raw[0].isdigit() or raw[0] == "$":
            num = _canonical_number(raw)
            if num is not None:
                out.append(num)
            continue
        word = raw.strip(".'&")
        if word.endswith("'s"):
            word = word[:-2]
        word = word.replace("'", "")
        if not word:
            continue
        out.append(_SYNONYMS.get(word, word))
    return out


_COMPARATOR_WORDS = frozenset({"above", "below", "atleast", "atmost", "ormore"})


def _comparator(toks: list[str], i: int) -> str:
    """Direction attached to the number at ``toks[i]``: '>', '>=', '<',
    '<=' or '' (exact). Reads "above N", "more than N" (folded to
    above), "at least N", "N+" / "N or more", "N or less"."""
    j = i - 1
    while j >= 0 and toks[j] in ("than", "by", "the", "$"):
        j -= 1
    before = toks[j] if j >= 0 else ""
    after = toks[i + 1] if i + 1 < len(toks) else ""
    after2 = toks[i + 2] if i + 2 < len(toks) else ""
    if after == "ormore" or (after == "or" and after2 == "above"):
        return ">="
    if after == "or" and after2 == "below":
        return "<="
    return {"above": ">", "below": "<", "atleast": ">=", "atmost": "<="}.get(before, "")


def _is_year(tok: str) -> bool:
    return tok.isdigit() and len(tok) == 4 and 1900 <= int(tok) <= 2100


def _is_number(tok: str) -> bool:
    return bool(tok) and (tok[0].isdigit())


def text_features(text: str) -> dict:
    """Split a title into content words, quantities, years and flags.

    A 1-31 number right after a month name is a day of month and goes to
    ``days``, not ``quantities``: "Oct 28, 2026" must not block a pair
    with "October 2026" on the number gate (the end-date gate covers it).
    """
    toks = tokenize(text)
    words: set[str] = set()
    quantities: set[str] = set()
    years: set[str] = set()
    days: set[str] = set()
    negated = False
    for i, tok in enumerate(toks):
        prev = toks[i - 1] if i > 0 else ""
        if _is_number(tok):
            if _is_year(tok):
                years.add(tok)
            elif prev in _MONTHS and tok.isdigit() and 1 <= int(tok) <= 31:
                days.add(tok)
            else:
                quantities.add(_comparator(toks, i) + tok)
        elif tok in _NEGATIONS:
            negated = True
        elif tok in _COMPARATOR_WORDS:
            if tok in ("above", "below"):
                words.add(tok)
        elif tok not in _STOPWORDS:
            words.add(tok)
    return {
        "words": words,
        "quantities": quantities,
        "years": years,
        "days": days,
        "negated": negated,
    }


# ---------------------------------------------------------------------------
# Pair scoring
# ---------------------------------------------------------------------------

def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00").replace(" ", "T"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _side_alignment(kalshi: dict, k_side: set[str], poly: dict) -> tuple[str | None, str]:
    """Which Polymarket outcome the Kalshi YES side corresponds to.

    Returns ``("aligned" | "inverted" | None, reason)``. ``inverted``
    means the Kalshi YES pays out on Polymarket's second outcome, so the
    Polymarket probability is flipped before the gap is taken.
    """
    outcomes = [str(o) for o in (poly.get("outcomes") or [])]
    labels = [set(tokenize(o)) for o in outcomes]
    poly_question = text_features(poly.get("question", ""))["words"]
    if len(outcomes) == 2 and {o.lower() for o in outcomes} == _YES_NO:
        if k_side and not k_side <= poly_question:
            missing = sorted(k_side - poly_question)
            return None, f"Kalshi YES side {sorted(k_side)} not named in Polymarket question (missing {missing})"
        if k_side:
            return "aligned", f"Kalshi YES side {sorted(k_side)} named in Polymarket question"
        return "aligned", "both venues price YES on the question as worded"
    if len(outcomes) != 2:
        return None, f"Polymarket market has {len(outcomes)} outcomes, not a binary pair"
    side = k_side or text_features(kalshi.get("question", ""))["words"]
    hits = [bool(side & (lab - _STOPWORDS)) for lab in labels]
    if hits == [True, False]:
        return "aligned", f"Kalshi YES side matches Polymarket outcome {outcomes[0]!r}"
    if hits == [False, True]:
        return "inverted", f"Kalshi YES side matches Polymarket outcome {outcomes[1]!r} (second outcome; price flipped)"
    if hits == [True, True]:
        return None, f"Kalshi YES side matches both Polymarket outcomes {outcomes}"
    return None, f"Kalshi YES side {sorted(side)} matches neither Polymarket outcome {outcomes}"


def score_pair(kalshi: dict, poly: dict, now: datetime | None = None) -> dict:
    """Score one Kalshi/Polymarket pair.

    Returns ``{"confidence": float, "rejected": bool, "reasons": [...],
    "side": "aligned" | "inverted" | None}``. A rejected pair has
    confidence 0.0 and its first failing gate as the last reason.
    """
    now = now or datetime.now(timezone.utc)
    k_text = " ".join(
        s for s in (kalshi.get("question", ""), kalshi.get("context", ""), kalshi.get("yes_side", "")) if s
    )
    kf = text_features(k_text)
    pf = text_features(" ".join(s for s in (poly.get("question", ""), poly.get("context", "")) if s))
    k_side_words = text_features(kalshi.get("yes_side", ""))["words"]
    reasons: list[str] = []

    def reject(reason: str) -> dict:
        reasons.append(reason)
        return {"confidence": 0.0, "rejected": True, "reasons": reasons, "side": None}

    shared = kf["words"] & pf["words"]
    union = len(kf["words"]) + len(pf["words"])
    dice = (2 * len(shared) / union) if union else 0.0
    reasons.append(
        f"title word overlap {dice:.2f} (dice; {len(shared)} shared of "
        f"{len(kf['words'])} Kalshi / {len(pf['words'])} Polymarket words)"
    )

    if kf["quantities"] != pf["quantities"]:
        return reject(
            f"quantities differ: Kalshi {sorted(kf['quantities'])} vs Polymarket {sorted(pf['quantities'])}"
        )
    if kf["quantities"]:
        reasons.append(f"quantities agree {sorted(kf['quantities'])}")

    if kf["years"] and pf["years"] and not (kf["years"] & pf["years"]):
        return reject(f"years differ: Kalshi {sorted(kf['years'])} vs Polymarket {sorted(pf['years'])}")

    if kf["negated"] != pf["negated"]:
        return reject("negation differs (one title says not/no/never/without, the other does not)")

    k_end, p_end = _parse_time(kalshi.get("end_time")), _parse_time(poly.get("end_time"))
    date_factor = 1.0
    if k_end is None or p_end is None:
        return reject("end date missing on one venue; cannot confirm the same event window")
    gap_days = abs((k_end - p_end).total_seconds()) / 86400
    horizon = max(0.0, (max(k_end, p_end) - now).total_seconds() / 86400)
    tolerance = max(_DATE_TOLERANCE_FLOOR_DAYS, _DATE_TOLERANCE_HORIZON_FRACTION * horizon)
    if gap_days > tolerance:
        return reject(f"end dates {gap_days:.1f} days apart (tolerance {tolerance:.1f} days)")
    date_factor = 1.0 - 0.5 * (gap_days / tolerance)
    reasons.append(f"end dates {gap_days:.1f} days apart (tolerance {tolerance:.1f} days)")

    side, side_reason = _side_alignment(kalshi, k_side_words, poly)
    if side is None:
        return reject(side_reason)
    reasons.append(side_reason)

    confidence = round(dice * date_factor, 3)
    return {"confidence": confidence, "rejected": False, "reasons": reasons, "side": side}


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def _blocking_index(markets: list[dict]) -> tuple[dict[str, list[int]], int]:
    index: dict[str, list[int]] = {}
    for i, m in enumerate(markets):
        words = text_features(" ".join((m.get("question", ""), m.get("context", ""))))["words"]
        for w in words:
            index.setdefault(w, []).append(i)
    return index, len(markets)


def match_markets(
    kalshi: list[dict],
    polymarket: list[dict],
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    now: datetime | None = None,
    near_miss_limit: int = 10,
) -> dict:
    """One-to-one deterministic matching, greedy by confidence.

    Candidate pairs share at least two title words that appear in fewer
    than 20% of the Polymarket set (blocking keeps the pair count
    tractable; a pair with fewer shared words could not reach the
    threshold anyway). Returns matched pairs, near misses (the highest
    similarity pairs that a gate or the threshold rejected) and counts.
    """
    now = now or datetime.now(timezone.utc)
    index, n_poly = _blocking_index(polymarket)
    common_cut = max(5, int(0.2 * n_poly))
    scored: list[tuple[float, int, int, dict]] = []
    near: list[tuple[float, int, int, dict]] = []
    pairs_scored = 0
    for ki, km in enumerate(kalshi):
        words = text_features(
            " ".join((km.get("question", ""), km.get("context", ""), km.get("yes_side", "")))
        )["words"]
        counts: dict[int, int] = {}
        for w in words:
            posting = index.get(w)
            if not posting or len(posting) > common_cut:
                continue
            for pi in posting:
                counts[pi] = counts.get(pi, 0) + 1
        for pi, c in counts.items():
            if c < 2:
                continue
            pairs_scored += 1
            result = score_pair(km, polymarket[pi], now=now)
            if not result["rejected"] and result["confidence"] >= min_confidence:
                scored.append((result["confidence"], ki, pi, result))
            else:
                overlap = _overlap_of(result)
                if not result["rejected"]:
                    result["reasons"].append(
                        f"confidence {result['confidence']:.3f} below threshold {min_confidence}"
                    )
                near.append((overlap, ki, pi, result))

    scored.sort(key=lambda t: (-t[0], t[1], t[2]))
    used_k: set[int] = set()
    used_p: set[int] = set()
    matched: list[dict] = []
    for conf, ki, pi, result in scored:
        if ki in used_k or pi in used_p:
            continue
        used_k.add(ki)
        used_p.add(pi)
        matched.append(_pair_record(kalshi[ki], polymarket[pi], result))

    near.sort(key=lambda t: (-t[0], t[1], t[2]))
    near_misses = [
        {
            "kalshi": _brief(kalshi[ki]),
            "polymarket": _brief(polymarket[pi]),
            "title_overlap": round(ov, 3),
            "reasons": result["reasons"],
        }
        for ov, ki, pi, result in near[:near_miss_limit]
    ]
    return {
        "matched": matched,
        "near_misses": near_misses,
        "pairs_scored": pairs_scored,
        "unmatched_kalshi": len(kalshi) - len(used_k),
        "unmatched_polymarket": len(polymarket) - len(used_p),
        "min_confidence": min_confidence,
    }


def _overlap_of(result: dict) -> float:
    m = re.match(r"title word overlap ([0-9.]+)", result["reasons"][0])
    return float(m.group(1)) if m else 0.0


def _brief(m: dict) -> dict:
    return {
        "id": m.get("id"),
        "question": m.get("question"),
        "yes_side": m.get("yes_side") or None,
        "outcomes": m.get("outcomes"),
        "end_time": m.get("end_time"),
        "probability": m.get("probability"),
        "url": m.get("url"),
    }


def _pair_record(km: dict, pm: dict, result: dict) -> dict:
    k_prob = km.get("probability")
    p_raw = pm.get("probability")
    p_aligned = None
    if p_raw is not None:
        p_aligned = round(1.0 - p_raw, 4) if result["side"] == "inverted" else p_raw
    gap = round(k_prob - p_aligned, 4) if k_prob is not None and p_aligned is not None else None
    return {
        "match_confidence": result["confidence"],
        "reasons": result["reasons"],
        "side": result["side"],
        "kalshi": _brief(km),
        "polymarket": _brief(pm),
        "kalshi_yes_probability": k_prob,
        "polymarket_yes_probability": p_aligned,
        "price_gap": gap,
    }


# ---------------------------------------------------------------------------
# Forecast scoring
# ---------------------------------------------------------------------------

def brier_score(forecasts: list[tuple[float, int]]) -> float | None:
    """Mean squared error of probability forecasts against 0/1 outcomes.

    ``forecasts`` is ``[(p_yes, outcome)]`` with outcome 1 for YES.
    0.0 is perfect; always forecasting 0.5 scores 0.25. None for an
    empty sample (no score is not a score of zero).
    """
    if not forecasts:
        return None
    total = 0.0
    for p, outcome in forecasts:
        if outcome not in (0, 1):
            raise ValueError(f"outcome must be 0 or 1, got {outcome!r}")
        if not 0.0 <= p <= 1.0:
            raise ValueError(f"probability must be in [0, 1], got {p!r}")
        total += (p - outcome) ** 2
    return total / len(forecasts)


def price_at_offset(
    history: list[tuple[int, float]],
    reference_ts: int,
    offset_hours: float,
    max_staleness_hours: float,
) -> tuple[float | None, str]:
    """Last price at or before ``reference_ts - offset``.

    ``history`` is ``[(unix_ts, price)]`` in any order. Returns
    ``(price, reason)``; price is None when there is no point inside
    ``[target - max_staleness, target]`` (the market had not traded yet,
    or its last trade is too old to stand for the offset).
    """
    target = reference_ts - int(offset_hours * 3600)
    floor = target - int(max_staleness_hours * 3600)
    best: tuple[int, float] | None = None
    for ts, price in history:
        if price is None or ts > target:
            continue
        if best is None or ts > best[0]:
            best = (ts, price)
    if best is None:
        return None, "no price at or before the offset"
    if best[0] < floor:
        return None, f"last price {round((target - best[0]) / 3600, 1)}h before the offset (limit {max_staleness_hours}h)"
    return best[1], "ok"
