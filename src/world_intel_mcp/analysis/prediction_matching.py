"""Cross-venue prediction-market matching and forecast scoring.

Pure functions, no I/O. Used by ``sources/prediction_cross.py`` to put
the same event on Kalshi and Polymarket side by side and to score
settled markets against their pre-settlement prices.

Matching is deterministic: normalized title tokens, quantities (value,
unit, "to" vs "by" and order), years, months, days of month, clock
times, negation, rate actions (cut / raise / hold), directions (above /
below), opposite words (rise / fall, positive / negative, before /
after, win / lose, approve / reject), distinguishing names, end dates
and the outcome side must all agree before a pair is scored, and a
pair below ``DEFAULT_MIN_CONFIDENCE`` is not a match. A wrong match is
worse than no match, so every gate is a hard reject and every reject
carries its reason. A gate decides from the two markets alone, never
from what else either venue lists.

``match_confidence`` (``confidence`` in ``score_pair``) is an ordinal
similarity score for ranking candidate pairs: title-word Dice overlap
times a date factor. It is not a calibrated probability that two
markets are the same event.

Normalized market shape (both venues) consumed here::

    {"venue": "kalshi" | "polymarket", "id": str, "question": str,
     "context": str,            # event title, folded into the token bag
     "yes_side": str,           # what YES pays out on ("" = the question itself)
     "outcomes": [str, str],    # Polymarket labels; Kalshi ["Yes", "No"]
     "end_time": iso8601 | None,
     "probability": float | None,   # YES implied probability, 0..1
     "price_basis": str, "spread": float | None}   # quote fields (optional)

Gaps / not covered:
- No synonym coverage beyond the small tables below (month names, rate
  verbs and their inflections, above/below, a few central-bank and
  country names). Pairs that word the same event differently ("Fed
  cuts" vs "FOMC lowers the target range") fall below the threshold
  and are reported unmatched, not matched.
- Team, ticker and person aliases ("KC" vs "Kansas City Chiefs") are
  not resolved, so most sports pairs stay unmatched.
- Resolution-rule equivalence is not checked: two markets can share a
  title and still settle on different sources or times. The reasons
  list says what was compared; it does not claim more.
- Ordinals written as words ("first", "second") are plain words, not
  quantities, and a number glued to a unit ("100mm") loses the unit.
  Number words are read from "two" to "twelve"; "one" stays a word.
- A fractional threshold worded strict on one venue and inclusive on
  the other ("above 4.5%" vs "4.5% or higher") is matched with a
  boundary flag; an integer one is not, because "more than 50" means
  "51 or more".
- Distinguishing names are read from capitalization (plus a short
  list of chambers, parties, compass points, qualifiers and
  countries), not from a name database: an all-lowercase title names
  nothing, and a title-case one names every capitalized word, which
  only rejects more pairs. A name on one venue only (an insertion) is
  a Dice penalty, except for the listed qualifiers ("core CPI" vs
  "CPI", "Bitcoin Cash" vs "Bitcoin"), which reject.
- The opposite-word and month/day/clock gates reject only when both
  titles state one; a title that leaves it out ("CPI above 0.3%" vs
  "CPI rise more than 0.3%", "October 2026" vs "Oct 28, 2026") is
  left to the other gates and the end-date window. A day written
  before its month ("20 October") is not read as a day.
- A deadline worded as a date on one venue and as a calendar year on
  the other ("before Jan 1, 2027" vs "in 2026") is rejected on the
  year gate (a false negative, not a wrong match).
"""

from __future__ import annotations

import math
import re
import unicodedata
from datetime import datetime, timezone

DEFAULT_MIN_CONFIDENCE = 0.6
# Pairs whose title overlap could reach the lower of min_confidence and
# this floor are scored, so near misses include pairs just under a high
# threshold.
NEAR_MISS_FLOOR = 0.5
# A pair is ranked by its price gap only when both prices are two-sided
# quote midpoints whose spread is at most this wide (price units, 0..1).
MAX_COMPARABLE_SPREAD = 0.10
# Upstream titles longer than this are cut (the longest seen live on
# 2026-10-17 was a 313-character Kalshi market title).
MAX_TEXT_CHARS = 500

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
    "increases": "raise", "raises": "raise", "holds": "hold", "maintain": "hold",
    "maintains": "hold", "unchanged": "hold", "pause": "hold", "pauses": "hold",
    "over": "above", "more": "above", "exceed": "above", "exceeds": "above",
    "higher": "above", "greater": "above", "under": "below", "fewer": "below",
    "less": "below", "wins": "win", "won": "win", "winner": "win",
    # "U.S." tokenizes to "u.s" (edge dots are stripped).
    "usa": "us", "u.s": "us", "u.s.a": "us", "bp": "bps", "basis": "bps",
    # Inflections of the rate verbs, and verbs that name the same move.
    "cutting": "cut", "lowered": "cut", "lowering": "cut", "decreased": "cut", "decreasing": "cut",
    "reduce": "cut", "reduces": "cut", "reduced": "cut", "reducing": "cut", "trim": "cut",
    "trims": "cut", "trimmed": "cut", "trimming": "cut", "ease": "cut",
    "eases": "cut", "eased": "cut", "easing": "cut", "raised": "raise", "raising": "raise",
    "hiked": "raise", "hiking": "raise", "increased": "raise", "increasing": "raise",
    "boost": "raise", "boosts": "raise", "boosted": "raise", "boosting": "raise",
    "tighten": "raise", "tightens": "raise", "tightened": "raise", "tightening": "raise",
    "held": "hold", "holding": "hold", "paused": "hold", "maintained": "hold",
    # Inflections of the opposite-word pairs (_POLES).
    "rises": "rise", "rising": "rise", "rose": "rise", "risen": "rise", "falls": "fall",
    "falling": "fall", "fell": "fall", "fallen": "fall", "drops": "drop", "dropped": "drop",
    "dropping": "drop", "loses": "lose", "losing": "lose", "lost": "lose", "loser": "lose",
    "approves": "approve", "approved": "approve", "approval": "approve", "rejects": "reject",
    "rejected": "reject", "rejection": "reject",
}

_MONTHS = frozenset(
    "january february march april may june july august september october "
    "november december".split()
)
# "May" is a month only next to a date or after one of these words; "who
# may win" is a verb.
_MAY_BEFORE = frozenset("in by before after of until since end early mid late from through".split())
_NEGATIONS = frozenset({
    "not", "no", "never", "without", "wont", "cannot", "cant", "dont", "doesnt", "didnt",
    "isnt", "arent", "wasnt", "fail", "fails", "failed", "neither", "nor", "none", "nobody",
})
_YES_NO = frozenset({"yes", "no"})
# Hard gates: two titles must name the same rate action and the same
# direction (after synonym folding), or the pair is rejected.
_ACTIONS = frozenset({"cut", "raise", "hold"})
_DIRECTIONS = {"above": "above", "below": "below", "atleast": "above", "atmost": "below", "ormore": "above"}
# "hold" is the rate action only next to one of these (after synonym and
# name folding); "hold an election" is not a rate decision.
_RATE_CONTEXT = frozenset(
    "rate rates interest bps fed ecb boe boj boc rba snb pboc monetary funds".split()
)
# Opposite words: when both titles use a word on the same axis, the poles
# must agree. A title that uses none is left to the other gates ("after
# the October meeting" vs "at their October meeting" is one event).
_POLES = {
    "rise": ("move", "up"), "climb": ("move", "up"), "climbs": ("move", "up"), "reach": ("move", "up"),
    "reaches": ("move", "up"), "hit": ("move", "up"), "hits": ("move", "up"), "surpass": ("move", "up"),
    "surpasses": ("move", "up"), "fall": ("move", "down"), "drop": ("move", "down"), "dip": ("move", "down"),
    "dips": ("move", "down"), "decline": ("move", "down"), "declines": ("move", "down"),
    "sink": ("move", "down"), "sinks": ("move", "down"), "plunge": ("move", "down"),
    "positive": ("sign", "positive"), "negative": ("sign", "negative"),
    "before": ("time", "before"), "after": ("time", "after"),
    "win": ("result", "win"), "lose": ("result", "lose"),
    "approve": ("approval", "approve"), "reject": ("approval", "reject"),
}
_SCALE_WORDS = {"thousand": 1e3, "million": 1e6, "billion": 1e9, "trillion": 1e12}
_MULTIPLIERS = {"": 1.0, "k": 1e3, "m": 1e6, "mn": 1e6, "b": 1e9, "bn": 1e9, "t": 1e12, "tn": 1e12}
_NUMBER_WORDS = {
    "once": 1, "twice": 2, "thrice": 3, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9,
    "ten": 10, "eleven": 11, "twelve": 12,
}

_NUM_RE = re.compile(r"^(-?)\$?(\d+(?:\.\d+)?)(k|mn|m|bn|b|tn|t)?$")
_ORDINAL_RE = re.compile(r"^(\d+)(st|nd|rd|th)$")
# Ordinal | number (a leading minus only when it is not a hyphen inside
# a word or range; never starting inside a digit run, so "100mm" cannot
# leave a partial "10") | word | percent sign (marks the number before it).
_TOKEN_RE = re.compile(
    r"(?<![\d.,])\d+(?:st|nd|rd|th)(?![a-z0-9])"
    r"|(?:(?<![a-z0-9.,])-)?\$?(?<![\d.,])\d(?:[\d,]*\d)?(?:\.\d+)?(?:(?:k|mn|m|bn|b|tn|t)(?![a-z]))?"
    r"|[a-z][a-z.&']*"
    r"|%"
)
_QUANTITY_RE = re.compile(r"^(>=|<=|>|<)?(.+)$")
# Dash characters that stand for a minus sign before a digit (U+2212 is
# mapped in _prepare too).
_DASH_RE = re.compile("[‐-―﹣－](?=\\s*\\d)")
_TIME_ZONES = r"(?:\s*\b(?:et|edt|est|ct|cdt|cst|mt|mdt|mst|pt|pdt|pst|utc|gmt)\b)?"
# Clock times: "11:59 pm et", "5pm", "17:00". Read into their own gate
# (both titles state one: they must agree) and kept out of quantities.
_CLOCK_12_RE = re.compile(r"(?<![\d.:])(\d{1,2})(?::([0-5]\d))?\s*([ap])\.?m\.?(?![a-z])" + _TIME_ZONES)
_CLOCK_24_RE = re.compile(r"(?<![\d.:])([01]?\d|2[0-3]):([0-5]\d)(?![\d:])" + _TIME_ZONES)
# "between 2.5% and 2.7%" is the two thresholds it names.
_BETWEEN_RE = re.compile(r"\bbetween\s+(\$?-?\d[\d.,]*\s*%?)\s*(?:and|to|-)\s*(?=\$?-?\d)")

# Distinguishing names. A capitalized word (or one in _LISTED_NAMES) that
# one title names and the other's text lacks tells two events apart when
# both titles have one ("House" vs "Senate", "Navarro" vs "Bannon"); a
# listed qualifier rejects even when only one title has it ("core CPI"
# vs "CPI"). Phrases fold to one name first so "Federal Reserve" is
# "Fed"; these folds feed only the name gate, not the overlap score.
_NAME_PHRASES = [
    (re.compile(r"\breserve bank of australia\b", re.I), "RBA"),
    (re.compile(r"\bpeople'?s bank of china\b", re.I), "PBoC"),
    (re.compile(r"\bfederal reserve\b", re.I), "Fed"),
    (re.compile(r"\bbank of england\b", re.I), "BoE"),
    (re.compile(r"\bbank of japan\b", re.I), "BoJ"),
    (re.compile(r"\bbank of canada\b", re.I), "BoC"),
    (re.compile(r"\beuropean central bank\b", re.I), "ECB"),
    (re.compile(r"\bunited states\b", re.I), "US"),
    (re.compile(r"\b(?:united kingdom|great britain)\b", re.I), "UK"),
]
_NAME_ALIASES = {
    "fomc": "fed", "democratic": "democrat", "democrats": "democrat", "dem": "democrat",
    "dems": "democrat", "republicans": "republican", "gop": "republican", "btc": "bitcoin",
    "britain": "uk", "british": "uk", "american": "us", "americans": "us", "chinese": "china",
    "japanese": "japan", "german": "germany", "french": "france", "russian": "russia",
    "ukrainian": "ukraine", "israeli": "israel", "iranian": "iran", "canadian": "canada",
    "mexican": "mexico", "brazilian": "brazil", "indian": "india", "korean": "korea",
    "taiwanese": "taiwan", "european": "europe", "eurozone": "europe", "turkish": "turkey",
}
_COUNTRIES = frozenset(
    """
    uk china japan germany france italy spain canada mexico brazil india russia ukraine israel
    iran iraq syria gaza lebanon yemen taiwan korea australia england scotland ireland europe eu
    argentina turkey saudi venezuela poland netherlands switzerland sweden norway egypt nigeria
    pakistan indonesia vietnam philippines colombia chile peru greece portugal belgium austria
    denmark finland hungary romania qatar uae cuba belarus
    """.split()
)
_LISTED_NAMES = _COUNTRIES | frozenset(
    "house senate democrat republican labour conservative tory north south east west core "
    "headline cash classic primary runoff".split()
)
_NOT_NAMES = frozenset(
    "who what which when where how why can could should would may might must yes no vs versus "
    "monday tuesday wednesday thursday friday saturday sunday today tomorrow tonight".split()
)


def clip_text(value: object, limit: int = MAX_TEXT_CHARS) -> str:
    """Upstream text as a string of at most ``limit`` characters."""
    text = value if isinstance(value, str) else ("" if value is None else str(value))
    return text if len(text) <= limit else text[: limit - 3] + "..."


# ---------------------------------------------------------------------------
# Tokenization
# ---------------------------------------------------------------------------

def _canonical_number(raw: str, scale: float = 1.0) -> str | None:
    m = _NUM_RE.match(raw.replace(",", ""))
    if not m:
        return None
    value = float(m.group(2)) * _MULTIPLIERS[m.group(3) or ""] * scale
    if m.group(1):
        value = -value
    return f"{value:.12g}" if value != int(value) else str(int(value))


def _ascii(text: str) -> str:
    """Typographic signs folded, dashes before a digit made minus signs,
    then ASCII; case kept."""
    text = (text or "").replace("’", "'").replace("≥", ">=").replace("≤", "<=").replace("−", "-")
    text = _DASH_RE.sub("-", text)
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()


def _clock_minutes(hour: str, minute: str | None, meridiem: str | None) -> str | None:
    h, m = int(hour), int(minute or 0)
    if meridiem:
        if not 1 <= h <= 12:
            return None
        h = h % 12 + (12 if meridiem == "p" else 0)
    return f"{h:02d}:{m:02d}"


def clock_times(text: str) -> set[str]:
    """Clock times a title states, as 24-hour "HH:MM" ("11:59 PM ET" ->
    "23:59", "5pm" -> "17:00")."""
    low = _ascii(text).lower()
    found = {_clock_minutes(*m.groups()) for m in _CLOCK_12_RE.finditer(low)}
    found |= {_clock_minutes(m.group(1), m.group(2), None) for m in _CLOCK_24_RE.finditer(low)}
    return {t for t in found if t is not None}


def _prepare(text: str) -> str:
    text = _ascii(text).lower()
    # Clock times are read by clock_times(); here they would leave their
    # hour and minute behind as quantities ("11:59 PM" as 11 and 59).
    text = _CLOCK_12_RE.sub(" ", text)
    text = _CLOCK_24_RE.sub(" ", text)
    text = _BETWEEN_RE.sub(r" atleast \1 and atmost ", text)
    text = re.sub(
        r"\b(" + "|".join(_NUMBER_WORDS) + r")\b", lambda m: str(_NUMBER_WORDS[m.group(1)]), text
    )
    text = re.sub(r"(\d)\s*%", r"\1 %", text)
    text = re.sub(r"\b(?:negative|minus)\s+(?=\d)", " -", text)
    # Comparators become words so a quantity keeps its direction:
    # ">25bps" and "25 bps" are different contracts.
    text = text.replace(">=", " atleast ").replace("<=", " atmost ")
    text = text.replace(">", " above ").replace("<", " below ")
    text = re.sub(r"(\d)\s*\+", r"\1 ormore ", text)
    text = re.sub(r"\bat least\b", " atleast ", text)
    text = re.sub(r"\bat most\b", " atmost ", text)
    # "lower" alone is the rate verb (folded to "cut"); before "than" or
    # after "or" it is a comparator.
    text = re.sub(r"\blower\s+than\b", " below ", text)
    text = re.sub(r"\bor\s+lower\b", " or below ", text)
    # "No change" names the hold outcome; it is not a negation.
    text = re.sub(r"\bno\s+changes?\b", " hold ", text)
    return re.sub(r"(\d)(bps|bp)\b", r"\1 \2", text)


def _fold_word(raw: str) -> str:
    """A lowercase word token as compared: edge punctuation, possessive
    and apostrophes dropped, synonyms folded ("" when nothing is left)."""
    word = raw.strip(".'&")
    if word.endswith("'s"):
        word = word[:-2]
    word = word.replace("'", "")
    return _SYNONYMS.get(word, word) if word else ""


def _scan(text: str) -> list[tuple[str, str]]:
    """``(token, kind)`` pairs; kind is word, number, money, percent, year
    or ordinal.

    ``year`` is only the shape (four plain digits, 1900-2100); whether it
    is a year or an amount depends on what precedes it (text_features).
    A "%" or "percent" after a number makes it ``percent`` and is not a
    token itself.
    """
    raws = _TOKEN_RE.findall(_prepare(text))
    out: list[tuple[str, str]] = []
    i = 0
    while i < len(raws):
        raw = raws[i]
        i += 1
        if raw == "%" or (raw in ("percent", "pct") and out and out[-1][1] in ("number", "year")):
            if out and out[-1][1] in ("number", "year"):
                out[-1] = (out[-1][0], "percent")
            continue
        ordinal = _ORDINAL_RE.match(raw)
        if ordinal:
            out.append((f"{int(ordinal.group(1))}{ordinal.group(2)}", "ordinal"))
            continue
        if raw[0].isdigit() or raw[0] in "$-":
            scale = 1.0
            if i < len(raws) and raws[i] in _SCALE_WORDS and raw[-1].isdigit():
                scale = _SCALE_WORDS[raws[i]]  # "$100 million" is "$100m"
                i += 1
            num = _canonical_number(raw, scale)
            if num is None:
                continue
            if "$" in raw:
                kind = "money"
            elif scale == 1.0 and raw.isdigit() and len(raw) == 4 and 1900 <= int(raw) <= 2100:
                kind = "year"
            else:
                kind = "number"
            out.append((num, kind))
            continue
        word = _fold_word(raw)
        if word:
            out.append((word, "word"))
    return out


def tokenize(text: str) -> list[str]:
    """Lowercase, fold synonyms, canonicalize numbers; stopwords kept.

    ``"25bps"`` -> ``["25", "bps"]``; ``"$100k"`` and ``"$100 thousand"``
    -> ``["100000"]``; ``"2.75%"`` -> ``["2.75"]``; ``"-0.5%"`` ->
    ``["-0.5"]``; ``"1st"`` -> ``["1st"]``; ``"Fed's"`` -> ``["fed"]``.
    """
    return [tok for tok, _ in _scan(text)]


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


def _day_of_month(tok: str, kind: str) -> str | None:
    digits = tok[:-2] if kind == "ordinal" else tok
    if kind in ("number", "ordinal") and digits.isdigit() and 1 <= int(digits) <= 31:
        return str(int(digits))
    return None


def _is_month(toks: list[str], kinds: list[str], i: int) -> bool:
    if toks[i] != "may":
        return toks[i] in _MONTHS
    after = kinds[i + 1] if i + 1 < len(kinds) else ""
    return after in ("number", "year", "ordinal") or (i > 0 and toks[i - 1] in _MAY_BEFORE)


def _mode(toks: list[str], i: int) -> str | None:
    """"to" (a level) or "by" (a change) before the number at ``toks[i]``."""
    j = i - 1
    while j >= 0 and toks[j] in ("than", "the", "$", "above", "below", "atleast", "atmost"):
        j -= 1
    return toks[j] if j >= 0 and toks[j] in ("to", "by") else None


def _typed_quantity(toks: list[str], kinds: list[str], i: int, comparator: str) -> dict:
    """One quantity with its unit ("$", "%" with bps read as hundredths
    of a percent, or None), its "to"/"by" mode and the key the gates
    compare (comparator plus the value in that unit)."""
    tok, kind = toks[i], kinds[i]
    unit = {"money": "$", "percent": "%"}.get(kind)
    unit_raw = unit
    value = tok
    if i + 1 < len(toks) and toks[i + 1] == "bps" and kind == "number":
        unit, unit_raw = "%", "bps"
        value = _canonical_number(str(float(tok) / 100)) or tok
    return {
        "raw": comparator + tok,
        "key": comparator + value,
        "unit": unit,
        "unit_raw": unit_raw,
        "mode": _mode(toks, i),
        # Strictness matters for whole numbers ("more than 50" is 51 or
        # more), not for fractions; bps count as whole numbers.
        "whole": kind != "ordinal" and "." not in tok,
    }


def text_features(text: str) -> dict:
    """Split a title into content words, quantities, years and flags.

    A 1-31 number (or ordinal) right after a month name is a day of
    month and goes to ``days`` (and ``dated``, by month), not
    ``quantities``: "Oct 28, 2026" must not block a pair with "October
    2026" on the number gate; the day gate compares days only when both
    titles state one for the same month. A four-digit 1900-2100 number
    is a year unless a dollar sign, a thousands comma, a percent sign or
    a comparator makes it an amount: "above $2,000" is the quantity
    ``>2000``. ``actions``, ``directions`` and ``poles`` are the rate
    verbs, above/below words and opposite words the title uses, after
    synonym folding. ``typed`` carries each quantity's unit and mode,
    ``order`` the quantity keys in order of first appearance, ``times``
    the clock times.
    """
    scanned = _scan(text)
    toks = [tok for tok, _ in scanned]
    kinds = [kind for _, kind in scanned]
    words: set[str] = set()
    quantities: set[str] = set()
    typed: list[dict] = []
    years: set[str] = set()
    days: set[str] = set()
    months: set[str] = set()
    dated: dict[str, set[str]] = {}
    actions: set[str] = set()
    directions: set[str] = set()
    poles: dict[str, set[str]] = {}
    negated = False
    for i, (tok, kind) in enumerate(scanned):
        if kind == "word":
            if tok in _ACTIONS:
                actions.add(tok)
            if tok in _DIRECTIONS:
                directions.add(_DIRECTIONS[tok])
            if tok in _POLES:
                axis, pole = _POLES[tok]
                poles.setdefault(axis, set()).add(pole)
            if tok in _MONTHS and _is_month(toks, kinds, i):
                months.add(tok)
            if tok in _NEGATIONS:
                negated = True
            elif tok in _COMPARATOR_WORDS:
                if tok in ("above", "below"):
                    words.add(tok)
            elif tok not in _STOPWORDS:
                words.add(tok)
            continue
        comparator = _comparator(toks, i)
        day = _day_of_month(tok, kind)
        if kind == "year" and not comparator:
            years.add(tok)
        elif i > 0 and toks[i - 1] in _MONTHS and _is_month(toks, kinds, i - 1) and day is not None:
            days.add(day)
            dated.setdefault(toks[i - 1], set()).add(day)
        else:
            quantities.add(comparator + tok)
            typed.append(_typed_quantity(toks, kinds, i, comparator))
    order: list[str] = []
    for q in typed:
        if q["key"] not in order:
            order.append(q["key"])
    return {
        "words": words,
        "quantities": quantities,
        "typed": typed,
        "order": order,
        "years": years,
        "days": days,
        "months": months,
        "dated": dated,
        "times": clock_times(text),
        "negated": negated,
        "actions": actions,
        "directions": directions,
        "poles": poles,
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


def _text(m: dict, *keys: str) -> str:
    return " ".join(s for s in (m.get(k) for k in keys) if isinstance(s, str) and s)


def _alias_phrases(text: str) -> str:
    for pattern, alias in _NAME_PHRASES:
        text = pattern.sub(f" {alias} ", text)
    return text


def _name_word(raw: str) -> str:
    word = _fold_word(raw.lower())
    return _NAME_ALIASES.get(word, word)


def _names(*texts: str) -> set[str]:
    """Distinguishing names in the texts: capitalized words and listed
    names, after phrase and name folding. Words that only shape the
    question (stopwords, months, rate verbs, directions, opposite words,
    negations, yes/no, number words) are never names."""
    names: set[str] = set()
    for text in texts:
        for match in re.finditer(r"[A-Za-z][A-Za-z.&']*", _alias_phrases(_ascii(text))):
            raw = match.group(0)
            word = _name_word(raw)
            if (
                len(word) < 2 or word in _STOPWORDS or word in _NOT_NAMES or word in _MONTHS
                or word in _ACTIONS or word in _DIRECTIONS or word in _COMPARATOR_WORDS
                or word in _POLES or word in _NEGATIONS or word in _NUMBER_WORDS
            ):
                continue
            if word in _LISTED_NAMES or raw[0].isupper():
                names.add(word)
    return names


def _present(text: str) -> set[str]:
    """Every word of ``text`` after phrase and name folding: what a name
    on the other venue is looked up in."""
    return {_NAME_ALIASES.get(tok, tok) for tok, kind in _scan(_alias_phrases(_ascii(text))) if kind == "word"}


def _market_features(m: dict, venue: str) -> dict:
    """Everything the gates read from one market, computed once.

    ``bag`` (question, event title and, on Kalshi, the YES side) feeds
    the overlap score, quantities, years, months, days, clock times and
    negation. Actions, directions, opposite words and names come from
    the contract's own wording (question plus YES side), not the event
    title, which can list every alternative; a name is looked up in the
    other venue's whole bag.
    """
    if venue == "kalshi":
        bag_text = _text(m, "question", "context", "yes_side")
        gate_texts = [m.get("question") or "", m.get("yes_side") or ""]
    else:
        bag_text = _text(m, "question", "context")
        gate_texts = [m.get("question") or ""]
    bag = text_features(bag_text)
    gate = text_features(" ".join(t for t in gate_texts if isinstance(t, str)))
    present = _present(bag_text)
    actions = set(gate["actions"])
    if "hold" in actions and not present & _RATE_CONTEXT:
        actions.discard("hold")  # "hold an election", "maintain control"
    outcomes = [str(o) for o in (m.get("outcomes") or [])]
    question = text_features(_text(m, "question"))
    return {
        "bag": bag,
        "actions": actions,
        "directions": gate["directions"],
        "poles": gate["poles"],
        "names": _names(*(t for t in gate_texts if isinstance(t, str))),
        "present": present,
        "order": question["order"],
        "question_words": question["words"],
        # "bps" is a unit, compared with its value by the quantity gates
        # ("Cut 25bps" against "cut by 0.25%").
        "side_words": text_features(_text(m, "yes_side"))["words"] - {"bps"},
        "outcomes": outcomes,
        "labels": [set(tokenize(o)) for o in outcomes],
        "end": _parse_time(m.get("end_time")),
    }


def _side_alignment(kf: dict, pf: dict) -> tuple[str | None, str]:
    """Which Polymarket outcome the Kalshi YES side corresponds to.

    Returns ``("aligned" | "inverted" | None, reason)``. ``inverted``
    means the Kalshi YES pays out on Polymarket's second outcome, so the
    Polymarket probability is flipped before the gap is taken.
    """
    k_side = kf["side_words"]
    outcomes, labels, poly_question = pf["outcomes"], pf["labels"], pf["question_words"]
    if len(outcomes) == 2 and {o.lower() for o in outcomes} == _YES_NO:
        if k_side and not k_side <= poly_question:
            missing = sorted(k_side - poly_question)
            return None, f"Kalshi YES side {sorted(k_side)} not named in Polymarket question (missing {missing})"
        if k_side:
            return "aligned", f"Kalshi YES side {sorted(k_side)} named in Polymarket question"
        return "aligned", "both venues price YES on the question as worded"
    if len(outcomes) != 2:
        return None, f"Polymarket market has {len(outcomes)} outcomes, not a binary pair"
    side = k_side or kf["question_words"]
    hits = [bool(side & (lab - _STOPWORDS)) for lab in labels]
    if hits == [True, False]:
        return "aligned", f"Kalshi YES side matches Polymarket outcome {outcomes[0]!r}"
    if hits == [False, True]:
        return "inverted", f"Kalshi YES side matches Polymarket outcome {outcomes[1]!r} (second outcome; price flipped)"
    if hits == [True, True]:
        return None, f"Kalshi YES side matches both Polymarket outcomes {outcomes}"
    return None, f"Kalshi YES side {sorted(side)} matches neither Polymarket outcome {outcomes}"


def _threshold_class(quantity: str) -> str:
    """A fractional threshold's direction and value, strictness dropped.

    ">4.5" and ">=4.5" differ only when the outcome lands exactly on
    4.5; ">50" and ">=50" differ by a whole unit, so integers keep it.
    """
    comparator, number = _QUANTITY_RE.match(quantity).groups()
    if comparator and "." in number:
        return ("above" if comparator[0] == ">" else "below") + number
    return quantity


def _boundary_only(k_quantities: set[str], p_quantities: set[str]) -> bool:
    return len(k_quantities) == len(p_quantities) and (
        {_threshold_class(q) for q in k_quantities} == {_threshold_class(q) for q in p_quantities}
    )


def _quantity_classes(typed: list[dict]) -> set[str]:
    """Keys with strictness dropped for fractional thresholds only."""
    return {_threshold_class(q["key"]) if not q["whole"] else q["key"] for q in typed}


def _quantities_gate(kb: dict, pb: dict) -> tuple[str, str]:
    """("agree" | "boundary" | "differ", reason). Values compare in one
    unit (bps as hundredths of a percent); reasons quote the titles'
    own numbers."""
    k_raw, p_raw = sorted(kb["quantities"]), sorted(pb["quantities"])
    k_keys = {q["key"] for q in kb["typed"]}
    p_keys = {q["key"] for q in pb["typed"]}
    if k_raw == p_raw:
        return "agree", f"quantities agree {k_raw}" if k_raw else ""
    if k_keys == p_keys:
        return "agree", f"quantities agree once bps are read as percent: Kalshi {k_raw} vs Polymarket {p_raw}"
    if _boundary_only(kb["quantities"], pb["quantities"]) or (
        len(k_keys) == len(p_keys) and _quantity_classes(kb["typed"]) == _quantity_classes(pb["typed"])
    ):
        return "boundary", (
            f"quantities agree except at the boundary: Kalshi {k_raw} vs Polymarket {p_raw} "
            "(the same unless the outcome lands exactly on the threshold)"
        )
    return "differ", f"quantities differ: Kalshi {k_raw} vs Polymarket {p_raw}"


def _by_key(typed: list[dict], field: str, key: str = "key") -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for q in typed:
        if q[field] is not None:
            out.setdefault(_threshold_class(q[key]), set()).add(q[field])
    return out


def _late_gates(kf: dict, pf: dict) -> str | None:
    """The gates added after the review of 2026-10-17, in order; the first
    failing one's reason, or None. They run after the original gates so
    a pair those reject keeps its reason."""
    kb, pb = kf["bag"], pf["bag"]
    if kb["times"] and pb["times"] and kb["times"] != pb["times"]:
        return f"clock times differ: Kalshi {sorted(kb['times'])} vs Polymarket {sorted(pb['times'])}"
    if kb["months"] and pb["months"] and not kb["months"] & pb["months"]:
        return f"months differ: Kalshi {sorted(kb['months'])} vs Polymarket {sorted(pb['months'])}"
    for month in sorted(set(kb["dated"]) & set(pb["dated"])):
        k_days, p_days = kb["dated"][month], pb["dated"][month]
        if k_days != p_days:
            return (
                f"days differ in {month}: Kalshi {sorted(k_days, key=int)} vs Polymarket {sorted(p_days, key=int)}"
            )
    for field, key, what in (
        ("unit", "key", "units"), ("unit_raw", "raw", "units"), ("mode", "key", "kinds ('to' a level vs 'by' a change)"),
    ):
        k_by, p_by = _by_key(kb["typed"], field, key), _by_key(pb["typed"], field, key)
        for key in sorted(set(k_by) & set(p_by)):
            if not k_by[key] & p_by[key]:
                return (
                    f"quantity {what} differ for {key}: Kalshi {sorted(k_by[key])} vs Polymarket {sorted(p_by[key])}"
                )
    k_order, p_order = kf["order"], pf["order"]
    if len(k_order) >= 2 and set(k_order) == set(p_order) and k_order != p_order:
        return f"quantities in a different order: Kalshi {k_order} vs Polymarket {p_order}"
    for axis in sorted(set(kf["poles"]) & set(pf["poles"])):
        if kf["poles"][axis] != pf["poles"][axis]:
            return f"opposite wording: Kalshi {sorted(kf['poles'][axis])} vs Polymarket {sorted(pf['poles'][axis])}"
    k_extra = kf["names"] - pf["present"]
    p_extra = pf["names"] - kf["present"]
    k_listed, p_listed = k_extra & _LISTED_NAMES, p_extra & _LISTED_NAMES
    if k_listed or p_listed:
        return f"a listed name on one venue only: Kalshi {sorted(k_listed)} vs Polymarket {sorted(p_listed)}"
    if k_extra and p_extra:
        return f"distinguishing names differ: Kalshi {sorted(k_extra)} vs Polymarket {sorted(p_extra)}"
    return None


def _score(kf: dict, pf: dict, now: datetime) -> dict:
    kb, pb = kf["bag"], pf["bag"]
    reasons: list[str] = []

    def reject(reason: str) -> dict:
        reasons.append(reason)
        return {"confidence": 0.0, "rejected": True, "reasons": reasons, "side": None, "boundary_differs": False}

    shared = kb["words"] & pb["words"]
    union = len(kb["words"]) + len(pb["words"])
    dice = (2 * len(shared) / union) if union else 0.0
    reasons.append(
        f"title word overlap {dice:.2f} (dice; {len(shared)} shared of "
        f"{len(kb['words'])} Kalshi / {len(pb['words'])} Polymarket words)"
    )

    verdict, why = _quantities_gate(kb, pb)
    if verdict == "differ":
        return reject(why)
    boundary = verdict == "boundary"
    if why:
        reasons.append(why)

    if kb["years"] and pb["years"] and not (kb["years"] & pb["years"]):
        return reject(f"years differ: Kalshi {sorted(kb['years'])} vs Polymarket {sorted(pb['years'])}")

    if kb["negated"] != pb["negated"]:
        return reject("negation differs (one title says not/no/never/without, the other does not)")

    if kf["actions"] != pf["actions"]:
        return reject(f"actions differ: Kalshi {sorted(kf['actions'])} vs Polymarket {sorted(pf['actions'])}")
    if kf["actions"]:
        reasons.append(f"actions agree {sorted(kf['actions'])}")
    if kf["directions"] != pf["directions"]:
        return reject(
            f"directions differ: Kalshi {sorted(kf['directions'])} vs Polymarket {sorted(pf['directions'])}"
        )
    if kf["directions"]:
        reasons.append(f"directions agree {sorted(kf['directions'])}")

    k_end, p_end = kf["end"], pf["end"]
    if k_end is None or p_end is None:
        return reject("end date missing on one venue; cannot confirm the same event window")
    gap_days = abs((k_end - p_end).total_seconds()) / 86400
    horizon = max(0.0, (max(k_end, p_end) - now).total_seconds() / 86400)
    tolerance = max(_DATE_TOLERANCE_FLOOR_DAYS, _DATE_TOLERANCE_HORIZON_FRACTION * horizon)
    if gap_days > tolerance:
        return reject(f"end dates {gap_days:.1f} days apart (tolerance {tolerance:.1f} days)")
    date_factor = 1.0 - 0.5 * (gap_days / tolerance)
    reasons.append(f"end dates {gap_days:.1f} days apart (tolerance {tolerance:.1f} days)")

    side, side_reason = _side_alignment(kf, pf)
    if side is None:
        return reject(side_reason)
    reasons.append(side_reason)

    late = _late_gates(kf, pf)
    if late:
        return reject(late)

    confidence = round(dice * date_factor, 3)
    return {"confidence": confidence, "rejected": False, "reasons": reasons, "side": side, "boundary_differs": boundary}


def score_pair(kalshi: dict, poly: dict, now: datetime | None = None) -> dict:
    """Score one Kalshi/Polymarket pair.

    Returns ``{"confidence": float, "rejected": bool, "reasons": [...],
    "side": "aligned" | "inverted" | None, "boundary_differs": bool}``.
    A rejected pair has confidence 0.0 and its first failing gate as the
    last reason. ``confidence`` is an ordinal score, not a probability.
    """
    now = now or datetime.now(timezone.utc)
    return _score(_market_features(kalshi, "kalshi"), _market_features(poly, "polymarket"), now)


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def _prefix(words: set[str], df: dict[str, int], t: float) -> list[str]:
    """The rarest ``n - need + 1`` of a market's words, in the total order
    (document frequency, then the word): the order every market shares."""
    ordered = sorted(words, key=lambda w: (df[w], w))
    need = math.ceil(t * len(ordered) / (2 - t) - 1e-9)  # rounding can only lengthen the prefix
    return ordered[: len(ordered) - max(need, 1) + 1]


def _candidate_pairs(k_feats: list[dict], p_feats: list[dict], min_dice: float) -> list[tuple[int, int]]:
    """Every pair whose title-word Dice can reach ``min_dice``.

    Prefix filtering: with both venues' words in one rarest-first order,
    each market keeps its first ``n - need + 1`` words, where ``need =
    ceil(min_dice * n / (2 - min_dice))``. Dice >= t means the overlap
    o satisfies o >= t * n / (2 - t) for either market's word count n
    (from 2o >= t * (n + m) and m >= o), and the first shared word sits
    within the first n - o + 1 words of each market, so both prefixes
    hold it. "First shared word" needs one total order for every
    market, so ties in rarity break on the word itself (``_prefix``).
    Pairs sharing no word are never candidates. The argument is checked
    by tests, not machine-proved: a brute-force scan comparison and a
    test of the total order.
    """
    t = min(max(float(min_dice), 0.0), 1.0)
    df: dict[str, int] = {}
    for f in k_feats + p_feats:
        for w in f["bag"]["words"]:
            df[w] = df.get(w, 0) + 1

    index: dict[str, list[int]] = {}
    for pi, f in enumerate(p_feats):
        for w in _prefix(f["bag"]["words"], df, t):
            index.setdefault(w, []).append(pi)
    pairs: set[tuple[int, int]] = set()
    for ki, f in enumerate(k_feats):
        for w in _prefix(f["bag"]["words"], df, t):
            for pi in index.get(w, ()):
                pairs.add((ki, pi))
    return sorted(pairs)


def match_markets(
    kalshi: list[dict],
    polymarket: list[dict],
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    now: datetime | None = None,
    near_miss_limit: int = 10,
) -> dict:
    """One-to-one deterministic matching, greedy by confidence.

    Every pair whose title-word overlap could reach the lower of
    ``min_confidence`` and ``NEAR_MISS_FLOOR`` is scored
    (``_candidate_pairs``), so no pair that would pass the threshold is
    skipped: the date factor only lowers a score. A pair sharing no
    title word is never scored. Returns matched pairs, near misses (the
    highest similarity pairs that a gate or the threshold rejected) and
    counts.
    """
    now = now or datetime.now(timezone.utc)
    k_feats = [_market_features(m, "kalshi") for m in kalshi]
    p_feats = [_market_features(m, "polymarket") for m in polymarket]
    # 0.001 under the floor: scores are rounded to 3 places, so a pair can
    # pass the threshold with an overlap up to 0.0005 below it.
    candidates = _candidate_pairs(k_feats, p_feats, min(min_confidence, NEAR_MISS_FLOOR) - 0.001)
    scored: list[tuple[float, int, int, dict]] = []
    near: list[tuple[float, int, int, dict]] = []
    for ki, pi in candidates:
        result = _score(k_feats[ki], p_feats[pi], now)
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
        "pairs_scored": len(candidates),
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
        "price_basis": m.get("price_basis"),
        "yes_bid": m.get("yes_bid"),
        "yes_ask": m.get("yes_ask"),
        "spread": m.get("spread"),
        "last_price": m.get("last_price"),
        "last_trade_time": m.get("last_trade_time"),
        "stale": bool(m.get("stale")),
        "stale_age_seconds": m.get("stale_age_seconds") if m.get("stale") else None,
        "url": m.get("url"),
    }


def _flip(value: object) -> float | None:
    return round(1.0 - value, 4) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _aligned_brief(m: dict, side: str | None) -> dict:
    """The Polymarket side's brief priced on the outcome the Kalshi YES
    pays on: for an inverted pair the quotes are flipped (YES bid = 1 -
    the first outcome's ask, and so on). ``quoted_outcome`` names it."""
    brief = _brief(m)
    outcomes = brief.get("outcomes") or []
    if side == "inverted" and len(outcomes) == 2:
        brief.update({
            "probability": _flip(m.get("probability")),
            "yes_bid": _flip(m.get("yes_ask")),
            "yes_ask": _flip(m.get("yes_bid")),
            "last_price": _flip(m.get("last_price")),
            "quoted_outcome": outcomes[1],
        })
    else:
        brief["quoted_outcome"] = outcomes[0] if outcomes else None
    return brief


def _quote_flags(venue: str, m: dict) -> list[str]:
    """Why one side's price is not a live two-sided quote ([] when it is)."""
    if m.get("probability") is None:
        return [f"{venue} price unavailable (basis: {m.get('price_basis') or 'not stated'})"]
    flags = []
    if m.get("stale"):
        age = m.get("stale_age_seconds")
        old = f"{age / 3600:.1f} h old" if isinstance(age, (int, float)) else "of unknown age"
        flags.append(f"{venue} price from a copy {old}, served from expired cache after a failed live fetch")
    basis = m.get("price_basis")
    if basis != "bid_ask_midpoint":
        flags.append(f"{venue} price is not a two-sided quote midpoint (basis: {basis or 'not stated'})")
    spread = m.get("spread")
    if isinstance(spread, (int, float)) and spread > MAX_COMPARABLE_SPREAD:
        flags.append(f"{venue} bid/ask spread {spread:.2f} is wider than {MAX_COMPARABLE_SPREAD:.2f}")
    return flags


def _pair_record(km: dict, pm: dict, result: dict) -> dict:
    k_prob = km.get("probability")
    p_raw = pm.get("probability")
    p_aligned = None
    if p_raw is not None:
        p_aligned = round(1.0 - p_raw, 4) if result["side"] == "inverted" else p_raw
    gap = round(k_prob - p_aligned, 4) if k_prob is not None and p_aligned is not None else None
    flags = _quote_flags("kalshi", km) + _quote_flags("polymarket", pm)
    if result.get("boundary_differs"):
        flags.append("thresholds differ at the boundary (strict on one venue, inclusive on the other)")
    return {
        "match_confidence": result["confidence"],
        "reasons": result["reasons"],
        "side": result["side"],
        "kalshi": _brief(km),
        "polymarket": _aligned_brief(pm, result["side"]),
        "kalshi_yes_probability": k_prob,
        "polymarket_yes_probability": p_aligned,
        "price_gap": gap,
        "flags": flags,
        "comparable": gap is not None and not flags,
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
