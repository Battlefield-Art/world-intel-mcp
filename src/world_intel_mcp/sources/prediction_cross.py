"""Cross-venue prediction-market views: same-event comparison and
settled-market forecast scoring (Kalshi and Polymarket).

Built on ``sources/kalshi.py`` and ``sources/prediction.py`` for the
fetches and ``analysis/prediction_matching.py`` for the matching and
scoring arithmetic. Each venue's failure is reported for that venue;
one venue being down never reads as "no matches" or "no settled
markets".

Gaps / not covered:
- The two venues' settled samples are different markets, so their
  Brier scores are not a head-to-head comparison; the response says so.
- Reference times differ by venue (stated in the response's
  ``comparability``). Kalshi's is ``close_time``, when trading actually
  ended (measured 2026-10-17 on 40 settled sports markets: 2.2 to 5.2 h
  before the expected expiration, settled 1.5 to 6.1 minutes later).
  Polymarket's is the earlier of ``endDate`` and ``closedTime`` (closed
  after resolution); Gamma publishes no time at which the outcome became
  known, so an early resolution can still put an offset point after it.
  Offsets default to 24h and 168h, not minutes, for that reason.
- A Kalshi market whose outcome was known well before a ``close_time``
  that did not move can lose its offset price to the staleness limit
  and drop out of the sample (not measured).
- Price history comes from venue endpoints whose retention was not
  measured beyond the recent markets probed on 2026-10-17.
- Samples are the most recently settled markets, so they cluster in
  time. Measured 2026-10-17 17:05 UTC: one football match supplied 11 of
  25 Polymarket markets (3 event ids), and two CPI releases supplied all
  24 Kalshi markets scored at 24h.
- Price histories served from expired cache are excluded, not scored,
  although a settled market's past window cannot change; the exclusion
  is the conservative reading of "never score old data as current".
- Polymarket scoring shares the ``polymarket`` circuit breaker and 1 s
  floor with Gamma (see sources/prediction.py): ``limit=100`` makes up
  to 100 sequential CLOB calls.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any

from ..analysis.prediction_matching import (
    DEFAULT_MIN_CONFIDENCE,
    MAX_COMPARABLE_SPREAD,
    brier_score,
    match_markets,
    price_at_offset,
)
from ..fetcher import Fetcher
from . import kalshi, prediction

logger = logging.getLogger("world-intel-mcp.sources.prediction_cross")

_MATCH_METHOD = (
    "Deterministic: titles lowercased with a small synonym table (month names, "
    "cut/raise/hold verbs, above/below); quantities must be equal sets (a four-digit "
    "number is a year unless a $ sign, a thousands comma or a comparator makes it an "
    "amount; signs and ordinals are kept), except that a fractional threshold strict on "
    "one venue and inclusive on the other is accepted with a flag; years must intersect "
    "when both titles state one; negation, rate actions (cut, raise, hold) and directions "
    "(above, below) must agree; end dates within max(3 days, 15% of the horizon); the "
    "Kalshi YES side must be named in the Polymarket question or match exactly one "
    "Polymarket outcome. match_confidence = title word overlap (Dice) x date factor "
    "(1.0 same day, 0.5 at the tolerance edge): an ordinal score for ranking candidate "
    "pairs, not a probability that two markets are the same event. One-to-one, greedy "
    "by confidence."
)

_RANKING = (
    "Pairs with comparable: true (both prices two-sided quote midpoints, spreads at most "
    f"{MAX_COMPARABLE_SPREAD:.2f}, no boundary flag) come first, largest absolute price gap "
    "first; flagged pairs follow, ranked the same way among themselves, because a gap "
    "taken from a last trade, a listed outcome price, a wide book or thresholds that "
    "differ at the boundary is not a like-for-like difference of live prices."
)

MAX_PAIRS = 500
# Matched pairs whose Kalshi price is a last trade get that trade's time
# looked up (one /markets/trades request each), for the first this many.
MAX_TRADE_LOOKUPS = 10
MAX_OFFSETS = 10
# A price window spans at most 8760 + 720 hours, so one market's hourly
# candles stay under the candlestick endpoint's 10,000 cap.
MAX_OFFSET_HOURS = 8760
MAX_STALENESS_HOURS = 720
MAX_VOLUME_FLOOR = 10**12  # keeps the floor's query string and formatting finite


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_number(value: Any) -> bool:
    """An int or a finite float; booleans, strings, None, NaN and infinity are not."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return not isinstance(value, float) or math.isfinite(value)


def _number_arg(value: Any, name: str, low: float, high: float) -> Any:
    """``value`` unchanged when it is a number in ``[low, high]``.

    Raises ValueError naming the argument otherwise, before any request
    is made.
    """
    if not _is_number(value):
        raise ValueError(f"{name} must be a finite number (got {value!r})")
    if not low <= value <= high:
        raise ValueError(f"{name} must be from {low:g} to {high:g} (got {value!r})")
    return value


def _offsets_arg(value: Any) -> list[float]:
    """Positive offsets in hours, sorted and de-duplicated.

    None or an empty list means the defaults (24, 168). More than
    MAX_OFFSETS values, a value that is not a finite number of hours up
    to MAX_OFFSET_HOURS, or a value of 0 or below raises ValueError: a
    bad offset is refused, never silently dropped.
    """
    raw = value or [24, 168]
    if not isinstance(raw, (list, tuple)):
        raise ValueError(f"offsets_hours must be a list of hours (got {value!r})")
    if len(raw) > MAX_OFFSETS:
        raise ValueError(f"offsets_hours takes at most {MAX_OFFSETS} values (got {len(raw)})")
    for o in raw:
        if not _is_number(o) or o > MAX_OFFSET_HOURS:
            raise ValueError(f"offsets_hours values must be numbers of hours up to {MAX_OFFSET_HOURS} (got {o!r})")
    if not any(o > 0 for o in raw):
        raise ValueError(f"offsets_hours must contain at least one positive number of hours (got {list(raw)!r})")
    for o in raw:
        if o <= 0:
            raise ValueError(f"offsets_hours values must be above 0 hours (got {o!r})")
    return sorted({float(o) for o in raw})


def _arg_error(message: str) -> dict:
    return {"error": message, "degraded": True, "source": "kalshi+polymarket", "timestamp": _utc_now_iso()}


async def fetch_prediction_compare(
    fetcher: Fetcher,
    category: str | None = None,
    series: str | None = None,
    kalshi_pages: int = 3,
    series_limit: int = 10,
    polymarket_limit: int = 300,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    limit: int = 25,
) -> dict:
    """The same events on Kalshi and Polymarket, side by side.

    Returns matched pairs (comparable pairs first, each group by largest
    absolute price gap; see ``ranking`` in the response), the
    highest-similarity rejected pairs with their reasons, and unmatched
    counts per venue. Below ``min_confidence`` a pair is not a match.
    Each side carries its price basis, quotes, spread and last-trade
    time where the venue publishes one. Data served from expired cache
    on either venue sets ``degraded``. Counts are clamped (kalshi_pages
    and series_limit 1..25, polymarket_limit 1..2000, limit 1..500); a
    count that is not a whole number, or a min_confidence outside 0..1,
    is an ``error`` answered before any request.
    """
    try:
        kalshi_pages = kalshi.count_arg(kalshi_pages, "kalshi_pages", kalshi.MAX_PAGES)
        series_limit = kalshi.count_arg(series_limit, "series_limit", kalshi.MAX_SERIES)
        polymarket_limit = kalshi.count_arg(polymarket_limit, "polymarket_limit", prediction.MAX_OPEN_MARKETS)
        limit = kalshi.count_arg(limit, "limit", MAX_PAIRS)
        min_confidence = float(_number_arg(min_confidence, "min_confidence", 0, 1))
    except ValueError as exc:
        return _arg_error(str(exc))
    k_raw = await kalshi.fetch_kalshi_open_markets(
        fetcher, category=category, series=series, pages=kalshi_pages, series_limit=series_limit
    )
    p_raw = await prediction.fetch_polymarket_open(fetcher, limit=polymarket_limit)
    k_markets, p_markets = k_raw["markets"], p_raw["markets"]

    gaps: list[str] = [f"kalshi: {g}" for g in k_raw["data_gaps"]]
    if p_raw.get("error"):
        gaps.append(f"polymarket: {p_raw['error']}")
    if p_raw.get("stale"):
        gaps.append("polymarket: served from expired cache after a failed live fetch")
    gaps.append(
        f"polymarket: top {p_raw['requested']} open markets by 24h volume requested; "
        f"{p_raw['non_binary_skipped']} non-binary rows skipped"
    )

    out: dict[str, Any] = {
        "source": "kalshi+polymarket",
        "kalshi_scan": k_raw["scan"],
        "kalshi_markets_compared": len(k_markets),
        "polymarket_markets_compared": len(p_markets),
        "min_confidence": min_confidence,
        "method": _MATCH_METHOD,
        "price_gap": "kalshi YES probability minus polymarket probability of the same side",
        "price_basis": (
            "per side: bid_ask_midpoint (two-sided book), last_trade (Kalshi's last trade; its time is "
            "looked up for the first matched pairs), gamma_outcome_price (Polymarket's listed outcome "
            "price; Gamma publishes no last-trade time) or no_price"
        ),
        "ranking": _RANKING,
        "units": {"kalshi": kalshi.UNITS, "polymarket": prediction.UNITS},
        "timestamp": _utc_now_iso(),
    }

    down = []
    if k_raw["nothing_read"]:
        down.append("kalshi")
    if p_raw.get("error") and not p_markets:
        down.append("polymarket")
    if down:
        out.update({
            "matched": [],
            "matched_count": None,
            "near_misses": [],
            "unmatched_kalshi": None if "kalshi" in down else len(k_markets),
            "unmatched_polymarket": None if "polymarket" in down else len(p_markets),
            "error": f"comparison not possible: {', '.join(down)} unavailable",
            "degraded": True,
            "data_gaps": gaps,
        })
        return out

    result = match_markets(k_markets, p_markets, min_confidence=min_confidence)
    matched = result["matched"]
    matched.sort(key=lambda r: (
        r["price_gap"] is None, not r["comparable"], -abs(r["price_gap"] or 0.0), -r["match_confidence"],
    ))
    shown = matched[:limit]
    lookups = [p for p in shown if p["kalshi"].get("price_basis") == "last_trade"][:MAX_TRADE_LOOKUPS]
    if lookups:
        times, trade_gaps = await kalshi.fetch_kalshi_last_trade_times(fetcher, [p["kalshi"]["id"] for p in lookups])
        for p in lookups:
            p["kalshi"]["last_trade_time"] = times.get(p["kalshi"]["id"])
        # Supplementary: a failed lookup leaves the time null and is named
        # here; the comparison itself is complete, so it is not degraded.
        gaps.extend(f"kalshi: {g}" for g in trade_gaps)
    out.update({
        "matched": shown,
        "matched_count": len(matched),
        "comparable_count": sum(1 for p in matched if p["comparable"]),
        "near_misses": result["near_misses"],
        "pairs_scored": result["pairs_scored"],
        "unmatched_kalshi": result["unmatched_kalshi"],
        "unmatched_polymarket": result["unmatched_polymarket"],
        "data_gaps": gaps,
    })
    if k_raw["errors"] or k_raw.get("stale") or p_raw.get("error") or p_raw.get("stale"):
        out["degraded"] = True
    return out


_STALE_HISTORY = "price history served from expired cache"


def _score_venue(
    markets: list[dict],
    histories: dict[str, list[tuple[int, float]] | None],
    offsets: list[float],
    max_staleness_hours: float,
    stale_ids: set[str] = frozenset(),
) -> tuple[dict, list[dict]]:
    scores: dict[str, Any] = {}
    sample: list[dict] = []
    per_market_prices: dict[str, dict[str, float | None]] = {m["id"]: {} for m in markets}
    for off in offsets:
        label = f"{off:g}h"
        forecasts: list[tuple[float, int]] = []
        events: set[str] = set()
        excluded: dict[str, int] = {}
        for m in markets:
            hist = histories.get(m["id"])
            if hist is None or m["id"] in stale_ids:
                key = "price history unavailable" if hist is None else _STALE_HISTORY
                excluded[key] = excluded.get(key, 0) + 1
                per_market_prices[m["id"]][label] = None
                continue
            price, reason = price_at_offset(hist, m["reference_ts"], off, max_staleness_hours)
            if price is not None and not 0.0 <= price <= 1.0:
                # A malformed upstream price (NaN, or outside 0..1) is
                # counted as an exclusion, never scored or raised.
                price, key = None, "price outside 0..1"
            elif price is None:
                key = reason if reason.startswith("no price") else "last price older than the staleness limit"
            per_market_prices[m["id"]][label] = price
            if price is None:
                excluded[key] = excluded.get(key, 0) + 1
                continue
            forecasts.append((price, m["outcome"]))
            events.add(m.get("event") or m["id"])
        brier = brier_score(forecasts)
        scores[label] = {
            "n": len(forecasts),
            "distinct_events": len(events),
            "brier": round(brier, 4) if brier is not None else None,
            "yes_rate": round(sum(o for _, o in forecasts) / len(forecasts), 4) if forecasts else None,
            "excluded": excluded,
        }
    for m in markets:
        sample.append({
            "id": m["id"],
            "event": m.get("event"),
            "question": m["question"],
            "yes_side": m.get("yes_side") or None,
            "outcome": m["outcome"],
            "result": m["result"],
            "volume": m["volume"],
            "settled_at": m["settled_at"],
            "reference": m.get("reference"),
            "price_before_reference": per_market_prices[m["id"]],
        })
    return scores, sample


async def fetch_prediction_settled(
    fetcher: Fetcher,
    limit: int = 25,
    offsets_hours: list[float] | None = None,
    kalshi_min_volume: float = 1000.0,
    polymarket_min_volume: float = 10000.0,
    kalshi_series: str | None = None,
    max_staleness_hours: float = 168.0,
    kalshi_category: str | None = "Economics",
    kalshi_series_limit: int = 10,
) -> dict:
    """Settled markets on both venues, scored against earlier prices.

    For each venue: the ``limit`` most recently settled binary markets
    passing the volume floor, the YES price at each offset before the
    reference time (the last price inside ``max_staleness_hours`` up to
    that point), and a Brier score per offset over the markets that had
    one. A venue whose price history could not be read is not scored.
    A price history served from expired cache is excluded (counted per
    offset); a listing served from expired cache is still scored and
    marked. ``limit`` is clamped to 1..100 and ``kalshi_series_limit``
    to 1..25; more than 10 offsets, an offset of 0 or below or above
    8760 hours, a staleness limit outside 0..720 hours, a volume floor
    outside 0..10**12 or a value that is not a finite number is an
    ``error`` answered before any request.
    """
    try:
        offsets = _offsets_arg(offsets_hours)
        limit = kalshi.count_arg(limit, "limit", kalshi.MAX_SETTLED)
        kalshi_series_limit = kalshi.count_arg(kalshi_series_limit, "kalshi_series_limit", kalshi.MAX_SERIES)
        kalshi_min_volume = _number_arg(kalshi_min_volume, "kalshi_min_volume", 0, MAX_VOLUME_FLOOR)
        polymarket_min_volume = _number_arg(polymarket_min_volume, "polymarket_min_volume", 0, MAX_VOLUME_FLOOR)
        max_staleness_hours = float(
            _number_arg(max_staleness_hours, "max_staleness_hours", 0, MAX_STALENESS_HOURS)
        )
    except ValueError as exc:
        return _arg_error(str(exc))
    window_start = max(offsets) + max_staleness_hours
    window_end = min(offsets)
    venues: dict[str, Any] = {}

    # Kalshi
    k = await kalshi.fetch_kalshi_settled(
        fetcher, limit=limit, min_volume=kalshi_min_volume, series=kalshi_series,
        category=None if kalshi_series else kalshi_category, series_limit=kalshi_series_limit,
        min_open_hours=window_end,
    )
    if kalshi_series:
        k_scope = f"series {kalshi_series}"
    elif kalshi_category:
        k_scope = (
            f"the {len(k['scope']['series_read'])} highest-lifetime-volume series in category "
            f"{kalshi_category!r}: {k['scope']['series_read']}"
        )
    else:
        k_scope = "the exchange-wide settled listing (intraday sports dominate it)"
    k_selection = (
        f"most recently settled Kalshi markets (combos excluded) from {k_scope}, with result yes/no, "
        f"lifetime volume >= {kalshi_min_volume:g} contracts and open >= {window_end:g}h "
        f"before close; first {limit} kept from {k['listing_rows_read']} listing rows read "
        f"({k['open_too_briefly_skipped']} skipped as open too briefly)"
    )
    if k.get("stale_responses"):
        k_selection += f"; {k['stale_responses']} listing response(s) served from expired cache"
    k_entry: dict[str, Any] = {
        "selection": k_selection,
        "reference_time": "close_time (when trading ended; the actual close when a market closes early)",
        "price_source": "batch /markets/candlesticks, hourly, price.close_dollars (last trade in the hour)",
        "settled_markets": len(k["markets"]),
    }
    listing_gaps = [k["stale_gap"]] if k.get("stale_gap") else []
    if k["error"] and not k["markets"]:
        k_entry.update({"error": k["error"], "scored": False, "scores": None, "sample": []})
        k_entry["history_errors"] = listing_gaps
    else:
        stale_candles: dict[str, float | None] = {}
        hist, h_errors = await kalshi.fetch_kalshi_price_history(
            fetcher, k["markets"], window_start, window_end, stale=stale_candles
        )
        histories = {m["id"]: hist.get(m["id"]) for m in k["markets"]}
        k_entry.update(_venue_result(
            k["markets"], histories, offsets, max_staleness_hours,
            h_errors + k["errors"] + listing_gaps, None, set(stale_candles),
        ))
        if k.get("data_gaps") and not k["markets"]:
            k_entry["not_scored_reason"] = "; ".join(k["data_gaps"])
    venues["kalshi"] = k_entry

    # Polymarket
    p = await prediction.fetch_polymarket_settled(
        fetcher, limit=limit, min_volume=polymarket_min_volume, min_open_hours=window_end
    )
    p_selection = (
        f"most recently closed Polymarket binary markets with volume >= "
        f"{polymarket_min_volume:g} USDC, UMA status resolved, final prices exactly 1/0, "
        f"open >= {window_end:g}h before the reference time; first {limit} kept from "
        f"{p['listing_rows_read']} listing rows read "
        f"({p.get('open_too_briefly_skipped', 0)} skipped as open too briefly"
        f"{', %d below the volume floor or unreadable' % p['volume_skipped'] if p.get('volume_skipped') else ''}); "
        "first outcome is the YES side"
    )
    if p.get("stale"):
        p_selection += "; the listing was served from expired cache"
    p_entry: dict[str, Any] = {
        "selection": p_selection,
        "reference_time": (
            "the earlier of endDate (scheduled end; a game's start) and closedTime (closed after "
            "resolution); each sample entry names which"
        ),
        "price_source": "CLOB /prices-history, hourly fidelity, first outcome token",
        "settled_markets": len(p["markets"]),
    }
    p_listing_gaps = list(p.get("data_gaps") or [])
    if p["error"]:
        p_entry.update({"error": p["error"], "scored": False, "scores": None, "sample": []})
    else:
        histories = {}
        failed = 0
        stale_ids: set[str] = set()
        stale_ages: list[float | None] = []
        for m in p["markets"]:
            ages: list[float | None] = []
            pts = await prediction.fetch_polymarket_price_history(
                fetcher,
                m["token_id"],
                m["reference_ts"] - int(window_start * 3600),
                m["reference_ts"] - int(window_end * 3600),
                stale=ages,
            )
            if pts is None:
                failed += 1
            elif ages:
                stale_ids.add(m["id"])
                stale_ages.extend(ages)
            histories[m["id"]] = pts
        h_errors = [f"price history unavailable for {failed} market(s)"] if failed else []
        if stale_ids:
            oldest = kalshi.max_stale_age(stale_ages)
            age = f", the oldest {oldest / 3600:.1f} h old" if oldest is not None else ""
            h_errors.append(
                f"price history for {len(stale_ids)} market(s) served from expired cache after a "
                f"failed live fetch{age}; not scored"
            )
        p_entry.update(_venue_result(
            p["markets"], histories, offsets, max_staleness_hours, h_errors + p_listing_gaps, None, stale_ids,
        ))
    venues["polymarket"] = p_entry

    gaps = []
    for name, entry in venues.items():
        if entry.get("error"):
            gaps.append(f"{name}: {entry['error']}")
        for e in entry.get("history_errors") or []:
            gaps.append(f"{name}: {e}")
        if entry.get("scored") is False and not entry.get("error"):
            gaps.append(f"{name}: not scored ({entry.get('not_scored_reason')})")
    out = {
        "source": "kalshi+polymarket",
        "offsets_hours": offsets,
        "max_staleness_hours": max_staleness_hours,
        "venues": venues,
        "comparability": (
            "Each venue is scored on its own sample of different markets; the Brier "
            "scores are not a head-to-head comparison of the same events. Markets of one "
            "event are correlated: n counts markets, distinct_events counts the venue's "
            "event ids behind them, and one real-world event can span several ids "
            "(Polymarket lists a match's result, halftime and totals markets as separate "
            "events). A strike ladder's far strikes trade near 0 or 1 and pull a Brier "
            "score down. Reference times differ by venue: Kalshi counts back from "
            "close_time, when trading actually ended (measured on 40 settled sports "
            "markets: 2.2 to 5.2 h before the expected expiration, settled 1.5 to 6.1 "
            "minutes later); Polymarket from the earlier of endDate (scheduled end; a "
            "game's start) and closedTime (closed after resolution), and Gamma publishes "
            "no time at which the outcome became known, so an early resolution can put an "
            "offset point after it. Each offset scores only the markets with a price "
            "inside the staleness limit at that offset, so samples differ by offset: "
            "compare n per offset."
        ),
        "brier": "mean of (price - outcome)^2 with outcome 1 for YES; 0 is perfect, always 0.5 scores 0.25",
        "data_gaps": gaps,
        "timestamp": _utc_now_iso(),
    }
    if gaps:
        out["degraded"] = True
    return out


def _venue_result(
    markets: list[dict],
    histories: dict[str, list[tuple[int, float]] | None],
    offsets: list[float],
    max_staleness_hours: float,
    history_errors: list[str],
    listing_error: str | None,
    stale_ids: set[str] = frozenset(),
) -> dict:
    entry: dict[str, Any] = {"history_errors": history_errors}
    if listing_error:
        entry["history_errors"] = history_errors + [listing_error]
    unusable = [m for m in markets if histories.get(m["id"]) is None or m["id"] in stale_ids]
    if markets and len(unusable) == len(markets):
        stale = sum(1 for m in unusable if histories.get(m["id"]) is not None)
        if not stale:
            reason = "price history unavailable for every market in the sample"
        elif stale == len(markets):
            reason = "price history served from expired cache for every market in the sample"
        else:
            reason = (
                "price history unavailable or served from expired cache for every market "
                f"in the sample ({stale} from expired cache)"
            )
        entry.update({"scored": False, "not_scored_reason": reason, "scores": None, "sample": []})
        return entry
    scores, sample = _score_venue(markets, histories, offsets, max_staleness_hours, stale_ids)
    entry.update({"scored": bool(markets), "scores": scores, "sample": sample})
    if not markets:
        entry["not_scored_reason"] = "no settled markets passed the selection"
    return entry
