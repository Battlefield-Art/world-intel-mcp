"""Prediction market data sources for world-intel-mcp.

Fetches active prediction markets from Polymarket via their public
Gamma API, and settled markets with their price history from the
public CLOB ``/prices-history`` endpoint. Every function takes a
Fetcher instance as its first argument and returns a dict.

An upstream failure is never an empty success: the response keeps its
documented keys and adds ``error`` and ``degraded: True``. Before
2026-10-17 an outage returned ``{"markets": [], "count": 0}`` with no
marker, shape-identical to a quiet market (ROADMAP Phase 24.5).

Gaps / not covered:
- Gamma serves lists, which cannot carry the fetcher's inline
  ``_stale`` marker; a stale serve is detected through the circuit
  breaker's per-source stale counter, which concurrent calls for the
  same source share, so ``stale`` may also be set by a neighbour's
  stale serve (over-reporting, never under-reporting). CLOB price
  histories are dicts and carry the inline marker.
- Gamma and the CLOB share the ``polymarket`` circuit breaker and its
  1 s request floor: three failed price-history calls open the breaker
  for every Polymarket read (including ``intel_prediction_markets``) for
  its 300 s cooldown, and scoring ``limit`` settled markets makes up to
  ``limit`` sequential CLOB calls (100 markets: over 100 s).
- Polymarket units: prices are USDC per share paying 1 USDC (directly
  a probability); volumes are USDC notional.
- Gamma publishes no last-trade time; ``last_trade_time`` is None.
"""

import json
import logging
import math
import re
from datetime import datetime, timezone
from urllib.parse import quote

from ..analysis.prediction_matching import clip_text
from ..fetcher import Fetcher

logger = logging.getLogger("world-intel-mcp.sources.prediction")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
_CLOB_PRICES_HISTORY_URL = "https://clob.polymarket.com/prices-history"
_GAMMA_PAGE_MAX = 100  # Gamma returns at most 100 rows per page (measured 2026-10-17)
MAX_OPEN_MARKETS = 2000  # fetch_polymarket_open: at most 20 Gamma pages per call
# Every sampled market and event slug (2026-10-17 captures) is lowercase
# letters, digits and hyphens; anything else never builds a URL.
_SLUG_RE = re.compile(r"[a-z0-9-]{1,200}")

UNITS = {
    "price": "Polymarket prices are USDC per share paying 1 USDC (0.63 = 63% implied probability)",
    "volume": "USDC notional",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_outcome_prices(raw: str | None) -> float | None:
    """Parse the outcomePrices JSON string and return YES probability.

    ``outcomePrices`` is a JSON-encoded list like ``"[0.85, 0.15]"``
    where the first element represents the YES probability.
    Returns None if the value is missing, unparseable, or not a finite
    price from 0 to 1.
    """
    if raw is None:
        return None
    try:
        prices = json.loads(raw)
        if isinstance(prices, list) and len(prices) > 0:
            return _price(prices[0])
    except (json.JSONDecodeError, ValueError, TypeError, IndexError):
        logger.debug("Failed to parse outcomePrices: %r", raw)
    return None


def _price(value) -> float | None:
    """A price as a finite float from 0 to 1, else None."""
    number = _float_or_none(value)
    if number is None or not math.isfinite(number) or not 0 <= number <= 1:
        return None
    return number


def _volume(value) -> float | None:
    """A notional volume: absent means 0.0; unparseable, non-finite or
    negative means None (unreadable)."""
    if value is None or value == "":
        return 0.0
    number = _float_or_none(value)
    if number is None or not math.isfinite(number) or number < 0:
        return None
    return number


def event_url(slug) -> str:
    """polymarket.com event URL for a well-formed slug, else ""."""
    if not isinstance(slug, str) or _SLUG_RE.fullmatch(slug) is None:
        return ""
    return f"https://polymarket.com/event/{quote(slug, safe='-')}"


def _classify_sentiment(yes_probability: float) -> str:
    """Classify a YES probability into a human-readable sentiment label."""
    if yes_probability > 0.85:
        return "strong_yes"
    if yes_probability > 0.65:
        return "leaning_yes"
    if yes_probability < 0.15:
        return "strong_no"
    if yes_probability < 0.35:
        return "leaning_no"
    return "uncertain"


def _stale_serves(fetcher: Fetcher, source: str) -> int:
    """The breaker's stale-serve count for a source (list payloads only)."""
    return int(fetcher.breaker.status().get(source, {}).get("total_stale_serves", 0))


def _json_list(raw) -> list:
    """Gamma encodes outcomes/prices/token ids as JSON strings."""
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, str):
        return []
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []
    return value if isinstance(value, list) else []


def _float_or_none(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_float(value) -> float:
    """Convert a value to float, returning 0.0 on failure."""
    if value is None:
        return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def fetch_prediction_markets(fetcher: Fetcher, limit: int = 20) -> dict:
    """Fetch active prediction markets from Polymarket sorted by 24h volume.

    Uses the Polymarket Gamma API to retrieve currently active (non-closed)
    markets ordered by trading volume.

    Returns::

        {"markets": [...], "count": N, "source": "polymarket",
         "timestamp": "<iso>"}
    """
    params = {
        "limit": str(limit),
        "active": "true",
        "closed": "false",
        "order": "volume24hr",
        "ascending": "false",
    }

    stale_before = _stale_serves(fetcher, "polymarket")
    data = await fetcher.get_json(
        _GAMMA_MARKETS_URL,
        source="polymarket",
        cache_key=f"prediction:polymarket:{limit}",
        cache_ttl=300,
        params=params,
    )
    stale = _stale_serves(fetcher, "polymarket") > stale_before

    if data is None:
        # Outage, not a quiet market: same keys, plus the markers.
        return {
            "markets": [],
            "count": 0,
            "source": "polymarket",
            "timestamp": _utc_now_iso(),
            "error": "Polymarket Gamma API unavailable (no live or cached data)",
            "degraded": True,
        }

    if not isinstance(data, list):
        logger.warning("Unexpected Polymarket response type: %s", type(data).__name__)
        return {
            "markets": [],
            "count": 0,
            "source": "polymarket",
            "timestamp": _utc_now_iso(),
            "error": f"Unexpected Polymarket response shape: {type(data).__name__}, expected a list",
            "degraded": True,
        }

    markets: list[dict] = []
    unpriced = 0
    for item in data:
        if not isinstance(item, dict):
            continue

        yes_probability = _parse_outcome_prices(item.get("outcomePrices"))
        if yes_probability is None:
            # Skip markets with unparseable outcome data (counted below)
            unpriced += 1
            continue

        volume_24h = _safe_float(item.get("volume24hr"))
        total_volume = _safe_float(item.get("volume"))
        liquidity = _safe_float(item.get("liquidity"))
        slug = item.get("slug", "")

        markets.append({
            "question": clip_text(item.get("question", "")),
            "yes_probability": round(yes_probability, 4),
            "sentiment": _classify_sentiment(yes_probability),
            "volume_24h": volume_24h,
            "total_volume": total_volume,
            "liquidity": liquidity,
            "category": item.get("category", ""),
            "url": event_url(slug),
        })

    # Ensure descending sort by 24h volume (API should already return
    # sorted, but enforce it defensively)
    markets.sort(key=lambda m: m["volume_24h"], reverse=True)

    result = {
        "markets": markets,
        "count": len(markets),
        "source": "polymarket",
        "timestamp": _utc_now_iso(),
    }
    gaps = []
    if stale:
        result["stale"] = True
        gaps.append("served from expired cache after a failed live fetch")
    if unpriced:
        gaps.append(f"{unpriced} market(s) left out: outcome price missing, unreadable or outside 0..1")
    if gaps:
        result["data_gaps"] = gaps
    return result


# ---------------------------------------------------------------------------
# Cross-venue shapes (used by tools/prediction_markets.py)
# ---------------------------------------------------------------------------

def normalize_gamma_market(item: dict) -> dict | None:
    """One Gamma market in the cross-venue shape; None if not binary.

    Probability is the first outcome's: midpoint of bestBid/bestAsk when
    both are live, else Gamma's ``outcomePrices[0]``. Prices outside 0..1
    or non-finite count as absent; ``volume`` is None when unreadable;
    ``spread`` is set only for a two-sided book.
    """
    outcomes = [clip_text(o, 100) for o in _json_list(item.get("outcomes"))]
    prices = [_price(p) for p in _json_list(item.get("outcomePrices"))]
    if len(outcomes) != 2 or len(prices) != 2:
        return None
    bid = _price(item.get("bestBid"))
    ask = _price(item.get("bestAsk"))
    two_sided = bid is not None and ask is not None and 0 < bid <= ask < 1
    if two_sided:
        prob, basis = round((bid + ask) / 2, 4), "bid_ask_midpoint"
    elif prices[0] is not None:
        prob, basis = round(prices[0], 4), "gamma_outcome_price"
    else:
        prob, basis = None, "no_price"
    events = item.get("events") if isinstance(item.get("events"), list) else []
    event = events[0] if events and isinstance(events[0], dict) else {}
    slug = event.get("slug") or item.get("slug") or ""
    tokens = [str(t) for t in _json_list(item.get("clobTokenIds"))]
    return {
        "venue": "polymarket",
        "id": str(item.get("id") or item.get("conditionId") or slug),
        "question": clip_text(item.get("question", "") or ""),
        "context": clip_text(event.get("title", "")) if event.get("title") != item.get("question") else "",
        "yes_side": "",
        "outcomes": outcomes,
        "outcome_prices": prices,
        "end_time": item.get("endDate"),
        "closed_time": item.get("closedTime"),
        "probability": prob,
        "price_basis": basis,
        "yes_bid": bid,
        "yes_ask": ask,
        "spread": round(ask - bid, 4) if two_sided else None,
        "last_price": _price(item.get("lastTradePrice")),
        "last_trade_time": None,
        "volume": _volume(item.get("volumeNum") if item.get("volumeNum") is not None else item.get("volume")),
        "volume_24h": _volume(item.get("volume24hr")),
        "token_ids": tokens,
        "event_id": str(event.get("id") or ""),
        "resolution_status": item.get("umaResolutionStatus"),
        "url": event_url(slug),
    }


async def fetch_polymarket_open(fetcher: Fetcher, limit: int = 300) -> dict:
    """Open binary Polymarket markets by 24h volume, cross-venue shape.

    Returns ``{"markets", "requested", "rows_read", "non_binary_skipped",
    "error"?, "degraded"?, "stale"?}``.
    """
    limit = max(1, min(int(limit), MAX_OPEN_MARKETS))
    rows: list = []
    offset = 0
    stale_before = _stale_serves(fetcher, "polymarket")
    error = None
    while offset < limit:
        size = min(_GAMMA_PAGE_MAX, limit - offset)
        data = await fetcher.get_json(
            _GAMMA_MARKETS_URL,
            source="polymarket",
            cache_key=f"prediction:polymarket:open:{size}:{offset}",
            cache_ttl=300,
            params={
                "limit": str(size), "offset": str(offset), "active": "true",
                "closed": "false", "order": "volume24hr", "ascending": "false",
            },
        )
        if not isinstance(data, list):
            error = (
                "Polymarket Gamma API unavailable (no live or cached data)"
                if data is None else f"Unexpected Polymarket response shape: {type(data).__name__}"
            )
            break
        rows.extend(r for r in data if isinstance(r, dict))
        if len(data) < size:
            break
        offset += size
    markets = [m for m in (normalize_gamma_market(r) for r in rows) if m is not None]
    out: dict = {
        "markets": markets,
        "requested": limit,
        "rows_read": len(rows),
        "non_binary_skipped": len(rows) - len(markets),
    }
    if error:
        out["error"] = error
        out["degraded"] = True
    if _stale_serves(fetcher, "polymarket") > stale_before:
        out["stale"] = True
    return out


def _closed_ts(value) -> int | None:
    if not value:
        return None
    text = str(value).replace(" ", "T").replace("Z", "+00:00")
    if text.endswith("+00"):
        text += ":00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


async def fetch_polymarket_settled(
    fetcher: Fetcher,
    limit: int = 25,
    min_volume: float = 10000.0,
    scan: int = 100,
    min_open_hours: float = 0.0,
) -> dict:
    """Recently closed binary markets resolved to exactly 1/0.

    Selection: Gamma closed markets ordered by ``closedTime`` descending
    with ``volume_num_min``; kept when UMA status is ``resolved``, the
    final outcome prices are exactly one 1 and one 0, the volume is
    readable and at least ``min_volume`` (checked here too, counted in
    ``volume_skipped``), and the market opened (``startDate``) at least
    ``min_open_hours`` before its reference time. The first outcome is
    the YES side for scoring.

    Reference time: the earlier of ``endDate`` (the scheduled end; for a
    game, its start) and ``closedTime`` (when the market closed, which
    comes after resolution: measured 2026-10-17 on 20 closed markets,
    2.7 to 5.5 h after ``endDate`` for football, the same time for
    5-minute crypto markets, and up to 164 h before ``endDate`` for a
    market that resolved early). Gamma publishes no time at which the
    outcome became known (``umaEndDate`` equalled ``closedTime`` in every
    measured row), so an early resolution can still put an offset point
    after the outcome was known. A listing served from expired cache
    sets ``stale``.
    """
    stale_before = _stale_serves(fetcher, "polymarket")
    data = await fetcher.get_json(
        _GAMMA_MARKETS_URL,
        source="polymarket",
        cache_key=f"prediction:polymarket:settled:{scan}:{min_volume}",
        cache_ttl=900,
        params={
            "limit": str(max(1, min(int(scan), _GAMMA_PAGE_MAX))), "closed": "true",
            "order": "closedTime", "ascending": "false", "volume_num_min": str(min_volume),
        },
    )
    stale = _stale_serves(fetcher, "polymarket") > stale_before
    if not isinstance(data, list):
        return {
            "markets": [],
            "listing_rows_read": 0,
            "error": "Polymarket closed-markets listing unavailable (no live or cached data)",
        }
    kept: list[dict] = []
    too_short = 0
    volume_skipped = 0
    for item in data:
        if not isinstance(item, dict):
            continue
        m = normalize_gamma_market(item)
        if m is None or m["resolution_status"] != "resolved" or not m["token_ids"]:
            continue
        if sorted(p for p in m["outcome_prices"] if p is not None) != [0.0, 1.0]:
            continue
        closed = _closed_ts(m["closed_time"])
        if closed is None:
            continue
        if m["volume"] is None or m["volume"] < min_volume:
            volume_skipped += 1
            continue
        scheduled = _closed_ts(m["end_time"])
        if scheduled is None:
            ref, reference = closed, "closedTime (no endDate published)"
        elif scheduled < closed:
            ref, reference = scheduled, "endDate (scheduled end; the market closed later, after resolution)"
        else:
            ref, reference = closed, "closedTime (closed at or before its scheduled endDate)"
        opened = _closed_ts(item.get("startDate"))
        if min_open_hours and (opened is None or ref - opened < min_open_hours * 3600):
            too_short += 1
            continue
        kept.append({
            "venue": "polymarket",
            "id": m["id"],
            "question": m["question"],
            "yes_side": m["outcomes"][0],
            "event": m["event_id"] or m["id"],
            "outcome": 1 if m["outcome_prices"][0] == 1.0 else 0,
            "result": m["outcomes"][0] if m["outcome_prices"][0] == 1.0 else m["outcomes"][1],
            "volume": m["volume"],
            "token_id": m["token_ids"][0],
            "reference_ts": ref,
            "reference": reference,
            "settled_at": m["closed_time"],
        })
        if len(kept) >= limit:
            break
    out = {
        "markets": kept,
        "listing_rows_read": len(data),
        "open_too_briefly_skipped": too_short,
        "volume_skipped": volume_skipped,
        "error": None,
    }
    if stale:
        out["stale"] = True
        out["data_gaps"] = ["closed-markets listing served from expired cache after a failed live fetch"]
    return out


async def fetch_polymarket_price_history(
    fetcher: Fetcher, token_id: str, start_ts: int, end_ts: int,
    *, stale: list[float | None] | None = None,
) -> list[tuple[int, float]] | None:
    """CLOB price history for one outcome token; None when unavailable.

    Hourly fidelity. An empty list means the endpoint answered with no
    points in the window, which differs from None (the call failed).
    When the fetcher served an expired cache entry (inline ``_stale``),
    its age in seconds (None if unknown) is appended to ``stale``.
    """
    data = await fetcher.get_json(
        _CLOB_PRICES_HISTORY_URL,
        source="polymarket",
        cache_key=f"prediction:polymarket:history:{token_id}:{start_ts}:{end_ts}",
        cache_ttl=21600,
        params={"market": token_id, "startTs": str(start_ts), "endTs": str(end_ts), "fidelity": "60"},
    )
    if not isinstance(data, dict) or not isinstance(data.get("history"), list):
        return None
    if stale is not None and data.get("_stale"):
        age = data.get("_stale_age_seconds")
        stale.append(float(age) if isinstance(age, (int, float)) and not isinstance(age, bool)
                     and math.isfinite(age) else None)
    points: list[tuple[int, float]] = []
    for row in data["history"]:
        if isinstance(row, dict) and isinstance(row.get("t"), int):
            p = _float_or_none(row.get("p"))
            if p is not None:
                points.append((row["t"], p))
    return points
