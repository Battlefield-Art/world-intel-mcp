"""Kalshi prediction-market data (public market-data API, no account).

Reads only the unauthenticated endpoints of Kalshi's trade API v2
(``https://api.elections.kalshi.com/trade-api/v2``), verified live
2026-10-17: ``/events`` (with nested markets), ``/series`` (with
``include_volume``), ``/markets`` (``status=settled``) and the batch
``/markets/candlesticks``. Nothing here places orders or needs keys.

Units, as the venue serves them: prices are strings in US dollars per
contract that pays $1 on YES (``yes_bid_dollars="0.6300"``), so a price
is directly a YES implied probability; volumes and open interest are
contract counts as strings (``volume_fp="3251.19"``).

Every function returns a dict. An upstream failure is never an empty
success: it carries ``error`` and ``degraded: True``; a partial scan
lists what was not read under ``data_gaps``.

Gaps / not covered:
- The open-events endpoint orders events by close time, furthest out
  first, and holds more than 8,000 open events (measured 2026-10-17:
  40 pages of 200 did not reach the end). An unfiltered call ranks only
  the pages it read; ``scan.exhaustive`` says so. Category calls rank
  the category's top series by lifetime volume instead.
- The ``/events`` ``category`` query parameter is ignored by the API
  (measured: identical results with and without it), so category is
  resolved through ``/series?category=`` and filtered client-side. That
  filter is loose too (measured: ``category=Economics`` returned 869
  series, 7 of them in other categories), so series are kept only on an
  exact category match. Category names are exact: ``Climate`` returns
  no series, ``Climate and Weather`` returns 444.
- Combo (multivariate) markets are excluded from settled samples
  (``mve_filter=exclude``); they never appear in the events listing.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

from ..fetcher import Fetcher

logger = logging.getLogger("world-intel-mcp.sources.kalshi")

_BASE = "https://api.elections.kalshi.com/trade-api/v2"
_SOURCE = "kalshi"
_PAGE_SIZE = 200
# Caps on caller-chosen counts. A category scan costs one /series call
# plus up to 5 /events pages (open) or 3 /markets pages (settled) per
# series, so MAX_SERIES bounds one scan at 126 requests (open) or 76
# (settled, before candlesticks).
MAX_PAGES = 25
MAX_SERIES = 25
MAX_RESULTS = 500
MAX_SETTLED = 100
_CANDLE_BUDGET = 9000  # API refuses a batch above 10,000 candlesticks (measured)

# Minimum spacing between Kalshi requests from one process. Kalshi has no
# entry in the fetcher's per-source limits, and two collector sources
# scanning the same series at once drew HTTP 429 (measured 2026-10-17:
# about 12 requests in 0.3 s). At 0.1 s spacing a 26-request category
# scan still drew one 429 on its 21st request, 2.0 s in; at 0.2 s the
# same scan drew none (26 requests in 5.0 s). Spacing comes before the
# fetcher's cache check, so a request repeated by a second caller is
# usually a cache hit.
_MIN_INTERVAL = 0.2
_pace_state: dict[str, Any] = {"loop": None, "lock": None, "last": 0.0}


async def _pace() -> None:
    loop = asyncio.get_running_loop()
    if _pace_state["loop"] is not loop:  # an asyncio.Lock is bound to one loop
        _pace_state.update(loop=loop, lock=asyncio.Lock())
    async with _pace_state["lock"]:
        wait = _MIN_INTERVAL - (time.monotonic() - _pace_state["last"])
        if wait > 0:
            await asyncio.sleep(wait)
        _pace_state["last"] = time.monotonic()

UNITS = {
    "price": (
        "Kalshi quotes YES contracts in US dollars per $1 payout (fields *_dollars, "
        "decimal strings); 0.63 means a 63% implied YES probability"
    ),
    "volume": "contracts (volume_fp, decimal strings); lifetime unless the field says 24h",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _num(value: Any) -> float | None:
    """Decimal string or number -> float; None when absent or unparseable."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def count_arg(value: Any, name: str, high: int) -> int:
    """A caller's count clamped to ``[1, high]``.

    Raises ValueError naming the argument when ``value`` is not a whole
    number (a string, None, a boolean, 2.5, NaN, infinity), so entry
    points can answer ``{"error": ...}`` before any request is made.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)) or (
        isinstance(value, float) and not value.is_integer()
    ):
        raise ValueError(f"{name} must be a whole number from 1 to {high} (got {value!r})")
    return max(1, min(int(value), high))


def implied_probability(market: dict) -> tuple[float | None, str]:
    """YES implied probability and the basis it came from.

    Midpoint of a two-sided book when both quotes are live (bid > 0 and
    ask < 1); otherwise the last trade; otherwise None ("no price").
    """
    bid = _num(market.get("yes_bid_dollars"))
    ask = _num(market.get("yes_ask_dollars"))
    last = _num(market.get("last_price_dollars"))
    if bid is not None and ask is not None and 0 < bid <= ask < 1:
        return round((bid + ask) / 2, 4), "bid_ask_midpoint"
    if last is not None and last > 0:
        return round(last, 4), "last_trade"
    return None, "no_price"


def normalize_market(market: dict, event: dict | None = None) -> dict:
    """One Kalshi market in the cross-venue shape plus venue fields."""
    event = event or {}
    prob, basis = implied_probability(market)
    event_ticker = market.get("event_ticker") or event.get("event_ticker") or ""
    series_ticker = event.get("series_ticker") or (event_ticker.split("-", 1)[0] if event_ticker else "")
    yes_side = market.get("yes_sub_title") or ""
    title = market.get("title") or ""
    return {
        "venue": "kalshi",
        "id": market.get("ticker", ""),
        "question": title,
        "context": event.get("title", ""),
        "yes_side": "" if yes_side.strip().lower() == title.strip().lower() else yes_side,
        "outcomes": ["Yes", "No"],
        "end_time": market.get("expected_expiration_time") or market.get("close_time"),
        "close_time": market.get("close_time"),
        "probability": prob,
        "price_basis": basis,
        "yes_bid": _num(market.get("yes_bid_dollars")),
        "yes_ask": _num(market.get("yes_ask_dollars")),
        "last_price": _num(market.get("last_price_dollars")),
        "volume": _num(market.get("volume_fp")) or 0.0,
        "volume_24h": _num(market.get("volume_24h_fp")) or 0.0,
        "open_interest": _num(market.get("open_interest_fp")),
        "event_ticker": event_ticker,
        "series_ticker": series_ticker,
        "category": event.get("category"),
        "status": market.get("status"),
        "url": f"https://kalshi.com/markets/{series_ticker.lower()}" if series_ticker else "",
    }


async def _get(fetcher: Fetcher, path: str, params: dict, cache_key: str, ttl: int) -> Any:
    await _pace()
    return await fetcher.get_json(
        f"{_BASE}{path}", source=_SOURCE, cache_key=cache_key, cache_ttl=ttl, params=params
    )


async def _events_pages(
    fetcher: Fetcher, params: dict, max_pages: int, key_prefix: str
) -> tuple[list[dict], int, bool, str | None]:
    """Follow the events cursor. Returns (events, pages, exhaustive, error)."""
    events: list[dict] = []
    cursor = ""
    pages = 0
    while pages < max_pages:
        q = dict(params, limit=str(_PAGE_SIZE), with_nested_markets="true")
        if cursor:
            q["cursor"] = cursor
        data = await _get(fetcher, "/events", q, f"{key_prefix}:{cursor or 'first'}", 300)
        if not isinstance(data, dict) or not isinstance(data.get("events"), list):
            return events, pages, False, (
                f"Kalshi /events page {pages + 1} unavailable (no live or cached data)"
            )
        pages += 1
        events.extend(e for e in data["events"] if isinstance(e, dict))
        cursor = data.get("cursor") or ""
        if not cursor:
            return events, pages, True, None
    return events, pages, False, None


async def _top_series(fetcher: Fetcher, category: str, series_limit: int) -> tuple[list[dict], str | None]:
    data = await _get(
        fetcher,
        "/series",
        {"category": category, "include_volume": "true"},
        f"kalshi:series:{category}",
        3600,
    )
    rows = data.get("series") if isinstance(data, dict) else None
    if rows is None and isinstance(data, dict) and "series" in data:
        rows = []  # a category with no series answers {"series": null}, HTTP 200 (measured)
    if not isinstance(rows, list):
        return [], f"Kalshi /series?category={category} unavailable (no live or cached data)"
    series = [
        s for s in rows
        if isinstance(s, dict) and s.get("ticker") and s.get("category") == category
    ]
    series.sort(key=lambda s: _num(s.get("volume_fp")) or 0.0, reverse=True)
    return series[:series_limit], None


def _no_series_gap(category: str) -> str:
    return (
        f"Kalshi lists no series in category {category!r}; category names are exact "
        "(e.g. 'Economics', 'Politics', 'Elections', 'Financials', 'Climate and Weather')"
    )


async def fetch_kalshi_open_markets(
    fetcher: Fetcher,
    category: str | None = None,
    series: str | None = None,
    pages: int = 3,
    series_limit: int = 10,
) -> dict:
    """Every open market the chosen scan reaches, normalized, unsorted.

    Scan modes: ``series`` reads that series' open events in full;
    ``category`` reads the category's ``series_limit`` highest-volume
    series; neither reads the first ``pages`` pages of all open events.
    ``pages`` and ``series_limit`` are clamped to 1..25.
    """
    pages = count_arg(pages, "pages", MAX_PAGES)
    series_limit = count_arg(series_limit, "series_limit", MAX_SERIES)
    gaps: list[str] = []
    errors: list[str] = []
    events: list[dict] = []
    scan: dict[str, Any]
    if series:
        events, n, exhaustive, err = await _events_pages(
            fetcher, {"status": "open", "series_ticker": series}, MAX_PAGES, f"kalshi:events:series:{series}"
        )
        scan = {"mode": "series", "series": series, "pages_fetched": n, "exhaustive": exhaustive}
        if err:
            errors.append(err)
    elif category:
        top, err = await _top_series(fetcher, category, series_limit)
        scan = {
            "mode": "category_top_series",
            "category": category,
            "series_ranked_by": "lifetime series volume (contracts)",
            "series_scanned": [s["ticker"] for s in top],
            "exhaustive": False,
        }
        if err:
            errors.append(err)
        elif not top:
            gaps.append(_no_series_gap(category))
        failed = []
        for s in top:
            ev, _, _, e_err = await _events_pages(
                fetcher, {"status": "open", "series_ticker": s["ticker"]}, 5, f"kalshi:events:series:{s['ticker']}"
            )
            if e_err:
                failed.append(s["ticker"])
            events.extend(ev)
        if failed:
            errors.append(f"open events unavailable for series {failed}")
    else:
        events, n, exhaustive, err = await _events_pages(
            fetcher, {"status": "open"}, pages, "kalshi:events:open"
        )
        scan = {
            "mode": "open_events_pages",
            "pages_fetched": n,
            "page_size": _PAGE_SIZE,
            "exhaustive": exhaustive,
            "api_order": "events by close time, furthest out first",
        }
        if err:
            errors.append(err)
        if not exhaustive and not err:
            gaps.append(
                f"scanned {n} page(s) of open events; more exist and were not read, "
                "so the ranking covers only the scanned events"
            )

    markets: list[dict] = []
    for ev in events:
        if category and ev.get("category") and ev["category"] != category:
            continue
        for m in ev.get("markets") or []:
            if isinstance(m, dict) and m.get("status") in (None, "active", "open"):
                markets.append(normalize_market(m, ev))
    scan["events_scanned"] = len(events)
    return {
        "markets": markets,
        "scan": scan,
        "errors": errors,
        "data_gaps": errors + gaps,
        "nothing_read": not events and bool(errors),
    }


async def fetch_kalshi_markets(
    fetcher: Fetcher,
    category: str | None = None,
    series: str | None = None,
    limit: int = 20,
    pages: int = 3,
    series_limit: int = 10,
    include_untraded: bool = False,
) -> dict:
    """Kalshi open markets ranked by 24h volume, then lifetime volume.

    Returns ``{"markets": [...], "count", "source": "kalshi", "scan",
    "units", "data_gaps", "timestamp"}``; on a total outage also
    ``error`` and ``degraded: True`` (never an empty success). ``limit``
    is clamped to 1..500; a count that is not a whole number is an
    ``error`` answered before any request.
    """
    try:
        limit = count_arg(limit, "limit", MAX_RESULTS)
        pages = count_arg(pages, "pages", MAX_PAGES)
        series_limit = count_arg(series_limit, "series_limit", MAX_SERIES)
    except ValueError as exc:
        return {"error": str(exc), "degraded": True, "source": "kalshi", "timestamp": _utc_now_iso()}
    raw = await fetch_kalshi_open_markets(
        fetcher, category=category, series=series, pages=pages, series_limit=series_limit
    )
    markets = raw["markets"]
    untraded = sum(1 for m in markets if m["volume"] <= 0)
    if not include_untraded:
        markets = [m for m in markets if m["volume"] > 0]
    markets.sort(key=lambda m: (m["volume_24h"], m["volume"]), reverse=True)
    out: dict[str, Any] = {
        "markets": markets[:limit],
        "count": min(len(markets), limit),
        "markets_considered": len(markets),
        "untraded_excluded": 0 if include_untraded else untraded,
        "source": "kalshi",
        "scan": raw["scan"],
        "units": UNITS,
        "data_gaps": raw["data_gaps"],
        "timestamp": _utc_now_iso(),
    }
    if raw["errors"]:
        out["degraded"] = True
        if raw["nothing_read"]:
            out["error"] = raw["errors"][0]
    return out


# ---------------------------------------------------------------------------
# Settled markets
# ---------------------------------------------------------------------------

def _ts(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


def _settled_record(m: dict, min_volume: float, min_open_hours: float) -> tuple[dict | None, str]:
    result = (m.get("result") or "").lower()
    volume = _num(m.get("volume_fp")) or 0.0
    if result not in ("yes", "no"):
        return None, "result"
    if volume < min_volume:
        return None, "volume"
    ref = _ts(m.get("close_time"))
    if ref is None:
        return None, "result"
    opened = _ts(m.get("open_time"))
    if min_open_hours and (opened is None or ref - opened < min_open_hours * 3600):
        return None, "too_short"
    return {
        "venue": "kalshi",
        "id": m.get("ticker", ""),
        "question": m.get("title", ""),
        "yes_side": m.get("yes_sub_title") or "",
        "event": m.get("event_ticker") or "",
        "outcome": 1 if result == "yes" else 0,
        "result": result,
        "volume": volume,
        "open_ts": opened,
        "reference_ts": ref,
        "reference": "close_time (trading end)",
        "settled_at": m.get("settlement_ts"),
    }, "ok"


async def fetch_kalshi_settled(
    fetcher: Fetcher,
    limit: int = 25,
    min_volume: float = 1000.0,
    series: str | None = None,
    category: str | None = None,
    series_limit: int = 10,
    pages: int = 10,
    min_open_hours: float = 0.0,
) -> dict:
    """Recently settled binary markets with a YES/NO result.

    Selection: settled markets (combos excluded) from one ``series``,
    or from the ``series_limit`` highest-volume series of ``category``,
    or else from the exchange-wide listing (first ``pages`` pages);
    kept when ``result`` is yes/no, lifetime volume >= ``min_volume``
    contracts and the market was open at least ``min_open_hours``
    before close (a market younger than the smallest offset can have no
    price at any offset). The most recently settled ``limit`` are kept.

    The exchange-wide listing is dominated by intraday sports markets
    (measured 2026-10-17: 1,000 rows spanned about 40 minutes and none
    was open 24h), which is why callers scoring at day-scale offsets
    pass a category or series. ``pages`` and ``series_limit`` are
    clamped to 1..25, ``limit`` to 1..100.
    """
    limit = count_arg(limit, "limit", MAX_SETTLED)
    pages = count_arg(pages, "pages", MAX_PAGES)
    series_limit = count_arg(series_limit, "series_limit", MAX_SERIES)
    candidates: list[dict] = []
    skipped = {"result": 0, "volume": 0, "too_short": 0}
    seen = 0
    fetched = 0
    errors: list[str] = []
    gaps: list[str] = []
    scope: list[str | None]
    if series:
        scope = [series]
    elif category:
        top, err = await _top_series(fetcher, category, series_limit)
        if err:
            errors.append(err)
        elif not top:
            gaps.append(_no_series_gap(category))
        scope = [s["ticker"] for s in top]
    else:
        scope = [None]

    for tick in scope:
        cursor = ""
        tick_pages = pages if tick is None else min(pages, 3)
        for _ in range(tick_pages):
            params = {"status": "settled", "mve_filter": "exclude", "limit": str(_PAGE_SIZE)}
            if tick:
                params["series_ticker"] = tick
            if cursor:
                params["cursor"] = cursor
            data = await _get(
                fetcher, "/markets", params, f"kalshi:settled:{tick or 'all'}:{cursor or 'first'}", 900
            )
            if not isinstance(data, dict) or not isinstance(data.get("markets"), list):
                errors.append(f"Kalshi settled markets unavailable for {tick or 'the exchange listing'}")
                break
            fetched += 1
            for m in data["markets"]:
                if not isinstance(m, dict):
                    continue
                seen += 1
                rec, why = _settled_record(m, min_volume, min_open_hours)
                if rec is None:
                    skipped[why] += 1
                else:
                    candidates.append(rec)
            cursor = data.get("cursor") or ""
            if not cursor or (tick is None and len(candidates) >= limit):
                break

    candidates.sort(key=lambda r: r["settled_at"] or "", reverse=True)
    return {
        "markets": candidates[:limit],
        "scope": {"series": series, "category": category, "series_read": [t for t in scope if t]},
        "listing_rows_read": seen,
        "skipped": skipped,
        "open_too_briefly_skipped": skipped["too_short"],
        "pages_fetched": fetched,
        "error": errors[0] if errors and not candidates else None,
        "errors": errors,
        "data_gaps": gaps,
    }


async def fetch_kalshi_price_history(
    fetcher: Fetcher,
    markets: list[dict],
    window_start_hours: float,
    window_end_hours: float,
) -> tuple[dict[str, list[tuple[int, float]]], list[str]]:
    """Hourly last-trade prices for each market over a window before close.

    The window is ``[reference - window_start_hours, reference -
    window_end_hours]`` per market, clipped to its open time. Uses the
    batch candlestick endpoint (no series ticker needed), splitting
    requests under the API's 10,000-candle cap. A candle's
    ``price.close_dollars`` is the last trade in that hour; hours with
    no trade carry no price and are skipped.

    Returns (``{ticker: [(end_period_ts, price)]}``, errors). A ticker
    absent from the dict had no readable history.
    """
    history: dict[str, list[tuple[int, float]]] = {}
    errors: list[str] = []
    batch: list[tuple[dict, int, int]] = []

    async def flush() -> None:
        nonlocal batch
        if not batch:
            return
        start = min(s for _, s, _ in batch)
        end = max(e for _, _, e in batch)
        tickers = ",".join(m["id"] for m, _, _ in batch)
        data = await _get(
            fetcher,
            "/markets/candlesticks",
            {"market_tickers": tickers, "start_ts": str(start), "end_ts": str(end), "period_interval": "60"},
            f"kalshi:candles:{tickers}:{start}:{end}",
            21600,
        )
        if not isinstance(data, dict) or not isinstance(data.get("markets"), list):
            errors.append(f"candlesticks unavailable for {len(batch)} market(s)")
        else:
            for entry in data["markets"]:
                if not isinstance(entry, dict):
                    continue
                points = []
                for c in entry.get("candlesticks") or []:
                    price = _num(((c or {}).get("price") or {}).get("close_dollars"))
                    ts = c.get("end_period_ts") if isinstance(c, dict) else None
                    if price is not None and isinstance(ts, int):
                        points.append((ts, price))
                history[entry.get("market_ticker", "")] = points
        batch = []

    for m in markets:
        ref = m["reference_ts"]
        start = ref - int(window_start_hours * 3600)
        if m.get("open_ts"):
            start = max(start, m["open_ts"])
        end = ref - int(window_end_hours * 3600)
        if end <= start:
            # Opened after the latest offset point: no history can exist,
            # which is "no price", not "history unavailable".
            history[m["id"]] = []
            continue
        # The batch shares one [start, end] range, so cost is the union.
        new_start = min([start] + [s for _, s, _ in batch])
        new_end = max([end] + [e for _, _, e in batch])
        cost = ((new_end - new_start) // 3600 + 1) * (len(batch) + 1)
        if batch and cost > _CANDLE_BUDGET:
            await flush()
        batch.append((m, start, end))
    await flush()
    return history, errors
