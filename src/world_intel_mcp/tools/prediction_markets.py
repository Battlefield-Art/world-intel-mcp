"""Prediction-market tools beyond Polymarket's open list: Kalshi open
markets, the same event on Kalshi and Polymarket side by side, and
settled markets scored against their pre-settlement prices (Brier).

Added in ROADMAP Phase 29. The Polymarket open-markets tool
(``intel_prediction_markets``) stays in ``tools/society.py`` unchanged.
Every count and number states its bounds in the input schema; the
source functions clamp counts again, so no caller reaches more Kalshi
series or pages than the stated maximum.
"""

from typing import Any

from mcp.types import Tool

from .. import runtime
from ..analysis.prediction_matching import DEFAULT_MIN_CONFIDENCE
from ..sources import kalshi, prediction, prediction_cross

TOOLS: list[Tool] = [
    Tool(
        name="intel_kalshi_markets",
        description=(
            "Kalshi open prediction markets from its public market-data API (no account), ranked by "
            "24h then lifetime volume. Prices are YES implied probabilities (Kalshi quotes dollars per "
            "$1 contract; raw bid/ask/last kept). Filter by category (e.g. 'Economics', 'Politics', "
            "'Elections', 'Financials', 'Climate and Weather') or series ticker (e.g. 'KXFED'). The "
            "response states which events were scanned and whether the scan was exhaustive; data "
            "served from expired cache after a failed fetch is marked stale and degraded, and each "
            "market from such a copy carries stale: true and its age."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "Kalshi category; scans its highest-volume series"},
                "series": {"type": "string", "description": "Kalshi series ticker, e.g. KXFED (scanned in full)"},
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": kalshi.MAX_RESULTS,
                    "description": f"Markets to return (default 20, max {kalshi.MAX_RESULTS})",
                    "default": 20,
                },
                "pages": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": kalshi.MAX_PAGES,
                    "description": (
                        f"Unfiltered scan only: pages of 200 open events to read (default 3, max {kalshi.MAX_PAGES})"
                    ),
                    "default": 3,
                },
                "series_limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": kalshi.MAX_SERIES,
                    "description": (
                        f"Category scan only: top series by lifetime volume to read (default 10, max {kalshi.MAX_SERIES})"
                    ),
                    "default": 10,
                },
                "include_untraded": {
                    "type": "boolean",
                    "description": "Keep markets with zero lifetime volume (default false)",
                    "default": False,
                },
            },
        },
    ),
    Tool(
        name="intel_prediction_compare",
        description=(
            "The same event on Kalshi and Polymarket side by side: deterministic matching on normalized "
            "titles, quantities (value, unit, to/by, order), years, months, days, clock times, end "
            "dates, negation, rate actions (cut/raise/hold), directions (above/below), opposite words "
            "(rise/fall, before/after, ...), distinguishing names (House/Senate, Fed/BoJ, Bitcoin/"
            "Bitcoin Cash) and outcome side, with a match_confidence (an ordinal score for ranking "
            "pairs, not a probability) and the reasons for each pair. Below min_confidence a pair is "
            "not a match. Returns matched pairs with the price gap, each side's price basis, spread "
            "and last-trade time with its lookup status (a pair is flagged and ranked after comparable "
            "ones when either side is not a live two-sided quote midpoint, comes from an expired "
            "cached copy or its spread is wide), the closest rejected pairs with why, and unmatched "
            "counts per venue."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "Kalshi category to compare (e.g. 'Economics')"},
                "series": {"type": "string", "description": "Kalshi series ticker to compare (e.g. KXFEDDECISION)"},
                "kalshi_pages": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": kalshi.MAX_PAGES,
                    "description": f"Unfiltered Kalshi scan pages (default 3, max {kalshi.MAX_PAGES})",
                    "default": 3,
                },
                "series_limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": kalshi.MAX_SERIES,
                    "description": f"Kalshi category scan: top series (default 10, max {kalshi.MAX_SERIES})",
                    "default": 10,
                },
                "polymarket_limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": prediction.MAX_OPEN_MARKETS,
                    "description": (
                        "Polymarket open markets by 24h volume to compare "
                        f"(default 300, max {prediction.MAX_OPEN_MARKETS})"
                    ),
                    "default": 300,
                },
                "min_confidence": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1,
                    "description": (
                        f"Match threshold on the ordinal match score, 0..1 (default {DEFAULT_MIN_CONFIDENCE})"
                    ),
                    "default": DEFAULT_MIN_CONFIDENCE,
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": prediction_cross.MAX_PAIRS,
                    "description": f"Matched pairs to return (default 25, max {prediction_cross.MAX_PAIRS})",
                    "default": 25,
                },
            },
        },
    ),
    Tool(
        name="intel_prediction_settled",
        description=(
            "Recently settled Kalshi and Polymarket markets with their outcome and the YES price at fixed "
            "offsets before close (from each venue's price-history endpoint), plus a Brier score per venue "
            "and offset with sample size, selection and exclusions stated. A venue whose price history is "
            "unavailable is not scored, and a history served from expired cache is excluded. The two "
            "venues' samples are different markets with different reference times (Kalshi close_time; "
            "Polymarket the earlier of endDate and closedTime), stated in the response."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": kalshi.MAX_SETTLED,
                    "description": f"Settled markets per venue (default 25, max {kalshi.MAX_SETTLED})",
                    "default": 25,
                },
                "offsets_hours": {
                    "type": "array",
                    "maxItems": prediction_cross.MAX_OFFSETS,
                    "items": {"type": "number", "exclusiveMinimum": 0, "maximum": prediction_cross.MAX_OFFSET_HOURS},
                    "description": (
                        "Hours before the reference time to read the price at (default [24, 168], also "
                        f"for an empty list; up to {prediction_cross.MAX_OFFSETS} values, each above 0 and at "
                        f"most {prediction_cross.MAX_OFFSET_HOURS})"
                    ),
                },
                "kalshi_min_volume": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": prediction_cross.MAX_VOLUME_FLOOR,
                    "description": "Kalshi volume floor, contracts (default 1000)",
                    "default": 1000,
                },
                "polymarket_min_volume": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": prediction_cross.MAX_VOLUME_FLOOR,
                    "description": "Polymarket volume floor, USDC (default 10000)",
                    "default": 10000,
                },
                "kalshi_category": {
                    "type": "string",
                    "description": (
                        "Kalshi sample: settled markets of this category's highest-volume series "
                        "(default 'Economics'; the exchange-wide listing is intraday sports)"
                    ),
                    "default": "Economics",
                },
                "kalshi_series_limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": kalshi.MAX_SERIES,
                    "description": f"Kalshi category sample: top series to read (default 10, max {kalshi.MAX_SERIES})",
                    "default": 10,
                },
                "kalshi_series": {"type": "string", "description": "Restrict the Kalshi sample to one series ticker"},
                "max_staleness_hours": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": prediction_cross.MAX_STALENESS_HOURS,
                    "description": (
                        "Oldest last price allowed to stand for an offset "
                        f"(default 168, max {prediction_cross.MAX_STALENESS_HOURS})"
                    ),
                    "default": 168,
                },
            },
        },
    ),
]


async def _kalshi_markets(arguments: dict[str, Any]) -> Any:
    return await kalshi.fetch_kalshi_markets(
        runtime.fetcher,
        category=arguments.get("category"),
        series=arguments.get("series"),
        limit=arguments.get("limit", 20),
        pages=arguments.get("pages", 3),
        series_limit=arguments.get("series_limit", 10),
        include_untraded=bool(arguments.get("include_untraded", False)),
    )


async def _prediction_compare(arguments: dict[str, Any]) -> Any:
    return await prediction_cross.fetch_prediction_compare(
        runtime.fetcher,
        category=arguments.get("category"),
        series=arguments.get("series"),
        kalshi_pages=arguments.get("kalshi_pages", 3),
        series_limit=arguments.get("series_limit", 10),
        polymarket_limit=arguments.get("polymarket_limit", 300),
        min_confidence=arguments.get("min_confidence", DEFAULT_MIN_CONFIDENCE),
        limit=arguments.get("limit", 25),
    )


async def _prediction_settled(arguments: dict[str, Any]) -> Any:
    return await prediction_cross.fetch_prediction_settled(
        runtime.fetcher,
        limit=arguments.get("limit", 25),
        offsets_hours=arguments.get("offsets_hours"),
        kalshi_min_volume=arguments.get("kalshi_min_volume", 1000),
        polymarket_min_volume=arguments.get("polymarket_min_volume", 10000),
        kalshi_series=arguments.get("kalshi_series"),
        max_staleness_hours=arguments.get("max_staleness_hours", 168),
        kalshi_category=arguments.get("kalshi_category", "Economics"),
        kalshi_series_limit=arguments.get("kalshi_series_limit", 10),
    )


HANDLERS = {
    "intel_kalshi_markets": _kalshi_markets,
    "intel_prediction_compare": _prediction_compare,
    "intel_prediction_settled": _prediction_settled,
}
