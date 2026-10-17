"""Liquidity and money tools: liquidity board, net liquidity, money supply,
and keyless series access to FRED liquidity series and the ECB Data
Portal (Phase 29). See sources/liquidity.py for units, alignment and the
blocked upstreams (ICI fund flows, FINRA margin statistics).
"""

from typing import Any

from mcp.types import Tool

from .. import runtime
from ..sources import liquidity

_LIMIT = {
    "type": "integer",
    "minimum": 0,
    "description": "Newest points to return (default 52); 0 returns the full history",
    "default": liquidity.DEFAULT_LIMIT,
}
_START = {
    "type": "string",
    "description": "Optional ISO date YYYY-MM-DD: only points observed on or after it",
}

TOOLS: list[Tool] = [
    Tool(
        name="intel_liquidity_board",
        description=(
            "Liquidity board: latest reading and change vs the previous observation for "
            "the Fed balance sheet (WALCL), Treasury General Account (WDTGAL), overnight "
            "reverse repo (RRPONTSYD), US net liquidity, US M2, euro area M1/M2/M3 and "
            "Eurosystem total assets. Each row names its units, observation date, "
            "frequency and staleness. Keyless (FRED graph CSV + ECB Data Portal)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "sort": {
                    "type": "string",
                    "enum": ["biggest_move", "newest"],
                    "description": (
                        "biggest_move (largest absolute change, in billions of each row's currency, "
                        "no FX conversion; percent change does not rank) or newest (latest period end)"
                    ),
                    "default": "biggest_move",
                },
            },
        },
    ),
    Tool(
        name="intel_net_liquidity",
        description=(
            "US net liquidity, weekly: Fed total assets (WALCL) minus Treasury General "
            "Account (WDTGAL, Wednesday level) minus overnight reverse repo (RRPONTSYD), "
            "all converted to USD billions and aligned on the WALCL Wednesday. Every point "
            "carries its components and the RRP date used; the response states the "
            "formula, unit conversion and alignment rule."
        ),
        inputSchema={
            "type": "object",
            "properties": {"limit": _LIMIT, "start": _START},
        },
    ),
    Tool(
        name="intel_money_supply",
        description=(
            "Money supply: US M2 (FRED M2SL, USD billions, monthly, seasonally adjusted) and "
            "euro area M1, M2 and M3 (ECB BSI: end-of-month stocks, monthly flows and annual "
            "growth rates). Aggregates nest and are never summed; US M3 is discontinued "
            "and says so."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "region": {
                    "type": "string",
                    "enum": ["all", "us", "euro_area"],
                    "description": "Which region (default: all)",
                    "default": "all",
                },
                "limit": {
                    **_LIMIT,
                    "description": "Newest points per series (default 13); 0 returns the full history",
                    "default": 13,
                },
            },
        },
    ),
    Tool(
        name="intel_fed_liquidity_series",
        description=(
            "One keyless FRED liquidity series with full history available: WALCL (Fed "
            "total assets), WDTGAL (TGA, Wednesday level), WTREGEN (TGA, week average), "
            "RRPONTSYD (overnight reverse repo), M2SL (US M2). Points carry observation "
            "date, value and units; blank source dates are counted, never read as zero."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "series_id": {
                    "type": "string",
                    "enum": sorted(liquidity.FRED_SERIES),
                    "description": "FRED series id",
                },
                "limit": _LIMIT,
                "start": _START,
            },
            "required": ["series_id"],
        },
    ),
    Tool(
        name="intel_ecb_series",
        description=(
            "ECB Data Portal series by SDMX key, keyless: BSI (euro area monetary "
            "aggregates and MFI balance sheets) or ILM (Eurosystem weekly financial "
            "statement). Units, scale and provisional flags come from the response; each "
            "series is typed as amount_outstanding, flow, growth_rate, index or unknown "
            "(a data type not recognized, named in data_gaps). "
            "Example key: BSI M.U2.Y.V.M30.X.1.U2.2300.Z01.E (euro area M3 stock)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "dataset": {
                    "type": "string",
                    "enum": sorted(liquidity.ECB_DATASETS),
                    "description": "ECB dataset (dataflow) id",
                    "default": "BSI",
                },
                "key": {
                    "type": "string",
                    "description": liquidity.ECB_KEY_RULES,
                },
                "limit": {
                    **_LIMIT,
                    "description": "Newest points per series (default 24); 0 returns the full history",
                    "default": 24,
                },
                "start": {
                    **_START,
                    "description": (
                        "Optional ISO date YYYY-MM-DD, not after today (UTC): only points observed "
                        "on or after it; a later date is refused without a request"
                    ),
                },
            },
            "required": ["key"],
        },
    ),
]


async def _liquidity_board(arguments: dict[str, Any]) -> Any:
    return await liquidity.fetch_liquidity_board(
        runtime.fetcher, sort=arguments.get("sort", "biggest_move")
    )


async def _net_liquidity(arguments: dict[str, Any]) -> Any:
    return await liquidity.fetch_net_liquidity(
        runtime.fetcher,
        limit=arguments.get("limit", liquidity.DEFAULT_LIMIT),
        start=arguments.get("start"),
    )


async def _money_supply(arguments: dict[str, Any]) -> Any:
    return await liquidity.fetch_money_supply(
        runtime.fetcher,
        region=arguments.get("region", "all"),
        limit=arguments.get("limit", 13),
    )


async def _fed_liquidity_series(arguments: dict[str, Any]) -> Any:
    return await liquidity.fetch_fed_liquidity_series(
        runtime.fetcher,
        series_id=arguments.get("series_id", ""),
        limit=arguments.get("limit", liquidity.DEFAULT_LIMIT),
        start=arguments.get("start"),
    )


async def _ecb_series(arguments: dict[str, Any]) -> Any:
    return await liquidity.fetch_ecb_series(
        runtime.fetcher,
        dataset=arguments.get("dataset", "BSI"),
        key=arguments.get("key", ""),
        limit=arguments.get("limit", 24),
        start=arguments.get("start"),
    )


HANDLERS = {
    "intel_liquidity_board": _liquidity_board,
    "intel_net_liquidity": _net_liquidity,
    "intel_money_supply": _money_supply,
    "intel_fed_liquidity_series": _fed_liquidity_series,
    "intel_ecb_series": _ecb_series,
}
