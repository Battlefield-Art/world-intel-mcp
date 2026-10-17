"""Lagged connections: correlation between stored series at leads and lags.

Reads the history store (``world_intel_mcp.history``) on the same
``db_path`` the runtime cache resolved and runs
``analysis/lagged_connections.py`` over it; nothing here fetches upstream
data or writes history.
"""

import asyncio
from typing import Any

from mcp.types import Tool

from .. import runtime
from ..analysis import lagged_connections as lc
from ..history import HistoryStore

TOOLS: list[Tool] = [
    Tool(
        name="intel_lagged_connections",
        description=(
            f"Correlation between 2 to {lc.MAX_SERIES} series in the history store (keys as "
            "intel_series_history lists them), aligned on a UTC day, week or month grid and "
            "tested at every lag from -max_lag to +max_lag. Default transform is period-over-"
            "period changes, because levels of trending series correlate spuriously; 'levels' "
            "on request. Per pair: the lag with the strongest evidence (smallest p) with "
            "Pearson r, overlap n, two-sided p from Student's t treating periods as "
            "independent, p_ar1 recomputed with an AR(1) effective sample size from both "
            "series' lag-1 autocorrelations, and the Benjamini-Hochberg q and q_ar1 across "
            f"every test in the call. Significant means q_ar1 <= {lc.DEFAULT_Q:g} by default "
            "(significant_if_independent applies it to q); serial_correlation_dominates and "
            "data_gaps name autocorrelated series, and an effective sample below 3 is "
            "reported as no evidence. "
            "Lag +k compares A at period t with B at t+k, so A leads B by k. Pair/lag "
            f"overlaps under {lc.DEFAULT_MIN_OVERLAP} points are listed as not tested. Revised "
            "points use the latest vintage; rows without observed_at (Yahoo quotes) are not "
            "used. States what it cannot prove: correlation is not causation and a lead is not "
            "a causal direction. History accrues only while the collector (intel-collector, "
            "or intel_collect) runs; nothing before its first recorded cycle exists."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "series_keys": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        f"2 to {lc.MAX_SERIES} series keys, e.g. ['space_weather:kp', "
                        "'central_bank_rates:federal_funds_rate']; pairs are formed in this order"
                    ),
                },
                "grid": {
                    "type": "string",
                    "enum": list(lc.GRIDS),
                    "description": "UTC calendar grid (default day; weeks run Monday to Sunday)",
                    "default": lc.DEFAULT_GRID,
                },
                "transform": {
                    "type": "string",
                    "enum": list(lc.TRANSFORMS),
                    "description": "changes (default, period-over-period differences) or levels",
                    "default": lc.DEFAULT_TRANSFORM,
                },
                "max_lag": {
                    "type": "integer",
                    "description": (
                        f"Largest lead or lag in grid periods (default {lc.DEFAULT_MAX_LAG}, "
                        f"clamped to 0..{lc.MAX_LAG_CAP}, reduced to keep a call within "
                        f"{lc.MAX_TESTS:,} tests)"
                    ),
                    "default": lc.DEFAULT_MAX_LAG,
                },
                "min_overlap": {
                    "type": "integer",
                    "description": (
                        f"Fewest overlapping periods a pair/lag needs to be tested (default "
                        f"{lc.DEFAULT_MIN_OVERLAP}, clamped to {lc.MIN_OVERLAP_FLOOR}.."
                        f"{lc.MAX_PERIODS})"
                    ),
                    "default": lc.DEFAULT_MIN_OVERLAP,
                },
                "q": {
                    "type": "number",
                    "description": (
                        f"False discovery rate threshold applied to q_ar1 (default "
                        f"{lc.DEFAULT_Q:g}, clamped to {lc.Q_FLOOR:g}..{lc.Q_CAP:g})"
                    ),
                    "default": lc.DEFAULT_Q,
                },
                "start": {
                    "type": "string",
                    "description": (
                        "First date YYYY-MM-DD; its grid period and later are used (moved "
                        f"later, and reported, when the window would exceed {lc.MAX_PERIODS:,} "
                        "periods)"
                    ),
                },
                "end": {
                    "type": "string",
                    "description": "Last date YYYY-MM-DD; its grid period and earlier are used",
                },
            },
            "required": ["series_keys"],
        },
    ),
]


def _run(db_path: Any, arguments: dict[str, Any]) -> dict:
    # Opened and closed in the worker thread: sqlite3 connections stay on
    # the thread that created them.
    store = HistoryStore(db_path)
    try:
        return lc.lagged_connections(
            store,
            arguments.get("series_keys"),
            grid=arguments.get("grid"),
            transform=arguments.get("transform"),
            max_lag=arguments.get("max_lag"),
            min_overlap=arguments.get("min_overlap"),
            q=arguments.get("q"),
            start=arguments.get("start"),
            end=arguments.get("end"),
        )
    finally:
        store.close()


async def _lagged_connections(arguments: dict[str, Any]) -> Any:
    # Up to 1,000 tests over up to 3,000 periods is CPU-bound for a moment,
    # so it runs off the event loop. The path is resolved here, on the loop
    # thread: a first touch of runtime.cache opens the shared Cache, whose
    # sqlite connection must not be born in a worker thread.
    db_path = runtime.cache.db_path
    return await asyncio.to_thread(_run, db_path, arguments)


HANDLERS = {
    "intel_lagged_connections": _lagged_connections,
}
