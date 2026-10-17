"""Day timeline tool: what the history store recorded for one UTC day.

Reads the history store that the collector writes into the cache DB, on
the ``db_path`` the runtime cache resolved, through a read-only
connection (``world_intel_mcp.history_timeline``); nothing here fetches
upstream data or writes history.
"""

import time
from typing import Any

from mcp.types import Tool

from .. import runtime
from ..history_timeline import (
    DEFAULT_LIMIT,
    DEFAULT_PER_SOURCE_LIMIT,
    MAX_LIMIT,
    MAX_PER_SOURCE_LIMIT,
    day_timeline,
)

TOOLS: list[Tool] = [
    Tool(
        name="intel_day_timeline",
        description=(
            "What the history store recorded for one UTC calendar day (YYYY-MM-DD; today "
            "allowed, a future day refused). by=collected (default): per source, that day's "
            "collector captures from the run log (attempts; ok / degraded / error counts with "
            "reasons; changed vs unchanged payloads; first and last capture; observations "
            "stored), sources with no capture that day and why, whether the collector ran at "
            "all, and the observations saved that day. by=observed: values whose observed_at "
            "falls on the day (dates and UTC instants); month and year values containing the "
            "day are listed apart as period values; values without observed_at are excluded "
            "and counted. Items carry observed_at, published_at and collected_at where known. "
            "Says which records are past retention (payload bodies 7 days, run log 30, "
            "observations 365 by default) instead of reading them as a quiet day. Output is "
            "capped and truncation is stated. Read-only: never fetches. History accrues only "
            "while the collector (intel-collector, or intel_collect) runs."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "day": {
                    "type": "string",
                    "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
                    "description": "UTC calendar date, YYYY-MM-DD (e.g. '2026-10-17')",
                },
                "by": {
                    "type": "string",
                    "enum": ["collected", "observed"],
                    "default": "collected",
                    "description": "collected: what was captured that day; observed: values "
                    "whose observed_at is that day",
                },
                "source": {
                    "type": "string",
                    "description": "One collector source name (e.g. 'space_weather'); omit for all",
                },
                "limit": {
                    "type": "integer",
                    "description": f"Most items listed in all (default {DEFAULT_LIMIT}, "
                    f"max {MAX_LIMIT})",
                    "default": DEFAULT_LIMIT,
                },
                "per_source_limit": {
                    "type": "integer",
                    "description": f"Most items listed per source (default "
                    f"{DEFAULT_PER_SOURCE_LIMIT}, max {MAX_PER_SOURCE_LIMIT})",
                    "default": DEFAULT_PER_SOURCE_LIMIT,
                },
            },
            "required": ["day"],
        },
    ),
]


async def _day_timeline(arguments: dict[str, Any]) -> Any:
    from ..collector import SOURCES

    if "day" not in arguments:
        return {"error": "day is required (YYYY-MM-DD, a UTC calendar date)"}
    return day_timeline(
        runtime.cache.db_path,
        arguments.get("day"),
        by=arguments.get("by", "collected"),
        source=arguments.get("source"),
        roster=[name for name, _, _, _ in SOURCES],
        limit=arguments.get("limit", DEFAULT_LIMIT),
        per_source_limit=arguments.get("per_source_limit", DEFAULT_PER_SOURCE_LIMIT),
        now=time.time(),
    )


HANDLERS = {
    "intel_day_timeline": _day_timeline,
}
