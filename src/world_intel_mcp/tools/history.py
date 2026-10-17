"""History tools: collection health, capture history, series, revisions.

Read the history store (``world_intel_mcp.history``) that the collector
writes into the cache DB. Each handler opens the store on the same
``db_path`` the runtime cache resolved, reads, and closes it; nothing here
fetches upstream data or writes history.
"""

from typing import Any

from mcp.types import Tool

from .. import runtime
from ..history import HistoryStore

_ACCRUAL = (
    "History accrues only while the collector (intel-collector, or intel_collect) "
    "runs; nothing before its first recorded cycle exists."
)

TOOLS: list[Tool] = [
    Tool(
        name="intel_collection_health",
        description=(
            "Per-source collection health from the collector's run log: last attempt, "
            "last success, consecutive failures, overdue against the expected cadence "
            "(default 300 s, overdue past 3 cadences), never-run sources, and history "
            "storage size. States what it cannot prove: a fresh capture does not mean "
            "upstream observations are current (see latest_observed_at). " + _ACCRUAL
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "description": "One collector source name (e.g. 'earthquakes'); omit for all",
                },
            },
        },
    ),
    Tool(
        name="intel_capture_history",
        description=(
            "A source's earlier collector captures, newest first: status (ok / degraded / "
            "error with reason), duration, record count, payload hash and whether it "
            "changed from the previous capture. Pass capture_id to replay one stored "
            "payload. " + _ACCRUAL
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "description": "Collector source name (e.g. 'space_weather')",
                },
                "limit": {
                    "type": "integer",
                    "description": "Captures to return (default 20, max 1000)",
                    "default": 20,
                },
                "capture_id": {
                    "type": "integer",
                    "description": "Return this capture with its stored payload body",
                },
            },
            "required": ["source"],
        },
    ),
    Tool(
        name="intel_series_history",
        description=(
            "Observations of one series with all three dates: observed_at (period the "
            "value describes, from the payload; null when the payload has none), "
            "published_at (publisher release, when known) and collected_at (when saved). "
            "Omit series_key to list available series. " + _ACCRUAL
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "series_key": {
                    "type": "string",
                    "description": "Series key from the listing, e.g. 'space_weather:kp'",
                },
                "source": {
                    "type": "string",
                    "description": "Filter the listing to one source",
                },
                "limit": {
                    "type": "integer",
                    "description": "Rows to return (default 100 points or 200 series, max 1000)",
                },
            },
        },
    ),
    Tool(
        name="intel_revisions",
        description=(
            "Values for the same observed_at that a later capture changed (for example "
            "USGS magnitude revisions, NOAA Kp updates), with both values and both "
            "collection times. Only changes between this server's own captures are "
            "visible. " + _ACCRUAL
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "source": {"type": "string", "description": "Filter to one source"},
                "series_key": {"type": "string", "description": "Filter to one series"},
                "limit": {
                    "type": "integer",
                    "description": "Revisions to return (default 100, max 1000)",
                    "default": 100,
                },
            },
        },
    ),
]


def _store() -> HistoryStore:
    return HistoryStore(runtime.cache.db_path)


async def _collection_health(arguments: dict[str, Any]) -> Any:
    from ..collector import SOURCES

    store = _store()
    try:
        return store.collection_health(
            [name for name, _, _, _ in SOURCES], source=arguments.get("source")
        )
    finally:
        store.close()


async def _capture_history(arguments: dict[str, Any]) -> Any:
    if not arguments.get("source"):
        return {"error": "source is required (a collector source name)"}
    store = _store()
    try:
        return store.capture_history(
            arguments["source"],
            limit=arguments.get("limit", 20),
            capture_id=arguments.get("capture_id"),
        )
    finally:
        store.close()


async def _series_history(arguments: dict[str, Any]) -> Any:
    store = _store()
    try:
        series_key = arguments.get("series_key")
        return store.series_history(
            series_key,
            source=arguments.get("source"),
            limit=arguments.get("limit", 100 if series_key else 200),
        )
    finally:
        store.close()


async def _revisions(arguments: dict[str, Any]) -> Any:
    store = _store()
    try:
        return store.revisions(
            source=arguments.get("source"),
            series_key=arguments.get("series_key"),
            limit=arguments.get("limit", 100),
        )
    finally:
        store.close()


HANDLERS = {
    "intel_collection_health": _collection_health,
    "intel_capture_history": _capture_history,
    "intel_series_history": _series_history,
    "intel_revisions": _revisions,
}
