"""History backfill tool: load the history FRED or the ECB Data Portal already serve into the history store.

The one history tool that fetches upstream and writes:
``world_intel_mcp.history_backfill`` reads FRED graph CSV or ECB Data
Portal series through the runtime Fetcher (rate floor, breaker, cache)
and stores the points in the cache DB's history store under
``backfill:fred`` or ``backfill:ecb``. Its caps are the module's, so the
CLI (``intel history backfill``) and this tool load at most the same per call.
"""

from typing import Any

from mcp.types import Tool

from .. import history_backfill as backfill
from .. import runtime
from ..history import HistoryStore

TOOLS: list[Tool] = [
    Tool(
        name="intel_history_backfill",
        description=(
            "Load the history FRED (graph CSV) or the ECB Data Portal already serve into the "
            "history store, for intel_series_history and intel_revisions: one capture per "
            "request (a FRED id, or an ECB key that may match several series) under "
            "backfill:fred or backfill:ecb, observed_at from the publisher's "
            "dates, published_at null (neither names a release date), collected_at the "
            "backfill time. Values are the publisher's current vintage, so revisions made "
            "before the first backfill are invisible; a re-run stores only new or changed "
            "values, and a changed value is a revision. FRED missing values are skipped and "
            "counted; ECB missing values cannot be counted (null). A failed fetch, or one "
            "answered only from a stale cache copy, is an "
            "error capture and stores no values. This tool "
            "writes to the history store and sends requests upstream, so it is capped: at "
            f"most {backfill.MAX_FRED_SERIES} FRED series or {backfill.MAX_ECB_KEYS} ECB keys "
            f"({backfill.MAX_ECB_COMBINATIONS} series each) per call, the newest "
            f"{backfill.MAX_POINTS:,} points per series and {backfill.MAX_POINTS_PER_CALL:,} "
            "per call. A repeat within the cache window (FRED 1 h, ECB 3 h) reads the cached "
            "response and sends no request."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "provider": {
                    "type": "string",
                    "enum": ["fred", "ecb"],
                    "description": "fred (FRED graph CSV) or ecb (ECB Data Portal, BSI and ILM)",
                },
                "series": {
                    "type": "array",
                    "items": {"type": "string"},
                    "maxItems": backfill.MAX_FRED_SERIES,
                    "description": (
                        "FRED series ids (e.g. ['WALCL', 'RRPONTSYD']) or ECB series keys "
                        "DATASET.KEY (e.g. ['BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.E'])"
                    ),
                },
                "start": {
                    "type": "string",
                    "description": "Only observations on or after this date, YYYY-MM-DD",
                },
            },
            "required": ["provider", "series"],
        },
    ),
]


async def _history_backfill(arguments: dict[str, Any]) -> Any:
    provider = str(arguments.get("provider") or "").strip().lower()
    if provider not in ("fred", "ecb"):
        return {"error": f"provider must be 'fred' or 'ecb', got {arguments.get('provider')!r}"}
    run = backfill.backfill_fred if provider == "fred" else backfill.backfill_ecb
    store = HistoryStore(runtime.cache.db_path)
    try:
        return await run(runtime.fetcher, store, arguments.get("series"), arguments.get("start"))
    finally:
        store.close()


HANDLERS = {"intel_history_backfill": _history_backfill}
