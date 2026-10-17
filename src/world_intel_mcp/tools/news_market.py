"""News vs market reaction tool (Phase 29).

Same-session moves of the companies and countries named in recent story
clusters; see ``analysis/news_market.py`` for the method.
"""

from typing import Any

from mcp.types import Tool

from .. import runtime

TOOLS: list[Tool] = [
    Tool(
        name="intel_news_market_reaction",
        description=(
            "Companies and countries named in recent news story clusters, mapped to tickers "
            "from the entity table (US tickers annotated with their SEC registrant when SEC "
            "lists them) and country indices, with the move on the story's market session from "
            "Yahoo daily bars: stock, benchmark (SPY or the exchange country's index) and excess "
            "return; country index return with a trailing z-score. A story after the close maps "
            "to the next session, weekends and holidays to the next trading day, all in the "
            "exchange's timezone; a session not yet closed or not yet final is pending. Unmapped "
            "and ambiguous names are listed with reasons; a failed quote or an expired cached "
            "copy is a data gap, never a 0% move. Same-session co-movement, not causation; not "
            "a trading signal."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "RSS feed category filter (finance, technology, geopolitics, energy, etc.). Default: all",
                },
                "limit": {
                    "type": "integer",
                    "description": "News items to cluster (default 150, max 300)",
                    "default": 150,
                },
                "threshold": {
                    "type": "number",
                    "description": "Jaccard similarity threshold for clustering (default 0.25)",
                    "default": 0.25,
                },
                "min_cluster_size": {
                    "type": "integer",
                    "description": "Minimum articles per story (default 2; 1 includes single-article stories)",
                    "default": 2,
                },
                "include_countries": {
                    "type": "boolean",
                    "description": "Also measure the primary index of countries named (default true)",
                    "default": True,
                },
                "max_symbols": {
                    "type": "integer",
                    "description": "Cap on tickers and indices fetched in one call (default 40, max 60)",
                    "default": 40,
                },
                "max_age_hours": {
                    "type": "number",
                    "description": "Leave out stories older than this many hours (default 168, max 2160)",
                    "default": 168,
                },
            },
        },
    ),
]


async def _news_market_reaction(arguments: dict[str, Any]) -> Any:
    from ..analysis.news_market import fetch_news_market_reaction

    extra: dict[str, Any] = {}
    if arguments.get("max_age_hours") is not None:
        extra["max_age_hours"] = arguments["max_age_hours"]
    return await fetch_news_market_reaction(
        runtime.fetcher,
        category=arguments.get("category"),
        limit=arguments.get("limit", 150),
        threshold=arguments.get("threshold", 0.25),
        min_cluster_size=arguments.get("min_cluster_size", 2),
        include_countries=arguments.get("include_countries", True),
        max_symbols=arguments.get("max_symbols", 40),
        **extra,
    )


HANDLERS = {
    "intel_news_market_reaction": _news_market_reaction,
}
