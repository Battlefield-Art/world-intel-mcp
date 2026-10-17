"""News-signal tools (Phase 29): hostility timeline, ranked situations,
top stories by outlet breadth. Logic lives in analysis/news_signals.py;
handlers reach shared infrastructure via ``runtime``.
"""

from typing import Any

from mcp.types import Tool

from .. import runtime

TOOLS: list[Tool] = [
    Tool(
        name="intel_hostility_timeline",
        description=(
            "Per-day news coverage of a country, or of a pair of countries, from GDELT: "
            "article count, articles using threat / sanction / force / criticism terms "
            "(GDELT keyword match, listed in the response), and GDELT average tone. "
            "A failed call, an expired cached copy, or a day GDELT did not return is "
            "null and listed in data_gaps (column_status per call), never zero; "
            "partial days are flagged per column."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "country": {
                    "type": "string",
                    "description": (
                        "Country name, or an ISO3 or ISO2 code from the 22-country table "
                        "(e.g. 'Iran', 'IRN' or 'IR'). Other names are searched verbatim; "
                        "other 2- and 3-letter codes are refused."
                    ),
                },
                "other": {
                    "type": "string",
                    "description": (
                        "Optional second country, same forms as country: coverage naming both."
                    ),
                },
                "days": {
                    "type": "integer",
                    "description": "Days back, including today (default: 14, max: 90).",
                    "default": 14,
                },
            },
            "required": ["country"],
        },
    ),
    Tool(
        name="intel_situations",
        description=(
            "Ranked situations between countries (headlines naming two or more) and "
            "inside a country (headlines naming one) over a recent window of the RSS "
            "feeds. Sort by most covered, most hostile (headline keyword heuristic), "
            "or biggest change against the preceding window; the response defines "
            "each measure and names excluded feeds. When no feed covers both windows, "
            "change is null with change_measured false and a change sort ranks by "
            "coverage (sort_applied)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "hours": {
                    "type": "number",
                    "description": "Window length in hours, 1 to 168 (default: 24); the prior window is the same length.",
                    "default": 24,
                },
                "sort": {
                    "type": "string",
                    "enum": ["covered", "hostile", "change"],
                    "description": "Ranking (default: covered).",
                    "default": "covered",
                },
                "kind": {
                    "type": "string",
                    "enum": ["all", "between", "inside"],
                    "description": "Situation kind filter (default: all).",
                    "default": "all",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max situations (default: 20).",
                    "default": 20,
                },
                "min_articles": {
                    "type": "integer",
                    "description": "Minimum current-window articles (default: 2).",
                    "default": 2,
                },
            },
        },
    ),
    Tool(
        name="intel_top_stories",
        description=(
            "Stories carried by at least min_outlets distinct outlets in a recent RSS "
            "window (sister feeds of one publisher count once), ranked by outlet count, "
            "plus attention: the countries and organizations whose headline mentions "
            "grew most against the preceding window (empty, with "
            "attention_change_measured false, when no feed covers both windows)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "hours": {
                    "type": "number",
                    "description": "Window length in hours, 1 to 168 (default: 24).",
                    "default": 24,
                },
                "min_outlets": {
                    "type": "integer",
                    "description": "Minimum distinct outlets per story (default: 3).",
                    "default": 3,
                },
                "limit": {
                    "type": "integer",
                    "description": "Max stories (default: 10).",
                    "default": 10,
                },
                "attention_limit": {
                    "type": "integer",
                    "description": "Max attention entries (default: 10).",
                    "default": 10,
                },
            },
        },
    ),
]


async def _hostility_timeline(arguments: dict[str, Any]) -> Any:
    from ..analysis.news_signals import fetch_hostility_timeline

    return await fetch_hostility_timeline(
        runtime.fetcher,
        country=arguments["country"],
        other=arguments.get("other"),
        days=arguments.get("days", 14),
    )


async def _situations(arguments: dict[str, Any]) -> Any:
    from ..analysis.news_signals import fetch_situations

    return await fetch_situations(
        runtime.fetcher,
        hours=arguments.get("hours", 24),
        sort=arguments.get("sort", "covered"),
        kind=arguments.get("kind", "all"),
        limit=arguments.get("limit", 20),
        min_articles=arguments.get("min_articles", 2),
    )


async def _top_stories(arguments: dict[str, Any]) -> Any:
    from ..analysis.news_signals import fetch_top_stories

    return await fetch_top_stories(
        runtime.fetcher,
        hours=arguments.get("hours", 24),
        min_outlets=arguments.get("min_outlets", 3),
        limit=arguments.get("limit", 10),
        attention_limit=arguments.get("attention_limit", 10),
    )


HANDLERS = {
    "intel_hostility_timeline": _hostility_timeline,
    "intel_situations": _situations,
    "intel_top_stories": _top_stories,
}
