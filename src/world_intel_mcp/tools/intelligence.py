"""Intelligence-analysis tools: country briefs and dossiers,
risk and instability scoring, signal convergence and summaries,
temporal anomalies, unrest, hotspot escalation, military surge, vessel
snapshot, cascade analysis, alert digest, weekly trends.

Phase 26 split: Tool definitions moved byte-identical and dispatch
case bodies verbatim from server.py; handlers reach shared
infrastructure via ``runtime``. See tools/system.py for the pattern.
"""

from typing import Any

from mcp.types import Tool

from .. import runtime
from ..sources import intelligence

TOOLS: list[Tool] = [
    # --- Intelligence (13 tools) ---
    Tool(
        name="intel_country_brief",
        description="Generate a country intelligence brief using Ollama LLM + World Bank + ACLED data. Falls back to data-only if LLM unavailable.",
        inputSchema={
            "type": "object",
            "properties": {
                "country_code": {
                    "type": "string",
                    "description": "ISO country code (default: US)",
                    "default": "US",
                },
            },
        },
    ),
    Tool(
        name="intel_country_dossier",
        description="Comprehensive country intelligence dossier: economy (GDP/inflation), stock market, elections, sanctions, news mentions, hotspots, and conflict zones. Aggregates 6 sources in parallel.",
        inputSchema={
            "type": "object",
            "properties": {
                "country": {
                    "type": "string",
                    "description": "ISO-2 or ISO-3 country code (e.g. US, USA, UA, UKR)",
                    "default": "US",
                },
            },
        },
    ),
    Tool(
        name="intel_risk_scores",
        description="Get country risk scores computed from ACLED conflict data vs historical baselines. Requires ACLED_EMAIL and ACLED_PASSWORD.",
        inputSchema={
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Top N countries (default 20)",
                    "default": 20,
                },
            },
        },
    ),
    Tool(
        name="intel_instability_index",
        description="Compute Country Instability Index v2 (0-100) from 4 weighted domains: unrest, conflict, security, information. Applies country-specific multipliers and UCDP floors.",
        inputSchema={
            "type": "object",
            "properties": {
                "country_code": {
                    "type": "string",
                    "description": "ISO alpha-3 code (e.g., UKR). Omit for top-10 focus countries.",
                },
            },
        },
    ),
    Tool(
        name="intel_signal_convergence",
        description="Detect geographic convergence of multi-domain signals (earthquakes, conflict, military) in hotspot regions.",
        inputSchema={
            "type": "object",
            "properties": {
                "lat": {
                    "type": "number",
                    "description": "Center latitude (omit for 5 global hotspots)",
                },
                "lon": {"type": "number", "description": "Center longitude"},
                "radius_deg": {
                    "type": "number",
                    "description": "Radius in degrees (default 5.0)",
                    "default": 5.0,
                },
            },
        },
    ),
    Tool(
        name="intel_focal_points",
        description="Detect focal points where multiple intelligence signals converge on the same entity (country, organization, leader). Cross-references news, military, protests, and infrastructure signals.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="intel_signal_summary",
        description="Aggregate all intelligence signals by country with convergence scoring. Combines conflict, displacement, earthquakes, fires, outages, military, and protests.",
        inputSchema={
            "type": "object",
            "properties": {
                "country": {
                    "type": "string",
                    "description": "Country name filter (optional)",
                },
            },
        },
    ),
    Tool(
        name="intel_temporal_anomalies",
        description="Detect temporal anomalies — activity levels that deviate from historical baselines using Welford's algorithm. Reports z-score deviations like 'Military flights 3.2x normal for Thursday'.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="intel_unrest_events",
        description="Get social unrest events (protests + riots) from ACLED with Haversine deduplication. Optional: country (name), days (default 7), limit (default 100).",
        inputSchema={
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "Country name filter"},
                "days": {
                    "type": "integer",
                    "description": "Lookback days (default 7)",
                    "default": 7,
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (default 100)",
                    "default": 100,
                },
            },
        },
    ),
    Tool(
        name="intel_hotspot_escalation",
        description="Dynamic escalation scores for 22 intel hotspots from baseline risk, military activity, and conflict signals (ACLED events near each hotspot). Each hotspot scored 0-100, renormalized over the signals actually measured. News-mention and geo-convergence components are not currently wired up and are reported as null (see unavailable_components), not a fabricated zero.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="intel_military_surge",
        description="Detect military surge anomalies — foreign aircraft concentration above baselines in 8 sensitive regions (Persian Gulf, Taiwan Strait, Baltic Sea, etc.).",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="intel_vessel_snapshot",
        description="Naval activity snapshot at 9 strategic waterways (Hormuz, Malacca, Suez, etc.) from NGA navigational warnings. A warning counts at a waterway only when a position it states lies within 300 km; naval warnings with no readable position are listed as unplaced. Each waterway scored clear/advisory/elevated/critical, stale when the feed's newest warning is past the staleness threshold (status_as_of gives the feed's date), or unknown when the feed is down, empty or carries no usable issue date (feed_stale null).",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="intel_cascade_analysis",
        description="Simulate infrastructure cascade — 'what if cable corridor X is disrupted?' Impact scoring across dependent countries. Optional: corridor name (default: simulate at-risk corridors).",
        inputSchema={
            "type": "object",
            "properties": {
                "corridor": {
                    "type": "string",
                    "description": "Cable corridor to simulate (e.g., red_sea, transpacific, asia_europe)",
                    "enum": [
                        "transatlantic_north",
                        "transatlantic_south",
                        "asia_europe",
                        "red_sea",
                        "transpacific",
                        "mediterranean",
                    ],
                },
            },
        },
    ),
    # --- Alert Digest (1 tool) ---
    Tool(
        name="intel_alert_digest",
        description="Cross-domain alert aggregation from 7 intelligence sources: space weather, instability, military surge, cable health, hotspot escalation, internet outages, shipping stress. Threshold-based prioritized alerts.",
        inputSchema={"type": "object", "properties": {}},
    ),
    # --- Weekly Trends (1 tool) ---
    Tool(
        name="intel_weekly_trends",
        description="Analyze weekly trends from temporal baselines. Reports volatility (coefficient of variation) and current anomalies across all tracked metrics.",
        inputSchema={"type": "object", "properties": {}},
    ),
]


async def _country_brief(arguments: dict[str, Any]) -> Any:
    return await intelligence.fetch_country_brief(
        runtime.fetcher, country_code=arguments.get("country_code", "US")
    )


async def _country_dossier(arguments: dict[str, Any]) -> Any:
    from ..analysis.dossier import fetch_country_dossier

    return await fetch_country_dossier(
        runtime.fetcher, country=arguments.get("country", "US")
    )


async def _risk_scores(arguments: dict[str, Any]) -> Any:
    return await intelligence.fetch_risk_scores(
        runtime.fetcher, limit=arguments.get("limit", 20)
    )


async def _instability_index(arguments: dict[str, Any]) -> Any:
    return await intelligence.fetch_instability_index(
        runtime.fetcher, country_code=arguments.get("country_code")
    )


async def _signal_convergence(arguments: dict[str, Any]) -> Any:
    return await intelligence.fetch_signal_convergence(
        runtime.fetcher,
        lat=arguments.get("lat"),
        lon=arguments.get("lon"),
        radius_deg=arguments.get("radius_deg", 5.0),
    )


async def _focal_points(arguments: dict[str, Any]) -> Any:
    return await intelligence.fetch_focal_points(runtime.fetcher)


async def _signal_summary(arguments: dict[str, Any]) -> Any:
    return await intelligence.fetch_signal_summary(
        runtime.fetcher, country=arguments.get("country")
    )


async def _temporal_anomalies(arguments: dict[str, Any]) -> Any:
    return await intelligence.fetch_temporal_anomalies(runtime.fetcher)


async def _unrest_events(arguments: dict[str, Any]) -> Any:
    return await intelligence.fetch_unrest_events(
        runtime.fetcher,
        country=arguments.get("country"),
        days=arguments.get("days", 7),
        limit=arguments.get("limit", 100),
    )


async def _hotspot_escalation(arguments: dict[str, Any]) -> Any:
    return await intelligence.fetch_hotspot_escalation(runtime.fetcher)


async def _military_surge(arguments: dict[str, Any]) -> Any:
    return await intelligence.fetch_military_surge(runtime.fetcher)


async def _vessel_snapshot(arguments: dict[str, Any]) -> Any:
    return await intelligence.fetch_vessel_snapshot(runtime.fetcher)


async def _cascade_analysis(arguments: dict[str, Any]) -> Any:
    return await intelligence.fetch_cascade_analysis(
        runtime.fetcher,
        corridor=arguments.get("corridor"),
    )


async def _alert_digest(arguments: dict[str, Any]) -> Any:
    from ..analysis.alerts import fetch_alert_digest

    return await fetch_alert_digest(runtime.fetcher)


async def _weekly_trends(arguments: dict[str, Any]) -> Any:
    from ..analysis.alerts import fetch_weekly_trends

    return await fetch_weekly_trends(runtime.fetcher)


HANDLERS = {
    "intel_country_brief": _country_brief,
    "intel_country_dossier": _country_dossier,
    "intel_risk_scores": _risk_scores,
    "intel_instability_index": _instability_index,
    "intel_signal_convergence": _signal_convergence,
    "intel_focal_points": _focal_points,
    "intel_signal_summary": _signal_summary,
    "intel_temporal_anomalies": _temporal_anomalies,
    "intel_unrest_events": _unrest_events,
    "intel_hotspot_escalation": _hotspot_escalation,
    "intel_military_surge": _military_surge,
    "intel_vessel_snapshot": _vessel_snapshot,
    "intel_cascade_analysis": _cascade_analysis,
    "intel_alert_digest": _alert_digest,
    "intel_weekly_trends": _weekly_trends,
}
