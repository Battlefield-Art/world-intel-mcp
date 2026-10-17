"""AOI geofences, briefs, and reports: daily digest, AOI
define/list/update/delete and change detection, AOI brief and
escalation, situation brief, PDF/HTML report generation.

Phase 26 split: Tool definitions moved byte-identical and dispatch
case bodies verbatim from server.py; handlers reach shared
infrastructure via ``runtime``. See tools/system.py for the pattern.
"""

from typing import Any

from mcp.types import Tool

from .. import runtime
from ..analysis import aoi

TOOLS: list[Tool] = [
    # --- Daily Digest (1 tool) ---
    Tool(
        name="intel_daily_digest",
        description="Cited markdown morning brief: top current events by domain (earthquakes, military, conflict, wildfires, cyber, health, air/traffic), recent headlines, and, when the optional vector store is installed, recent activity trends and a 24h timeline. Every listed item carries a [n] citation into a numbered sources list. Degrades honestly via data_gaps when the vector store or a domain fetch is unavailable, instead of showing an empty section as a quiet day.",
        inputSchema={"type": "object", "properties": {}},
    ),
    # --- AOI Geofences (5 tools) ---
    Tool(
        name="intel_aoi_define",
        description="Define a named area of interest (AOI/geofence): a point plus a radius in kilometers, persisted for intel_aoi_brief, intel_aoi_escalation, intel_aoi_list, and intel_aoi_delete. Required: name, lat (-90..90), lon (-180..180), radius_km (1..2000). Rejects a duplicate name (case-insensitive) politely, echoing the existing definition instead of overwriting it.",
        inputSchema={
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Unique AOI name (case-insensitive)",
                },
                "lat": {
                    "type": "number",
                    "description": "Center latitude, -90..90",
                },
                "lon": {
                    "type": "number",
                    "description": "Center longitude, -180..180",
                },
                "radius_km": {
                    "type": "number",
                    "description": "Radius in kilometers, 1..2000",
                },
            },
            "required": ["name", "lat", "lon", "radius_km"],
        },
    ),
    Tool(
        name="intel_aoi_list",
        description="List all user-defined areas of interest (AOIs/geofences).",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="intel_aoi_delete",
        description="Delete a user-defined area of interest by name.",
        inputSchema={
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "AOI name to delete"},
            },
            "required": ["name"],
        },
    ),
    Tool(
        name="intel_aoi_brief",
        description="Cited brief for a user-defined AOI: earthquakes, military flights (bbox-derived), wildfires (region-mapped), ACLED conflict events, sampled aviation traffic, nearby static infrastructure (bases, ports, pipelines, nuclear, cables, datacenters, spaceports) with distances in km, and news headline mentions of the AOI name. Every item carries a [n] citation into a numbered sources list; data_gaps names every domain that could not be scoped to the AOI rather than omitting it silently. Required: name (must already be defined via intel_aoi_define).",
        inputSchema={
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "AOI name"},
            },
            "required": ["name"],
        },
    ),
    Tool(
        name="intel_aoi_escalation",
        description="Run the hotspot escalation scoring (baseline, news, military, conflict, social-unrest and convergence components; 0-100) on a user-defined AOI instead of only the 22 built-in intel hotspots. News: the rise in the area's share of GDELT coverage, the last 7 days against the 7 before, for the reverse-geocoded place (news_signal shows the counts). Convergence: multi-domain convergence of the earthquakes, military flights, ACLED events and wildfires inside the AOI's exact shape (circle, polygon or corridor; convergence_signal shows the cells). If the news or convergence inputs fail, that component is null and explained in data_gaps, never a 0; computed_from lists the components the score was computed from, and zero_filled_components any of those that are a 0 only because the military or ACLED fetch failed. Required: name (must already be defined via intel_aoi_define).",
        inputSchema={
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "AOI name"},
            },
            "required": ["name"],
        },
    ),
    Tool(
        name="intel_aoi_update",
        description="Update a user-defined AOI in place: rename it and/or change its center or radius, without losing its identity. Required: name. Optional: new_name, lat, lon, radius_km (same validation as intel_aoi_define; at least one must be provided). A rename keeps the AOI's change-detection history; changing center or radius drops it, because the old baseline described a different area.",
        inputSchema={
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Current AOI name"},
                "new_name": {
                    "type": "string",
                    "description": "New name (case-insensitive, must not collide)",
                },
                "lat": {
                    "type": "number",
                    "description": "New center latitude, -90..90",
                },
                "lon": {
                    "type": "number",
                    "description": "New center longitude, -180..180",
                },
                "radius_km": {
                    "type": "number",
                    "description": "New radius in kilometers, 1..2000",
                },
            },
            "required": ["name"],
        },
    ),
    Tool(
        name="intel_aoi_define_polygon",
        description="Define a polygon AOI/geofence: a named area bounded by 3-64 [lat, lon] vertices (a border region, a strait, an EEZ - shapes a radius cannot express). Works with every intel_aoi_* tool: briefs, escalation, and change detection scope to the exact polygon (line-feature infrastructure - pipelines, cables - matches the bounding circle, disclosed in data_gaps). Polygons may cross the antimeridian; they may span at most 180 degrees of longitude and a 2000 km bounding radius. Required: name, vertices.",
        inputSchema={
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Unique AOI name (case-insensitive)",
                },
                "vertices": {
                    "type": "array",
                    "description": "3-64 [lat, lon] pairs tracing the polygon",
                    "items": {
                        "type": "array",
                        "items": {"type": "number"},
                        "minItems": 2,
                        "maxItems": 2,
                    },
                    "minItems": 3,
                    "maxItems": 64,
                },
            },
            "required": ["name", "vertices"],
        },
    ),
    Tool(
        name="intel_aoi_define_corridor",
        description="Define a corridor AOI/geofence: a route of 2-64 [lat, lon] waypoints plus a total width in km (1-500) - a shipping lane, supply road, or cable run. Membership means within width/2 of the great-circle route between consecutive waypoints. Works with every intel_aoi_* tool; distances in results are measured to the route, not to a center. Required: name, waypoints, width_km.",
        inputSchema={
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Unique AOI name (case-insensitive)",
                },
                "waypoints": {
                    "type": "array",
                    "description": "2-64 [lat, lon] pairs tracing the route",
                    "items": {
                        "type": "array",
                        "items": {"type": "number"},
                        "minItems": 2,
                        "maxItems": 2,
                    },
                    "minItems": 2,
                    "maxItems": 64,
                },
                "width_km": {
                    "type": "number",
                    "description": "Total corridor width in km, 1..500",
                },
            },
            "required": ["name", "waypoints", "width_km"],
        },
    ),
    Tool(
        name="intel_aoi_changes",
        description="What entered or left a user-defined AOI since the last sweep: geofence change detection over earthquakes, military flights, ACLED conflict events, wildfire clusters, and news mentions. The first call establishes a baseline (nothing is claimed to have entered or left); subsequent calls report new, departed, and unchanged items per domain. A domain whose fetch failed is reported in data_gaps and excluded from the diff (a failed fetch never reads as 'everything left'), keeping its last real observation for the next successful sweep. Sampled aviation is excluded by design: a 1-in-10 sample churns every sweep. Required: name (must already be defined via intel_aoi_define).",
        inputSchema={
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "AOI name"},
            },
            "required": ["name"],
        },
    ),
    # --- Situation Brief (1 tool) ---
    Tool(
        name="intel_aoi_digest",
        description="Run the change sweep across every defined AOI (or a named subset) in one call: per-AOI new/departed/unchanged items, counts, data_gaps, and a combined markdown digest. This IS a sweep - each AOI's change snapshot advances - so a scheduler can call this one tool on an interval to learn what entered or left all watched areas. First-ever sweep per AOI is an explicit baseline. Optional: names (list of AOI names; unknown names are an error, no AOIs defined is a note).",
        inputSchema={
            "type": "object",
            "properties": {
                "names": {
                    "type": "array",
                    "description": "Optional subset of AOI names to sweep",
                    "items": {"type": "string"},
                },
            },
        },
    ),
    Tool(
        name="intel_situation_brief",
        description="Cited situational awareness brief, generated on demand over MCP (previously reachable only through the dashboard). Gathers a bounded server-side overview (earthquakes, military flights, ACLED conflict events, wildfires, cyber threats, disease outbreaks, news headlines, space weather, strategic posture, alert digest; not the dashboard's full 47-source fan-out), then synthesizes a 3-paragraph brief via local Ollama, or a mechanically-cited fallback summary when Ollama is unreachable. Returns brief, ai_generated, model, metrics_snapshot, a numbered sources list, and a cited flag that is true only when the brief text references a real source number.",
        inputSchema={"type": "object", "properties": {}},
    ),
    # --- Reports (1 tool) ---
    Tool(
        name="intel_generate_report",
        description="Generate a PDF or HTML intelligence report covering markets, conflicts, earthquakes, cyber threats, health, infrastructure, and more. Returns the file path. Optional: sections (list of section names), title (string), format ('pdf' or 'html').",
        inputSchema={
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Report title (default: 'World Intelligence Report')",
                },
                "sections": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Sections to include: world_brief, strategic_posture, alerts, markets, economic, earthquakes, wildfires, conflicts, military, infrastructure, maritime, cyber, health, news, climate, nuclear, shipping, service_status. Default: all.",
                },
                "format": {
                    "type": "string",
                    "enum": ["pdf", "html"],
                    "description": "Output format (default: pdf). Use html if weasyprint is not installed.",
                    "default": "pdf",
                },
            },
        },
    ),
]


async def _daily_digest(arguments: dict[str, Any]) -> Any:
    from ..analysis.daily_digest import fetch_daily_digest

    return await fetch_daily_digest(runtime.fetcher, vector_store=runtime.vector_store)


async def _aoi_define(arguments: dict[str, Any]) -> Any:
    return aoi.define_aoi(
        runtime.aoi_store,
        name=arguments.get("name"),
        lat=arguments.get("lat"),
        lon=arguments.get("lon"),
        radius_km=arguments.get("radius_km"),
    )


async def _aoi_list(arguments: dict[str, Any]) -> Any:
    return aoi.list_aois(runtime.aoi_store)


async def _aoi_delete(arguments: dict[str, Any]) -> Any:
    return aoi.delete_aoi(runtime.aoi_store, name=arguments.get("name"))


async def _aoi_brief(arguments: dict[str, Any]) -> Any:
    return await aoi.fetch_aoi_brief(
        runtime.fetcher, runtime.aoi_store, name=arguments.get("name")
    )


async def _aoi_escalation(arguments: dict[str, Any]) -> Any:
    return await aoi.fetch_aoi_escalation(
        runtime.fetcher, runtime.aoi_store, name=arguments.get("name")
    )


async def _aoi_update(arguments: dict[str, Any]) -> Any:
    return aoi.update_aoi(
        runtime.aoi_store,
        name=arguments.get("name"),
        new_name=arguments.get("new_name"),
        lat=arguments.get("lat"),
        lon=arguments.get("lon"),
        radius_km=arguments.get("radius_km"),
    )


async def _aoi_define_polygon(arguments: dict[str, Any]) -> Any:
    return aoi.define_polygon_aoi(
        runtime.aoi_store,
        name=arguments.get("name"),
        vertices=arguments.get("vertices"),
    )


async def _aoi_define_corridor(arguments: dict[str, Any]) -> Any:
    return aoi.define_corridor_aoi(
        runtime.aoi_store,
        name=arguments.get("name"),
        waypoints=arguments.get("waypoints"),
        width_km=arguments.get("width_km"),
    )


async def _aoi_digest(arguments: dict[str, Any]) -> Any:
    return await aoi.fetch_aoi_digest(
        runtime.fetcher, runtime.aoi_store, names=arguments.get("names")
    )


async def _aoi_changes(arguments: dict[str, Any]) -> Any:
    return await aoi.fetch_aoi_changes(
        runtime.fetcher, runtime.aoi_store, name=arguments.get("name")
    )


async def _situation_brief(arguments: dict[str, Any]) -> Any:
    from ..analysis.situation import fetch_live_situation_brief

    return await fetch_live_situation_brief(runtime.fetcher)


async def _generate_report(arguments: dict[str, Any]) -> Any:
    from ..reports import generate_report

    return await generate_report(
        runtime.fetcher,
        title=arguments.get("title"),
        sections=arguments.get("sections"),
        fmt=arguments.get("format", "pdf"),
    )


HANDLERS = {
    "intel_daily_digest": _daily_digest,
    "intel_aoi_define": _aoi_define,
    "intel_aoi_list": _aoi_list,
    "intel_aoi_delete": _aoi_delete,
    "intel_aoi_brief": _aoi_brief,
    "intel_aoi_escalation": _aoi_escalation,
    "intel_aoi_update": _aoi_update,
    "intel_aoi_define_polygon": _aoi_define_polygon,
    "intel_aoi_define_corridor": _aoi_define_corridor,
    "intel_aoi_changes": _aoi_changes,
    "intel_aoi_digest": _aoi_digest,
    "intel_situation_brief": _situation_brief,
    "intel_generate_report": _generate_report,
}
