"""Natural-hazard and space-domain tools: earthquakes,
wildfires, climate anomalies, space weather, environmental events,
disaster alerts, severe weather, rocket launches, volcanoes, cyclones.

Phase 26 split: Tool definitions moved byte-identical and dispatch
case bodies verbatim from server.py; handlers reach shared
infrastructure via ``runtime``. See tools/system.py for the pattern.
"""

from typing import Any

from mcp.types import Tool

from .. import runtime
from ..sources import (
    seismology,
    wildfire,
    climate,
    space_weather,
    environmental,
    weather,
    launches,
    volcano,
    cyclones,
)

TOOLS: list[Tool] = [
    # --- Natural (2 tools) ---
    Tool(
        name="intel_earthquakes",
        description="Get recent earthquakes from USGS. No API key needed.",
        inputSchema={
            "type": "object",
            "properties": {
                "min_magnitude": {
                    "type": "number",
                    "description": "Minimum magnitude (default 4.5)",
                    "default": 4.5,
                },
                "hours": {
                    "type": "integer",
                    "description": "Lookback hours (default 24)",
                    "default": 24,
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (default 50)",
                    "default": 50,
                },
            },
        },
    ),
    Tool(
        name="intel_wildfires",
        description="Get active wildfires from NASA FIRMS (9 global regions). Requires NASA_FIRMS_API_KEY.",
        inputSchema={
            "type": "object",
            "properties": {
                "region": {
                    "type": "string",
                    "description": "Specific region (north_america, europe, etc.) or omit for all 9",
                    "enum": [
                        "north_america",
                        "south_america",
                        "europe",
                        "africa",
                        "middle_east",
                        "south_asia",
                        "east_asia",
                        "southeast_asia",
                        "oceania",
                    ],
                },
            },
        },
    ),
    # --- Climate (1 tool) ---
    Tool(
        name="intel_climate_anomalies",
        description="Detect temperature and precipitation anomalies across 15 global climate zones (vs. prior year baseline).",
        inputSchema={
            "type": "object",
            "properties": {
                "zones": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Zone keys to check (default: all 15)",
                },
            },
        },
    ),
    # --- Space Weather (1 tool) ---
    Tool(
        name="intel_space_weather",
        description="Get solar activity: Kp geomagnetic index, X-ray flare class, solar wind, and SWPC alerts from NOAA.",
        inputSchema={"type": "object", "properties": {}},
    ),
    # --- Environmental (2 tools) ---
    Tool(
        name="intel_environmental_events",
        description="Natural events from NASA EONET: wildfires, severe storms, volcanoes, floods, icebergs, drought. Includes geolocation and source links.",
        inputSchema={
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Look back N days (default 30)",
                    "default": 30,
                },
                "category": {
                    "type": "string",
                    "description": "Filter by category: wildfires, severeStorms, volcanoes, floods, earthquakes, drought, seaLakeIce",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max events (default 50)",
                    "default": 50,
                },
            },
        },
    ),
    Tool(
        name="intel_disaster_alerts",
        description="Global disaster alerts from GDACS (UN): earthquakes, floods, cyclones, droughts, wildfires. Severity levels (green/orange/red) with affected populations.",
        inputSchema={
            "type": "object",
            "properties": {
                "alert_level": {
                    "type": "string",
                    "description": "Filter by level: green, orange, red",
                },
                "event_type": {
                    "type": "string",
                    "description": "Filter by type: EQ, FL, TC, DR, WF, VO",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max alerts (default 30)",
                    "default": 30,
                },
            },
        },
    ),
    # --- Hazard & Space Domains (4 tools, Phase 25) ---
    Tool(
        name="intel_weather_alerts",
        description="Get active severe weather alerts (CAP) from the US National Weather Service: event, severity, urgency, headline, affected area, effective/expires times. US coverage only - there is no global feed. Lat/lon present only for polygon-based alerts (zone-based alerts return null coordinates rather than fabricated ones). No API key needed. Optional: area (two-letter US state code), severity (Extreme, Severe, Moderate, Minor, Unknown), limit (default 50, applied client-side).",
        inputSchema={
            "type": "object",
            "properties": {
                "area": {
                    "type": "string",
                    "description": "Two-letter US state/territory code (e.g. TX)",
                },
                "severity": {
                    "type": "string",
                    "description": "Filter: Extreme, Severe, Moderate, Minor, or Unknown",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max alerts (default 50, applied client-side)",
                    "default": 50,
                },
            },
        },
    ),
    Tool(
        name="intel_launch_schedule",
        description="Get upcoming rocket launches from Launch Library 2: name, provider, vehicle, pad with coordinates, launch time (NET), status, mission description. The free tier allows ~15 requests/hour, so results are cached for an hour; recently launched missions may appear with their final status. No API key needed. Optional: limit (default 20).",
        inputSchema={
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Max launches (default 20)",
                    "default": 20,
                },
            },
        },
    ),
    Tool(
        name="intel_volcano_activity",
        description="Get the Smithsonian GVP / USGS Weekly Volcanic Activity Report: volcano name, country, coordinates, activity status (new vs continuing), and summary. Updated weekly (Thursdays); a curated summary of activity meeting GVP reporting criteria, not a comprehensive eruption list. No API key needed.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="intel_cyclones",
        description="Get active tropical cyclones from the US National Hurricane Center: name, classification, intensity (knots), pressure (mb), position, movement, by-basin grouping. Covers the Atlantic, Eastern and Central Pacific basins only - Western Pacific / Indian Ocean storms (JTWC) are not included. Zero storms means a quiet tropics, not an outage; a fetch failure carries an explicit error. No API key needed. Optional: include_forecast (default false) adds each storm's official forecast track from its NHC forecast/advisory (positions with UTC valid times and forecast hours, max wind and gusts in knots, post-tropical/dissipated status) and the 5-day cone of uncertainty as [lat, lon] vertices simplified to 0.01 degrees (NHC's KMZ linked); a track or cone that could not be fetched is named in data_gaps and marks the response degraded.",
        inputSchema={
            "type": "object",
            "properties": {
                "include_forecast": {
                    "type": "boolean",
                    "description": "Add each storm's forecast track and cone (one text and one KMZ fetch per storm, at most 10 storms per call). Default false.",
                    "default": False,
                },
            },
        },
    ),
    Tool(
        name="intel_meteoalarm_alerts",
        description="Get active severe-weather warnings from Meteoalarm (EUMETNET): event type, severity, awareness color, area, onset/expires. Covers Europe only - 39 participating countries, each with its own feed (no Europe-wide feed exists); call without country to list available countries. Awareness color is derived from the warning title. Zero alerts means calm weather, not an outage; a fetch failure carries an explicit error. Content CC BY 4.0-equivalent. No API key needed.",
        inputSchema={
            "type": "object",
            "properties": {
                "country": {
                    "type": "string",
                    "description": "Country name or slug (e.g. france, united-kingdom, uk). Omit to get the available-country roster.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max alerts (default 50); one warning may span several area entries",
                    "default": 50,
                },
            },
        },
    ),
    Tool(
        name="intel_jtwc_cyclones",
        description="Get active tropical cyclone warnings from the Joint Typhoon Warning Center: storm id, name, classification, warning number, issue time, warning text/graphic links, by-basin grouping, plus significant tropical weather advisories. Covers the Northwest Pacific, North Indian Ocean, and Southern Hemisphere; its Central/Eastern Pacific item overlaps NHC (intel_cyclones). Positions and intensities are not in the feed - they live in the linked warning products. Zero storms means quiet basins, not an outage. No API key needed. Optional: include_forecast (default false) fetches each storm's warning text and adds its warning position, intensity and pressure plus the forecast track (positions with UTC valid times, forecast hours, max sustained winds and gusts in knots, status lines such as EXTRATROPICAL); JTWC publishes no cone of uncertainty, so cone is null. A warning text that could not be fetched is named in data_gaps and marks the response degraded.",
        inputSchema={
            "type": "object",
            "properties": {
                "include_forecast": {
                    "type": "boolean",
                    "description": "Add each storm's forecast track from its warning text (one fetch per storm, at most 10 storms per call). Default false.",
                    "default": False,
                },
            },
        },
    ),
]


async def _earthquakes(arguments: dict[str, Any]) -> Any:
    return await seismology.fetch_earthquakes(
        runtime.fetcher,
        min_magnitude=arguments.get("min_magnitude", 4.5),
        hours=arguments.get("hours", 24),
        limit=arguments.get("limit", 50),
    )


async def _wildfires(arguments: dict[str, Any]) -> Any:
    return await wildfire.fetch_wildfires(
        runtime.fetcher, region=arguments.get("region")
    )


async def _climate_anomalies(arguments: dict[str, Any]) -> Any:
    return await climate.fetch_climate_anomalies(
        runtime.fetcher, zones=arguments.get("zones")
    )


async def _space_weather(arguments: dict[str, Any]) -> Any:
    return await space_weather.fetch_space_weather(runtime.fetcher)


async def _environmental_events(arguments: dict[str, Any]) -> Any:
    return await environmental.fetch_environmental_events(
        runtime.fetcher,
        days=arguments.get("days", 30),
        category=arguments.get("category"),
        limit=arguments.get("limit", 50),
    )


async def _disaster_alerts(arguments: dict[str, Any]) -> Any:
    return await environmental.fetch_disaster_alerts(
        runtime.fetcher,
        alert_level=arguments.get("alert_level"),
        event_type=arguments.get("event_type"),
        limit=arguments.get("limit", 30),
    )


async def _weather_alerts(arguments: dict[str, Any]) -> Any:
    return await weather.fetch_weather_alerts(
        runtime.fetcher,
        area=arguments.get("area"),
        severity=arguments.get("severity"),
        limit=arguments.get("limit", 50),
    )


async def _launch_schedule(arguments: dict[str, Any]) -> Any:
    return await launches.fetch_launch_schedule(
        runtime.fetcher, limit=arguments.get("limit", 20)
    )


async def _volcano_activity(arguments: dict[str, Any]) -> Any:
    return await volcano.fetch_volcano_activity(runtime.fetcher)


async def _cyclones(arguments: dict[str, Any]) -> Any:
    return await cyclones.fetch_cyclones(
        runtime.fetcher, include_forecast=arguments.get("include_forecast") is True
    )


async def _meteoalarm_alerts(arguments: dict[str, Any]) -> Any:
    from ..sources import meteoalarm

    return await meteoalarm.fetch_meteoalarm_alerts(
        runtime.fetcher,
        country=arguments.get("country"),
        limit=arguments.get("limit", 50),
    )


async def _jtwc_cyclones(arguments: dict[str, Any]) -> Any:
    from ..sources import jtwc

    return await jtwc.fetch_jtwc_cyclones(
        runtime.fetcher, include_forecast=arguments.get("include_forecast") is True
    )


HANDLERS = {
    "intel_earthquakes": _earthquakes,
    "intel_wildfires": _wildfires,
    "intel_climate_anomalies": _climate_anomalies,
    "intel_space_weather": _space_weather,
    "intel_environmental_events": _environmental_events,
    "intel_disaster_alerts": _disaster_alerts,
    "intel_weather_alerts": _weather_alerts,
    "intel_launch_schedule": _launch_schedule,
    "intel_volcano_activity": _volcano_activity,
    "intel_cyclones": _cyclones,
    "intel_meteoalarm_alerts": _meteoalarm_alerts,
    "intel_jtwc_cyclones": _jtwc_cyclones,
}
