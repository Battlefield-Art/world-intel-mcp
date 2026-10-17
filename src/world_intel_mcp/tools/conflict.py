"""Conflict and military tools: ACLED and UCDP events,
humanitarian summaries, military flights and aircraft lookups, nuclear
test-site monitoring, USNI fleet disposition.

Phase 26 split: Tool definitions moved byte-identical and dispatch
case bodies verbatim from server.py; handlers reach shared
infrastructure via ``runtime``. See tools/system.py for the pattern.
"""

from typing import Any

from mcp.types import Tool

from .. import runtime
from ..sources import (
    conflict,
    military,
    nuclear,
    usni_fleet,
)

TOOLS: list[Tool] = [
    # --- Conflict (3 tools) ---
    Tool(
        name="intel_acled_events",
        description="Get armed conflict events from ACLED. Optional: country (name), days (default 7), limit (default 100). Requires ACLED_EMAIL and ACLED_PASSWORD.",
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
        name="intel_ucdp_events",
        description="Get organized violence events (state-based, non-state and one-sided) from UCDP GED. Requires UCDP_ACCESS_TOKEN; without it, or when the API refuses it, returns an error. Optional: days (default 30), limit (page size, default 100).",
        inputSchema={
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Lookback days (default 30)",
                    "default": 30,
                },
                "limit": {
                    "type": "integer",
                    "description": "Page size (default 100, max 1000); every page is fetched, so this does not cap results",
                    "default": 100,
                },
            },
        },
    ),
    Tool(
        name="intel_humanitarian_summary",
        description="Get humanitarian crisis datasets from HDX. No API key needed. Optional: country code.",
        inputSchema={
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "ISO country code filter"},
            },
        },
    ),
    # --- Military (3 tools) ---
    Tool(
        name="intel_military_flights",
        description="Track military aircraft via OpenSky Network (ICAO hex + callsign filtering). Optional: bbox (lamin,lomin,lamax,lomax).",
        inputSchema={
            "type": "object",
            "properties": {
                "bbox": {
                    "type": "string",
                    "description": "Bounding box: lamin,lomin,lamax,lomax",
                },
            },
        },
    ),
    Tool(
        name="intel_theater_posture",
        description="Get military aircraft presence across 5 theaters: European, Indo-Pacific, Middle East, Arctic, Korean Peninsula.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="intel_aircraft_details",
        description="Get aircraft details from hexdb.io by ICAO24 hex code (free, no API key).",
        inputSchema={
            "type": "object",
            "properties": {
                "icao24": {"type": "string", "description": "ICAO24 hex code"},
            },
            "required": ["icao24"],
        },
    ),
    # --- Military Extended (1 tool) ---
    Tool(
        name="intel_aircraft_batch",
        description="Batch lookup of aircraft details by ICAO24 hex codes (max 20). Returns registration, type, operator from hexdb.io.",
        inputSchema={
            "type": "object",
            "properties": {
                "icao24_list": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of ICAO24 hex addresses (max 20)",
                },
            },
            "required": ["icao24_list"],
        },
    ),
    # --- Nuclear (1 tool) ---
    Tool(
        name="intel_nuclear_monitor",
        description="Monitor seismic activity near 5 known nuclear test sites (Punggye-ri, Lop Nur, Novaya Zemlya, Nevada NTS, Semipalatinsk). Concern scoring based on depth, magnitude, distance.",
        inputSchema={
            "type": "object",
            "properties": {
                "hours": {
                    "type": "integer",
                    "description": "Lookback hours (default 72)",
                    "default": 72,
                },
            },
        },
    ),
    # --- USNI Fleet (1 tool) ---
    Tool(
        name="intel_usni_fleet",
        description="US Navy fleet disposition from USNI News Fleet Tracker. Extracts ships, hull numbers, carrier strike groups, regional deployment, and force totals from the latest weekly report.",
        inputSchema={"type": "object", "properties": {}},
    ),
]


async def _acled_events(arguments: dict[str, Any]) -> Any:
    return await conflict.fetch_acled_events(
        runtime.fetcher,
        country=arguments.get("country"),
        days=arguments.get("days", 7),
        limit=arguments.get("limit", 100),
    )


async def _ucdp_events(arguments: dict[str, Any]) -> Any:
    return await conflict.fetch_ucdp_events(
        runtime.fetcher,
        days=arguments.get("days", 30),
        limit=arguments.get("limit", 100),
    )


async def _humanitarian_summary(arguments: dict[str, Any]) -> Any:
    return await conflict.fetch_humanitarian_summary(
        runtime.fetcher,
        country=arguments.get("country"),
    )


async def _military_flights(arguments: dict[str, Any]) -> Any:
    return await military.fetch_military_flights(
        runtime.fetcher, bbox=arguments.get("bbox")
    )


async def _theater_posture(arguments: dict[str, Any]) -> Any:
    return await military.fetch_theater_posture(runtime.fetcher)


async def _aircraft_details(arguments: dict[str, Any]) -> Any:
    return await military.fetch_aircraft_details(
        runtime.fetcher, icao24=arguments["icao24"]
    )


async def _aircraft_batch(arguments: dict[str, Any]) -> Any:
    return await military.fetch_aircraft_details_batch(
        runtime.fetcher,
        icao24_list=arguments["icao24_list"],
    )


async def _nuclear_monitor(arguments: dict[str, Any]) -> Any:
    return await nuclear.fetch_nuclear_monitor(
        runtime.fetcher,
        hours=arguments.get("hours", 72),
    )


async def _usni_fleet(arguments: dict[str, Any]) -> Any:
    return await usni_fleet.fetch_usni_fleet(runtime.fetcher)


HANDLERS = {
    "intel_acled_events": _acled_events,
    "intel_ucdp_events": _ucdp_events,
    "intel_humanitarian_summary": _humanitarian_summary,
    "intel_military_flights": _military_flights,
    "intel_theater_posture": _theater_posture,
    "intel_aircraft_details": _aircraft_details,
    "intel_aircraft_batch": _aircraft_batch,
    "intel_nuclear_monitor": _nuclear_monitor,
    "intel_usni_fleet": _usni_fleet,
}
