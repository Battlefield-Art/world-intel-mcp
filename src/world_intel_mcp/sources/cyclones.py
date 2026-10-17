"""NHC tropical cyclone source for world-intel-mcp.

Provides active tropical cyclones from the National Hurricane Center's
CurrentStorms.json feed. No API key required. Covers the NHC/CPHC
basins (Atlantic, Eastern Pacific, Central Pacific) — not JTWC's
Western Pacific / Indian Ocean areas of responsibility.

Shape facts, verified live 2026-09-01 against four active storms:
intensity and pressure arrive as strings (knots and millibars per
NHC's CurrentStorms.json documentation; the 945 mb / "120" pairing on
a major hurricane is consistent with knots), positions as
latitudeNumeric/longitudeNumeric floats, movement as movementDir
(degrees) and movementSpeed integers.

An empty activeStorms array is a VALID quiet-season result and is
returned without degradation markers; only an actual fetch failure is
marked degraded. The quiet-season shape itself (activeStorms: []) is
assumed from the populated shape, not observed — four storms were
active on the day this was written.

``include_forecast=True`` adds each storm's official forecast track and
5-day cone (sources/cyclone_forecasts.py, one text and one KMZ fetch per
storm, capped per call) plus a ``data_gaps`` list; the default call
fetches CurrentStorms.json only.
"""

import logging
from datetime import datetime, timezone

from ..fetcher import Fetcher
from . import cyclone_forecasts

logger = logging.getLogger("world-intel-mcp.sources.cyclones")

_NHC_CURRENT_STORMS_URL = "https://www.nhc.noaa.gov/CurrentStorms.json"

_CLASSIFICATION_NAMES: dict[str, str] = {
    "TD": "Tropical Depression",
    "TS": "Tropical Storm",
    "HU": "Hurricane",
    "MH": "Major Hurricane",
    "SD": "Subtropical Depression",
    "SS": "Subtropical Storm",
    "PTC": "Potential Tropical Cyclone",
    "PC": "Post-tropical Cyclone",
    "STD": "Subtropical Depression",
    "STS": "Subtropical Storm",
}

_BASIN_NAMES: dict[str, str] = {
    "al": "atlantic",
    "ep": "eastern_pacific",
    "cp": "central_pacific",
}


async def fetch_cyclones(fetcher: Fetcher, include_forecast: bool = False) -> dict:
    """Fetch active tropical cyclones from the NHC.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.
        include_forecast: Also fetch each storm's forecast track and cone
            (adds ``forecast`` per storm and ``data_gaps``; a failed
            track or cone is named there and marks the response
            degraded).

    Returns:
        Dict with storms list (name, classification, intensity,
        position, movement), count, by_basin counts, source, and
        timestamp. Zero storms with no degraded marker means a quiet
        tropics, not a failure.
    """
    now = datetime.now(timezone.utc)

    data = await fetcher.get_json(
        url=_NHC_CURRENT_STORMS_URL,
        source="nhc",
        cache_key="cyclones:nhc:current",
        cache_ttl=600,
    )

    if data is None:
        logger.warning("NHC CurrentStorms feed returned no data")
        return {
            "error": "NHC CurrentStorms feed unavailable (no live or cached data)",
            "degraded": True,
            "reason": "nhc_fetch_failed",
            "storms": [],
            "count": 0,
            "by_basin": {},
            "source": "nhc",
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    storms = []
    entries: list[tuple[dict, dict]] = []  # (output storm, raw record)
    by_basin: dict[str, int] = {}
    for storm in data.get("activeStorms", []):
        storm_id = storm.get("id") or ""
        basin = _BASIN_NAMES.get(storm_id[:2].lower(), storm_id[:2].lower() or None)
        if basin:
            by_basin[basin] = by_basin.get(basin, 0) + 1

        classification = storm.get("classification")
        advisory = storm.get("publicAdvisory") or {}

        storms.append(
            {
                "id": storm_id or None,
                "name": storm.get("name"),
                "basin": basin,
                "classification": classification,
                "classification_name": _CLASSIFICATION_NAMES.get(
                    classification, classification
                ),
                "intensity_kt": _safe_int(storm.get("intensity")),
                "pressure_mb": _safe_int(storm.get("pressure")),
                "latitude": storm.get("latitudeNumeric"),
                "longitude": storm.get("longitudeNumeric"),
                "movement_dir_deg": storm.get("movementDir"),
                "movement_speed_kt": storm.get("movementSpeed"),
                "last_update": storm.get("lastUpdate"),
                "advisory_url": advisory.get("url"),
            }
        )
        entries.append((storms[-1], storm))

    response = {
        "storms": storms,
        "count": len(storms),
        "by_basin": by_basin,
        "source": "nhc",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if include_forecast:
        gaps, degraded = await cyclone_forecasts.attach_nhc_forecasts(fetcher, entries)
        response["data_gaps"] = gaps
        if degraded:
            response["degraded"] = True
    return response


def _safe_int(value: str | int | None) -> int | None:
    """Convert a value to int, returning None when absent or unparseable."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
