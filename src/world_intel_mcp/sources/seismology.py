"""USGS earthquake data source for world-intel-mcp.

Provides real-time earthquake data from the USGS GeoJSON API.
No API key required.
"""

import logging
from datetime import datetime, timezone, timedelta

from ..fetcher import Fetcher
from .freshness import json_answer_freshness

logger = logging.getLogger("world-intel-mcp.sources.seismology")

_USGS_ENDPOINT = "https://earthquake.usgs.gov/fdsnws/event/1/query"


async def fetch_earthquakes(
    fetcher: Fetcher,
    min_magnitude: float = 4.5,
    hours: int = 24,
    limit: int = 50,
) -> dict:
    """Fetch recent earthquakes from the USGS GeoJSON API.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.
        min_magnitude: Minimum earthquake magnitude to include.
        hours: How far back to search (in hours from now).
        limit: Maximum number of results.

    Returns:
        Dict with earthquakes list, count, query params, source, timestamp
        and ``fetched_at`` (when USGS answered; earlier than ``timestamp``
        for a cached answer). An expired cached copy served because USGS
        did not answer carries ``_stale`` and ``_stale_age_seconds``. A
        failed fetch with no copy is ``error`` / ``degraded`` /
        ``reason: "usgs_fetch_failed"`` with the list keys kept empty, so
        it never reads as a quiet planet.
    """
    now = datetime.now(timezone.utc)
    starttime = (now - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S")

    params = {
        "format": "geojson",
        "minmagnitude": min_magnitude,
        "orderby": "time",
        "limit": limit,
        "starttime": starttime,
    }

    # The limit is part of the key: a 5-quake answer cached for one caller
    # must not be served to another that asked for 50 and then reads the
    # short list as complete.
    cache_key = f"seismology:quakes:{min_magnitude}:{hours}:{limit}"
    data = await fetcher.get_json(
        url=_USGS_ENDPOINT,
        source="usgs",
        cache_key=cache_key,
        cache_ttl=300,
        params=params,
    )

    if data is None:
        logger.warning("USGS API returned no data")
        # A failed fetch must not be byte-identical to a quiet planet, the
        # same contract ACLED and GDELT follow. earthquakes/count keep their
        # normal empty shape so callers that don't check "error" or
        # "degraded" still iterate an empty list.
        return {
            "error": "USGS earthquake fetch failed (API error, timeout, or malformed response)",
            "degraded": True,
            "reason": "usgs_fetch_failed",
            "earthquakes": [],
            "count": 0,
            "query": {"min_magnitude": min_magnitude, "hours": hours},
            "source": "usgs",
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    earthquakes = []
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates", [0, 0, 0])

        # Convert epoch milliseconds to ISO 8601 UTC
        epoch_ms = props.get("time")
        if epoch_ms is not None:
            eq_time = datetime.fromtimestamp(
                epoch_ms / 1000, tz=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            eq_time = None

        earthquakes.append({
            "id": feature.get("id"),
            "magnitude": props.get("mag"),
            "place": props.get("place"),
            "time": eq_time,
            "depth_km": coords[2] if len(coords) > 2 else None,
            "latitude": coords[1] if len(coords) > 1 else None,
            "longitude": coords[0] if len(coords) > 0 else None,
            "tsunami_alert": props.get("tsunami"),
            "felt_reports": props.get("felt"),
            "alert_level": props.get("alert"),
            "url": props.get("url"),
        })

    return {
        "earthquakes": earthquakes,
        "count": len(earthquakes),
        "query": {"min_magnitude": min_magnitude, "hours": hours},
        "source": "usgs",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        **json_answer_freshness(fetcher, cache_key, data),
    }
