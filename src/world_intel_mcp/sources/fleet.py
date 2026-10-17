"""Naval fleet activity report.

Aggregates theater posture, vessel snapshot at strategic waterways,
military surge detections, and military base data into a fleet-focused
intelligence report.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

logger = logging.getLogger("world-intel-mcp.sources.fleet")


async def _safe(coro, label: str) -> dict:
    """Await one report component; a raise or a non-dict result comes back
    as an error dict (error / degraded / reason), never as an empty one.

    This used to return {}, which made a component that raised read exactly
    like one that found nothing: with all three down, the report scored
    LOW_ACTIVITY with no sign that it had no data.
    """
    try:
        result = await coro
    except Exception as exc:
        logger.warning("Fleet report: %s failed: %s", label, exc)
        return {
            "error": f"{type(exc).__name__}: {exc}"[:200],
            "degraded": True,
            "reason": "component_raised",
        }
    if not isinstance(result, dict):
        logger.warning(
            "Fleet report: %s returned %s, not a dict", label, type(result).__name__
        )
        return {
            "error": f"returned {type(result).__name__}, not a dict",
            "degraded": True,
            "reason": "component_bad_shape",
        }
    return result


def _fleet_readiness(theaters: dict, waterways: list, surges: list) -> tuple[str, int]:
    """Assess overall fleet readiness from component data.

    *theaters* is a dict keyed by theater name (e.g. ``{"europe": {"count": 5, ...}}``).
    Returns (level, score 0-100).
    """
    score = 0.0

    # Theater activity: more aircraft = higher activity
    total_aircraft = sum(
        t.get("count", 0) for t in theaters.values() if isinstance(t, dict)
    )
    score += min(30.0, total_aircraft * 0.5)

    # Waterway status: elevated/critical waterways raise score
    status_map = {"clear": 0, "advisory": 10, "elevated": 20, "critical": 30}
    for ww in waterways:
        score += status_map.get(ww.get("status", "clear"), 0) / max(len(waterways), 1)

    # Active surges
    score += min(30.0, len(surges) * 15.0)

    score = min(100.0, score)

    if score >= 70:
        level = "HIGH_ACTIVITY"
    elif score >= 40:
        level = "ELEVATED_ACTIVITY"
    elif score >= 15:
        level = "NORMAL_OPERATIONS"
    else:
        level = "LOW_ACTIVITY"

    return level, round(score)


async def fetch_fleet_report(fetcher) -> dict:
    """Generate naval fleet activity report.

    Aggregates:
    - Theater posture (5 theaters, aircraft counts)
    - Vessel snapshot (9 strategic waterways, naval status)
    - Military surge detections (anomalous foreign aircraft concentration)
    - Naval bases (filtered from static dataset)

    A component that raised or reported an error is named in
    unavailable_components and data_gaps, and the report is degraded; the
    readiness score is then computed without it. With all three down the
    report carries error / reason, so an outage never reads as
    LOW_ACTIVITY. data_gaps also carries each component's own gaps.
    """
    from . import intelligence, military, geospatial

    (
        posture_data,
        vessel_data,
        surge_data,
    ) = await asyncio.gather(
        _safe(military.fetch_theater_posture(fetcher), "theater_posture"),
        _safe(intelligence.fetch_vessel_snapshot(fetcher), "vessel_snapshot"),
        _safe(intelligence.fetch_military_surge(fetcher), "military_surge"),
    )

    # Get naval bases (sync, no fetcher needed)
    naval_bases = await geospatial.fetch_military_bases(base_type="naval_base")
    naval_base_count = naval_bases.get("count", 0)

    components = {
        "military_surge": surge_data,
        "theater_posture": posture_data,
        "vessel_snapshot": vessel_data,
    }
    unavailable = [name for name, data in components.items() if data.get("error")]
    data_gaps: list[str] = []
    for name, data in components.items():
        if data.get("error"):
            data_gaps.append(
                f"{name}: unavailable ({data['error']}); readiness_score is "
                "computed without it"
            )
            continue
        gaps = data.get("data_gaps") or []
        data_gaps.extend(f"{name}: {gap}" for gap in gaps)
        if data.get("degraded") and not gaps:
            data_gaps.append(f"{name}: degraded")

    # Extract key data
    theaters = posture_data.get("theaters", {})
    if not isinstance(theaters, dict):
        theaters = {}
    waterways = vessel_data.get("waterways", [])
    surges = surge_data.get("surges", [])

    # Compute fleet readiness
    readiness_level, readiness_score = _fleet_readiness(theaters, waterways, surges)

    # Theater summary — theaters is {name: {count, countries, top_types, ...}}
    theater_summary = []
    for name, t in theaters.items():
        if not isinstance(t, dict):
            continue
        theater_summary.append(
            {
                "name": name,
                "aircraft_count": t.get("count", 0),
                "top_types": t.get("top_types", [])[:3],
            }
        )

    # Waterway summary
    waterway_summary = []
    for ww in waterways:
        waterway_summary.append(
            {
                "name": ww.get("name", "Unknown"),
                "status": ww.get("status", "unknown"),
                # fetch_vessel_snapshot emits the count as "naval_warnings";
                # "warning_count" is kept as a fallback for older shapes.
                # Reading only "warning_count" made this 0 in every real
                # fleet report (the silent-zero class).
                "warning_count": ww.get("naval_warnings", ww.get("warning_count", 0)),
            }
        )

    # Active surges
    active_surges = []
    for s in surges:
        active_surges.append(
            {
                "region": s.get("region", "Unknown"),
                "aircraft_count": s.get("aircraft_count", 0),
                "baseline": s.get("baseline", 0),
                "ratio": s.get("ratio", 0),
            }
        )

    response = {
        "readiness_level": readiness_level,
        "readiness_score": readiness_score,
        "theater_summary": theater_summary,
        "theater_count": len(theater_summary),
        "waterway_summary": waterway_summary,
        "waterway_count": len(waterway_summary),
        "active_surges": active_surges,
        "surge_count": len(active_surges),
        "naval_base_count": naval_base_count,
        "total_tracked_aircraft": sum(
            t.get("count", 0) for t in theaters.values() if isinstance(t, dict)
        ),
        "data_gaps": data_gaps,
        "source": "fleet-activity-report",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if unavailable:
        # The zeroed fields above stay for consumers that read them;
        # these say which inputs they lack.
        response["unavailable_components"] = unavailable
        response["degraded"] = True
        if len(unavailable) == len(components):
            response["error"] = "All fleet report components unavailable: " + ", ".join(
                unavailable
            )
            response["reason"] = "fleet_components_unavailable"
    elif any(data.get("degraded") for data in components.values()):
        response["degraded"] = True
    return response
