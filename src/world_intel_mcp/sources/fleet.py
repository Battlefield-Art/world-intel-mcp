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
    unavailable_components and data_gaps, and the report is degraded; so is
    a vessel snapshot whose NGA feed is stale (feed_stale), since its
    warnings describe the feed's last update, not today. The readiness
    score leaves unavailable inputs out, which makes it a lower bound
    (readiness_lower_bound True, with the reason in data_gaps); so does a
    theater with no flight source (posture's unavailable_theaters) or a
    surge region left unchecked. With all three components unavailable the
    report carries error / reason, readiness_level "UNKNOWN" and
    readiness_score None, so an outage never reads as LOW_ACTIVITY.
    data_gaps also carries each available component's own gaps.
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
    data_gaps: list[str] = []
    unavailable: list[str] = []
    for name, data in components.items():
        if data.get("error"):
            unavailable.append(name)
            data_gaps.append(f"{name}: unavailable ({data['error']})")
            continue
        if data.get("feed_stale") is True:
            # Three 2024 warnings near Hormuz used to score as a critical
            # waterway today.
            unavailable.append(name)
            data_gaps.append(
                f"{name}: stale (its newest NGA warning was issued "
                f"{data.get('newest_issue_date')}, past the "
                f"{data.get('stale_after_days')}-day threshold)"
            )
            continue
        gaps = data.get("data_gaps") or []
        data_gaps.extend(f"{name}: {gap}" for gap in gaps)
        if data.get("degraded") and not gaps:
            data_gaps.append(f"{name}: degraded")
    scored = [name for name in components if name not in unavailable]

    # Extract key data. An unavailable component contributes nothing, and
    # neither does a theater no flight source answered for: its count of 0
    # means unknown.
    theaters = posture_data.get("theaters", {}) if "theater_posture" in scored else {}
    if not isinstance(theaters, dict):
        theaters = {}
    no_source = [t for t in posture_data.get("unavailable_theaters") or [] if t in theaters]
    theaters = {name: t for name, t in theaters.items() if name not in no_source}
    waterways = vessel_data.get("waterways", [])
    scored_waterways = waterways if "vessel_snapshot" in scored else []
    surges = surge_data.get("surges", []) if "military_surge" in scored else []
    unchecked = (
        (surge_data.get("unchecked_regions") or []) if "military_surge" in scored else []
    )

    # Compute fleet readiness
    readiness_lower_bound: bool | None
    if not scored:
        readiness_level, readiness_score = "UNKNOWN", None
        readiness_lower_bound = None
        data_gaps.append(
            "readiness_level is UNKNOWN and readiness_score is null: no component "
            "was available to score"
        )
    else:
        readiness_level, readiness_score = _fleet_readiness(
            theaters, scored_waterways, surges
        )
        left_out = list(unavailable)
        if no_source:
            left_out.append(f"the aircraft of {', '.join(no_source)}")
        if unchecked:
            left_out.append(f"surges in {', '.join(unchecked)}")
        readiness_lower_bound = bool(left_out)
        if left_out:
            gap = (
                "readiness_level and readiness_score are lower bounds: they "
                f"leave out {'; '.join(left_out)}"
            )
            if unavailable:
                # Each component adds at most 30 (see _fleet_readiness).
                gap += f", so the score can reach at most {30 * len(scored)} of 90"
            data_gaps.append(gap)

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
        "readiness_lower_bound": readiness_lower_bound,
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
    if no_source:
        response["unavailable_theaters"] = no_source
    if unavailable:
        # The zeroed fields above stay for consumers that read them;
        # these say which inputs they lack.
        response["unavailable_components"] = unavailable
        response["degraded"] = True
        if not scored:
            response["error"] = "All fleet report components unavailable: " + ", ".join(
                unavailable
            )
            response["reason"] = "fleet_components_unavailable"
    elif readiness_lower_bound or any(data.get("degraded") for data in components.values()):
        response["degraded"] = True
    return response
