"""Alert digest and weekly trends analysis for world-intel-mcp.

Provides cross-domain alert aggregation (intel_alert_digest) and
temporal trend analysis (intel_weekly_trends). Both use lazy imports
to avoid circular dependencies with source modules.
"""

import asyncio
import logging
import math
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("world-intel-mcp.analysis.alerts")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _safe_fetch(coro, label: str) -> dict:
    """Run a coroutine safely, returning empty dict on failure."""
    try:
        return await coro
    except Exception as exc:
        logger.warning("Alert digest: %s failed: %s", label, exc)
        return {}


# ---------------------------------------------------------------------------
# Alert Digest
# ---------------------------------------------------------------------------

_ALERT_THRESHOLDS: dict[str, dict] = {
    "space_weather": {
        "field": "current_kp",
        "threshold": 5.0,
        "priority": "high",
        "domain": "space",
        "message_template": "Geomagnetic storm: Kp={value} ({level})",
    },
    "instability": {
        "field": "countries",
        "threshold": 70,
        "priority": "critical",
        "domain": "political",
        "message_template": "{count} countries above instability threshold",
    },
    "military_surge": {
        "field": "surge_count",
        "threshold": 1,
        "priority": "high",
        "domain": "military",
        "message_template": "{count} military surge anomalies detected",
    },
    "cable_health": {
        "field": "corridors",
        "threshold": 2,
        "priority": "high",
        "domain": "infrastructure",
        "message_template": "{count} cable corridors at risk",
    },
    "hotspot_escalation": {
        "field": "hotspots",
        "threshold": 60,
        "priority": "critical",
        "domain": "security",
        "message_template": "{count} hotspots above escalation threshold",
    },
    "internet_outages": {
        "field": "outage_count",
        "threshold": 5,
        "priority": "medium",
        "domain": "infrastructure",
        "message_template": "{count} internet outages active",
    },
    "shipping_stress": {
        "field": "stress_score",
        "threshold": 30.0,
        "priority": "medium",
        "domain": "economic",
        "message_template": "Shipping stress index at {value}",
    },
}


async def fetch_alert_digest(fetcher) -> dict:
    """Aggregate alerts from 7 intelligence domains.

    Calls existing source functions in parallel, applies threshold-based
    alerting, and returns a prioritized alert list.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.

    Returns:
        Dict with alerts, alert_count, by_priority, domains_checked, source.
    """
    # Lazy imports to avoid circular deps
    from ..sources import space_weather, infrastructure
    from ..sources import intelligence
    from ..sources import shipping

    # Fetch all sources in parallel
    (
        sw_data,
        instability_data,
        surge_data,
        cable_data,
        hotspot_data,
        outage_data,
        shipping_data,
    ) = await asyncio.gather(
        _safe_fetch(space_weather.fetch_space_weather(fetcher), "space_weather"),
        _safe_fetch(intelligence.fetch_instability_index(fetcher), "instability"),
        _safe_fetch(intelligence.fetch_military_surge(fetcher), "military_surge"),
        _safe_fetch(infrastructure.fetch_cable_health(fetcher), "cable_health"),
        _safe_fetch(intelligence.fetch_hotspot_escalation(fetcher), "hotspot_escalation"),
        _safe_fetch(infrastructure.fetch_internet_outages(fetcher), "internet_outages"),
        _safe_fetch(shipping.fetch_shipping_index(fetcher), "shipping"),
    )

    alerts: list[dict] = []

    # Space weather: Kp >= 5
    kp = sw_data.get("current_kp")
    if kp is not None and kp >= _ALERT_THRESHOLDS["space_weather"]["threshold"]:
        alerts.append({
            "domain": "space",
            "priority": "high",
            "message": f"Geomagnetic storm: Kp={kp} ({sw_data.get('kp_level', 'Unknown')})",
            "value": kp,
        })

    # Instability: countries above threshold
    countries = instability_data.get("countries", [])
    high_instability = [c for c in countries if c.get("instability_index", 0) >= 70]
    if high_instability:
        alerts.append({
            "domain": "political",
            "priority": "critical",
            "message": f"{len(high_instability)} countries above instability threshold (>=70)",
            "countries": [c.get("country_name", c.get("country_code")) for c in high_instability[:5]],
            "value": len(high_instability),
        })

    # Military surge
    surge_count = surge_data.get("surge_count", 0)
    if surge_count >= 1:
        alerts.append({
            "domain": "military",
            "priority": "high",
            "message": f"{surge_count} military surge anomalies detected",
            "surges": surge_data.get("surges", [])[:3],
            "value": surge_count,
        })

    # Cable health: corridors with status_score >= 2
    corridors = cable_data.get("corridors", {})
    at_risk = [
        name for name, info in corridors.items()
        if isinstance(info, dict) and info.get("status_score", 0) >= 2
    ]
    if at_risk:
        alerts.append({
            "domain": "infrastructure",
            "priority": "high",
            "message": f"{len(at_risk)} cable corridors at elevated risk: {', '.join(at_risk[:3])}",
            "corridors": at_risk,
            "value": len(at_risk),
        })

    # Hotspot escalation: hotspots with score >= 60
    hotspots = hotspot_data.get("hotspots", [])
    hot = [h for h in hotspots if h.get("score", 0) >= 60]
    if hot:
        alerts.append({
            "domain": "security",
            "priority": "critical",
            "message": f"{len(hot)} hotspots above escalation threshold",
            "hotspots": [h.get("name") for h in hot[:5]],
            "value": len(hot),
        })

    # Internet outages
    outage_count = outage_data.get("outage_count", 0)
    if outage_count >= 5:
        alerts.append({
            "domain": "infrastructure",
            "priority": "medium",
            "message": f"{outage_count} internet outages active",
            "value": outage_count,
        })

    # Shipping stress
    stress = shipping_data.get("stress_score", 0)
    if stress >= 30:
        alerts.append({
            "domain": "economic",
            "priority": "medium" if stress < 60 else "high",
            "message": f"Shipping stress index at {stress} ({shipping_data.get('assessment', 'unknown')})",
            "value": stress,
        })

    # Sort: critical > high > medium
    priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    alerts.sort(key=lambda a: priority_order.get(a.get("priority", "low"), 4))

    # Group by priority
    by_priority: dict[str, int] = {}
    for a in alerts:
        p = a.get("priority", "unknown")
        by_priority[p] = by_priority.get(p, 0) + 1

    return {
        "alerts": alerts,
        "alert_count": len(alerts),
        "by_priority": by_priority,
        "domains_checked": [
            "space_weather", "instability", "military_surge",
            "cable_health", "hotspot_escalation", "internet_outages",
            "shipping_stress",
        ],
        "source": "alert-digest",
        "timestamp": _utc_now_iso(),
    }


# ---------------------------------------------------------------------------
# Weekly Trends
# ---------------------------------------------------------------------------

# A metric needs this many observations before its volatility is reported.
_MIN_TREND_OBSERVATIONS = 5


def _read_baselines(db_path: str) -> tuple[list[tuple], str | None]:
    """Every baseline row, or the reason none could be read.

    The file is opened read-only: reading trends must never leave an empty
    database where the baselines were expected.
    """
    if not os.path.exists(db_path):
        return [], f"no temporal baseline database at {db_path}"
    try:
        conn = sqlite3.connect(f"{Path(db_path).absolute().as_uri()}?mode=ro", uri=True)
        try:
            rows = conn.execute(
                "SELECT key, count, mean, m2, updated_at FROM baselines"
            ).fetchall()
        finally:
            conn.close()
    except (sqlite3.Error, OSError) as exc:
        logger.warning("Failed to read temporal baselines: %s", exc)
        return [], f"could not read {db_path}: {exc}"
    return rows, None


async def fetch_weekly_trends(fetcher) -> dict:
    """Analyze weekly trends from temporal baselines.

    Reads the TemporalBaseline SQLite database to compute volatility
    (coefficient of variation) for each tracked metric, and calls
    temporal_anomalies to get current deviations.

    A failed input never reads as zero. ``data_gaps`` names every input
    that failed or has no history to judge by. When the baseline database
    is missing or unreadable, ``trend_count`` is None and ``error`` says
    why; when anomaly detection fails, ``current_anomaly_count`` is None.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.

    Returns:
        Dict with trends, trend_count, current_anomalies,
        current_anomaly_count, data_gaps, analysis_period, source, and
        error when the baselines could not be read.
    """
    from ..analysis.temporal import _default_db_path
    from ..sources import intelligence

    db_path = _default_db_path()
    # Anomaly detection records this cycle's observations and creates the
    # database on first use, so look first: a baseline born in this call has
    # no history, which is not the same as a quiet week.
    baseline_existed = os.path.exists(db_path)
    data_gaps: list[str] = []

    # Get current anomalies
    current_anomalies: list[dict] = []
    current_anomaly_count: int | None = None
    try:
        anomaly_data = await intelligence.fetch_temporal_anomalies(fetcher)
    except Exception as exc:
        logger.warning("Weekly trends: temporal anomalies failed: %s", exc)
        data_gaps.append(f"Current anomalies: {type(exc).__name__}: {exc}")
    else:
        anomalies = (
            anomaly_data.get("anomalies") if isinstance(anomaly_data, dict) else None
        )
        if isinstance(anomalies, list):
            current_anomalies = anomalies
            current_anomaly_count = len(anomalies)
            data_gaps.extend(
                f"Current anomalies: {gap}" for gap in anomaly_data.get("data_gaps") or []
            )
        else:
            data_gaps.append(
                "Current anomalies: anomaly detection returned no anomaly list"
            )

    # Read baselines from SQLite
    rows, baseline_problem = _read_baselines(db_path)
    trends: list[dict] = []
    trend_count: int | None = None

    if baseline_problem is not None:
        data_gaps.append(f"Baselines: {baseline_problem}")
    else:
        for key, n, mean, m2, updated_at in rows:
            if n < _MIN_TREND_OBSERVATIONS:
                continue
            parts = key.split(":")
            if len(parts) < 2:
                continue

            event_type = parts[0]
            region = parts[1]
            weekday = parts[2] if len(parts) > 2 else ""
            month = parts[3] if len(parts) > 3 else ""

            # Compute coefficient of variation (volatility)
            variance = m2 / (n - 1) if n > 1 else 0.0
            std = math.sqrt(variance) if variance > 0 else 0.0
            cv = (std / mean * 100) if mean > 0 else 0.0

            trends.append({
                "metric": event_type,
                "region": region,
                "weekday": weekday,
                "month": month,
                "observations": n,
                "mean": round(mean, 2),
                "std_dev": round(std, 2),
                "volatility_cv": round(cv, 1),
                "last_updated": updated_at,
            })
        trend_count = len(trends)

        if not baseline_existed:
            data_gaps.append(
                f"Baselines: none existed at {db_path} before this call, so "
                "trends and anomalies have no history yet"
            )
        elif not any(row[1] >= _MIN_TREND_OBSERVATIONS for row in rows):
            data_gaps.append(
                "Baselines: no tracked metric has the "
                f"{_MIN_TREND_OBSERVATIONS} observations volatility needs yet "
                f"({len(rows)} tracked)"
            )

    # Sort by volatility descending
    trends.sort(key=lambda t: t.get("volatility_cv", 0), reverse=True)

    result = {
        "trends": trends[:50],
        "trend_count": trend_count,
        "current_anomalies": current_anomalies,
        "current_anomaly_count": current_anomaly_count,
        "data_gaps": data_gaps,
        "analysis_period": "weekly (by weekday+month seasonality)",
        "source": "temporal-weekly-trends",
        "timestamp": _utc_now_iso(),
    }
    if baseline_problem is not None:
        result["error"] = f"Temporal baselines unavailable: {baseline_problem}"
    return result
