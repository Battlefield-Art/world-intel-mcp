"""Cohort air-activity early warning: anomaly vs calendar baseline, and
where an excess came from.

Two analyses over adsb.lol half-hour heatmaps (see
``sources/jet_cohort.py`` for the data path and format):

``fetch_cohort_activity``
    Peak concurrent airborne cohort aircraft in one half-hour slot,
    compared with the same UTC slot on prior same-weekday dates
    (``baseline_weeks``, US federal holiday-adjacent dates swapped for
    earlier weeks). Robust z = (current - median) / scale, with scale
    floored by counting noise. A second z on the cohort's share of all
    tracked aircraft guards against feeder-network coverage swings;
    the alert level uses the smaller of the two.

``fetch_excess_origins``
    Infers takeoffs and landings of cohort aircraft in a window and the
    same window on prior weeks, clusters the endpoints by density
    (default 12-mile radius), and reports clusters whose current count
    exceeds the baseline median: the airport (group) an excess departed
    from or converged on, labelled anchor / regional / diffuse.

Honest limits, surfaced in every result: the baseline is a handful of
same-weekday slots fetched now (no year-long history, no learned
holiday profiles); the cohort is learned from live snapshots and only
members first seen ``min_learning_lead_hours`` before the window are
used, so a fresh install reports ``warming_up`` until it has history.

Approach (business-jet cohorts vs calendar baselines; endpoint
clustering of excess departures) inspired by kylemcdonald/ews; this
implementation is independent.

Gaps / not covered:
- The holiday rule is binary exclusion of +/-1 day around US federal
  holidays; non-US holidays and multi-day events are not modelled.
- Thresholds (alarm_sigma, cluster shares) are chosen defaults, not
  calibrated against labelled events.
"""

from __future__ import annotations

import math
import statistics
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from ..fetcher import Fetcher
from ..sources import jet_cohort as jc

_EASTERN = ZoneInfo("America/New_York")
MIN_COHORT = 25
MIN_BASELINE = 3
MIN_COVERAGE = 0.8
MAX_ORIGIN_FILES = 12
EARTH_RADIUS_MI = 3958.8
SELECTION_BIAS_NOTE = (
    "cohort selection bias: min_learning_lead_hours < 0 admits members first "
    "seen after the window started, which inflates the current window"
)


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    nxt = date(year + (month == 12), month % 12 + 1, 1)
    last = nxt - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def us_federal_holidays(year: int) -> dict[date, str]:
    """US federal holidays (5 U.S.C. 6103) for ``year``, plus the weekday
    each fixed-date holiday is observed on when it falls on a weekend."""
    fixed = {
        date(year, 1, 1): "New Year's Day",
        date(year, 6, 19): "Juneteenth",
        date(year, 7, 4): "Independence Day",
        date(year, 11, 11): "Veterans Day",
        date(year, 12, 25): "Christmas Day",
    }
    out: dict[date, str] = dict(fixed)
    for d, name in fixed.items():
        if d.weekday() == 5:
            out[d - timedelta(days=1)] = f"{name} (observed)"
        elif d.weekday() == 6:
            out[d + timedelta(days=1)] = f"{name} (observed)"
    out[_nth_weekday(year, 1, 0, 3)] = "Martin Luther King Jr. Day"
    out[_nth_weekday(year, 2, 0, 3)] = "Washington's Birthday"
    out[_last_weekday(year, 5, 0)] = "Memorial Day"
    out[_nth_weekday(year, 9, 0, 1)] = "Labor Day"
    out[_nth_weekday(year, 10, 0, 2)] = "Columbus Day"
    out[_nth_weekday(year, 11, 3, 4)] = "Thanksgiving Day"
    return out


def holiday_near(day: date, window_days: int = 1) -> str | None:
    """Name of a US federal holiday within +/- ``window_days`` of ``day``."""
    for delta in range(-window_days, window_days + 1):
        d = day + timedelta(days=delta)
        name = us_federal_holidays(d.year).get(d)
        if name:
            return name
    return None


def _local_day(start: datetime) -> date:
    return start.astimezone(_EASTERN).date()


def baseline_starts(
    target: datetime, weeks: int, max_extra: int = 2
) -> tuple[list[datetime], list[dict[str, str]]]:
    """Same UTC slot on prior same-weekday dates, skipping holiday-adjacent
    dates (replaced by earlier weeks, at most ``max_extra`` extra)."""
    chosen: list[datetime] = []
    skipped: list[dict[str, str]] = []
    k = 1
    while len(chosen) < weeks and k <= weeks + max_extra:
        cand = target - timedelta(weeks=k)
        hol = holiday_near(_local_day(cand))
        if hol:
            skipped.append({"date": cand.date().isoformat(), "reason": f"near {hol}"})
        else:
            chosen.append(cand)
        k += 1
    return chosen, skipped


# ---------------------------------------------------------------------------
# Activity metrics
# ---------------------------------------------------------------------------

def _is_airborne(fix: tuple) -> bool:
    return not fix[4] and (fix[5] is None or fix[5] >= 80.0)


def _is_groundish(fix: tuple) -> bool:
    return fix[4] or (fix[5] is not None and fix[5] < 40.0)


def concurrent_profile(slot: jc.SlotData, hold_seconds: float = 120.0) -> dict[str, Any]:
    """Peak / mean concurrent airborne aircraft across the slot's slices.

    An aircraft counts as airborne at time t when its latest fix at or
    before t is airborne and no older than ``hold_seconds`` (readsb only
    writes a fix when the trace advances, so single slices undercount).
    """
    events: list[tuple[float, int]] = []
    unique_airborne = 0
    for track in slot.tracks.values():
        intervals: list[list[float]] = []
        for i, fix in enumerate(track):
            if not _is_airborne(fix):
                continue
            end = fix[0] + hold_seconds
            if i + 1 < len(track):
                end = min(end, track[i + 1][0]) if not _is_airborne(track[i + 1]) else end
            if intervals and fix[0] <= intervals[-1][1]:
                intervals[-1][1] = max(intervals[-1][1], end)
            else:
                intervals.append([fix[0], end])
        if intervals:
            unique_airborne += 1
        for a, b in intervals:
            events.append((a, 1))
            events.append((b, -1))
    events.sort(key=lambda e: (e[0], e[1]))
    counts: list[int] = []
    level = 0
    j = 0
    for t in sorted(slot.slice_times):
        while j < len(events) and events[j][0] <= t:
            level += events[j][1]
            j += 1
        counts.append(level)
    return {
        "peak_concurrent": max(counts) if counts else 0,
        "mean_concurrent": round(sum(counts) / len(counts), 1) if counts else 0.0,
        "unique_airborne": unique_airborne,
        "all_tracked_aircraft": slot.total_aircraft,
        "coverage": round(slot.coverage, 3),
    }


def robust_score(current: float, baseline: list[float], alarm_sigma: float = 5.0) -> dict[str, Any]:
    """Robust z of ``current`` against ``baseline`` with a counting-noise floor."""
    center = statistics.median(baseline)
    mad = statistics.median([abs(b - center) for b in baseline])
    floor = max(math.sqrt(max(center, 0.0)), 0.03 * abs(center), 1.0)
    scale = max(1.4826 * mad, floor)
    z = (current - center) / scale
    return {
        "baseline_median": round(center, 3),
        "baseline_values": [round(b, 4) for b in baseline],
        "scale": round(scale, 3),
        "z": round(z, 2),
    }


def alert_level(z: float, alarm_sigma: float) -> tuple[int, str]:
    """1..5 from the positive z; deficits are reported separately."""
    if z >= alarm_sigma:
        return 5, "alarm"
    if z >= 4.0:
        return 4, "high"
    if z >= 3.0:
        return 3, "elevated"
    if z >= 2.0:
        return 2, "watch"
    return 1, "normal"


def _slot_label(start: datetime) -> str:
    return start.strftime("%Y-%m-%dT%H:%MZ")


def _parse_slot(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return jc.slot_start(dt)


# ---------------------------------------------------------------------------
# Tool 1: activity anomaly
# ---------------------------------------------------------------------------

async def fetch_cohort_activity(
    fetcher: Fetcher,
    cohort: str = "bizjet",
    region: str | None = None,
    baseline_weeks: int = 4,
    slot: str | None = None,
    alarm_sigma: float = 5.0,
    min_learning_lead_hours: float = 24.0,
    store: jc.CohortStore | None = None,
    disk: jc.HeatmapDiskCache | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Cohort concurrent-activity anomaly vs a same-weekday baseline."""
    now = now or datetime.now(timezone.utc)
    base = {"cohort": cohort, "source": "adsb.lol globe_history heatmaps", "license": "ODbL-1.0"}
    if cohort not in jc.COHORTS:
        return {**base, "error": f"cohort must be one of {list(jc.COHORTS)}"}
    if not 2 <= int(baseline_weeks) <= 8:
        return {**base, "error": "baseline_weeks must be between 2 and 8"}
    if alarm_sigma <= 2.0:
        return {**base, "error": "alarm_sigma must be greater than 2"}
    if not -168.0 <= min_learning_lead_hours <= 168.0:
        return {**base, "error": "min_learning_lead_hours must be between -168 and 168"}
    try:
        region_name, bbox = jc.resolve_region(region)
        target = _parse_slot(slot) if slot else None
    except ValueError as exc:
        return {**base, "error": str(exc)}
    if target is not None and target > jc.slot_start(now) - timedelta(minutes=30):
        return {**base, "error": "slot must be a completed half-hour in the past"}

    earliest = target or (jc.slot_start(now) - timedelta(minutes=90))
    learned_before = earliest.timestamp() - min_learning_lead_hours * 3600.0
    own_store = store is None
    store = store or jc.default_store(fetcher)
    try:
        members = store.members(cohort, learned_before=learned_before)
        stats = store.stats(cohort)
    finally:
        if own_store:
            store.close()
    base.update({"region": region_name, "bbox": list(bbox) if bbox else None})
    cohort_info = {
        **stats,
        "eligible_members": len(members),
        "min_learning_lead_hours": min_learning_lead_hours,
    }
    if len(members) < MIN_COHORT:
        return {
            **base,
            "status": "warming_up",
            "cohort_store": cohort_info,
            "message": (
                f"Only {len(members)} cohort members were learned at least "
                f"{min_learning_lead_hours:g} h before the window (need {MIN_COHORT}). "
                "Run intel_jet_cohort_snapshot periodically (or the collector "
                "daemon) to learn the cohort; a cohort learned during the window "
                "would bias the count upward."
            ),
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    wanted = set(members)
    disk = disk or jc.default_disk_cache(fetcher)
    if target is None:
        current = await jc.latest_published_slot(fetcher, wanted, bbox, disk, now=now)
    else:
        current = await jc.fetch_slot(fetcher, target, wanted, bbox, disk)
    if current.status != "ok" or current.data is None:
        return {
            **base,
            "status": "error",
            "error": f"current slot {_slot_label(current.start)} unavailable: "
            f"{current.error or current.status}",
            "cohort_store": cohort_info,
        }

    cur_metrics = concurrent_profile(current.data)
    starts, skipped = baseline_starts(current.start, int(baseline_weeks))
    windows: list[dict[str, Any]] = []
    data_gaps: list[str] = []
    for start in starts:
        res = await jc.fetch_slot(fetcher, start, wanted, bbox, disk)
        if res.status != "ok" or res.data is None:
            data_gaps.append(f"baseline {_slot_label(start)}: {res.error or res.status}")
            continue
        m = concurrent_profile(res.data)
        if m["coverage"] < MIN_COVERAGE:
            data_gaps.append(
                f"baseline {_slot_label(start)}: coverage {m['coverage']} < {MIN_COVERAGE}, excluded"
            )
            continue
        windows.append({"slot": _slot_label(start), "cached": res.cached, **m})

    result: dict[str, Any] = {
        **base,
        "window": {"slot": _slot_label(current.start), "cached": current.cached, **cur_metrics},
        "baseline_windows": windows,
        "baseline_skipped_holidays": skipped,
        "current_near_holiday": holiday_near(_local_day(current.start)),
        "cohort_store": cohort_info,
        "data_gaps": data_gaps,
        "method": (
            "peak concurrent airborne cohort aircraft (120 s hold) in one UTC "
            "half-hour slot vs the same slot on prior same-weekday dates; robust "
            "z = (x - median) / max(1.4826*MAD, sqrt(median), 3% of median, 1); "
            "level uses min(z on count, z on share of all tracked aircraft)"
        ),
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if cur_metrics["coverage"] < MIN_COVERAGE:
        data_gaps.append(f"current slot coverage {cur_metrics['coverage']} < {MIN_COVERAGE}")
    if min_learning_lead_hours < 0:
        data_gaps.append(SELECTION_BIAS_NOTE)
    if len(windows) < MIN_BASELINE:
        result["status"] = "insufficient_baseline"
        result["message"] = f"{len(windows)} usable baseline windows (need {MIN_BASELINE})"
        return result

    count_score = robust_score(
        cur_metrics["peak_concurrent"], [w["peak_concurrent"] for w in windows], alarm_sigma
    )
    share = cur_metrics["peak_concurrent"] / max(cur_metrics["all_tracked_aircraft"], 1)
    share_base = [w["peak_concurrent"] / max(w["all_tracked_aircraft"], 1) for w in windows]
    share_med = statistics.median(share_base)
    share_mad = statistics.median([abs(s - share_med) for s in share_base])
    # Same noise floor as the count, expressed in share units.
    count_floor = max(math.sqrt(max(count_score["baseline_median"], 0.0)), 1.0)
    share_floor = share_med * count_floor / max(count_score["baseline_median"], 1.0)
    share_scale = max(1.4826 * share_mad, share_floor, 1e-9)
    share_z = round((share - share_med) / share_scale, 2)

    z_used = min(count_score["z"], share_z)
    level, label = alert_level(z_used, alarm_sigma)
    if min_learning_lead_hours < 0:
        # A biased cohort must not raise (or clear) an alarm.
        level, label = None, "suppressed: selection-biased cohort"
    result.update(
        {
            "status": "degraded" if data_gaps else "ok",
            "score": {
                "count": count_score,
                "share_of_all_traffic": {
                    "current": round(share, 5),
                    "baseline_median": round(share_med, 5),
                    "z": share_z,
                },
                "z_used": z_used,
                "excess_aircraft": round(
                    cur_metrics["peak_concurrent"] - count_score["baseline_median"], 1
                ),
            },
            "alert_level": level,
            "alert": label,
            "alarm_sigma": alarm_sigma,
            "deficit": (z_used <= -3.0) if level is not None else None,
        }
    )
    return result


# ---------------------------------------------------------------------------
# Endpoint inference and clustering
# ---------------------------------------------------------------------------

def _climbs(seg: list[tuple], i: int, horizon: float = 600.0, min_ft: int = 1500) -> bool:
    base = seg[i][3]
    if base is None:
        return False
    alts = [f[3] for f in seg[i + 1:] if f[0] - seg[i][0] <= horizon and f[3] is not None]
    return bool(alts) and max(alts) - base >= min_ft


def _descends(seg: list[tuple], i: int, horizon: float = 600.0, min_ft: int = 1500) -> bool:
    base = seg[i][3]
    if base is None:
        return False
    alts = [f[3] for f in seg[:i] if seg[i][0] - f[0] <= horizon and f[3] is not None]
    return bool(alts) and max(alts) - base >= min_ft


def infer_endpoints(
    track: list[tuple],
    window_start: float,
    window_end: float,
    gap_seconds: float = 1200.0,
    edge_seconds: float = 120.0,
    low_ft: int = 10_000,
) -> list[tuple[str, tuple, str]]:
    """Takeoff / landing events ``(kind, fix, reason)`` for one aircraft.

    Direct evidence: a ground (or < 40 kt) fix followed within 10 min by an
    airborne (>= 80 kt) fix, or the reverse. Coverage-edge evidence: a
    track segment that begins (ends) inside the window below ``low_ft``
    and climbs (descends) >= 1500 ft within 10 min, i.e. the aircraft rose
    into (sank out of) receiver coverage near an airport.
    """
    if not track:
        return []
    segments: list[list[tuple]] = [[track[0]]]
    for fix in track[1:]:
        if fix[0] - segments[-1][-1][0] > gap_seconds:
            segments.append([fix])
        else:
            segments[-1].append(fix)

    events: list[tuple[str, tuple, str]] = []
    for si, seg in enumerate(segments):
        for a, b in zip(seg, seg[1:]):
            if b[0] - a[0] > 600.0:
                continue
            if _is_groundish(a) and _is_airborne(b):
                events.append(("takeoff", a, "ground_to_air"))
            elif _is_airborne(a) and _is_groundish(b):
                events.append(("landing", b, "air_to_ground"))
        first, last = seg[0], seg[-1]
        starts_inside = si > 0 or first[0] - window_start > edge_seconds
        if (
            starts_inside
            and _is_airborne(first)
            and first[3] is not None
            and first[3] <= low_ft
            and _climbs(seg, 0)
        ):
            events.append(("takeoff", first, "appeared_climbing"))
        ends_inside = si < len(segments) - 1 or window_end - last[0] > edge_seconds
        if (
            ends_inside
            and _is_airborne(last)
            and last[3] is not None
            and last[3] <= low_ft
            and _descends(seg, len(seg) - 1)
        ):
            events.append(("landing", last, "vanished_descending"))

    events.sort(key=lambda e: e[1][0])
    deduped: list[tuple[str, tuple, str]] = []
    last_time: dict[str, float] = {}
    for kind, fix, reason in events:
        if fix[0] - last_time.get(kind, -1e18) < 600.0:
            continue
        last_time[kind] = fix[0]
        deduped.append((kind, fix, reason))
    return deduped


def haversine_mi(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dl = math.radians(lon2 - lon1)
    h = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_MI * math.asin(min(1.0, math.sqrt(h)))


def density_clusters(
    points: list[dict[str, Any]], radius_mi: float, min_points: int
) -> list[int]:
    """DBSCAN labels (-1 = noise) using a 3-D grid on the sphere so the
    neighbour search is distortion-free at any latitude."""
    cell = radius_mi
    xyz: list[tuple[float, float, float]] = []
    grid: dict[tuple[int, int, int], list[int]] = {}
    for idx, p in enumerate(points):
        la, lo = math.radians(p["lat"]), math.radians(p["lon"])
        v = (
            EARTH_RADIUS_MI * math.cos(la) * math.cos(lo),
            EARTH_RADIUS_MI * math.cos(la) * math.sin(lo),
            EARTH_RADIUS_MI * math.sin(la),
        )
        xyz.append(v)
        grid.setdefault(tuple(int(math.floor(c / cell)) for c in v), []).append(idx)

    def neighbours(i: int) -> list[int]:
        cx, cy, cz = (int(math.floor(c / cell)) for c in xyz[i])
        out = []
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for dz in (-1, 0, 1):
                    for j in grid.get((cx + dx, cy + dy, cz + dz), ()):
                        if haversine_mi(points[i]["lat"], points[i]["lon"], points[j]["lat"], points[j]["lon"]) <= radius_mi:
                            out.append(j)
        return out

    labels = [None] * len(points)  # type: list[int | None]
    cluster_id = -1
    for i in range(len(points)):
        if labels[i] is not None:
            continue
        nb = neighbours(i)
        if len(nb) < min_points:
            labels[i] = -1
            continue
        cluster_id += 1
        labels[i] = cluster_id
        queue = [j for j in nb if j != i]
        while queue:
            j = queue.pop()
            if labels[j] == -1:
                labels[j] = cluster_id
            if labels[j] is not None:
                continue
            labels[j] = cluster_id
            nb_j = neighbours(j)
            if len(nb_j) >= min_points:
                queue.extend(k for k in nb_j if labels[k] is None or labels[k] == -1)
    return [lab if lab is not None else -1 for lab in labels]


def summarize_clusters(
    points: list[dict[str, Any]],
    n_baseline: int,
    radius_mi: float,
    min_points: int,
    ref: tuple[float, float] | None = None,
    top: int = 8,
) -> dict[str, Any]:
    """Per-cluster current vs baseline-median counts for one event kind."""
    labels = density_clusters(points, radius_mi, min_points)
    groups: dict[int, list[dict[str, Any]]] = {}
    for p, lab in zip(points, labels):
        if lab >= 0:
            groups.setdefault(lab, []).append(p)

    clusters = []
    for items in groups.values():
        cur = [p for p in items if p["window"] == 0]
        base_counts = [sum(1 for p in items if p["window"] == w) for w in range(1, n_baseline + 1)]
        med = statistics.median(base_counts) if base_counts else 0.0
        centre_src = cur or items
        clat = sum(p["lat"] for p in centre_src) / len(centre_src)
        clon = sum(p["lon"] for p in centre_src) / len(centre_src)
        delta = len(cur) - med
        entry = {
            "center": {"lat": round(clat, 4), "lon": round(clon, 4)},
            "current": len(cur),
            "baseline_counts": base_counts,
            "baseline_median": med,
            "delta": delta,
            "unique_aircraft_current": len({p["hex"] for p in cur}),
            "radius_mi": round(max(haversine_mi(clat, clon, p["lat"], p["lon"]) for p in items), 1),
            "evidence": dict(Counter(p["reason"] for p in cur)),
            "significant": delta >= max(3.0, 2.0 * math.sqrt(max(med, 1.0))),
        }
        if ref is not None:
            entry["distance_to_reference_mi"] = round(haversine_mi(ref[0], ref[1], clat, clon), 1)
        clusters.append(entry)

    positive = sorted((c for c in clusters if c["delta"] > 0), key=lambda c: (-c["delta"], -c["current"]))
    pos_total = sum(c["delta"] for c in positive)
    for c in positive:
        c["share_of_excess"] = round(c["delta"] / pos_total, 3) if pos_total else 0.0
    current_total = sum(1 for p in points if p["window"] == 0)
    base_totals = [sum(1 for p in points if p["window"] == w) for w in range(1, n_baseline + 1)]
    return {
        "current_events": current_total,
        "baseline_events": base_totals,
        "baseline_median_events": statistics.median(base_totals) if base_totals else 0,
        "positive_excess_total": pos_total,
        "clusters_total": len(clusters),
        "top_excess_clusters": positive[:top],
    }


def classify_excess(takeoffs: dict[str, Any], landings: dict[str, Any]) -> dict[str, str]:
    """Label the spatial shape of the excess from the top cluster shares."""

    def top(summary: dict[str, Any]) -> tuple[float, float, bool]:
        clusters = summary["top_excess_clusters"]
        if not clusters:
            return 0.0, 0.0, False
        c = clusters[0]
        return c["share_of_excess"], c["delta"], c["significant"]

    t_share, t_delta, t_sig = top(takeoffs)
    l_share, l_delta, l_sig = top(landings)
    dep_first = t_share * t_delta >= l_share * l_delta
    ordered = [("departure", t_share, t_sig), ("arrival", l_share, l_sig)]
    if not dep_first:
        ordered.reverse()
    for phase, share, sig in ordered:
        if sig and share >= 0.25:
            return {
                "pattern": f"{phase}_anchor",
                "explanation": (
                    f"One endpoint cluster holds {share:.0%} of the excess {phase}s: "
                    f"consistent with a specific airport or airport group "
                    f"{'emptying out' if phase == 'departure' else 'drawing traffic in'}."
                ),
            }
    for phase, share, sig in ordered:
        if sig and share >= 0.12:
            return {
                "pattern": f"regional_{phase}_concentration",
                "explanation": (
                    f"The strongest {phase} cluster holds {share:.0%} of the excess: a "
                    "leading region, but too spread out to name one airport."
                ),
            }
    if takeoffs["positive_excess_total"] > 0 or landings["positive_excess_total"] > 0:
        return {
            "pattern": "diffuse",
            "explanation": "Excess endpoints are spread across many clusters with no "
            "significant concentration (broad redistribution, not one origin).",
        }
    return {"pattern": "no_excess", "explanation": "No cluster exceeds its baseline median."}


# ---------------------------------------------------------------------------
# Tool 2: excess origins
# ---------------------------------------------------------------------------

async def _window_tracks(
    fetcher: Fetcher,
    start: datetime,
    slots: int,
    wanted: set[int],
    bbox: tuple[float, float, float, float] | None,
    disk: jc.HeatmapDiskCache,
) -> tuple[dict[int, list[tuple]], list[str], int]:
    tracks: dict[int, list[tuple]] = {}
    gaps: list[str] = []
    ok = 0
    for i in range(slots):
        s = start + timedelta(minutes=30 * i)
        res = await jc.fetch_slot(fetcher, s, wanted, bbox, disk)
        if res.status != "ok" or res.data is None:
            gaps.append(f"{_slot_label(s)}: {res.error or res.status}")
            continue
        ok += 1
        for addr, tr in res.data.tracks.items():
            tracks.setdefault(addr, []).extend(tr)
    for tr in tracks.values():
        tr.sort(key=lambda f: f[0])
    return tracks, gaps, ok


async def fetch_excess_origins(
    fetcher: Fetcher,
    cohort: str = "bizjet",
    region: str | None = None,
    start: str | None = None,
    slots: int = 2,
    baseline_weeks: int = 3,
    ref_lat: float | None = None,
    ref_lon: float | None = None,
    cluster_radius_mi: float = 12.0,
    min_cluster_points: int = 3,
    min_learning_lead_hours: float = 24.0,
    store: jc.CohortStore | None = None,
    disk: jc.HeatmapDiskCache | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Where did excess cohort departures/arrivals in a window come from?"""
    now = now or datetime.now(timezone.utc)
    base = {"cohort": cohort, "source": "adsb.lol globe_history heatmaps", "license": "ODbL-1.0"}
    if cohort not in jc.COHORTS:
        return {**base, "error": f"cohort must be one of {list(jc.COHORTS)}"}
    slots, baseline_weeks = int(slots), int(baseline_weeks)
    if not 1 <= slots <= 4:
        return {**base, "error": "slots must be between 1 and 4 (half-hours)"}
    if not 1 <= baseline_weeks <= 4:
        return {**base, "error": "baseline_weeks must be between 1 and 4"}
    if slots * (1 + baseline_weeks) > MAX_ORIGIN_FILES:
        return {
            **base,
            "error": f"slots x (1 + baseline_weeks) must be <= {MAX_ORIGIN_FILES} "
            "heatmap files (~14 MB each) per call",
        }
    if not 1.0 <= cluster_radius_mi <= 50.0:
        return {**base, "error": "cluster_radius_mi must be between 1 and 50"}
    if not -168.0 <= min_learning_lead_hours <= 168.0:
        return {**base, "error": "min_learning_lead_hours must be between -168 and 168"}
    if (ref_lat is None) != (ref_lon is None):
        return {**base, "error": "ref_lat and ref_lon must be given together"}
    try:
        region_name, bbox = jc.resolve_region(region)
        window_start = _parse_slot(start) if start else None
    except ValueError as exc:
        return {**base, "error": str(exc)}

    latest_complete = jc.slot_start(now) - timedelta(minutes=30)
    if window_start is not None and window_start + timedelta(minutes=30 * (slots - 1)) > latest_complete:
        return {**base, "error": "window must end at a completed half-hour in the past"}

    earliest = window_start or (latest_complete - timedelta(minutes=30 * (slots + 1)))
    learned_before = earliest.timestamp() - min_learning_lead_hours * 3600.0
    own_store = store is None
    store = store or jc.default_store(fetcher)
    try:
        members = store.members(cohort, learned_before=learned_before)
        stats = store.stats(cohort)
    finally:
        if own_store:
            store.close()
    base.update({"region": region_name, "bbox": list(bbox) if bbox else None})
    cohort_info = {**stats, "eligible_members": len(members), "min_learning_lead_hours": min_learning_lead_hours}
    if len(members) < MIN_COHORT:
        return {
            **base,
            "status": "warming_up",
            "cohort_store": cohort_info,
            "message": f"Only {len(members)} cohort members learned >= "
            f"{min_learning_lead_hours:g} h before the window (need {MIN_COHORT}); "
            "run intel_jet_cohort_snapshot periodically first.",
        }
    wanted = set(members)
    disk = disk or jc.default_disk_cache(fetcher)

    if window_start is None:
        latest = await jc.latest_published_slot(fetcher, wanted, bbox, disk, now=now)
        if latest.status != "ok":
            return {**base, "status": "error", "error": f"no recent heatmap published: {latest.error or latest.status}"}
        window_start = latest.start - timedelta(minutes=30 * (slots - 1))

    starts = [window_start]
    baseline, skipped = baseline_starts(window_start, baseline_weeks)
    starts.extend(baseline)

    takeoff_pts: list[dict[str, Any]] = []
    landing_pts: list[dict[str, Any]] = []
    data_gaps: list[str] = []
    windows_meta = []
    usable_baselines = 0
    for w_index, ws in enumerate(starts):
        tracks, gaps, ok = await _window_tracks(fetcher, ws, slots, wanted, bbox, disk)
        data_gaps.extend(gaps)
        if ok < slots:
            if w_index == 0:
                return {
                    **base,
                    "status": "error",
                    "error": "current window incomplete: " + "; ".join(gaps),
                    "cohort_store": cohort_info,
                }
            continue  # an incomplete baseline window would bias the median low
        label = 0 if w_index == 0 else usable_baselines + 1
        if w_index > 0:
            usable_baselines += 1
        t0 = ws.timestamp()
        t1 = t0 + 1800.0 * slots
        n_t = n_l = 0
        for addr, tr in tracks.items():
            for kind, fix, reason in infer_endpoints(tr, t0, t1):
                pt = {"lat": fix[1], "lon": fix[2], "window": label, "hex": addr, "reason": reason}
                if kind == "takeoff":
                    takeoff_pts.append(pt)
                    n_t += 1
                else:
                    landing_pts.append(pt)
                    n_l += 1
        windows_meta.append(
            {"start": _slot_label(ws), "role": "current" if w_index == 0 else "baseline", "takeoffs": n_t, "landings": n_l, "aircraft_tracked": len(tracks)}
        )

    if usable_baselines == 0:
        return {
            **base,
            "status": "insufficient_baseline",
            "windows": windows_meta,
            "data_gaps": data_gaps,
            "cohort_store": cohort_info,
        }
    if min_learning_lead_hours < 0:
        data_gaps.append(SELECTION_BIAS_NOTE)
    ref = (ref_lat, ref_lon) if ref_lat is not None else None
    take = summarize_clusters(takeoff_pts, usable_baselines, cluster_radius_mi, min_cluster_points, ref)
    land = summarize_clusters(landing_pts, usable_baselines, cluster_radius_mi, min_cluster_points, ref)
    return {
        **base,
        "status": "degraded" if data_gaps else "ok",
        "window": {
            "start": _slot_label(window_start),
            "end": _slot_label(window_start + timedelta(minutes=30 * slots)),
            "near_holiday": holiday_near(_local_day(window_start)),
        },
        "windows": windows_meta,
        "baseline_skipped_holidays": skipped,
        "classification": (
            classify_excess(take, land)
            if min_learning_lead_hours >= 0
            else {
                "pattern": "suppressed",
                "explanation": "Selection-biased cohort (members learned after the "
                "window): cluster deltas are shown but not classified.",
            }
        ),
        "takeoffs": take,
        "landings": land,
        "cluster_radius_mi": cluster_radius_mi,
        "min_cluster_points": min_cluster_points,
        "reference": {"lat": ref_lat, "lon": ref_lon} if ref else None,
        "cohort_store": cohort_info,
        "data_gaps": data_gaps,
        "method": (
            "takeoffs/landings inferred from ground<->air transitions and from "
            "low-altitude climbs/descents at coverage edges; endpoints from the "
            "current and baseline windows density-clustered together; each "
            "cluster's current count compared with its baseline median"
        ),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now.timestamp())),
    }
