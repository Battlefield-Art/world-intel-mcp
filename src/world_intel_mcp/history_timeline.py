"""Day timeline: what the history store holds for one UTC calendar day
(ROADMAP Phase 29).

Two views over the store that ``history.py`` keeps in the cache database,
both read through a read-only SQLite connection (``mode=ro``), so this
module cannot write the store; it never fetches upstream either.

- ``by="collected"``: the run log for the day. Per source: attempts,
  ok / degraded / error counts with their reasons, captures whose payload
  changed or matched the capture before it, first and last capture time,
  observations stored, and how many of the day's payload bodies are still
  kept. A source without a capture that day says why: the collector
  recorded no capture of any source that day, it ran but not this source,
  the source has no capture in the retained run log (``never_run``), or
  the day is past run-log retention. The observations saved that day are
  listed too.
- ``by="observed"``: observations whose ``observed_at`` falls on the day,
  as a date (``YYYY-MM-DD``) or a UTC instant, one item per series and
  observed_at with its latest stored value and how many values were
  stored for it. Month (``YYYY-MM``) and year (``YYYY``) values that
  contain the day are listed separately, as period values. Observations
  without ``observed_at`` cannot be placed on any day: they are excluded,
  and the ones collected that day are counted.

The day is ``YYYY-MM-DD`` in UTC, the half-open window
``[day 00:00:00Z, next day 00:00:00Z)``. Collection times are compared as
epoch seconds, ``observed_at`` as stored text (the store keeps instants in
UTC as ``YYYY-MM-DDTHH:MM:SSZ``). A future day is an error; today is
allowed and reported as in progress.

Retention (``history.retention_days()``) is stated per record kind for the
requested day, so a day whose records were pruned reads as pruned, never
as a day on which nothing happened.

Queries use the existing indexes: the run log and the observations saved
on a day range-scan ``idx_history_captures_time`` and
``idx_history_obs_time``; the observed-date view walks the distinct
series keys of ``idx_history_obs_series`` (one seek per key, a loose index
scan) and range-seeks each key's ``observed_at``, so only matching rows
are read.

Gaps / not covered:
- Retention windows come from this process's environment. The collector
  prunes with its own, so a collector run with different
  ``WORLD_INTEL_HISTORY_*`` values makes the windows stated here wrong.
- A date-only ``observed_at`` is the publisher's calendar date; it is
  matched to the requested day by its label, not converted from the
  publisher's time zone, which the payloads do not state.
- Collector cycles are not stored as such: "the collector did not run"
  means no capture of any source is recorded in the day's window of this
  database.
- The observed-date view costs one index seek per distinct series key.
  USGS quakes get a series per event, so the cost grows with the event
  count: about 12 ms for 11,000 series in 1.75 million rows, measured on
  a synthetic store, not on a live one.
"""

from __future__ import annotations

import re
import sqlite3
import time
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from .history import HISTORY_ACCRUAL_NOTE, retention_days

DAY_FORMAT = "YYYY-MM-DD"
VIEWS = ("collected", "observed")
DEFAULT_LIMIT = 100
DEFAULT_PER_SOURCE_LIMIT = 10
MAX_LIMIT = 5000
MAX_PER_SOURCE_LIMIT = 1000
# Distinct (status, reason) pairs shown per source; the rest are counted.
MAX_REASONS = 5

_DAY = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_TABLES = ("history_captures", "history_payloads", "history_observations")

# ---------------------------------------------------------------------------
# SQL. Every statement is a constant; values travel as named parameters.
# ---------------------------------------------------------------------------

_CAPTURE_SUMMARY_SQL = """
SELECT c.source, COUNT(*),
       SUM(c.status = 'ok'), SUM(c.status = 'degraded'), SUM(c.status = 'error'),
       SUM(c.status != 'error' AND c.changed IS 1),
       SUM(c.status != 'error' AND c.changed IS 0),
       SUM(c.status != 'error' AND c.payload_hash IS NOT NULL AND c.changed IS NULL),
       SUM(c.status != 'error' AND c.payload_hash IS NULL),
       MIN(c.collected_at), MAX(c.collected_at),
       SUM(c.observations_stored), COUNT(c.observations_extracted),
       SUM(c.payload_hash IS NOT NULL AND p.payload_hash IS NOT NULL),
       SUM(c.payload_hash IS NOT NULL AND p.payload_hash IS NULL)
FROM history_captures c
LEFT JOIN history_payloads p ON p.source = c.source AND p.payload_hash = c.payload_hash
WHERE c.collected_at >= :start AND c.collected_at < :end
  AND (:source IS NULL OR c.source = :source)
GROUP BY c.source
"""

_CAPTURE_REASONS_SQL = """
SELECT source, status, error, COUNT(*), MIN(collected_at), MAX(collected_at)
FROM history_captures
WHERE collected_at >= :start AND collected_at < :end AND error IS NOT NULL
  AND (:source IS NULL OR source = :source)
GROUP BY source, status, error
ORDER BY source, COUNT(*) DESC, MAX(collected_at) DESC
"""

_COLLECTOR_DAY_SQL = """
SELECT COUNT(*), COUNT(DISTINCT source), MIN(collected_at), MAX(collected_at)
FROM history_captures WHERE collected_at >= :start AND collected_at < :end
"""

_LONGEST_INTERNAL_GAP_SQL = """
SELECT prev, collected_at FROM (
    SELECT collected_at, LAG(collected_at) OVER (ORDER BY collected_at) AS prev
    FROM history_captures WHERE collected_at >= :start AND collected_at < :end
) WHERE prev IS NOT NULL
ORDER BY collected_at - prev DESC, collected_at
LIMIT 1
"""

# Two scalar subqueries: SQLite turns a lone MIN or MAX into one index
# seek, but MIN and MAX in the same SELECT scan the whole index.
_RUN_LOG_BOUNDS_SQL = (
    "SELECT (SELECT MIN(collected_at) FROM history_captures), "
    "(SELECT MAX(collected_at) FROM history_captures)"
)

_SOURCE_NEIGHBOURS_SQL = """
SELECT (SELECT MAX(collected_at) FROM history_captures
        WHERE source = :name AND collected_at < :start),
       (SELECT MIN(collected_at) FROM history_captures
        WHERE source = :name AND collected_at >= :end)
"""

# Distinct values of an indexed leading column, one index seek each.
_CAPTURE_SOURCES_SQL = """
WITH RECURSIVE s(name) AS (
    SELECT MIN(source) FROM history_captures
    UNION ALL
    SELECT (SELECT MIN(source) FROM history_captures WHERE source > s.name)
    FROM s WHERE s.name IS NOT NULL
)
SELECT name FROM s WHERE name IS NOT NULL
"""

_OBSERVATION_SOURCES_SQL = """
WITH RECURSIVE s(name) AS (
    SELECT MIN(source) FROM history_observations
    UNION ALL
    SELECT (SELECT MIN(source) FROM history_observations WHERE source > s.name)
    FROM s WHERE s.name IS NOT NULL
)
SELECT name FROM s WHERE name IS NOT NULL
"""

_COLLECTED_OBS_COUNTS_SQL = """
SELECT source, COUNT(*)
FROM history_observations
WHERE collected_at >= :start AND collected_at < :end
  AND (:source IS NULL OR source = :source)
GROUP BY source
"""

_ITEM_COLUMNS = (
    "source, series_key, observed_at, published_at, collected_at, value, unit, label, "
    "capture_id, (SELECT 1 FROM history_captures c WHERE c.id = capture_id) IS NOT NULL"
)

_COLLECTED_OBS_ITEMS_SQL = (
    "SELECT " + _ITEM_COLUMNS + " FROM ("
    "  SELECT o.*, ROW_NUMBER() OVER (PARTITION BY source ORDER BY collected_at, id) AS in_source"
    "  FROM history_observations o"
    "  WHERE collected_at >= :start AND collected_at < :end"
    "    AND (:source IS NULL OR source = :source)"
    ") WHERE in_source <= :per_source ORDER BY collected_at, id LIMIT :limit"
)

_SERIES_KEYS_CTE = """
WITH RECURSIVE keys(k) AS (
    SELECT MIN(series_key) FROM history_observations
    UNION ALL
    SELECT (SELECT MIN(series_key) FROM history_observations WHERE series_key > keys.k)
    FROM keys WHERE keys.k IS NOT NULL
)
"""

# Every stored string with the day as prefix sorts between :day and
# :day_last ('<day>T23:59:59Z'); the CASE keeps the two recognized forms
# and leaves anything else (only a writer bypassing record_cycle can
# store it) NULL, to be counted rather than shown.
_DAY_MATCHED_CTE = """
, matched AS (
    SELECT o.id, o.source, o.series_key, o.observed_at, o.published_at, o.collected_at,
           o.value, o.unit, o.label, o.capture_id,
           CASE WHEN o.observed_at = :day THEN 'date'
                WHEN o.observed_at GLOB :instant_glob THEN 'instant' END AS precision
    FROM keys JOIN history_observations o
      ON o.series_key = keys.k AND o.observed_at >= :day AND o.observed_at <= :day_last
    WHERE :source IS NULL OR o.source = :source
)
"""

_PERIOD_MATCHED_CTE = """
, matched AS (
    SELECT o.id, o.source, o.series_key, o.observed_at, o.published_at, o.collected_at,
           o.value, o.unit, o.label, o.capture_id,
           CASE o.observed_at WHEN :month THEN 'month' WHEN :year THEN 'year' END AS precision
    FROM keys JOIN history_observations o
      ON o.series_key = keys.k AND o.observed_at IN (:month, :year)
    WHERE :source IS NULL OR o.source = :source
)
"""

_POINTS_CTE = """
, points AS (
    SELECT matched.*,
           ROW_NUMBER() OVER (PARTITION BY series_key, observed_at
                              ORDER BY collected_at DESC, id DESC) AS latest,
           COUNT(*) OVER (PARTITION BY series_key, observed_at) AS stored_values,
           FIRST_VALUE(value) OVER (PARTITION BY series_key, observed_at
                                    ORDER BY collected_at, id) AS first_value,
           FIRST_VALUE(collected_at) OVER (PARTITION BY series_key, observed_at
                                           ORDER BY collected_at, id) AS first_collected_at
    FROM matched
)
"""

_POINT_COUNTS_SELECT = """
SELECT source,
       SUM(precision IS NOT NULL AND latest = 1),
       SUM(precision IS NOT NULL AND latest = 1 AND stored_values > 1),
       SUM(precision IS NULL)
FROM points GROUP BY source
"""

_POINT_ITEMS_SELECT = (
    "SELECT " + _ITEM_COLUMNS + ", precision, stored_values, first_value, first_collected_at "
    "FROM ("
    "  SELECT points.*, ROW_NUMBER() OVER (PARTITION BY source ORDER BY observed_at, series_key)"
    "    AS in_source"
    "  FROM points WHERE latest = 1 AND precision IS NOT NULL"
    ") WHERE in_source <= :per_source ORDER BY observed_at, series_key, source LIMIT :limit"
)

_DAY_COUNTS_SQL = _SERIES_KEYS_CTE + _DAY_MATCHED_CTE + _POINTS_CTE + _POINT_COUNTS_SELECT
_DAY_ITEMS_SQL = _SERIES_KEYS_CTE + _DAY_MATCHED_CTE + _POINTS_CTE + _POINT_ITEMS_SELECT
_PERIOD_COUNTS_SQL = _SERIES_KEYS_CTE + _PERIOD_MATCHED_CTE + _POINTS_CTE + _POINT_COUNTS_SELECT
_PERIOD_ITEMS_SQL = _SERIES_KEYS_CTE + _PERIOD_MATCHED_CTE + _POINTS_CTE + _POINT_ITEMS_SELECT

_NO_OBSERVED_AT_SQL = """
SELECT source, COUNT(*) FROM history_observations
WHERE collected_at >= :start AND collected_at < :end AND observed_at IS NULL
  AND (:source IS NULL OR source = :source)
GROUP BY source
"""

# ---------------------------------------------------------------------------
# Explanations returned with every answer
# ---------------------------------------------------------------------------

STATES_EXPLAINED = {
    "changed": "answered that day, and at least one delivered payload differed from the "
    "source's capture before it (or was the source's first capture)",
    "unchanged": "answered that day, and every delivered payload was identical to the "
    "capture before it",
    "answered": "answered that day, but no payload hash was stored, so change is unknown",
    "degraded": "no ok capture that day; every delivered payload was marked stale or "
    "degraded, was empty, or lacked the measures its extractor expects",
    "failed": "every attempt that day was an error (reasons lists why)",
    "not_attempted": "the collector captured other sources that day but not this one; "
    "this source has captures on other days",
    "collector_did_not_run": "no capture of any source is recorded that day",
    "never_run": "no capture of this source anywhere in the retained run log",
    "unknown_past_retention": "part or all of the day is past run-log retention and no "
    "capture of this source from it remains: whether it ran cannot be read",
}

PRECISION_EXPLAINED = {
    "date": "observed_at is the day itself, as the publisher dates it",
    "instant": "observed_at is a UTC time on the day",
    "month": "a value for the whole month containing the day, not a value for the day",
    "year": "a value for the whole year containing the day, not a value for the day",
}

_NO_OBSERVED_AT_REASON = (
    "the payload carries no observation date (for example Yahoo quotes); collected_at is "
    "when this server saved the value, not when it was observed, so it cannot be placed "
    "on an observation date"
)

_RETENTION_ENV_NOTE = (
    "Retention windows are read from this server's environment "
    "(WORLD_INTEL_HISTORY_PAYLOAD_DAYS, WORLD_INTEL_HISTORY_CAPTURE_DAYS, "
    "WORLD_INTEL_HISTORY_DAYS); the collector prunes with its own, at the end of each cycle."
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (OverflowError, OSError, ValueError):
        return None


def _bounded(value: Any, default: int, maximum: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return max(1, min(maximum, number))


def parse_day(value: Any, now: float) -> tuple[date | None, str | None]:
    """``YYYY-MM-DD`` -> (date, None), or (None, problem)."""
    if not isinstance(value, str) or not _DAY.fullmatch(value):
        return None, f"day must be {DAY_FORMAT}, a UTC calendar date; got {value!r}"
    try:
        day = date.fromisoformat(value)
    except ValueError:
        return None, f"day must be a real calendar date in {DAY_FORMAT}; got {value!r}"
    today = datetime.fromtimestamp(now, tz=timezone.utc).date()
    if day > today:
        return None, f"day {value} is in the future (today is {today.isoformat()} UTC)"
    return day, None


@dataclass(frozen=True)
class _Window:
    day: date
    start: float
    end: float
    now: float

    @classmethod
    def for_day(cls, day: date, now: float) -> "_Window":
        start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp()
        return cls(day, start, start + 86400.0, now)

    @property
    def in_progress(self) -> bool:
        return self.now < self.end

    def describe(self) -> dict:
        nxt = self.day + timedelta(days=1)
        out = {
            "start": f"{self.day.isoformat()}T00:00:00Z",
            "end": f"{nxt.isoformat()}T00:00:00Z",
            "end_exclusive": True,
            "complete": not self.in_progress,
        }
        if self.in_progress:
            out["through"] = _iso(self.now)
        return out


class _Reader:
    """Read-only access; with no store every query reads as empty."""

    def __init__(self, conn: sqlite3.Connection | None):
        self.conn = conn

    def all(self, sql: str, params: dict | None = None) -> list[tuple]:
        if self.conn is None:
            return []
        return self.conn.execute(sql, params or {}).fetchall()

    def one(self, sql: str, params: dict | None, default: tuple) -> tuple:
        rows = self.all(sql, params)
        return rows[0] if rows else default


def _connect(db_path: Path) -> sqlite3.Connection | None:
    """A read-only connection, or None when the database does not exist
    (opening it read-only never creates it)."""
    path = Path(db_path).resolve()
    if not path.exists():
        return None
    return sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=10)


def _has_history_tables(conn: sqlite3.Connection) -> bool:
    found = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (?, ?, ?)",
            _TABLES,
        )
    }
    return found == set(_TABLES)


def _retention(window: _Window) -> dict:
    """Configured window per record kind, against the requested day."""
    days = retention_days()
    out: dict[str, Any] = {"note": _RETENTION_ENV_NOTE}
    for label, key in (
        ("payload_bodies", "payload_days"),
        ("run_log", "capture_days"),
        ("observations", "history_days"),
    ):
        span = days[key]
        if span <= 0:
            out[label] = {"days": span, "cutoff": None, "day": "kept_forever"}
            continue
        cutoff = window.now - span * 86400
        if window.end <= cutoff:
            status = "past_window"
        elif window.start < cutoff:
            status = "partly_past_window"
        else:
            status = "within_window"
        out[label] = {"days": span, "cutoff": _iso(cutoff), "cutoff_ts": cutoff, "day": status}
    return out


def _public_retention(retention: dict) -> dict:
    return {
        k: ({kk: vv for kk, vv in v.items() if kk != "cutoff_ts"} if isinstance(v, dict) else v)
        for k, v in retention.items()
    }


def _item(row: tuple) -> dict:
    (source, series_key, observed_at, published_at, collected_at, value, unit, label,
     capture_id, capture_retained) = row[:10]
    return {
        "source": source,
        "series_key": series_key,
        "observed_at": observed_at,
        "published_at": published_at,
        "collected_at": _iso(collected_at),
        "value": value,
        "unit": unit,
        "label": label,
        "capture_id": capture_id,
        "capture_retained": bool(capture_retained),
    }


def _point(row: tuple) -> dict:
    item = _item(row)
    precision, stored_values, first_value, first_collected_at = row[10:14]
    item["precision"] = precision
    item["stored_values"] = stored_values
    if stored_values > 1:
        item["first_value"] = first_value
        item["first_collected_at"] = _iso(first_collected_at)
    return item


def _truncation(matched_by_source: dict[str, int], items: list[dict], limit: int,
                per_source: int) -> dict:
    returned = Counter(item["source"] for item in items)
    matched = sum(matched_by_source.values())
    return {
        "matched": matched,
        "returned": len(items),
        "truncated": len(items) < matched,
        "limit": limit,
        "per_source_limit": per_source,
        "sources_truncated": {
            name: {"matched": count, "returned": returned.get(name, 0)}
            for name, count in sorted(matched_by_source.items())
            if returned.get(name, 0) < count
        },
    }


def _truncation_gap(what: str, block: dict, order: str) -> str | None:
    if not block["truncated"]:
        return None
    return (
        f"{what}: {block['returned']:,} of {block['matched']:,} returned ({order}; at most "
        f"{block['per_source_limit']:,} per source and {block['limit']:,} in all). The per-source "
        "counts are complete; raise limit or per_source_limit, or pass source, to see more."
    )


# ---------------------------------------------------------------------------
# by="collected"
# ---------------------------------------------------------------------------


def _collector_block(r: _Reader, w: _Window, run_log: dict) -> dict:
    params = {"start": w.start, "end": w.end}
    captures, sources, first, last = r.one(_COLLECTOR_DAY_SQL, params, (0, 0, None, None))
    log_first, log_last = r.one(_RUN_LOG_BOUNDS_SQL, None, (None, None))
    block: dict[str, Any] = {
        "captures": captures,
        "sources_captured": sources,
        "run_log_first_capture_at": _iso(log_first),
        "run_log_last_capture_at": _iso(log_last),
    }
    if captures:
        # The retained part of the day starts at the run-log cutoff when
        # the day straddles it; time before that is unknown, not silent.
        lower = w.start
        if run_log["day"] == "partly_past_window":
            lower = min(max(w.start, run_log["cutoff_ts"]), first)
        spans = [(lower, first), (last, min(w.end, w.now))]
        internal = r.one(_LONGEST_INTERNAL_GAP_SQL, params, (None, None))
        if internal[0] is not None:
            spans.append(internal)
        gap_from, gap_to = max(spans, key=lambda span: span[1] - span[0])
        block.update(
            state="ran",
            first_capture_at=_iso(first),
            last_capture_at=_iso(last),
            longest_gap_s=round(max(0.0, gap_to - gap_from), 1),
            longest_gap_from=_iso(gap_from),
            longest_gap_to=_iso(gap_to),
            explanation=f"{captures:,} captures of {sources} source(s) are recorded that day.",
        )
        return block
    if run_log["day"] in ("past_window", "partly_past_window"):
        block.update(
            state="unknown_past_retention",
            explanation=(
                f"No capture from this day remains, and the day is past the "
                f"{run_log['days']:g}-day run-log retention (cutoff {run_log['cutoff']}): "
                "whether the collector ran cannot be read."
            ),
        )
        return block
    if log_first is None:
        context, why = "empty_run_log", "the run log has no capture at all in this database"
    elif log_first >= w.end:
        context, why = "before_first_capture", f"the run log starts later, at {_iso(log_first)}"
    elif log_last < w.start:
        context, why = "after_last_capture", f"no capture is recorded since {_iso(log_last)}"
    else:
        context, why = "between_captures", "captures exist before and after this day"
    block.update(
        state="did_not_run",
        context=context,
        explanation=f"The collector recorded no capture of any source that day: {why}.",
    )
    return block


def _captured_state(ok: int, degraded: int, changed: int, unchanged: int, first: int) -> str:
    if ok == 0 and degraded == 0:
        return "failed"
    if ok == 0:
        return "degraded"
    if changed or first:
        return "changed"
    if unchanged:
        return "unchanged"
    return "answered"


def _collected_view(r: _Reader, w: _Window, names: list[str], roster: set[str],
                    source: str | None, limit: int, per_source: int,
                    retention: dict) -> dict:
    params = {"start": w.start, "end": w.end, "source": source}
    run_log = retention["run_log"]
    summary = {row[0]: row for row in r.all(_CAPTURE_SUMMARY_SQL, params)}
    reasons: dict[str, list[tuple]] = {}
    for row in r.all(_CAPTURE_REASONS_SQL, params):
        reasons.setdefault(row[0], []).append(row[1:])
    obs_counts = dict(r.all(_COLLECTED_OBS_COUNTS_SQL, params))
    collector = _collector_block(r, w, run_log)

    if source is None:
        names = names + sorted((set(summary) | set(obs_counts)) - set(names))
    rows = []
    for name in names:
        retained_obs = obs_counts.get(name, 0)
        entry: dict[str, Any] = {"source": name, "in_collector_roster": name in roster}
        cap = summary.get(name)
        if cap is None:
            if run_log["day"] in ("past_window", "partly_past_window"):
                state = "unknown_past_retention"
            else:
                before, after = r.one(
                    _SOURCE_NEIGHBOURS_SQL, {"name": name, "start": w.start, "end": w.end},
                    (None, None),
                )
                if before is None and after is None:
                    state = "never_run"
                elif collector["captures"] == 0:
                    state = "collector_did_not_run"
                else:
                    state = "not_attempted"
                entry["last_capture_before"] = _iso(before)
                entry["next_capture_after"] = _iso(after)
            entry.update(day_state=state, attempts=0, observations_retained=retained_obs)
            rows.append(entry)
            continue
        (_, attempts, ok, degraded, error, changed, unchanged, first, not_stored,
         first_at, last_at, obs_stored, with_extractor, bodies_kept, bodies_pruned) = cap
        source_reasons = reasons.get(name, [])
        entry.update(
            day_state=_captured_state(ok, degraded, changed, unchanged, first),
            attempts=attempts,
            status_counts={"ok": ok, "degraded": degraded, "error": error},
            payload_changes={
                "changed": changed, "unchanged": unchanged, "first": first,
                "not_stored": not_stored,
            },
            first_capture_at=_iso(first_at),
            last_capture_at=_iso(last_at),
            observations_stored=obs_stored if with_extractor else None,
            observations_retained=retained_obs,
            captures_with_body_retained=bodies_kept,
            captures_with_body_pruned=bodies_pruned,
            reasons=[
                {"status": status, "reason": reason, "count": count,
                 "first_at": _iso(r_first), "last_at": _iso(r_last)}
                for status, reason, count, r_first, r_last in source_reasons[:MAX_REASONS]
            ],
        )
        if len(source_reasons) > MAX_REASONS:
            entry["more_reasons"] = len(source_reasons) - MAX_REASONS
        rows.append(entry)

    items = [
        _item(row)
        for row in r.all(
            _COLLECTED_OBS_ITEMS_SQL, {**params, "per_source": per_source, "limit": limit}
        )
    ]
    truncation = _truncation(obs_counts, items, limit, per_source)

    gaps = []
    retained_captures = sum(row[1] for row in summary.values())
    if run_log["day"] == "past_window":
        if retained_captures:
            gaps.append(
                f"This day is past the {run_log['days']:g}-day run-log retention (cutoff "
                f"{run_log['cutoff']}); its {retained_captures:,} captures are still here only "
                "because pruning runs at the end of a collector cycle, and the next cycle "
                "removes them."
            )
        else:
            gaps.append(
                f"The run log keeps captures {run_log['days']:g} days (cutoff "
                f"{run_log['cutoff']}); this day is past it, so its captures are gone. Whether "
                "the collector or any source ran that day cannot be read here: absence is "
                "not evidence that nothing happened."
            )
    elif run_log["day"] == "partly_past_window":
        gaps.append(
            f"Captures before {run_log['cutoff']} (the {run_log['days']:g}-day run-log cutoff) "
            "are pruned: this day's earlier hours may be missing from the counts above, and "
            "a source shown as unknown_past_retention may have run then."
        )
    pruned_bodies = sum(row[14] for row in summary.values())
    kept_bodies = sum(row[13] for row in summary.values())
    if pruned_bodies:
        gaps.append(
            f"Payload bodies of {pruned_bodies:,} of this day's captures were pruned (bodies "
            f"are kept {retention['payload_bodies']['days']:g} days after the payload was last "
            "seen); intel_capture_history can replay only captures whose body is retained "
            f"({kept_bodies:,} of {kept_bodies + pruned_bodies:,})."
        )
    observations = retention["observations"]
    if observations["day"] in ("past_window", "partly_past_window"):
        gaps.append(
            f"Observations collected before {observations['cutoff']} are past the "
            f"{observations['days']:g}-day retention and pruned; observations_retained counts "
            "only what remains."
        )
    truncated = _truncation_gap("Observations saved that day", truncation, "earliest first")
    if truncated:
        gaps.append(truncated)

    states: dict[str, int] = {}
    for entry in rows:
        states[entry["day_state"]] = states.get(entry["day_state"], 0) + 1
    return {
        "collector": collector,
        "sources": rows,
        "state_counts": states,
        "observations": items,
        "truncation": truncation,
        "data_gaps": gaps,
        "states_explained": STATES_EXPLAINED,
        "cannot_prove": [
            "This is the run log of the database this server reads; a collector writing "
            "to another cache database is invisible here.",
            "changed compares each capture's payload hash with the source's previous "
            "capture: the stored payload differed, which does not by itself show the "
            "upstream published new data.",
            "A degraded capture with no records cannot tell a quiet period from a silent "
            "upstream failure.",
            HISTORY_ACCRUAL_NOTE,
        ],
    }


# ---------------------------------------------------------------------------
# by="observed"
# ---------------------------------------------------------------------------


def _observed_view(r: _Reader, w: _Window, obs_sources: list[str], roster: set[str],
                   source: str | None, limit: int, per_source: int,
                   retention: dict) -> dict:
    day = w.day.isoformat()
    base = {"source": source, "per_source": per_source, "limit": limit}
    day_params = {**base, "day": day, "day_last": f"{day}T23:59:59Z",
                  "instant_glob": f"{day}T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z"}
    period_params = {**base, "month": day[:7], "year": day[:4]}

    day_counts = {row[0]: row[1:] for row in r.all(_DAY_COUNTS_SQL, day_params)}
    period_counts = {row[0]: row[1:] for row in r.all(_PERIOD_COUNTS_SQL, period_params)}
    no_date = dict(r.all(_NO_OBSERVED_AT_SQL, {"start": w.start, "end": w.end, "source": source}))
    day_items = [_point(row) for row in r.all(_DAY_ITEMS_SQL, day_params)]
    period_items = [_point(row) for row in r.all(_PERIOD_ITEMS_SQL, period_params)]

    names = [source] if source is not None else sorted(
        set(obs_sources) | set(day_counts) | set(period_counts) | set(no_date)
    )
    rows = []
    for name in names:
        values, revised, unrecognized = day_counts.get(name, (0, 0, 0))
        periods = period_counts.get(name, (0, 0, 0))
        rows.append({
            "source": name,
            "in_collector_roster": name in roster,
            "day_values": values,
            "day_values_revised": revised,
            "period_values": periods[0],
            "unrecognized_observed_at": unrecognized,
            "without_observed_at_collected_that_day": no_date.get(name, 0),
        })

    day_truncation = _truncation(
        {name: c[0] for name, c in day_counts.items() if c[0]}, day_items, limit, per_source
    )
    period_truncation = _truncation(
        {name: c[0] for name, c in period_counts.items() if c[0]}, period_items, limit,
        per_source,
    )
    gaps = []
    excluded = sum(no_date.values())
    if excluded:
        by_source = ", ".join(f"{name} {count:,}" for name, count in sorted(no_date.items()))
        gaps.append(
            f"{excluded:,} observation(s) collected this day have no observed_at "
            f"({by_source}) and are excluded: {_NO_OBSERVED_AT_REASON}. by=collected lists them."
        )
    unrecognized_total = sum(c[2] for c in day_counts.values())
    if unrecognized_total:
        gaps.append(
            f"{unrecognized_total:,} stored observed_at value(s) on this day are in neither "
            f"recognized form ({day} or {day}THH:MM:SSZ) and are excluded; only a writer "
            "that bypasses the history store's validation can store them."
        )
    observations = retention["observations"]
    if observations["day"] in ("past_window", "partly_past_window"):
        gaps.append(
            f"Observations are pruned {observations['days']:g} days after collection (cutoff "
            f"{observations['cutoff']}): values for this day collected before then are gone, "
            "and only later ones (revisions, late reports) remain."
        )
    pruned_captures = sum(
        1 for item in day_items + period_items if not item["capture_retained"]
    )
    if pruned_captures:
        gaps.append(
            f"{pruned_captures:,} listed value(s) outlived the capture that produced them "
            f"(the run log keeps {retention['run_log']['days']:g} days); capture_retained "
            "marks them."
        )
    for what, block in (("Day values", day_truncation), ("Period values", period_truncation)):
        truncated = _truncation_gap(what, block, "by observed_at, then series")
        if truncated:
            gaps.append(truncated)
    if w.in_progress:
        gaps.append("The day is in progress: values observed later today may still arrive.")
    return {
        "day_values": day_items,
        "period_values": period_items,
        "period_values_note": (
            f"Period values describe the whole month {day[:7]} or year {day[:4]}, not "
            f"{day}; they are listed apart from the day's values."
        ),
        "sources": rows,
        "without_observed_at": {
            "collected_that_day": excluded,
            "by_source": dict(sorted(no_date.items())),
            "reason": _NO_OBSERVED_AT_REASON,
        },
        "truncation": {"day_values": day_truncation, "period_values": period_truncation},
        "precision_explained": PRECISION_EXPLAINED,
        "data_gaps": gaps,
        "cannot_prove": [
            "observed_at comes from each payload. A date-only value is the publisher's "
            "calendar date, matched to this day by its label, not converted from the "
            "publisher's time zone.",
            "Only values this server saw are here: a value published and revised between "
            "two captures, or while the collector was not running, is not.",
            "Only sources with a registered history extractor store observations; the "
            "others appear only in by=collected.",
            HISTORY_ACCRUAL_NOTE,
        ],
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def day_timeline(
    db_path: Path | str,
    day: Any,
    *,
    by: Any = "collected",
    source: Any = None,
    roster: Iterable[str] = (),
    limit: Any = DEFAULT_LIMIT,
    per_source_limit: Any = DEFAULT_PER_SOURCE_LIMIT,
    now: float | None = None,
) -> dict:
    """What the history store at ``db_path`` holds for one UTC day.

    ``roster`` is the collector's source list (sources that should have a
    run-log state even with no capture). Read-only: opens the database
    with ``mode=ro`` and never fetches.
    """
    now = time.time() if now is None else now
    parsed, problem = parse_day(day, now)
    if problem:
        return {"error": problem, "accepted_format": DAY_FORMAT, "timezone": "UTC"}
    if by not in VIEWS:
        return {"error": f"by must be 'collected' or 'observed'; got {by!r}"}
    if source is not None and (not isinstance(source, str) or not source):
        return {"error": f"source must be a source name; got {source!r}"}
    limit = _bounded(limit, DEFAULT_LIMIT, MAX_LIMIT)
    per_source = _bounded(per_source_limit, DEFAULT_PER_SOURCE_LIMIT, MAX_PER_SOURCE_LIMIT)
    window = _Window.for_day(parsed, now)
    roster_names = list(dict.fromkeys(roster))

    try:
        conn = _connect(Path(db_path))
    except sqlite3.Error as exc:
        return {"error": f"history store could not be opened read-only: {exc}"}
    try:
        ready = conn is not None and _has_history_tables(conn)
        r = _Reader(conn if ready else None)
        logged = [row[0] for row in r.all(_CAPTURE_SOURCES_SQL)]
        obs_sources = [row[0] for row in r.all(_OBSERVATION_SOURCES_SQL)]
        known = set(roster_names) | set(logged) | set(obs_sources)
        if source is not None and source not in known:
            return {
                "error": f"unknown source {source!r}: not a collector source and nothing "
                "recorded under that name",
                "known_sources": sorted(known),
            }
        retention = _retention(window)
        if by == "collected":
            names = [source] if source is not None else roster_names + sorted(
                set(logged) - set(roster_names)
            )
            view = _collected_view(r, window, names, set(roster_names), source, limit,
                                   per_source, retention)
        else:
            view = _observed_view(r, window, obs_sources, set(roster_names), source, limit,
                                  per_source, retention)
    except sqlite3.Error as exc:
        return {"error": f"history store could not be read: {type(exc).__name__}: {exc}"}
    finally:
        if conn is not None:
            conn.close()

    if not ready:
        view["data_gaps"].insert(
            0,
            f"No history tables in {Path(db_path)}: the collector has not recorded a cycle "
            "in this database, so there is nothing to read for any day.",
        )
    if by == "collected" and window.in_progress:
        view["data_gaps"].append(
            f"The day is in progress: counts run through {_iso(now)}, and sources not yet "
            "attempted today may still run."
        )
    return {
        "day": parsed.isoformat(),
        "timezone": "UTC",
        "by": by,
        "window": window.describe(),
        "source_filter": source,
        **view,
        "retention": _public_retention(retention),
        "db_path": str(db_path),
        "note": HISTORY_ACCRUAL_NOTE,
    }
