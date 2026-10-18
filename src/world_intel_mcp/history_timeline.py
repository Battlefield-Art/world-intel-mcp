"""Day timeline: what the history store holds for one UTC calendar day
(ROADMAP Phase 29).

Two views over the store that ``history.py`` keeps in the cache database,
both read through a read-only SQLite connection (``mode=ro``) inside one
read transaction: this module cannot write the store, every statement of
a call reads the same snapshot, and nothing here fetches upstream.

- ``by="collected"``: the run log for the day. Per source: attempts,
  ok / degraded / error counts with their reasons, captures whose
  extractor raised or whose points the store refused, captures whose
  payload changed or matched the capture before it, first and last
  capture time, observations stored, and how many of the day's payload
  bodies are still kept. A source that failed some attempts and answered
  others is flagged ``partial_failure``. A source without a capture that
  day says why: the collector recorded no capture of any source that day,
  it ran but not this source, the source has no capture in the retained
  run log (``never_run``), the day is before this database's run log
  starts (``before_run_log``), or the day is past run-log retention. The
  observations saved that day are listed too.
- ``by="observed"``: observations whose ``observed_at`` falls on the day,
  as a date (``YYYY-MM-DD``) or a UTC instant, one item per series and
  observed_at with its latest stored value and how many values were
  stored for it. Month (``YYYY-MM``) and year (``YYYY``) values that
  contain the day are listed separately, as period values; the two lists
  share one ``limit`` and ``per_source_limit``. Observations without
  ``observed_at`` cannot be placed on any day: they are excluded, and the
  ones collected that day are counted. Captures of the day and the day
  after whose extractor failed are counted, since their values for the
  day are missing.

The day is ``YYYY-MM-DD`` in UTC, the half-open window
``[day 00:00:00Z, next day 00:00:00Z)``. Collection times are compared as
epoch seconds, ``observed_at`` as stored text (the store keeps instants in
UTC as ``YYYY-MM-DDTHH:MM:SSZ``). A future day is an error; today is
allowed and reported as in progress.

Coverage. Pruning deletes run-log captures older than one cutoff, for
every source at once, and the history store stamps each capture with the
time it inserts it. So a stored capture older than the day proves that
nothing of the day was pruned, and the run log covers the whole day.
When the run log's oldest capture falls on or after the day's start, the
time before it is not covered: the database was not recording yet, or
older captures were pruned, and the store cannot say which. Counts,
states and the longest gap then cover only the part from that capture
on, and a day before the run log starts reads ``not_recorded``, never as
a day on which the collector did not run.

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
- Coverage assumes captures are stamped when they are inserted and
  removed only by the store's pruning. A capture written with an older
  timestamp (a restored backup, a hand edit) would make a pruned day read
  as covered.
- A date-only ``observed_at`` is the publisher's calendar date; it is
  matched to the requested day by its label, not converted from the
  publisher's time zone, which the payloads do not state.
- Collector cycles are not stored as such: "the collector did not run"
  means no capture of any source is recorded in the covered part of the
  day's window of this database.
- The observed-date view checks for extractor failures only in captures
  collected on the day and the day after; a failed capture outside that
  span that held values for the day is not counted.
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
# Distinct (kind, status, reason) entries shown per source; the rest are counted.
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

# Capture reasons, then extraction notes the capture reason does not
# already quote (an extractor whose points were all refused makes the
# capture degraded with the note in its reason).
_CAPTURE_REASONS_SQL = """
SELECT source, 'capture', status, error, COUNT(*), MIN(collected_at), MAX(collected_at)
FROM history_captures
WHERE collected_at >= :start AND collected_at < :end AND error IS NOT NULL
  AND (:source IS NULL OR source = :source)
GROUP BY source, status, error
UNION ALL
SELECT source, 'extraction', status, extract_error, COUNT(*), MIN(collected_at),
       MAX(collected_at)
FROM history_captures
WHERE collected_at >= :start AND collected_at < :end AND extract_error IS NOT NULL
  AND (error IS NULL OR instr(error, extract_error) = 0)
  AND (:source IS NULL OR source = :source)
GROUP BY source, status, extract_error
ORDER BY 1, 5 DESC, 7 DESC, 2, 4
"""

# history._extract leaves observations_extracted NULL when the extractor
# raised, and sets it (with a note) when it refused some returned points.
_EXTRACTION_SQL = """
SELECT source, SUM(observations_extracted IS NULL), SUM(observations_extracted IS NOT NULL)
FROM history_captures
WHERE collected_at >= :start AND collected_at < :end AND extract_error IS NOT NULL
  AND (:source IS NULL OR source = :source)
GROUP BY source
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

# Every stored string with the day as prefix sorts in [:day, :day_upper),
# :day_upper being the day with its last character incremented
# ('2026-10-19' -> '2026-10-1:'). The CASE keeps the two recognized forms
# (hours 00 to 23) and leaves anything else (only a writer bypassing
# record_cycle can store it) NULL, to be counted rather than shown.
_DAY_MATCHED_CTE = """
, matched AS (
    SELECT o.id, o.source, o.series_key, o.observed_at, o.published_at, o.collected_at,
           o.value, o.unit, o.label, o.capture_id,
           CASE WHEN o.observed_at = :day THEN 'date'
                WHEN o.observed_at GLOB :instant_glob
                     AND substr(o.observed_at, 12, 2) <= '23' THEN 'instant' END AS precision
    FROM keys JOIN history_observations o
      ON o.series_key = keys.k AND o.observed_at >= :day AND o.observed_at < :day_upper
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
    "not_attempted": "the collector captured other sources that day (in the part of it the "
    "run log covers) but not this one; this source has captures on other days",
    "collector_did_not_run": "no capture of any source is recorded that day, and the run "
    "log covers the whole day (it still holds an older capture)",
    "never_run": "no capture of this source anywhere in the retained run log",
    "before_run_log": "the day is before the first capture in this database's run log (it "
    "was not recording yet, or older captures were pruned): whether this source ran cannot "
    "be read",
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
    """A count clamped to 1..maximum. An integral float (MCP clients send
    JSON 7 as 7.0) is that integer; a bool, a fraction, inf or nan gets
    the default rather than a truncated or coerced value."""
    if isinstance(value, bool) or (isinstance(value, float) and not value.is_integer()):
        return default
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return max(1, min(maximum, number))


def _per_source(counts: dict[str, int]) -> str:
    return ", ".join(f"{name} {count:,}" for name, count in sorted(counts.items()))


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

    @property
    def available(self) -> bool:
        return self.conn is not None

    def all(self, sql: str, params: dict | None = None) -> list[tuple]:
        if self.conn is None:
            return []
        return self.conn.execute(sql, params or {}).fetchall()

    def one(self, sql: str, params: dict | None, default: tuple) -> tuple:
        rows = self.all(sql, params)
        return rows[0] if rows else default


def _connect(db_path: Path) -> sqlite3.Connection | None:
    """A read-only connection inside one read transaction, or None when the
    database does not exist (opening it read-only never creates it).

    The transaction gives every statement of a call the snapshot its first
    read saw, so a collector cycle committed mid-call cannot make the
    answer disagree with itself; closing the connection ends it."""
    path = Path(db_path).resolve()
    if not path.exists():
        return None
    conn = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=10)
    try:
        conn.execute("BEGIN")
    except sqlite3.Error:
        conn.close()
        raise
    return conn


def _history_tables(conn: sqlite3.Connection) -> set[str]:
    return {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (?, ?, ?)",
            _TABLES,
        )
    }


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


def _covers_whole_day(log_first: float | None, w: _Window) -> bool:
    """True when the run log still holds a capture older than the day:
    pruning cuts every source at one cutoff, so none of the day was pruned."""
    return log_first is not None and log_first < w.start


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


def _within_budget(items: list[dict], room: int, per_source: int, used: Counter) -> list[dict]:
    """The leading items that fit in ``room`` more in all and in what
    ``used`` left of ``per_source`` for each source."""
    kept: list[dict] = []
    taken = Counter(used)
    for item in items:
        if len(kept) >= room:
            break
        if taken[item["source"]] < per_source:
            taken[item["source"]] += 1
            kept.append(item)
    return kept


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


def _collector_block(r: _Reader, w: _Window, run_log: dict, log_first: float | None,
                     log_last: float | None) -> dict:
    params = {"start": w.start, "end": w.end}
    captures, sources, first, last = r.one(_COLLECTOR_DAY_SQL, params, (0, 0, None, None))
    whole_day = _covers_whole_day(log_first, w)
    block: dict[str, Any] = {
        "captures": captures,
        "sources_captured": sources,
        "run_log_first_capture_at": _iso(log_first),
        "run_log_last_capture_at": _iso(log_last),
    }
    if captures:
        # The run log covers the day from its oldest capture on; time
        # before that capture is unrecorded, not a stretch without one.
        covered_from = w.start if whole_day else log_first
        spans = [(covered_from, first), (last, min(w.end, w.now))]
        internal = r.one(_LONGEST_INTERNAL_GAP_SQL, params, (None, None))
        if internal[0] is not None:
            spans.append(internal)
        gap_from, gap_to = max(spans, key=lambda span: span[1] - span[0])
        block.update(
            state="ran",
            covered_from=_iso(covered_from),
            first_capture_at=_iso(first),
            last_capture_at=_iso(last),
            longest_gap_s=round(max(0.0, gap_to - gap_from), 1),
            longest_gap_from=_iso(gap_from),
            longest_gap_to=_iso(gap_to),
            explanation=f"{captures:,} captures of {sources} source(s) are recorded that day.",
        )
        return block
    if run_log["day"] in ("past_window", "partly_past_window") and not whole_day:
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
        block.update(
            state="not_recorded",
            context="empty_run_log",
            explanation="The run log in this database has no capture at all: nothing is "
            "recorded here, so whether the collector ran that day cannot be read.",
        )
        return block
    if log_first >= w.end:
        block.update(
            state="not_recorded",
            context="before_first_capture",
            explanation=f"The run log in this database starts later, at {_iso(log_first)}: "
            "no capture before then is recorded here, so whether the collector ran that day "
            "cannot be read.",
        )
        return block
    if log_last < w.start:
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


def _run_log_gaps(r: _Reader, w: _Window, run_log: dict, collector: dict, whole_day: bool,
                  retained_captures: int, retained_obs: int) -> list[str]:
    """Retention and coverage of the day's run log, most important first."""
    gaps = []
    oldest = collector["run_log_first_capture_at"]
    if run_log["day"] == "past_window":
        if retained_captures:
            gaps.append(
                f"This day is past the {run_log['days']:g}-day run-log retention (cutoff "
                f"{run_log['cutoff']}); its {retained_captures:,} captures are still here only "
                "because pruning runs at the end of a collector cycle, and the next cycle "
                "removes them."
            )
        elif whole_day:
            gaps.append(
                f"This day is past the {run_log['days']:g}-day run-log retention (cutoff "
                f"{run_log['cutoff']}), but the run log still holds an older capture "
                f"({oldest}), so nothing from this day was pruned: the collector recorded no "
                "capture of any source that day."
            )
        else:
            # Observations collected that day prove it had captures; without
            # them the day may equally predate this database.
            what = "its captures are gone" if retained_obs else "any captures from it are gone"
            gaps.append(
                f"The run log keeps captures {run_log['days']:g} days (cutoff "
                f"{run_log['cutoff']}); this day is past it, so {what}. Whether the collector "
                "or any source ran that day cannot be read here: absence is not evidence "
                "that nothing happened."
            )
    elif run_log["day"] == "partly_past_window":
        if whole_day:
            gaps.append(
                f"Part of this day is past the {run_log['days']:g}-day run-log retention "
                f"(cutoff {run_log['cutoff']}), but the run log still holds an older capture "
                f"({oldest}), so no capture from this day has been pruned yet. Pruning runs "
                "at the end of each collector cycle, so the next one removes this day's "
                "captures from before the cutoff."
            )
        else:
            gaps.append(
                f"Captures before {run_log['cutoff']} (the {run_log['days']:g}-day run-log "
                "cutoff) are pruned: this day's earlier hours may be missing from the counts "
                "above, and a source shown as unknown_past_retention may have run then."
            )
    if retained_captures and not whole_day:
        gaps.append(
            f"The run log holds no capture before {oldest}, its oldest: this day before then "
            "is not covered (this database was not recording yet, or older captures were "
            f"pruned), so counts, states and the longest gap cover only {oldest} onward."
        )
    if collector["state"] == "not_recorded" and r.available:
        day = w.day.isoformat()
        if collector["context"] == "before_first_capture":
            gaps.append(
                f"This database's run log starts at {oldest}: no capture before then is "
                "recorded here (the database was not recording yet, or older captures were "
                f"pruned), so whether the collector or any source ran on {day} cannot be read. "
                "Absence is not evidence that nothing ran."
            )
        else:
            gaps.append(
                "The run log in this database has no capture: no collector cycle is recorded "
                f"here yet, so whether the collector or any source ran on {day} cannot be read."
            )
    return gaps


def _collected_view(r: _Reader, w: _Window, names: list[str], roster: set[str],
                    source: str | None, limit: int, per_source: int,
                    retention: dict) -> dict:
    params = {"start": w.start, "end": w.end, "source": source}
    run_log = retention["run_log"]
    summary = {row[0]: row for row in r.all(_CAPTURE_SUMMARY_SQL, params)}
    reasons: dict[str, list[tuple]] = {}
    for row in r.all(_CAPTURE_REASONS_SQL, params):
        reasons.setdefault(row[0], []).append(row[1:])
    extraction = {row[0]: row[1:] for row in r.all(_EXTRACTION_SQL, params)}
    obs_counts = dict(r.all(_COLLECTED_OBS_COUNTS_SQL, params))
    log_first, log_last = r.one(_RUN_LOG_BOUNDS_SQL, None, (None, None))
    whole_day = _covers_whole_day(log_first, w)
    collector = _collector_block(r, w, run_log, log_first, log_last)

    if source is None:
        names = names + sorted((set(summary) | set(obs_counts)) - set(names))
    rows = []
    for name in names:
        retained_obs = obs_counts.get(name, 0)
        entry: dict[str, Any] = {"source": name, "in_collector_roster": name in roster}
        cap = summary.get(name)
        if cap is None:
            if run_log["day"] in ("past_window", "partly_past_window") and not whole_day:
                state = "unknown_past_retention"
            else:
                before, after = r.one(
                    _SOURCE_NEIGHBOURS_SQL, {"name": name, "start": w.start, "end": w.end},
                    (None, None),
                )
                if before is None and after is None:
                    state = "never_run"
                elif log_first >= w.end:
                    state = "before_run_log"
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
        raised, skipped = extraction.get(name, (0, 0))
        source_reasons = reasons.get(name, [])
        entry.update(
            day_state=_captured_state(ok, degraded, changed, unchanged, first),
            attempts=attempts,
            status_counts={"ok": ok, "degraded": degraded, "error": error},
            partial_failure=bool(error and (ok or degraded)),
            payload_changes={
                "changed": changed, "unchanged": unchanged, "first": first,
                "not_stored": not_stored,
            },
            first_capture_at=_iso(first_at),
            last_capture_at=_iso(last_at),
            observations_stored=obs_stored if with_extractor else None,
            observations_retained=retained_obs,
            captures_extractor_raised=raised,
            captures_points_skipped=skipped,
            captures_with_body_retained=bodies_kept,
            captures_with_body_pruned=bodies_pruned,
            reasons=[
                {"kind": kind, "status": status, "reason": reason, "count": count,
                 "first_at": _iso(r_first), "last_at": _iso(r_last)}
                for kind, status, reason, count, r_first, r_last in source_reasons[:MAX_REASONS]
            ],
        )
        if raised:
            entry["observations_stored_note"] = (
                f"the extractor raised on {raised:,} capture(s), which stored nothing; "
                + (f"this counts the {with_extractor:,} it ran on" if with_extractor
                   else "it ran on no capture that day, so the count is unknown")
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

    retained_captures = sum(row[1] for row in summary.values())
    gaps = _run_log_gaps(r, w, run_log, collector, whole_day, retained_captures,
                         sum(obs_counts.values()))
    raised_by = {name: c[0] for name, c in extraction.items() if c[0]}
    skipped_by = {name: c[1] for name, c in extraction.items() if c[1]}
    if raised_by:
        gaps.append(
            f"History extractors raised on {sum(raised_by.values()):,} capture(s) that day "
            f"({_per_source(raised_by)}): those captures stored no observations, so "
            "observations_stored and the observed-date view miss whatever they held; each "
            "source's reasons give the error."
        )
    if skipped_by:
        gaps.append(
            f"{sum(skipped_by.values()):,} capture(s) that day had points their extractor "
            f"returned but the store refused as invalid ({_per_source(skipped_by)}): those "
            "points were not stored."
        )
    pruned_bodies = sum(row[14] for row in summary.values())
    kept_bodies = sum(row[13] for row in summary.values())
    if pruned_bodies:
        body_days = retention["payload_bodies"]["days"]
        kept = (
            f"bodies are kept {body_days:g} days after the payload was last seen"
            if body_days > 0 else "this server's settings keep bodies forever, so the "
            "collector pruned them with its own WORLD_INTEL_HISTORY_PAYLOAD_DAYS"
        )
        gaps.append(
            f"Payload bodies of {pruned_bodies:,} of this day's captures were pruned ({kept}); "
            "intel_capture_history can replay only captures whose body is retained "
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
        "partial_failure_sources": [row["source"] for row in rows if row.get("partial_failure")],
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
            "Coverage is inferred: a stored capture older than the day means none of the "
            "day was pruned, because the store stamps captures when it inserts them.",
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
    day_params = {**base, "day": day, "day_upper": day[:-1] + chr(ord(day[-1]) + 1),
                  "instant_glob": f"{day}T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z"}
    period_params = {**base, "month": day[:7], "year": day[:4]}
    # Captures of the day and the next can hold values observed on the day
    # (the K-index payload spans 24 hours); failed extractions there lose them.
    checked_to = w.end + 86400.0
    extraction = {
        row[0]: row[1:]
        for row in r.all(_EXTRACTION_SQL, {"start": w.start, "end": checked_to, "source": source})
    }

    day_counts = {row[0]: row[1:] for row in r.all(_DAY_COUNTS_SQL, day_params)}
    period_counts = {row[0]: row[1:] for row in r.all(_PERIOD_COUNTS_SQL, period_params)}
    no_date = dict(r.all(_NO_OBSERVED_AT_SQL, {"start": w.start, "end": w.end, "source": source}))
    day_items = [_point(row) for row in r.all(_DAY_ITEMS_SQL, day_params)]
    # One budget for both lists: period values get what the day values left,
    # in all and per source. The query's own caps return enough rows for that.
    period_items = _within_budget(
        [_point(row) for row in r.all(_PERIOD_ITEMS_SQL, period_params)],
        limit - len(day_items), per_source, Counter(item["source"] for item in day_items),
    )

    names = [source] if source is not None else sorted(
        set(obs_sources) | set(day_counts) | set(period_counts) | set(no_date) | set(extraction)
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
        gaps.append(
            f"{excluded:,} observation(s) collected this day have no observed_at "
            f"({_per_source(no_date)}) and are excluded: {_NO_OBSERVED_AT_REASON}. by=collected "
            "lists them."
        )
    unrecognized_total = sum(c[2] for c in day_counts.values())
    if unrecognized_total:
        gaps.append(
            f"{unrecognized_total:,} stored observed_at value(s) on this day are in neither "
            f"recognized form ({day} or {day}THH:MM:SSZ) and are excluded; only a writer "
            "that bypasses the history store's validation can store them."
        )
    raised_by = {name: c[0] for name, c in extraction.items() if c[0]}
    skipped_by = {name: c[1] for name, c in extraction.items() if c[1]}
    span = f"from {_iso(w.start)} to {_iso(checked_to)}"
    if raised_by:
        gaps.append(
            f"History extractors raised on {sum(raised_by.values()):,} capture(s) collected "
            f"{span} ({_per_source(raised_by)}): values for {day} that those captures held "
            "were not stored, so day_values may miss them. Captures collected outside that "
            "span are not checked."
        )
    if skipped_by:
        gaps.append(
            f"{sum(skipped_by.values()):,} capture(s) collected {span} had points the store "
            f"refused as invalid ({_per_source(skipped_by)}); values for {day} among them "
            "are missing."
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
        log_days = retention["run_log"]["days"]
        kept = (
            f"the run log keeps {log_days:g} days" if log_days > 0
            else "this server's settings keep the run log forever, so the collector pruned "
            "it with its own WORLD_INTEL_HISTORY_CAPTURE_DAYS"
        )
        gaps.append(
            f"{pruned_captures:,} listed value(s) outlived the capture that produced them "
            f"({kept}); capture_retained marks them."
        )
    for what, block, order in (
        ("Day values", day_truncation, "by observed_at, then series"),
        ("Period values", period_truncation,
         "by observed_at, then series, after the day values, which share these caps"),
    ):
        truncated = _truncation_gap(what, block, order)
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
        "extraction_problems": {
            "captures_from": _iso(w.start),
            "captures_to": _iso(checked_to),
            "end_exclusive": True,
            "by_source": {
                name: {"extractor_raised": c[0], "points_skipped": c[1]}
                for name, c in sorted(extraction.items())
            },
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
        found = _history_tables(conn) if conn is not None else set()
        if found and found != set(_TABLES):
            missing = sorted(set(_TABLES) - found)
            return {
                "error": f"history store in {Path(db_path)} is incomplete: table(s) "
                f"{', '.join(missing)} missing, so no day can be read from it",
                "missing_tables": missing,
                "db_path": str(db_path),
            }
        ready = bool(found)
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
