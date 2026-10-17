"""Observation history store and collection health (ROADMAP Phase 29).

The cache keeps one latest value per key, so on its own nothing can be
correlated, revised or replayed. This module keeps what the collector
saw, in three tables of the same SQLite file the process-wide ``Cache``
opened (pass ``cache.db_path`` in, as ``analysis/aoi.py`` and
``sources/jet_cohort.py`` do, so this lands in the literal file the cache
settled on, fallback path included):

- ``history_captures``: one row per collector source run, the run log.
  ``status`` is ``ok``, ``degraded`` or ``error``; a failed fetch is an
  ``error`` row with a reason and a null record count, never an ``ok``
  row with zero records. A payload with no records is ``degraded``,
  because a quiet period and a silent upstream failure look the same.
- ``history_payloads``: the payload body as zlib-compressed JSON, stored
  once per ``(source, payload_hash)``. The hash ignores the top-level
  fetch-time stamps in ``VOLATILE_KEYS`` (they change on every call while
  the data do not); the body kept is the first capture's.
- ``history_observations``: dated numeric points pulled out of payloads by
  per-source extractors, each with three dates. ``observed_at`` is the
  period or instant the value describes, taken from the payload; when a
  payload carries no observation date it is NULL, never ``collected_at``.
  ``published_at`` is the publisher's release time when the payload names
  one, else NULL. ``collected_at`` is when this server saved the capture.
  A point is stored when it is new or its value differs from the last
  stored value for the same series and ``observed_at``, so successive
  rows for one ``(series_key, observed_at)`` are revisions.

History accrues while the collector runs, plus what a backfill
(``history_backfill.py``) loads on demand from FRED and the ECB Data
Portal, which serve their whole history; a source the collector does not
run reads ``never_run``. Backfills are captures under ``backfill:*``
sources, so their rows never read as collector captures, and
``collection_health`` lists them apart from the collector's roster.

Registering an extractor (the public API)::

    from world_intel_mcp.history import Observation, register_extractor

    @register_extractor("my_source")          # the collector SOURCES name
    def _extract(payload: dict) -> list[Observation]:
        return [Observation("rate", 4.33, observed_at="2026-10-15", unit="percent")]

The store prefixes each ``series_key`` with the source name
(``my_source:rate``), so extractors cannot collide. ``observed_at`` must be
``YYYY``, ``YYYY-MM``, ``YYYY-MM-DD`` or a timezone-aware ISO 8601
instant (use ``normalize_observed_at`` to convert, with ``assume_utc=True``
only when the publisher documents UTC); a point with an unparseable date
or a non-finite value is skipped and counted in the capture's
``extract_error``. Register at import time of your source module: the
collector imports a source's module before it records that source's
capture. A registered extractor that finds nothing in a non-error payload
marks the capture ``degraded``.

Retention, each ``0`` = keep forever, pruned at the end of every recorded
cycle in the same transaction:

- ``WORLD_INTEL_HISTORY_PAYLOAD_DAYS`` (default 7) bounds payload bodies by
  last sighting. Measured 2026-10-17 over three full 55-source cycles (45 s
  each, so about 250 cycles a day at ``--interval 300``): about 70 KB of
  new compressed bodies every cycle from the sources whose data changes
  each time (crypto quotes 30 KB, domestic flights 19 KB, news 9 KB, disease
  outbreaks 6 KB, weather alerts 4.5 KB), plus 5-121 KB when the military
  flight snapshot succeeds; 90 KB and 193 KB in the two measured follow-up
  cycles. About 19-48 MB a day, so about 130-340 MB at the default.
- ``WORLD_INTEL_HISTORY_CAPTURE_DAYS`` (default 30) bounds the run log:
  55 capture rows per cycle at about 250 bytes each with indexes, about
  3.4 MB a day, so about 100 MB at the default.
- ``WORLD_INTEL_HISTORY_DAYS`` (default 365) bounds observations, which are
  stored only when new or changed. An observation can outlive the capture
  that produced it; its ``capture_id`` then names a pruned capture.

Gaps / not covered:
- Growth was measured on a Saturday with markets closed: the 15 Yahoo quote
  series, which have no observation date, gain a row per price change while
  markets trade, and that rate is not measured.
- Backfill covers FRED graph CSV and ECB Data Portal series only, and
  stores the publisher's current vintage: revisions made before a
  backfill are invisible (see ``history_backfill.py``). Every other source
  has only what the collector saw from its first recorded cycle on.
- ``published_at`` is NULL for every built-in extractor: none of their
  payloads names a release time.
- ``record_count`` is a size hint (the payload's longest top-level list,
  else its integer ``count``), not a semantic count; payloads shaped as
  nested dicts read NULL.
- Degradation is read from top-level ``_stale`` / ``degraded`` markers;
  a source that degrades without marking its payload reads ``ok`` unless
  it is empty or its extractor finds nothing.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import sqlite3
import time
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

logger = logging.getLogger("world-intel-mcp.history")

# Top-level keys that carry this server's fetch time, not upstream data.
VOLATILE_KEYS = frozenset({"timestamp", "fetched_at", "generated_at", "_stale_age_seconds"})

# Expected seconds between collector attempts. The shipped launchd plist
# and the daemon default both run every 300 s; an operator running a
# different --interval sets WORLD_INTEL_HISTORY_CADENCE_S.
DEFAULT_CADENCE_S = 300.0
# Per-source overrides. Every source runs every cycle today, so this is
# empty in code; a deployment that runs some sources from a second, slower
# collector sets WORLD_INTEL_HISTORY_CADENCES="aoi_digest=900,news_feed=600".
SOURCE_CADENCES: dict[str, float] = {}
# A source is overdue when its last delivered payload is older than this
# many cadences.
OVERDUE_MULTIPLE = 3.0

_DEFAULT_PAYLOAD_DAYS = 7.0
_DEFAULT_CAPTURE_DAYS = 30.0
_DEFAULT_HISTORY_DAYS = 365.0
_MAX_LIMIT = 1000

HISTORY_ACCRUAL_NOTE = (
    "History accrues only while the collector (intel-collector) runs; "
    "nothing before its first recorded cycle exists here, except series under "
    "backfill:* sources, which a backfill loaded from the publisher's current data."
)

# Source-name prefix of backfill captures (history_backfill.py). They run on
# demand, so collection_health reports them apart from the cadence roster.
BACKFILL_PREFIX = "backfill:"
BACKFILL_VINTAGE_NOTE = (
    "Backfilled: each value is the publisher's vintage on the day it was "
    "collected (collected_at is the backfill time), so revisions made before the "
    "first backfill are invisible; later backfills store changed values as revisions."
)


# ---------------------------------------------------------------------------
# Extractor registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Observation:
    """One numeric point an extractor read from a payload."""

    series_key: str
    value: float
    observed_at: str | None = None
    unit: str | None = None
    label: str | None = None
    published_at: str | None = None


Extractor = Callable[[Any], Iterable[Observation]]
_EXTRACTORS: dict[str, Extractor] = {}


def register_extractor(
    source: str, fn: Extractor | None = None, *, replace: bool = False
) -> Any:
    """Register ``fn`` as the extractor for collector source ``source``.

    Usable as ``register_extractor("src", fn)`` or as a decorator. A second
    registration for the same source raises unless ``replace=True``.
    """

    def _register(f: Extractor) -> Extractor:
        existing = _EXTRACTORS.get(source)
        if existing is not None and existing is not f and not replace:
            raise ValueError(f"an extractor is already registered for {source!r}")
        _EXTRACTORS[source] = f
        return f

    return _register(fn) if fn is not None else _register


def registered_extractors() -> dict[str, Extractor]:
    """Snapshot of the source -> extractor map."""
    return dict(_EXTRACTORS)


_DATE_PERIOD = re.compile(r"\d{4}(-\d{2}(-\d{2})?)?")


def normalize_observed_at(value: Any, *, assume_utc: bool = False) -> str | None:
    """Payload date -> stored form, or None when it cannot be read.

    ``YYYY``, ``YYYY-MM`` and ``YYYY-MM-DD`` periods are kept as written.
    An ISO 8601 instant becomes ``YYYY-MM-DDTHH:MM:SSZ`` in UTC. A naive
    instant (no offset) is only accepted with ``assume_utc=True``.
    """
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    if not text:
        return None
    if _DATE_PERIOD.fullmatch(text):
        return text
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00").replace(" ", "T", 1))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        if not assume_utc:
            return None
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _finite(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


@register_extractor("space_weather")
def _extract_space_weather(payload: dict) -> list[Observation]:
    """NOAA planetary K-index rows. SWPC time tags are UTC by product spec."""
    points = []
    for row in payload.get("kp_recent") or []:
        kp = _finite(row.get("kp"))
        observed = normalize_observed_at(row.get("time"), assume_utc=True)
        if kp is None or observed is None:
            continue
        points.append(
            Observation("kp", kp, observed_at=observed, unit="Kp index",
                        label="NOAA planetary K-index")
        )
    return points


@register_extractor("earthquakes")
def _extract_earthquakes(payload: dict) -> list[Observation]:
    """Per-event USGS magnitude at the event's origin time. USGS revises
    magnitudes after review, which ``revisions`` then shows."""
    points = []
    for quake in payload.get("earthquakes") or []:
        magnitude = _finite(quake.get("magnitude"))
        observed = normalize_observed_at(quake.get("time"))
        event_id = quake.get("id")
        if magnitude is None or observed is None or not event_id:
            continue
        points.append(
            Observation(f"{event_id}:magnitude", magnitude, observed_at=observed,
                        unit="magnitude", label=quake.get("place"))
        )
    return points


@register_extractor("central_bank_rates")
def _extract_central_bank_rates(payload: dict) -> list[Observation]:
    """FRED-sourced policy rates only, dated by the FRED observation date.
    Curated rows are values written into this repo, not observations this
    capture fetched, so they are skipped."""
    points = []
    for rate in payload.get("rates") or []:
        if rate.get("source") != "fred":
            continue
        value = _finite(rate.get("rate"))
        observed = normalize_observed_at(rate.get("as_of"))
        label = rate.get("label") or rate.get("bank")
        if value is None or observed is None or not label:
            continue
        points.append(
            Observation(_slug(label), value, observed_at=observed, unit="percent",
                        label=f"{rate.get('bank')}: {label}")
        )
    return points


def _quote_extractor(list_key: str, label_key: str, unit_key: str | None):
    def _extract(payload: dict) -> list[Observation]:
        """Yahoo quotes. The source module keeps price and change only, not
        the quote time, so observed_at is NULL (unknown), not collected_at."""
        points = []
        for quote in payload.get(list_key) or []:
            price = _finite(quote.get("price"))
            symbol = quote.get("symbol")
            if price is None or not symbol:
                continue
            points.append(
                Observation(f"{symbol}:price", price, observed_at=None,
                            unit=quote.get(unit_key) if unit_key else None,
                            label=quote.get(label_key) or symbol)
            )
        return points

    return _extract


register_extractor("market_quotes", _quote_extractor("quotes", "symbol", "currency"))
register_extractor("commodity_quotes", _quote_extractor("commodities", "name", None))


# ---------------------------------------------------------------------------
# Capture classification
# ---------------------------------------------------------------------------


@dataclass
class SourceOutcome:
    """What one collector source run produced, handed to ``record_cycle``."""

    source: str
    collected_at: float
    duration_s: float | None = None
    payload: Any = None
    error: str | None = None


@dataclass
class _Classified:
    status: str
    reason: str | None
    record_count: int | None
    observations: list[Observation] = field(default_factory=list)
    extract_error: str | None = None
    extracted: int | None = None


def record_count(payload: Any) -> int | None:
    """Size hint: longest top-level list, else an integer ``count``."""
    if isinstance(payload, list):
        return len(payload)
    if not isinstance(payload, dict):
        return None
    lengths = [len(v) for v in payload.values() if isinstance(v, list)]
    if lengths:
        return max(lengths)
    count = payload.get("count")
    if isinstance(count, int) and not isinstance(count, bool):
        return count
    return None


def payload_hash(payload: Any) -> str:
    """sha256 of the canonical JSON, top-level ``VOLATILE_KEYS`` removed."""
    if isinstance(payload, dict):
        payload = {k: v for k, v in payload.items() if k not in VOLATILE_KEYS}
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _extract(source: str, payload: Any) -> tuple[list[Observation], str | None, int | None]:
    extractor = _EXTRACTORS.get(source)
    if extractor is None:
        return [], None, None
    try:
        raw = list(extractor(payload))
    except Exception as exc:  # an extractor bug must not lose the capture
        logger.warning("History extractor for %s failed: %s", source, exc)
        return [], f"extractor raised: {type(exc).__name__}: {str(exc)[:200]}", None
    valid: list[Observation] = []
    skipped = 0
    for obs in raw:
        if (
            not isinstance(obs, Observation)
            or not isinstance(obs.series_key, str)
            or not obs.series_key
            or _finite(obs.value) is None
            or (obs.observed_at is not None and normalize_observed_at(obs.observed_at) != obs.observed_at)
            or (obs.published_at is not None and normalize_observed_at(obs.published_at) != obs.published_at)
        ):
            skipped += 1
            continue
        valid.append(obs)
    note = f"{skipped} invalid point(s) skipped" if skipped else None
    return valid, note, len(raw)


def classify(outcome: SourceOutcome) -> _Classified:
    """Status, reason and observations for one source run."""
    if outcome.error is not None:
        return _Classified("error", outcome.error[:500], None)
    payload = outcome.payload
    if payload is None:
        return _Classified("error", "fetch returned no payload", None)
    if isinstance(payload, dict) and payload.get("error"):
        return _Classified("error", str(payload["error"])[:500], None)

    count = record_count(payload)
    observations, extract_error, extracted = _extract(outcome.source, payload)
    status, reason = "ok", None
    if isinstance(payload, dict) and payload.get("_stale"):
        age = payload.get("_stale_age_seconds")
        status, reason = "degraded", (
            f"served stale cache (age {age} s)" if age is not None else "served stale cache"
        )
    elif isinstance(payload, dict) and (
        payload.get("degraded") is True or payload.get("status") == "degraded"
    ):
        status, reason = "degraded", "payload marked degraded"
    elif count == 0:
        status, reason = "degraded", (
            "payload has no records: a quiet period and a silent upstream "
            "failure look the same here"
        )
    elif extracted is not None and not observations:
        status, reason = "degraded", "registered extractor found no measures in this payload" + (
            f" ({extract_error})" if extract_error else ""
        )
    return _Classified(status, reason, count, observations, extract_error, extracted)


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


def _env_days(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        days = float(raw)
    except ValueError:
        logger.warning("%s=%r is not a number; using %s", name, raw, default)
        return default
    if not math.isfinite(days) or days < 0:
        logger.warning("%s=%r must be >= 0; using %s", name, raw, default)
        return default
    return days


def retention_days() -> dict[str, float]:
    """Configured retention (bodies, run log, observations); 0 = keep forever."""
    return {
        "payload_days": _env_days("WORLD_INTEL_HISTORY_PAYLOAD_DAYS", _DEFAULT_PAYLOAD_DAYS),
        "capture_days": _env_days("WORLD_INTEL_HISTORY_CAPTURE_DAYS", _DEFAULT_CAPTURE_DAYS),
        "history_days": _env_days("WORLD_INTEL_HISTORY_DAYS", _DEFAULT_HISTORY_DAYS),
    }


def cadence_config() -> tuple[float, dict[str, float]]:
    """(default cadence, per-source overrides) after env overrides."""
    default = DEFAULT_CADENCE_S
    raw = os.environ.get("WORLD_INTEL_HISTORY_CADENCE_S")
    if raw:
        try:
            value = float(raw)
            if value > 0 and math.isfinite(value):
                default = value
            else:
                logger.warning("WORLD_INTEL_HISTORY_CADENCE_S=%r must be > 0", raw)
        except ValueError:
            logger.warning("WORLD_INTEL_HISTORY_CADENCE_S=%r is not a number", raw)
    overrides = dict(SOURCE_CADENCES)
    for part in (os.environ.get("WORLD_INTEL_HISTORY_CADENCES") or "").split(","):
        name, sep, secs = part.partition("=")
        if not sep:
            continue
        try:
            value = float(secs)
        except ValueError:
            logger.warning("Ignoring cadence override %r", part)
            continue
        if value > 0 and math.isfinite(value) and name.strip():
            overrides[name.strip()] = value
    return default, overrides


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clamp(limit: Any, default: int) -> int:
    try:
        value = int(limit)
    except (TypeError, ValueError):
        return default
    return max(1, min(_MAX_LIMIT, value))


class HistoryStore:
    """Capture log, payload bodies and observations in the cache DB."""

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), timeout=10)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS history_captures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                collected_at REAL NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('ok', 'degraded', 'error')),
                duration_s REAL,
                record_count INTEGER,
                error TEXT,
                payload_hash TEXT,
                changed INTEGER,
                observations_extracted INTEGER,
                observations_stored INTEGER,
                extract_error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_history_captures_source
                ON history_captures(source, collected_at);
            CREATE INDEX IF NOT EXISTS idx_history_captures_time
                ON history_captures(collected_at);
            CREATE TABLE IF NOT EXISTS history_payloads (
                source TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                first_seen_at REAL NOT NULL,
                last_seen_at REAL NOT NULL,
                raw_bytes INTEGER NOT NULL,
                body BLOB NOT NULL,
                PRIMARY KEY (source, payload_hash)
            );
            CREATE INDEX IF NOT EXISTS idx_history_payloads_seen
                ON history_payloads(last_seen_at);
            CREATE TABLE IF NOT EXISTS history_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                capture_id INTEGER NOT NULL,
                source TEXT NOT NULL,
                series_key TEXT NOT NULL,
                observed_at TEXT,
                published_at TEXT,
                collected_at REAL NOT NULL,
                value REAL NOT NULL,
                unit TEXT,
                label TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_history_obs_series
                ON history_observations(series_key, observed_at, collected_at);
            CREATE INDEX IF NOT EXISTS idx_history_obs_source
                ON history_observations(source, collected_at);
            CREATE INDEX IF NOT EXISTS idx_history_obs_time
                ON history_observations(collected_at);
            """
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # -- writes -------------------------------------------------------------

    def record_cycle(self, outcomes: list[SourceOutcome], now: float | None = None) -> dict:
        """Record every outcome of one collector cycle in one transaction,
        then prune past retention in the same transaction."""
        conn = self._conn
        counts = {"ok": 0, "degraded": 0, "error": 0}
        payloads_stored = 0
        observations_stored = 0
        with conn:
            for outcome in outcomes:
                c = classify(outcome)
                counts[c.status] += 1
                digest = changed = body = None
                if c.status != "error":
                    try:
                        digest = payload_hash(outcome.payload)
                        body = json.dumps(
                            outcome.payload, separators=(",", ":"), default=str
                        ).encode()
                    except (TypeError, ValueError) as exc:
                        # One unserializable payload (mixed-type dict keys,
                        # a reference cycle) must not cost the cycle's
                        # other captures; the capture says what was lost.
                        digest = body = None
                        note = f"payload not stored in history: {type(exc).__name__}: {exc}"
                        c.reason = (f"{c.reason}; {note}" if c.reason else note)[:500]
                if digest is not None and body is not None:
                    prev = conn.execute(
                        "SELECT payload_hash FROM history_captures "
                        "WHERE source = ? AND payload_hash IS NOT NULL "
                        "ORDER BY collected_at DESC, id DESC LIMIT 1",
                        (outcome.source,),
                    ).fetchone()
                    changed = None if prev is None else int(prev[0] != digest)
                    inserted = conn.execute(
                        "INSERT OR IGNORE INTO history_payloads "
                        "(source, payload_hash, first_seen_at, last_seen_at, raw_bytes, body) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (outcome.source, digest, outcome.collected_at, outcome.collected_at,
                         len(body), zlib.compress(body, 6)),
                    ).rowcount
                    if inserted:
                        payloads_stored += 1
                    else:
                        conn.execute(
                            "UPDATE history_payloads SET last_seen_at = MAX(last_seen_at, ?) "
                            "WHERE source = ? AND payload_hash = ?",
                            (outcome.collected_at, outcome.source, digest),
                        )
                capture_id = conn.execute(
                    "INSERT INTO history_captures (source, collected_at, status, duration_s, "
                    "record_count, error, payload_hash, changed, observations_extracted, "
                    "extract_error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (outcome.source, outcome.collected_at, c.status,
                     None if outcome.duration_s is None else round(outcome.duration_s, 3),
                     c.record_count, c.reason, digest, changed, c.extracted, c.extract_error),
                ).lastrowid
                stored = self._store_observations(capture_id, outcome, c.observations)
                observations_stored += stored
                if c.extracted is not None:
                    conn.execute(
                        "UPDATE history_captures SET observations_stored = ? WHERE id = ?",
                        (stored, capture_id),
                    )
            pruned = self._prune(now if now is not None else time.time())
        return {
            "recorded": len(outcomes),
            "status_counts": counts,
            "payloads_stored": payloads_stored,
            "observations_stored": observations_stored,
            "pruned": pruned,
            "db_path": str(self.db_path),
        }

    def _store_observations(
        self, capture_id: int, outcome: SourceOutcome, observations: list[Observation]
    ) -> int:
        stored = 0
        for obs in observations:
            key = f"{outcome.source}:{obs.series_key}"
            last = self._conn.execute(
                "SELECT value FROM history_observations "
                "WHERE series_key = ? AND observed_at IS ? "
                "ORDER BY collected_at DESC, id DESC LIMIT 1",
                (key, obs.observed_at),
            ).fetchone()
            value = float(obs.value)
            if last is not None and last[0] == value:
                continue
            self._conn.execute(
                "INSERT INTO history_observations (capture_id, source, series_key, "
                "observed_at, published_at, collected_at, value, unit, label) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (capture_id, outcome.source, key, obs.observed_at, obs.published_at,
                 outcome.collected_at, value, obs.unit,
                 None if obs.label is None else str(obs.label)[:300]),
            )
            stored += 1
        return stored

    def _prune(self, now: float) -> dict:
        days = retention_days()
        pruned = {"payloads": 0, "captures": 0, "observations": 0}
        if days["payload_days"] > 0:
            cutoff = now - days["payload_days"] * 86400
            pruned["payloads"] = self._conn.execute(
                "DELETE FROM history_payloads WHERE last_seen_at < ?", (cutoff,)
            ).rowcount
        if days["capture_days"] > 0:
            cutoff = now - days["capture_days"] * 86400
            pruned["captures"] = self._conn.execute(
                "DELETE FROM history_captures WHERE collected_at < ?", (cutoff,)
            ).rowcount
        if days["history_days"] > 0:
            cutoff = now - days["history_days"] * 86400
            pruned["observations"] = self._conn.execute(
                "DELETE FROM history_observations WHERE collected_at < ?", (cutoff,)
            ).rowcount
        return pruned

    # -- reads --------------------------------------------------------------

    def storage_stats(self) -> dict:
        conn = self._conn
        captures = conn.execute("SELECT COUNT(*) FROM history_captures").fetchone()[0]
        payloads, compressed, raw = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(body)), 0), COALESCE(SUM(raw_bytes), 0) "
            "FROM history_payloads"
        ).fetchone()
        observations = conn.execute("SELECT COUNT(*) FROM history_observations").fetchone()[0]
        series = conn.execute(
            "SELECT COUNT(DISTINCT series_key) FROM history_observations"
        ).fetchone()[0]
        first = conn.execute("SELECT MIN(collected_at) FROM history_captures").fetchone()[0]
        return {
            "captures": captures,
            "payload_bodies": payloads,
            "payload_bytes_compressed": compressed,
            "payload_bytes_raw": raw,
            "observations": observations,
            "series": series,
            "first_capture_at": _iso(first),
            "retention": retention_days(),
            "db_path": str(self.db_path),
        }

    def collection_health(
        self, roster: Iterable[str], source: str | None = None, now: float | None = None
    ) -> dict:
        """Per-source attempt / success / failure state from the run log."""
        now = time.time() if now is None else now
        default_cadence, overrides = cadence_config()
        roster = list(roster)
        known = set(roster)
        conn = self._conn
        logged = [r[0] for r in conn.execute("SELECT DISTINCT source FROM history_captures")]
        backfilled = sorted(s for s in logged if s.startswith(BACKFILL_PREFIX))
        names = roster + sorted(set(logged) - known - set(backfilled))
        if source is not None:
            if source not in names and source not in backfilled:
                return {
                    "error": f"unknown source {source!r}: not a collector source and never captured",
                    "known_sources": sorted(names + backfilled),
                }
            names = [source] if source in names else []
            backfilled = [source] if source in backfilled else []

        rows = []
        for name in names:
            cadence = overrides.get(name, default_cadence)
            overdue_after = OVERDUE_MULTIPLE * cadence
            last = conn.execute(
                "SELECT collected_at, status, error, extract_error FROM history_captures "
                "WHERE source = ? ORDER BY collected_at DESC, id DESC LIMIT 1",
                (name,),
            ).fetchone()
            entry: dict[str, Any] = {
                "source": name,
                "in_collector_roster": name in known,
                "cadence_s": cadence,
                "overdue_after_s": overdue_after,
            }
            if last is None:
                entry.update(state="never_run", attempts=0, overdue=None,
                             last_attempt_at=None, last_success_at=None)
                rows.append(entry)
                continue
            last_at, last_status, last_reason, last_extract_error = last
            attempts = conn.execute(
                "SELECT COUNT(*) FROM history_captures WHERE source = ?", (name,)
            ).fetchone()[0]
            last_success = conn.execute(
                "SELECT MAX(collected_at) FROM history_captures "
                "WHERE source = ? AND status IN ('ok', 'degraded')",
                (name,),
            ).fetchone()[0]
            last_ok = conn.execute(
                "SELECT MAX(collected_at) FROM history_captures WHERE source = ? AND status = 'ok'",
                (name,),
            ).fetchone()[0]
            # Errors recorded after the last non-error capture, in insertion
            # order (robust to a clock that stepped backwards).
            consecutive = conn.execute(
                "SELECT COUNT(*) FROM history_captures WHERE source = ? AND status = 'error' "
                "AND id > COALESCE((SELECT MAX(id) FROM history_captures "
                "WHERE source = ? AND status != 'error'), 0)",
                (name, name),
            ).fetchone()[0]
            obs = conn.execute(
                "SELECT MAX(observed_at), COUNT(DISTINCT series_key), "
                "SUM(observed_at IS NULL) FROM history_observations WHERE source = ?",
                (name,),
            ).fetchone()
            age_attempt = now - last_at
            age_success = None if last_success is None else now - last_success
            overdue = age_success is None or age_success > overdue_after
            attempt_overdue = age_attempt > overdue_after
            # A non-error last capture is itself the last success, so overdue
            # can differ from attempt_overdue only for a failing source; it is
            # reported there as the overdue flag, not as a state of its own.
            if attempt_overdue:
                state = "not_attempted_recently"
            elif last_status == "error":
                state = "failing"
            else:
                state = last_status
            entry.update(
                state=state,
                attempts=attempts,
                last_attempt_at=_iso(last_at),
                last_attempt_age_s=round(age_attempt, 1),
                last_status=last_status,
                last_reason=last_reason,
                last_success_at=_iso(last_success),
                last_success_age_s=None if age_success is None else round(age_success, 1),
                last_ok_at=_iso(last_ok),
                consecutive_failures=consecutive,
                overdue=overdue,
                attempt_overdue=attempt_overdue,
            )
            if last_extract_error:
                entry["last_extract_error"] = last_extract_error
            if obs[1]:
                entry["observation_series"] = obs[1]
                entry["latest_observed_at"] = obs[0]
                if obs[2]:
                    entry["observations_without_observed_at"] = obs[2]
            rows.append(entry)

        states: dict[str, int] = {}
        for entry in rows:
            states[entry["state"]] = states.get(entry["state"], 0) + 1
        return {
            "sources": rows,
            "state_counts": states,
            # On-demand loads, not judged against a cadence.
            "backfills": [self._backfill_row(name) for name in backfilled],
            "checked_at": _iso(now),
            "default_cadence_s": default_cadence,
            "overdue_multiple": OVERDUE_MULTIPLE,
            "storage": self.storage_stats(),
            "cannot_prove": [
                "A recent capture shows the collector reached the upstream and saved a "
                "payload; it does not show the upstream's observations are current. "
                "Check latest_observed_at here or observed_at in intel_series_history.",
                "Cadence is the stated expectation (default and overrides above), not "
                "the live daemon's --interval; a collector deliberately run less often "
                "reads overdue. The daemon sleeps --interval after each cycle, so its "
                "real period is longer by the cycle's own duration.",
                "A degraded capture with no records cannot tell a quiet period from a "
                "silent upstream failure.",
                HISTORY_ACCRUAL_NOTE + " A source the collector never ran reads never_run, "
                "not healthy.",
            ],
            "states_explained": {
                "ok": "last capture delivered records with no degradation marker",
                "degraded": "last capture delivered a payload marked stale/degraded, empty, "
                "or without the measures its extractor expects",
                "failing": "last capture was an error (consecutive_failures counts the run; "
                f"overdue is true when no payload arrived within {OVERDUE_MULTIPLE:g} "
                "cadences)",
                "not_attempted_recently": f"the collector has not attempted it within "
                f"{OVERDUE_MULTIPLE:g} cadences (overdue is then always true)",
                "never_run": "no capture recorded",
            },
        }

    def _backfill_row(self, name: str) -> dict:
        """One backfill source: its captures and the series it stored."""
        conn = self._conn
        captures, last_at = conn.execute(
            "SELECT COUNT(*), MAX(collected_at) FROM history_captures WHERE source = ?", (name,)
        ).fetchone()
        last_status, last_reason = conn.execute(
            "SELECT status, error FROM history_captures WHERE source = ? "
            "ORDER BY collected_at DESC, id DESC LIMIT 1",
            (name,),
        ).fetchone()
        series, points, first_obs, last_obs = conn.execute(
            "SELECT COUNT(DISTINCT series_key), COUNT(*), MIN(observed_at), MAX(observed_at) "
            "FROM history_observations WHERE source = ?",
            (name,),
        ).fetchone()
        return {
            "source": name,
            "captures": captures,
            "last_run_at": _iso(last_at),
            "last_status": last_status,
            "last_reason": last_reason,
            "series": series,
            "observations": points,
            "first_observed_at": first_obs,
            "latest_observed_at": last_obs,
            "note": BACKFILL_VINTAGE_NOTE,
        }

    def capture_history(
        self, source: str, limit: Any = 20, capture_id: Any = None
    ) -> dict:
        """A source's earlier captures, newest first, changed vs unchanged."""
        conn = self._conn
        columns = (
            "c.id, c.collected_at, c.status, c.duration_s, c.record_count, c.error, "
            "c.payload_hash, c.changed, c.observations_extracted, c.observations_stored, "
            "c.extract_error, p.body IS NOT NULL"
        )
        join = (
            "FROM history_captures c LEFT JOIN history_payloads p "
            "ON p.source = c.source AND p.payload_hash = c.payload_hash"
        )
        if capture_id is not None:
            try:
                cid = int(capture_id)
            except (TypeError, ValueError):
                return {"error": f"capture_id must be an integer, got {capture_id!r}"}
            row = conn.execute(
                f"SELECT {columns}, p.body {join} WHERE c.id = ? AND c.source = ?",
                (cid, source),
            ).fetchone()
            if row is None:
                return {"error": f"no capture {cid} for source {source!r}"}
            capture = self._capture_row(row[:-1])
            if row[-1] is not None:
                capture["payload"] = json.loads(zlib.decompress(row[-1]))
            elif capture["payload_hash"]:
                capture["payload"] = None
                capture["payload_note"] = "body pruned past WORLD_INTEL_HISTORY_PAYLOAD_DAYS"
            return {"source": source, "capture": capture, "note": HISTORY_ACCRUAL_NOTE}

        n = _clamp(limit, 20)
        rows = conn.execute(
            f"SELECT {columns} {join} WHERE c.source = ? "
            "ORDER BY c.collected_at DESC, c.id DESC LIMIT ?",
            (source, n),
        ).fetchall()
        total, changed, distinct = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(changed), 0), COUNT(DISTINCT payload_hash) "
            "FROM history_captures WHERE source = ?",
            (source,),
        ).fetchone()
        result = {
            "source": source,
            "captures": [self._capture_row(r) for r in rows],
            "total_captures": total,
            "changed_captures": changed,
            "distinct_payloads": distinct,
            "note": HISTORY_ACCRUAL_NOTE,
        }
        if total == 0:
            result["data_gaps"] = [
                f"no captures recorded for {source!r}: the collector has not run it "
                "against this database"
            ]
        return result

    @staticmethod
    def _capture_row(row: tuple) -> dict:
        (cid, collected_at, status, duration, count, error, digest, changed,
         extracted, stored, extract_error, body_retained) = row
        return {
            "capture_id": cid,
            "collected_at": _iso(collected_at),
            "status": status,
            "duration_s": duration,
            "record_count": count,
            "reason": error,
            "payload_hash": digest,
            "change": (
                None if digest is None
                else "first" if changed is None
                else "changed" if changed else "unchanged"
            ),
            "body_retained": bool(body_retained) if digest else None,
            "observations_extracted": extracted,
            "observations_stored": stored,
            "extract_error": extract_error,
        }

    def series_history(
        self, series_key: str | None = None, source: str | None = None, limit: Any = 100
    ) -> dict:
        """One series' observations with all three dates, or (no key) the
        listing of available series."""
        conn = self._conn
        if not series_key:
            params: list[Any] = []
            where = ""
            if source:
                where, params = "WHERE source = ?", [source]
            rows = conn.execute(
                "SELECT series_key, source, MAX(unit), MAX(label), COUNT(*), "
                "MIN(observed_at), MAX(observed_at), SUM(observed_at IS NULL), "
                f"MAX(collected_at) FROM history_observations {where} "
                "GROUP BY series_key ORDER BY MAX(collected_at) DESC, series_key LIMIT ?",
                (*params, _clamp(limit, 200)),
            ).fetchall()
            total = conn.execute(
                f"SELECT COUNT(DISTINCT series_key) FROM history_observations {where}", params
            ).fetchone()[0]
            result = {
                "series": [
                    {
                        "series_key": r[0], "source": r[1], "unit": r[2], "label": r[3],
                        "points": r[4], "first_observed_at": r[5], "last_observed_at": r[6],
                        "points_without_observed_at": r[7],
                        "last_collected_at": _iso(r[8]),
                    }
                    for r in rows
                ],
                "total_series": total,
                "sources_with_extractors": sorted(_EXTRACTORS),
                "note": HISTORY_ACCRUAL_NOTE,
            }
            if total == 0:
                result["data_gaps"] = [
                    "no observations recorded yet"
                    + (f" for source {source!r}" if source else "")
                ]
            return result

        rows = conn.execute(
            "SELECT observed_at, published_at, collected_at, value, unit, label, capture_id, "
            "source FROM history_observations WHERE series_key = ? "
            "ORDER BY observed_at IS NULL, observed_at DESC, collected_at DESC, id DESC LIMIT ?",
            (series_key, _clamp(limit, 100)),
        ).fetchall()
        if not rows:
            return {
                "error": f"no observations for series {series_key!r}; call without "
                "series_key to list available series",
            }
        observations = [
            {
                "observed_at": r[0], "published_at": r[1], "collected_at": _iso(r[2]),
                "value": r[3], "unit": r[4], "label": r[5], "capture_id": r[6],
            }
            for r in rows
        ]
        gaps = []
        missing_observed = sum(1 for o in observations if o["observed_at"] is None)
        if missing_observed:
            gaps.append(
                f"{missing_observed} point(s) have no observed_at: the payload carries no "
                "observation date, and collected_at is when this server saved it, not when "
                "the value was observed"
            )
        if all(o["published_at"] is None for o in observations):
            gaps.append("published_at unknown: this source's payload names no release time")
        if str(rows[0][7]).startswith(BACKFILL_PREFIX):
            gaps.append(BACKFILL_VINTAGE_NOTE)
        return {
            "series_key": series_key,
            "source": rows[0][7],
            "observations": observations,
            "data_gaps": gaps,
            "note": (
                "Rows for the same observed_at are successive values this server saw "
                "(revisions); a row is stored only when new or changed. "
                + HISTORY_ACCRUAL_NOTE
            ),
        }

    def revisions(
        self, source: str | None = None, series_key: str | None = None, limit: Any = 100
    ) -> dict:
        """Values for the same observed_at that a later capture changed."""
        filters = ["observed_at IS NOT NULL"]
        params: list[Any] = []
        if source:
            filters.append("source = ?")
            params.append(source)
        if series_key:
            filters.append("series_key = ?")
            params.append(series_key)
        rows = self._conn.execute(
            "SELECT series_key, source, observed_at, prev_value, prev_collected_at, "
            "prev_capture_id, value, collected_at, capture_id, unit, label FROM ("
            "  SELECT *, LAG(value) OVER w AS prev_value, "
            "  LAG(collected_at) OVER w AS prev_collected_at, "
            "  LAG(capture_id) OVER w AS prev_capture_id "
            f"  FROM history_observations WHERE {' AND '.join(filters)} "
            "  WINDOW w AS (PARTITION BY series_key, observed_at ORDER BY collected_at, id)"
            ") WHERE prev_value IS NOT NULL AND value != prev_value "
            "ORDER BY collected_at DESC, id DESC LIMIT ?",
            (*params, _clamp(limit, 100)),
        ).fetchall()
        return {
            "revisions": [
                {
                    "series_key": r[0], "source": r[1], "observed_at": r[2],
                    "previous_value": r[3], "previous_collected_at": _iso(r[4]),
                    "previous_capture_id": r[5], "revised_value": r[6],
                    "revised_collected_at": _iso(r[7]), "revised_capture_id": r[8],
                    "delta": round(r[6] - r[3], 10), "unit": r[9], "label": r[10],
                }
                for r in rows
            ],
            "count": len(rows),
            "cannot_prove": [
                "Only changes between two of this server's own captures are visible; a "
                "revision made before the first capture, or while the collector was not "
                "running, is not.",
                "Series without observed_at (for example Yahoo quotes) cannot be checked "
                "for revisions: there is no observation date to hold fixed.",
            ],
            "note": HISTORY_ACCRUAL_NOTE,
        }


# Extractors for further collector sources live in their own module. Importing
# it here registers them wherever a capture is classified, whichever module
# imported the store first.
from . import history_extractors  # noqa: E402,F401  (registers on import)
