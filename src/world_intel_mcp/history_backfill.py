"""History backfill: load the history FRED and the ECB Data Portal already
serve into the history store (ROADMAP Phase 29).

The collector records only what it sees from its first cycle on. Two of
its upstreams serve a series' whole history on every request: FRED's
keyless graph CSV and the ECB Data Portal. A backfill reads them through
the fetch and parse code the liquidity tools use (``sources/liquidity.py``:
``_fred_rows`` and ``fetch_ecb_series``), so every request goes through the
project's Fetcher with its FRED rate floor, circuit breaker and cache, and
writes the points into the history store (``history.py``):

- One capture per request (one FRED series id, or one ECB series key,
  which may match several series) under the source ``backfill:fred`` or
  ``backfill:ecb``, so a backfilled
  row never reads as a collector capture. Series keys are
  ``backfill:fred:<ID>`` and ``backfill:ecb:<DATASET>.<KEY>``. Every run is
  also logged in ``history_backfills`` (request, start, per-series counts,
  when the response was fetched), a table created here on first use with
  ``CREATE TABLE IF NOT EXISTS``: a database made before it existed gains
  it in place, its rows untouched.
- Three dates: ``observed_at`` is the period the publisher dates a value
  by (FRED dates as written, a monthly value at the first day of its month;
  ECB months as written, an ECB week as the Sunday that ends the ISO week,
  any other ECB period as the period end its response states). A start
  keeps values whose ``observed_at`` is on or after it, for both
  providers: the ECB applies ``startPeriod`` to a period's first day, so
  the ECB request asks from one period earlier (6 days for weeks, 92 for
  quarters, 183 for half-years) and the window is cut here;
  ``published_at`` is NULL because neither response names a release or
  vintage date (the FRED graph CSV carries observation dates and values
  only); ``collected_at`` is the backfill time, one stamp per call taken
  after its last fetch, so never earlier than a response it stores.
- Idempotent under the store's rule: a value is stored only when its series
  and ``observed_at`` have no stored value or a different one. A re-run
  stores nothing unless the publisher changed a value, and a changed value
  is then a revision (``intel_revisions``).
- FRED missing values ("." and blank cells) are skipped and counted, never
  stored as zero; non-finite values are skipped and counted apart. A
  stored value inside the response's date span that the response no
  longer serves is kept (the store never deletes a value) and reported as
  ``absent_from_response``.
- A failure is an error, never an empty success: an HTTP error, an empty or
  malformed body, a window without values, and a response available only
  as a stale cache copy (not the publisher's current data) each record an
  error capture and store no values. A store write failure names the
  request it failed on, the requests fetched but not written, and the
  upstream failures it could or could not log.
- Payload bodies of backfill captures hold a summary of the response
  (series, counts, date span, the full response's hash), not the response:
  one FRED series can carry 26,000 rows, and replaying it through
  ``intel_capture_history`` would flood a client. The values loaded are in
  the observations.
- Bounded: ``MAX_FRED_SERIES`` series or ``MAX_ECB_KEYS`` keys per call, at
  most ``MAX_ECB_COMBINATIONS`` series per ECB key, the newest
  ``MAX_POINTS`` points per series and ``MAX_POINTS_PER_CALL`` per call,
  optionally only from a start date. Values a series cap leaves out are
  the oldest, and no later backfill can load them (there is no end bound);
  values a call's budget leaves out load in a call of their own.

Vintage: a backfilled value is the publisher's vintage on the backfill day,
already revised, so revisions made before the first backfill are
invisible. Retention: a backfill prunes only backfill rows (``backfill:*``
captures, payload bodies and observations, and ``history_backfills``),
using the retention settings of the process that runs it; collector rows
are left to the collector's own prune and settings. Observations are
pruned by ``collected_at`` (``WORLD_INTEL_HISTORY_DAYS``, default 365),
never by ``observed_at``, so a 1962 value is kept as long as any value
stored the same day. An unchanged value is not stored again, so its
``collected_at`` stays at the backfill that first stored it; past
retention the row is pruned, and the next backfill (which prunes before
it stores) stores it again as new.

Gaps / not covered:
- Only FRED graph CSV series and ECB Data Portal BSI and ILM series; no
  other source that serves history is wired.
- FRED vintages are not read: the graph CSV serves the current vintage
  only, so FRED revisions show up only between two backfills.
- ECB missing values cannot be counted: the shared SDMX-JSON parser drops
  null observations before this code sees them.
- Units of a FRED series outside ``liquidity.FRED_SERIES`` are unknown
  (stored NULL and reported as a data gap): the CSV carries no units.
- Couples to private parts of two modules: ``liquidity._fred_rows``, the
  liquidity cache-key formats (read for ``copy_fetched_at``) and the
  ECB key rules, and the store's ``_conn``, ``_store_observations`` and
  ``_prune``. A change there needs a matching change here; the tests pin
  the behaviour, not the coupling.
- The collector's prune also deletes backfill rows, under the collector's
  settings; it does not touch ``history_backfills``, whose rows are pruned
  here, on the next backfill, past the observation retention.
- No end bound: the oldest values of a series longer than ``MAX_POINTS``
  cannot be backfilled.
- An ECB key that matches only some of its combinations is not an error:
  each ``+`` code that matched no series at all is named in the gaps, but
  a combination of matched codes that is absent is not.
- FRED's terms of use speak of personal, non-commercial use of FRED data;
  whether a deployment's use fits them is the operator's call, not this
  module's.
"""

from __future__ import annotations

import json
import math
import re
import sqlite3
import time
import zlib
from datetime import date, datetime, timedelta, timezone
from typing import Any

from .fetcher import Fetcher
from .history import (
    BACKFILL_PREFIX,
    HistoryStore,
    Observation,
    SourceOutcome,
    normalize_observed_at,
    payload_hash,
    retention_days,
)
from .history_extractors import period_date
from .sources import liquidity

FRED_SOURCE = BACKFILL_PREFIX + "fred"
ECB_SOURCE = BACKFILL_PREFIX + "ecb"

MAX_FRED_SERIES = 5
MAX_ECB_KEYS = 2
MAX_ECB_COMBINATIONS = 20
MAX_POINTS = 20_000
MAX_POINTS_PER_CALL = 100_000

_FRED_ID = re.compile(r"[A-Z0-9_]{1,40}")
_START = re.compile(r"\d{4}-\d{2}-\d{2}")
# Dimensions in a key of each dataset: the rule fetch_ecb_series applies,
# checked here so a bad key is refused before any request of the call.
_ECB_DIMENSIONS = {"BSI": 11, "ILM": 6}
# Days before the start an ECB request asks from, by frequency (the key's
# first dimension), so a period that starts before the start but is dated
# on or after it (weeks by their Sunday, quarters and half-years by their
# end) is returned; the window is then cut on observed_at.
_ECB_LOOKBACK_DAYS = {"W": 6, "Q": 92, "H": 183}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS history_backfills (
    capture_id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    request TEXT NOT NULL,
    start TEXT,
    ran_at REAL NOT NULL,
    copy_fetched_at REAL,
    status TEXT NOT NULL CHECK (status IN ('ok', 'error')),
    payload_hash TEXT,
    detail TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_history_backfills_request
    ON history_backfills(source, request, start, ran_at);
"""

VINTAGE_GAP = (
    "Values are the publisher's current vintage as of this backfill (collected_at "
    "is the backfill time): revisions the publisher made before the first backfill "
    "are invisible, and a later backfill that reads a changed value stores it as a "
    "revision (intel_revisions)."
)
RETENTION_GAP = (
    "Retention: a backfill prunes only backfill rows, with this process's retention "
    "settings; collector rows are left to the collector. Observations are pruned by "
    "collected_at (WORLD_INTEL_HISTORY_DAYS, default 365, 0 keeps forever), not by "
    "observed_at. "
    "An unchanged value is not stored again, so its collected_at stays at the "
    "backfill that first stored it; once that is past retention the row is pruned "
    "and the next backfill stores it again. Capture rows (WORLD_INTEL_HISTORY_CAPTURE_DAYS, "
    "default 30) and payload bodies (WORLD_INTEL_HISTORY_PAYLOAD_DAYS, default 7) age out sooner."
)
BODY_NOTE = (
    "Summary only: a backfill keeps this summary instead of the response, which can "
    "hold tens of thousands of rows. The values loaded are in the history store "
    "(intel_series_history); payload_hash is the full response's hash."
)
COPY_GAP = (
    "copy_fetched_at is when the response this backfill read was fetched: a copy "
    "cached by an earlier call (FRED 1 h, ECB 3 h) is reused without a request, "
    "and a stale copy is refused."
)
FRED_PUBLISHED_GAP = (
    "published_at is NULL: the FRED graph CSV carries observation dates and values "
    "only, no release or vintage date."
)
FRED_DATE_GAP = (
    "observed_at is the date FRED gives each value: a monthly value is dated at the "
    "first day of its month, a weekly H.4.1 value at its Wednesday."
)
ECB_PUBLISHED_GAP = (
    "published_at is NULL: the ECB Data Portal response carries no release dates."
)
ECB_DATE_GAP = (
    "observed_at: monthly periods as written (2026-08); a weekly period (2026-W41) "
    "as the Sunday that ends the ISO week, the period end the ECB response states; "
    "any other period as the period end the response states."
)
ECB_MISSING_GAP = (
    "ECB missing values are not counted (missing_skipped is null): the SDMX-JSON "
    "parser drops null observations before the backfill sees them."
)


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _requests(raw: Any) -> list[str]:
    """Request strings from a list, tuple or comma/space separated string,
    in order and without repeats."""
    if isinstance(raw, str):
        items = re.split(r"[\s,]+", raw)
    elif isinstance(raw, (list, tuple)):
        items = [str(x) for x in raw]
    else:
        items = []
    out: list[str] = []
    for item in items:
        item = item.strip()
        if item and item not in out:
            out.append(item)
    return out


def _start(start: Any) -> date | None:
    if start in (None, ""):
        return None
    if not isinstance(start, str) or not _START.fullmatch(start.strip()):
        raise ValueError(f"start must be a date YYYY-MM-DD, got {start!r}")
    try:
        day = date.fromisoformat(start.strip())
    except ValueError:
        raise ValueError(f"start must be a date YYYY-MM-DD, got {start!r}") from None
    if day > datetime.now(timezone.utc).date():
        raise ValueError(f"start {start} is in the future")
    return day


def _ecb_request(raw: str) -> tuple[str, str]:
    """``DATASET.KEY`` -> (dataset, key), or ValueError naming the rule."""
    dataset, _, key = raw.partition(".")
    dataset = dataset.upper()
    if dataset not in _ECB_DIMENSIONS:
        raise ValueError(
            f"{raw!r}: an ECB series key starts with its dataset, one of "
            f"{sorted(_ECB_DIMENSIONS)} (for example BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.E)"
        )
    if not liquidity._ECB_KEY_RE.fullmatch(key):
        raise ValueError(f"{raw!r}: key may hold letters, digits, '.', '+' and '_' only")
    dims = key.split(".")
    if len(dims) != _ECB_DIMENSIONS[dataset] or any(
        not code for dim in dims for code in dim.split("+")
    ):
        raise ValueError(
            f"{raw!r}: a {dataset} key names all {_ECB_DIMENSIONS[dataset]} dimensions, "
            "without wildcards"
        )
    combos = math.prod(len(dim.split("+")) for dim in dims)
    if combos > MAX_ECB_COMBINATIONS:
        raise ValueError(
            f"{raw!r}: {combos} series combinations, more than the {MAX_ECB_COMBINATIONS} "
            "a backfill loads per key"
        )
    return dataset, key


def ensure_schema(store: HistoryStore) -> None:
    """Create the backfill log in place; idempotent, other tables untouched."""
    store._conn.executescript(_SCHEMA)
    store._conn.commit()


class _Run:
    """One backfill call: point budget, then every write at one time.

    Requests are fetched first and written together by ``commit``, stamped
    after the last fetch, so ``collected_at`` is never earlier than a
    response it stores (the collector stamps a capture when its fetch
    finished, too).
    """

    def __init__(self, store: HistoryStore, source: str, start: date | None):
        self.store = store
        self.source = source
        self.start = start
        self.start_text = start.isoformat() if start else None
        self.now = 0.0  # set by commit()
        self.budget = MAX_POINTS_PER_CALL
        self.requests: list[dict] = []
        self._pending: list[tuple[str, tuple]] = []
        self.failed_at: int | None = None  # index in _pending of a failed write

    def defer_fail(self, request: str, error: str, duration: float) -> None:
        self._pending.append(("fail", (request, error, duration)))

    def defer_store(self, request: str, payload: dict, series: list[dict], duration: float,
                    copy_at: float | None) -> None:
        self._pending.append(("store", (request, payload, series, duration, copy_at)))

    def commit(self, now: float | None) -> dict:
        """Create the log table if needed, prune, then write each request
        in its own transaction; returns what the prune removed. On a store
        error ``failed_at`` marks the write that failed."""
        ensure_schema(self.store)
        self.now = time.time() if now is None else now
        pruned = self.prune()
        for i, (kind, args) in enumerate(self._pending):
            try:
                if kind == "store":
                    self.store_request(*args)
                else:
                    self.fail(*args)
            except sqlite3.Error:
                self.failed_at = i
                raise
        return pruned

    def prune(self) -> dict:
        """Prune backfill rows only, before storing, so a value pruned past
        retention is stored again by this run instead of vanishing. Collector
        rows are the collector's to prune, under its own settings."""
        conn = self.store._conn
        days = retention_days()
        like = BACKFILL_PREFIX + "%"
        pruned = {"payloads": 0, "captures": 0, "observations": 0, "backfill_log": 0}
        with conn:
            if days["payload_days"] > 0:
                pruned["payloads"] = conn.execute(
                    "DELETE FROM history_payloads WHERE source LIKE ? AND last_seen_at < ?",
                    (like, self.now - days["payload_days"] * 86400),
                ).rowcount
            if days["capture_days"] > 0:
                pruned["captures"] = conn.execute(
                    "DELETE FROM history_captures WHERE source LIKE ? AND collected_at < ?",
                    (like, self.now - days["capture_days"] * 86400),
                ).rowcount
            if days["history_days"] > 0:
                cutoff = self.now - days["history_days"] * 86400
                pruned["observations"] = conn.execute(
                    "DELETE FROM history_observations WHERE source LIKE ? AND collected_at < ?",
                    (like, cutoff),
                ).rowcount
                pruned["backfill_log"] = conn.execute(
                    "DELETE FROM history_backfills WHERE ran_at < ?", (cutoff,)
                ).rowcount
        return pruned

    def _previous(self, request: str) -> tuple[str | None, float | None]:
        row = self.store._conn.execute(
            "SELECT payload_hash, ran_at FROM history_backfills WHERE source = ? AND "
            "request = ? AND start IS ? AND status = 'ok' ORDER BY ran_at DESC, capture_id DESC "
            "LIMIT 1",
            (self.source, request, self.start_text),
        ).fetchone()
        return (row[0], row[1]) if row else (None, None)

    def fail(self, request: str, error: str, duration: float, copy_at: float | None = None) -> dict:
        conn = self.store._conn
        with conn:
            capture_id = conn.execute(
                "INSERT INTO history_captures (source, collected_at, status, duration_s, error) "
                "VALUES (?, ?, 'error', ?, ?)",
                (self.source, self.now, round(duration, 3), error[:500]),
            ).lastrowid
            conn.execute(
                "INSERT INTO history_backfills (capture_id, source, request, start, ran_at, "
                "copy_fetched_at, status, detail) VALUES (?, ?, ?, ?, ?, ?, 'error', ?)",
                (capture_id, self.source, request, self.start_text, self.now, copy_at,
                 json.dumps({"error": error})),
            )
        entry = {
            "request": request,
            "status": "error",
            "error": error,
            "capture_id": capture_id,
            "copy_fetched_at": _iso(copy_at),
            "series": [],
        }
        self.requests.append(entry)
        return entry

    def store_request(
        self,
        request: str,
        payload: dict,
        series: list[dict],
        duration: float,
        copy_at: float | None,
    ) -> dict:
        """Write one request: payload body, capture, observations, log row.

        ``series`` items carry ``key`` (suffix after the source), ``label``,
        ``unit``, ``points`` (``(observed_at, value)`` oldest first, already
        valid) and their skip counts.
        """
        conn = self.store._conn
        digest = payload_hash(payload)
        body = json.dumps({
            "provider": payload.get("provider"),
            "request": request,
            "start": self.start_text,
            "payload_hash": digest,
            "body": BODY_NOTE,
            "series": [
                {
                    "series_key": f"{self.source}:{s['key']}",
                    "points_in_response": s["points_in_response"],
                    "in_window": s["in_window"],
                    "loaded": len(s["points"]),
                    "first_observed_at": s["points"][0][0] if s["points"] else None,
                    "last_observed_at": s["points"][-1][0] if s["points"] else None,
                }
                for s in series
            ],
        }, separators=(",", ":"), default=str).encode()
        prev_hash, prev_at = self._previous(request)
        changed = None if prev_hash is None else int(prev_hash != digest)
        results = []
        with conn:
            inserted = conn.execute(
                "INSERT OR IGNORE INTO history_payloads "
                "(source, payload_hash, first_seen_at, last_seen_at, raw_bytes, body) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (self.source, digest, self.now, self.now, len(body), zlib.compress(body, 6)),
            ).rowcount
            if not inserted:
                conn.execute(
                    "UPDATE history_payloads SET last_seen_at = MAX(last_seen_at, ?) "
                    "WHERE source = ? AND payload_hash = ?",
                    (self.now, self.source, digest),
                )
            notes = [
                f"{s['key']}: " + ", ".join(
                    f"{n} {what}" for n, what in (
                        (s["missing_skipped"], "missing value(s) skipped"),
                        (s["invalid_skipped"], "invalid value(s) skipped"),
                        (s["truncated"], "older value(s) past the point cap not loaded"),
                    ) if n
                )
                for s in series
                if s["missing_skipped"] or s["invalid_skipped"] or s["truncated"]
            ]
            capture_id = conn.execute(
                "INSERT INTO history_captures (source, collected_at, status, duration_s, "
                "record_count, payload_hash, changed, observations_extracted, extract_error) "
                "VALUES (?, ?, 'ok', ?, ?, ?, ?, ?, ?)",
                (self.source, self.now, round(duration, 3),
                 sum(s["points_in_response"] for s in series), digest, changed,
                 sum(len(s["points"]) for s in series),
                 "; ".join(notes)[:500] or None),
            ).lastrowid
            outcome = SourceOutcome(source=self.source, collected_at=self.now)
            stored_total = 0
            for s in series:
                key = f"{self.source}:{s['key']}"
                prior = {
                    r[0] for r in conn.execute(
                        "SELECT DISTINCT observed_at FROM history_observations "
                        "WHERE series_key = ?",
                        (key,),
                    )
                }
                observations = [
                    Observation(s["key"], value, observed_at=observed, unit=s["unit"],
                                label=s["label"])
                    for observed, value in s["points"]
                ]
                stored = self.store._store_observations(capture_id, outcome, observations)
                stored_total += stored
                new = sum(1 for observed, _ in s["points"] if observed not in prior)
                loaded = len(s["points"])
                # Stored values inside the response's date span that it no
                # longer serves (withdrawn, or now a missing cell).
                lo, hi = s.get("span") or (None, None)
                served = s.get("served") or set()
                absent = sorted(
                    d for d in prior if d is not None and lo is not None
                    and lo <= d <= hi and d not in served
                )
                results.append({
                    "series_key": key,
                    "label": s["label"],
                    "unit": s["unit"],
                    "points_in_response": s["points_in_response"],
                    "in_window": s["in_window"],
                    "missing_skipped": s["missing_skipped"],
                    "invalid_skipped": s["invalid_skipped"],
                    "truncated": s["truncated"],
                    "truncated_by": (
                        None if not s["truncated"]
                        else "call budget" if s.get("budget_limited") else "series cap"
                    ),
                    "loaded": loaded,
                    "first_observed_at": s["points"][0][0] if loaded else None,
                    "last_observed_at": s["points"][-1][0] if loaded else None,
                    "stored_new": new,
                    "stored_revised": stored - new,
                    "unchanged": loaded - stored,
                    "absent_from_response": len(absent),
                    "absent_first": absent[0] if absent else None,
                    "absent_last": absent[-1] if absent else None,
                    **({"provisional": s["provisional"]} if "provisional" in s else {}),
                })
            conn.execute(
                "UPDATE history_captures SET observations_stored = ? WHERE id = ?",
                (stored_total, capture_id),
            )
            conn.execute(
                "INSERT INTO history_backfills (capture_id, source, request, start, ran_at, "
                "copy_fetched_at, status, payload_hash, detail) "
                "VALUES (?, ?, ?, ?, ?, ?, 'ok', ?, ?)",
                (capture_id, self.source, request, self.start_text, self.now, copy_at, digest,
                 json.dumps({"series": results})),
            )
        entry = {
            "request": request,
            "status": "ok",
            "capture_id": capture_id,
            "copy_fetched_at": _iso(copy_at),
            "previous_backfill_at": _iso(prev_at),
            "payload_changed": None if changed is None else bool(changed),
            "series": results,
        }
        self.requests.append(entry)
        return entry

    def window(
        self, points: list[tuple[str, float]]
    ) -> tuple[list[tuple[str, float]], int, bool]:
        """Newest points within the per-series cap and the call's budget,
        oldest first, how many older ones were left out, and whether the
        call's budget (not the series cap) left them out."""
        cap = min(MAX_POINTS, self.budget)
        kept = points[-cap:] if cap > 0 else []
        self.budget -= len(kept)
        truncated = len(points) - len(kept)
        return kept, truncated, bool(truncated) and cap < MAX_POINTS

    def result(self, provider: str, gaps: list[str], pruned: dict) -> dict:
        failed = [r for r in self.requests if r["status"] == "error"]
        series = [s for r in self.requests for s in r["series"]]
        totals = {
            "requests": len(self.requests),
            "failed": len(failed),
            "series": len(series),
            **{
                name: sum(s[name] for s in series)
                for name in ("loaded", "stored_new", "stored_revised", "unchanged",
                             "invalid_skipped", "truncated", "absent_from_response")
            },
            # Unknown, not zero, when any series could not count them (ECB).
            "missing_skipped": (
                None if any(s["missing_skipped"] is None for s in series)
                else sum(s["missing_skipped"] for s in series)
            ),
        }
        gaps = list(gaps)
        if failed:
            gaps.insert(0, f"{len(failed)} of {len(self.requests)} request(s) failed and stored "
                        "nothing: " + "; ".join(f"{r['request']}: {r['error']}" for r in failed))
        for s in series:
            if not s["truncated"]:
                continue
            before = f"{s['truncated']:,} older value(s) before {s['first_observed_at']}"
            if s["truncated_by"] == "call budget":
                alone = s["loaded"] + s["truncated"]
                gaps.append(
                    f"Point cap: {s['series_key']}: {before} were not loaded because this "
                    f"call's {MAX_POINTS_PER_CALL:,}-point budget ran out; backfill it in a "
                    "call of its own"
                    + (f", which loads its newest {MAX_POINTS:,}" if alone > MAX_POINTS else "")
                    + "."
                )
            else:
                gaps.append(
                    f"Point cap: {s['series_key']}: {before} cannot be loaded by a "
                    f"backfill, which keeps only the newest {MAX_POINTS:,} values of a series."
                )
        for s in series:
            if s["absent_from_response"]:
                gaps.append(
                    f"{s['series_key']}: {s['absent_from_response']} stored value(s) from "
                    f"{s['absent_first']} to {s['absent_last']} are absent from this response "
                    "(withdrawn, or now a missing value); the store keeps them as an earlier "
                    "backfill read them."
                )
        out = {
            "source": self.source,
            "provider": provider,
            "collected_at": _iso(self.now),
            "start": self.start_text,
            "requests": self.requests,
            "totals": totals,
            "pruned": pruned,
            "data_gaps": gaps + [VINTAGE_GAP, RETENTION_GAP, COPY_GAP],
            "db_path": str(self.store.db_path),
        }
        if failed and len(failed) == len(self.requests):
            out["error"] = "no request succeeded: " + "; ".join(
                f"{r['request']}: {r['error']}" for r in failed
            )[:500]
        return out


def _first_day(observed: str | None) -> date | None:
    """The date a stored observed_at is compared with a start by: the day
    itself, or a month's or year's first day (as FRED dates a month)."""
    if not observed:
        return None
    try:
        if len(observed) == 10:
            return date.fromisoformat(observed)
        if len(observed) == 7:
            return date.fromisoformat(observed + "-01")
        if len(observed) == 4:
            return date.fromisoformat(observed + "-01-01")
    except ValueError:
        return None
    return None


def _unmatched_codes(key: str, series: list[dict]) -> list[str]:
    """Each '+' alternative of an ECB key that no returned series carries."""
    returned = [str(s.get("key") or "").split(".")[1:] for s in series]
    out = []
    for i, dim in enumerate(key.split(".")):
        codes = dim.split("+")
        if len(codes) > 1:
            present = {r[i] for r in returned if len(r) > i}
            out += [code for code in codes if code not in present]
    return out


def _copy_at(fetcher: Fetcher, cache_key: str) -> float | None:
    meta = fetcher.cache.get_stale_meta(cache_key)
    return None if meta is None else float(meta[1])


def _refusal(what: str, age: Any) -> str:
    return (
        f"{what}: the upstream fetch failed and only a stale cached copy"
        + (f" ({age} s old)" if age is not None else "")
        + " was available; a backfill stores only the publisher's current data, so "
        "nothing was stored"
    )


def _error_result(source: str, provider: str, error: str) -> dict:
    return {"error": error, "source": source, "provider": provider, "requests": []}


def _store_failure(source: str, provider: str, exc: sqlite3.Error, run: _Run) -> dict:
    """A store error mid-commit: each request is its own transaction, so the
    requests listed were committed and the failing one was rolled back.

    Names the request the write failed on, the requests fetched but not
    written, and the upstream failures after it: their error captures are
    written if the store now accepts them, else listed as not logged."""
    committed = list(run.requests)
    out = _error_result(source, provider, f"history store write failed: {exc}")
    out["error"] += f"; {len(committed)} earlier request(s) were committed"
    out["requests"] = committed
    out["collected_at"] = _iso(run.now) if run.now else None
    failing = None if run.failed_at is None else run._pending[run.failed_at]
    rest = run._pending if run.failed_at is None else run._pending[run.failed_at + 1:]
    out["failed_request"] = failing[1][0] if failing else None
    out["fetched_not_written"] = [args[0] for kind, args in rest if kind == "store"]
    logged, not_logged = [], []
    if failing and failing[0] == "fail":
        not_logged.append(failing[1][0])
    for kind, args in rest:
        if kind != "fail":
            continue
        try:
            run.fail(*args)
            logged.append(args[0])
        except sqlite3.Error:
            not_logged.append(args[0])
    out["upstream_failures_logged"] = logged
    out["upstream_failures_not_logged"] = not_logged
    return out


async def backfill_fred(
    fetcher: Fetcher,
    store: HistoryStore,
    series_ids: Any,
    start: Any = None,
    *,
    now: float | None = None,
) -> dict:
    """Load FRED graph CSV series into the store under ``backfill:fred``."""
    provider = "FRED graph CSV (fred.stlouisfed.org/graph/fredgraph.csv)"
    ids = list(dict.fromkeys(s.upper() for s in _requests(series_ids)))
    if not ids:
        return _error_result(FRED_SOURCE, provider, "name at least one FRED series id")
    if len(ids) > MAX_FRED_SERIES:
        return _error_result(
            FRED_SOURCE, provider,
            f"{len(ids)} series requested; a backfill loads at most {MAX_FRED_SERIES} per call",
        )
    bad = [s for s in ids if not _FRED_ID.fullmatch(s)]
    if bad:
        return _error_result(
            FRED_SOURCE, provider,
            f"not a FRED series id (letters, digits and '_', up to 40): {', '.join(bad)}",
        )
    try:
        start_d = _start(start)
    except ValueError as exc:
        return _error_result(FRED_SOURCE, provider, str(exc))

    run = _Run(store, FRED_SOURCE, start_d)
    gaps = [FRED_PUBLISHED_GAP, FRED_DATE_GAP]
    for sid in ids:
        began = time.monotonic()
        rows, err, meta = await liquidity._fred_rows(fetcher, sid)
        duration = time.monotonic() - began
        if rows is None:
            run.defer_fail(sid, err or f"FRED graph CSV for {sid} unavailable", duration)
            continue
        if meta.get("served_from_stale_cache"):
            run.defer_fail(sid, _refusal(f"FRED graph CSV for {sid}",
                                         meta.get("cache_age_seconds")), duration)
            continue
        in_window = [(d, v) for d, v in rows if start_d is None or d >= start_d]
        missing = sum(1 for _, v in in_window if v is None)
        valid = [(d.isoformat(), v) for d, v in in_window
                 if v is not None and math.isfinite(v)]
        dates = [d.isoformat() for d, _ in in_window]
        if not valid:
            run.defer_fail(sid, f"FRED graph CSV for {sid} has no values"
                           + (f" on or after {start_d}" if start_d else "")
                           + (f" ({missing} missing)" if missing else ""), duration)
            continue
        points, truncated, budget_limited = run.window(valid)
        if not points:
            run.defer_fail(sid, f"not loaded: this call's {MAX_POINTS_PER_CALL:,}-point budget "
                           "went to earlier series; backfill it in its own call", duration)
            continue
        catalog = liquidity.FRED_SERIES.get(sid)
        if catalog is None:
            gaps.append(
                f"{sid}: units unknown (stored NULL): the FRED graph CSV carries no units "
                "and the series is not in this server's FRED catalog; see its FRED page."
            )
        payload = {
            "provider": "fred",
            "series_id": sid,
            "start": run.start_text,
            "rows": [[d.isoformat(), v] for d, v in in_window],
        }
        run.defer_store(
            sid,
            payload,
            [{
                "key": sid,
                "label": f"FRED {sid}" + (f": {catalog['label']}" if catalog else ""),
                "unit": catalog["units"] if catalog else None,
                "points": points,
                "points_in_response": len(rows),
                "in_window": len(in_window),
                "missing_skipped": missing,
                "invalid_skipped": len(in_window) - missing - len(valid),
                "truncated": truncated,
                "budget_limited": budget_limited,
                "served": {d for d, _ in valid},
                "span": (min(dates), max(dates)) if dates else None,
            }],
            duration,
            _copy_at(fetcher, f"liquidity:fred:{sid}"),
        )
    try:
        pruned = run.commit(now)
    except sqlite3.Error as exc:
        return _store_failure(FRED_SOURCE, provider, exc, run)
    return run.result(provider, gaps, pruned)


async def backfill_ecb(
    fetcher: Fetcher,
    store: HistoryStore,
    keys: Any,
    start: Any = None,
    *,
    now: float | None = None,
) -> dict:
    """Load ECB Data Portal series into the store under ``backfill:ecb``.

    Each key is ``DATASET.KEY`` as the series key reads (for example
    ``BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.E``); a key with ``+`` alternatives
    loads every series it matches, at most ``MAX_ECB_COMBINATIONS``.
    """
    provider = "ECB Data Portal (data-api.ecb.europa.eu)"
    raw = _requests(keys)
    if not raw:
        return _error_result(ECB_SOURCE, provider, "name at least one ECB series key")
    if len(raw) > MAX_ECB_KEYS:
        return _error_result(
            ECB_SOURCE, provider,
            f"{len(raw)} keys requested; a backfill loads at most {MAX_ECB_KEYS} per call",
        )
    try:
        parsed = list(dict.fromkeys(_ecb_request(r) for r in raw))
        start_d = _start(start)
    except ValueError as exc:
        return _error_result(ECB_SOURCE, provider, str(exc))

    run = _Run(store, ECB_SOURCE, start_d)
    gaps = [ECB_PUBLISHED_GAP, ECB_DATE_GAP, ECB_MISSING_GAP]
    for dataset, key in parsed:
        request = f"{dataset}.{key}"
        fetch_d = None
        if start_d is not None:
            back = max(_ECB_LOOKBACK_DAYS.get(f, 0) for f in key.split(".")[0].split("+"))
            fetch_d = start_d - timedelta(days=back)
        began = time.monotonic()
        res = await liquidity.fetch_ecb_series(
            fetcher, dataset=dataset, key=key, limit=0,
            start=fetch_d.isoformat() if fetch_d else None,
        )
        duration = time.monotonic() - began
        if res.get("error"):
            run.defer_fail(request, str(res["error"]), duration)
            continue
        stale = [s for s in res["series"] if s.get("served_from_stale_cache")]
        if stale:
            run.defer_fail(request, _refusal(f"ECB Data Portal {request}",
                                             stale[0].get("cache_age_seconds")), duration)
            continue
        unmatched = _unmatched_codes(key, res["series"])
        if unmatched:
            gaps.append(
                f"{request}: '+' alternative(s) that matched no series: {', '.join(unmatched)}"
            )
        series = []
        for s in res["series"]:
            in_window: list[tuple[str | None, dict]] = []
            for p in reversed(s["points"]):  # oldest first
                observed = period_date(p.get("observed_at")) or normalize_observed_at(
                    p.get("period_end")
                )
                first_day = _first_day(observed)
                if start_d is not None and first_day is not None and first_day < start_d:
                    continue  # asked for one period early; before the window
                in_window.append((observed, p))
            valid: list[tuple[str, float]] = []
            seen: set[str] = set()
            for observed, p in in_window:
                value = p.get("value")
                if (observed is None or observed in seen or isinstance(value, bool)
                        or not isinstance(value, (int, float)) or not math.isfinite(value)):
                    continue
                seen.add(observed)
                valid.append((observed, float(value)))
            points, truncated, budget_limited = run.window(valid)
            dates = [o for o, _ in in_window if o is not None]
            series.append({
                "key": s["key"],
                "label": s.get("title") or s["key"],
                "unit": s.get("units"),
                "points": points,
                "points_in_response": len(s["points"]),
                "in_window": len(in_window),
                "missing_skipped": None,
                "invalid_skipped": len(in_window) - len(valid),
                "truncated": truncated,
                "budget_limited": budget_limited,
                "served": {o for o, _ in valid},
                "span": (min(dates), max(dates)) if dates else None,
                "provisional": sum(
                    1 for _, p in in_window
                    if "provisional" in str(p.get("status") or "").lower()
                ),
            })
        if not any(s["points"] for s in series):
            run.defer_fail(request, f"ECB Data Portal {request}: no values loaded"
                           + (" (this call's point budget went to earlier series)"
                              if run.budget <= 0 else ""), duration)
            continue
        payload = {
            "provider": "ecb",
            "request": request,
            "start": run.start_text,
            "series": [
                {
                    "key": s["key"], "title": s.get("title"), "units": s.get("units"),
                    "frequency": s.get("frequency"),
                    "points": [[p["observed_at"], p.get("period_end"), p["value"],
                                p.get("status")] for p in reversed(s["points"])],
                }
                for s in res["series"]
            ],
        }
        if any(s["provisional"] for s in series):
            gaps.append(
                f"{request}: {sum(s['provisional'] for s in series)} point(s) the ECB marks "
                "provisional; a later backfill stores their final values as revisions."
            )
        run.defer_store(
            request, payload, series, duration,
            _copy_at(fetcher, f"liquidity:ecb:{dataset}:{key}:0:{fetch_d}"),
        )
    try:
        pruned = run.commit(now)
    except sqlite3.Error as exc:
        return _store_failure(ECB_SOURCE, provider, exc, run)
    return run.result(provider, gaps, pruned)
