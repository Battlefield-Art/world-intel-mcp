"""News signals: hostility timeline, ranked situations, story breadth.

Three views over coverage the server already reads:

- ``fetch_hostility_timeline``: per UTC day, how much GDELT-indexed
  coverage names a country (or a pair of countries), how much of it uses
  threat / sanction / force / criticism vocabulary, and GDELT's average
  tone. Built on the GDELT DOC 2.0 ``timelinevolraw`` and ``timelinetone``
  modes, measured live 2026-10-17 (see ROADMAP Phase 29).
- ``fetch_situations``: country pairs ("between") and single countries
  ("inside") ranked over a recent window of the RSS feeds by coverage,
  hostile coverage, or change against the preceding window.
- ``fetch_top_stories``: story clusters carried by at least N distinct
  outlets, plus the countries and organizations whose coverage grew most.

Honesty contract: a failed fetch is a data gap, never a zero. A day GDELT
did not return is null, not a quiet day; a GDELT column that came back
only as an expired cached copy is null and named with its age, never shown
as current counts; a feed that cannot reach back across the comparison
window, or that came back as an expired cached copy, is excluded from
change figures and named. When no feed can be compared, change is null
and ``change_measured`` is false, never "no change".

Gaps / not covered:
- Hostility categories are keyword heuristics (GDELT's own keyword match
  for the timeline, ``classifier.classify_hostility`` over headlines for
  situations). Neither parses who acts on whom, negation or idiom.
- Country detection uses ``entities.extract_entities`` (the 22-country
  TIER1 table plus leader names); countries outside it (France, Egypt,
  Saudi Arabia, ...) are invisible to ``fetch_situations`` and to the
  attention ranking. Its whole-word match does not know compound names:
  it counts "South Sudan" as Sudan, "Indian Ocean" as India, "Latin
  American" as the United States and "New Mexico" as Mexico (checked
  2026-10-17). The hostility timeline searches any name verbatim and
  refuses unresolved 2- and 3-letter codes.
- RSS feeds keep different amounts of history; change figures use only
  feeds whose oldest retained item predates the prior window, so a feed
  that keeps ten items never fakes a surge, but it also never counts.
- Partial days are judged per GDELT column from the hours its buckets
  cover. GDELT sent explicit zero buckets on the one query measured
  (``Iran Israel``, 2026-10-17); if it omits them for a sparse query, quiet
  hours read as missing and the day is over-flagged as partial, never
  under-flagged. Tone is not judged: a missing hour narrows a day's
  average tone but does not shrink it the way it shrinks a count.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from itertools import combinations
from typing import Any

from ..config.countries import TIER1_COUNTRIES
from ..sources import news
from .classifier import HOSTILITY_CATEGORIES, HOSTILITY_METHOD, classify_hostility
from .clustering import _tokenize, cluster_articles
from .dossier import _ISO2_TO_ISO3
from .entities import extract_entities

logger = logging.getLogger("world-intel-mcp.analysis.news_signals")

# RSS reads take everything the feeds hold; windows are applied here.
_ALL_ITEMS = 1_000_000

# A published stamp this far past "now" is a publisher clock or timezone
# error; such items are excluded and counted, never placed in a window.
_FUTURE_TOLERANCE = timedelta(hours=1)

_SORTS = ("covered", "hostile", "change")

# Window bounds. Below an hour almost nothing lands in a window; past a
# week almost no feed keeps enough history to be comparable.
_MIN_HOURS = 1.0
_MAX_HOURS = 168.0


def _clamp_hours(hours: Any) -> float:
    return max(_MIN_HOURS, min(float(hours), _MAX_HOURS))


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _country_name(iso3: str) -> str:
    info = TIER1_COUNTRIES.get(iso3)
    return info["name"] if info else iso3


def _countries_of(ents: dict) -> set[str]:
    """ISO3 codes in extracted entities, directly or through a leader."""
    found = {c["iso3"] for c in ents["countries"]}
    for leader in ents["leaders"]:
        if leader.get("country") in TIER1_COUNTRIES:
            found.add(leader["country"])
    return found


def _headline_countries(title: str) -> set[str]:
    """ISO3 codes a headline names, directly or through a leader."""
    return _countries_of(extract_entities(title)["entities"])


class _Windows:
    """Current and prior RSS windows, plus which feeds can be compared.

    current = (now - hours, now]; prior = (now - 2*hours, now - hours].
    A feed is *comparable* when its oldest retained item is at or before
    the prior window's start: its history covers both windows, so a zero
    in the prior window is a real zero for that feed.
    """

    def __init__(self, feed: dict, hours: float, now: datetime):
        self.now = now
        self.hours = hours
        self.current_start = now - timedelta(hours=hours)
        self.prior_start = now - timedelta(hours=2 * hours)
        self.current: list[dict] = []
        self.prior: list[dict] = []
        self.undated = 0
        # Per feed: a feed whose items carry no date at all (Nikkei Asia,
        # measured 2026-10-17: 50 of 50) is absent from every window, and
        # only this count says so.
        self.undated_by_feed: dict[str, int] = defaultdict(int)
        self.future_dated = 0
        oldest: dict[str, datetime] = {}
        seen_links: set[str] = set()
        for item in feed.get("items", []):
            ts = _parse_ts(item.get("published"))
            name = item.get("feed_name") or ""
            if ts is None:
                self.undated += 1
                self.undated_by_feed[name or "(unnamed feed)"] += 1
                continue
            if ts > now + _FUTURE_TOLERANCE:
                self.future_dated += 1
                continue
            if name and (name not in oldest or ts < oldest[name]):
                oldest[name] = ts
            if ts <= self.prior_start:
                continue
            link = item.get("link") or ""
            if link:
                if link in seen_links:
                    continue
                seen_links.add(link)
            row = dict(item, _ts=ts)
            if ts > self.current_start:
                self.current.append(row)
            else:
                self.prior.append(row)
        # A feed served from an expired cached copy is missing its newest
        # items, so it would fake a drop: kept in the windows, never compared.
        self.stale_feeds = sorted(feed.get("feeds_stale") or [])
        self.comparable_feeds = {
            name
            for name, ts in oldest.items()
            if ts <= self.prior_start and name not in self.stale_feeds
        }
        self.not_comparable_feeds = sorted(
            set(oldest) - self.comparable_feeds - set(self.stale_feeds)
        )

    def describe(self) -> dict:
        return {
            "hours": self.hours,
            "current": {"start": _iso(self.current_start), "end": _iso(self.now)},
            "prior": {"start": _iso(self.prior_start), "end": _iso(self.current_start)},
        }

    def coverage(self, feed: dict) -> dict:
        comparable_current = sum(
            1 for r in self.current if r.get("feed_name") in self.comparable_feeds
        )
        return {
            "items_read": len(feed.get("items", [])),
            "items_current_window": len(self.current),
            # Change figures rest on this subset only; a small share means
            # the change ranking is a thin sample.
            "items_current_window_comparable": comparable_current,
            "items_prior_window": len(self.prior),
            "undated_items": self.undated,
            "future_dated_items": self.future_dated,
            "feeds_ok": feed.get("feeds_ok"),
            "feeds_failed": feed.get("feeds_failed", []),
            "feeds_comparable": len(self.comparable_feeds),
            "feeds_not_comparable": self.not_comparable_feeds,
            "feeds_stale": self.stale_feeds,
        }

    @property
    def change_measured(self) -> bool:
        """True when at least one feed covers both windows."""
        return bool(self.comparable_feeds)

    def change_gaps(self, what: str) -> list[str]:
        """Why ``what`` (change or attention) is unmeasured or thin."""
        if not self.change_measured:
            return [
                f"{what} not measured: no feed answered live with history back to "
                f"{_iso(self.prior_start)} (the prior window's start), so no feed "
                "covers both windows; change, change_ratio and prior are null, not zero"
            ]
        comparable = sum(1 for r in self.current if r.get("feed_name") in self.comparable_feeds)
        if comparable * 2 < len(self.current):
            return [
                f"{what} figures rest on {comparable} of {len(self.current)} "
                f"current-window items ({len(self.comparable_feeds)} comparable feeds); "
                "a thin sample"
            ]
        return []

    def gaps(self, feed: dict) -> list[str]:
        out: list[str] = []
        if feed.get("degraded") and not feed.get("error"):
            # fetch_news_feed marks a read where fewer than half of the
            # feeds answered live (reason partial_feeds_failed).
            out.append(
                f"only {feed.get('feeds_ok')} of {feed.get('feeds_attempted')} RSS feeds "
                "answered live; rankings rest on a minority of the feeds"
            )
        failed = feed.get("feeds_failed") or []
        if failed:
            out.append(
                f"{len(failed)} RSS feeds failed or timed out and are absent: "
                + ", ".join(failed)
            )
        if self.stale_feeds:
            out.append(
                f"{len(self.stale_feeds)} feeds failed live and were served from an "
                "expired cache copy; their items are kept but excluded from change "
                "figures: " + ", ".join(self.stale_feeds)
            )
        if self.not_comparable_feeds:
            out.append(
                f"{len(self.not_comparable_feeds)} feeds keep no history before "
                f"{_iso(self.prior_start)} and are excluded from change figures: "
                + ", ".join(self.not_comparable_feeds)
            )
        if self.undated:
            by_feed = ", ".join(
                f"{name} {n}" for name, n in sorted(self.undated_by_feed.items())
            )
            out.append(
                f"{self.undated} items had no parseable publish time and were "
                f"skipped (by feed: {by_feed})"
            )
        if self.future_dated:
            out.append(
                f"{self.future_dated} items were stamped more than 1 h in the future "
                "and were skipped"
            )
        return out


def _change_fields(current_comparable: int, prior: int) -> dict:
    return {
        "current_comparable": current_comparable,
        "prior": prior,
        "change": current_comparable - prior,
        "change_ratio": round((current_comparable + 1) / (prior + 1), 3),
    }


# With no comparable feed, a 0 - 0 change would read as "no change".
_UNMEASURED_CHANGE = {
    "current_comparable": None,
    "prior": None,
    "change": None,
    "change_ratio": None,
}


def _feed_marker(feed: dict) -> dict:
    """A partial RSS outage's degraded/reason, passed on with the ranking,
    plus the feed counts it rests on (the CLI's partial-outage line reads
    them)."""
    return {
        k: feed[k] for k in ("degraded", "reason", "feeds_attempted", "feeds_ok") if k in feed
    }


_CHANGE_DEFINITION = (
    "change = articles in the current window minus articles in the prior "
    "window of equal length, both counted only over comparable feeds (feeds "
    "whose oldest retained item is at or before the prior window's start). "
    "change_ratio = (current_comparable + 1) / (prior + 1). 'change' sort "
    "orders by change, then by current articles. With no comparable feed "
    "nothing is compared: change_measured (attention_change_measured for "
    "attention) is false, the change fields are null and attention is empty, "
    "and a 'change' sort ranks by coverage instead (sort_applied)."
)


def _degraded_feed_response(feed: dict, base: dict) -> dict:
    out = dict(base)
    out.update(
        {
            "error": feed.get("error", "RSS feed read failed"),
            "degraded": True,
            "reason": feed.get("reason", "rss_unavailable"),
            "data_gaps": [
                "every RSS feed failed: no coverage could be read, which is "
                "not the same as no coverage"
            ],
        }
    )
    return out


# ---------------------------------------------------------------------------
# Hostility timeline (GDELT DOC 2.0 timelines)
# ---------------------------------------------------------------------------

# GDELT word forms per category. GDELT matches the words given (this
# server has not measured any stemming), so inflections are spelled out.
GDELT_HOSTILITY_TERMS: dict[str, list[str]] = {
    "threat": [
        "threat", "threats", "threaten", "threatens", "threatened", "warns",
        "ultimatum", "retaliation", "retaliate",
    ],
    "sanction": [
        "sanction", "sanctions", "sanctioned", "embargo", "blacklist",
        "tariff", "tariffs", '"export controls"',
    ],
    "force": [
        "airstrike", "airstrikes", "missile", "missiles", '"drone strike"',
        "attack", "attacks", "attacked", "bombing", "shelling", "invasion",
        "troops", "clashes", "artillery",
    ],
    "criticism": [
        "condemns", "condemned", "condemnation", "criticizes", "criticized",
        "criticises", "criticised", "denounces", "denounced", "rebuke",
        "accuses", "accused",
    ],
}

# GDELT asks for at most one request per 5 s (its 429 body, measured
# 2026-10-17). The fetcher enforces that floor for every live source="gdelt"
# request, across the processes that share one cache DB
# (fetcher._SOURCE_RATE_LIMITS, shared_floor; cache hits are not delayed),
# and backs off a 429 by at least the floor. This is the extra pause before
# each call of the one retry pass over failed calls.
_GDELT_RETRY_WAIT_S = 10.0

# The DOC API serves roughly the last three months.
_MAX_DAYS = 90

# GDELT ``date_resolution`` values seen live 2026-10-17: "15m" (timespan=3d)
# and "hour" (7d). A day's completeness is judged by the hours its buckets
# cover, not by bucket count: in 15m mode GDELT returned 79 of 96
# quarter-hours for 2026-10-16 yet covered all 24 hours, with the same
# daily total (732) as hourly mode. Other resolutions are unmeasured.
_SUBDAILY_RESOLUTIONS = {"15m", "hour"}

# The five volume columns, in response order; "tone" is the sixth call.
_VOLUME_COLUMNS = ("articles", *GDELT_HOSTILITY_TERMS)

# How a column's GDELT call ended, worst first: "failed" (nothing usable),
# "stale" (the live call failed and the fetcher served an expired cached
# copy, which is excluded), "ok" (a live answer or a cache entry within
# its TTL). A failed retry never replaces a better earlier outcome.
_STATUS_RANK = {"failed": 0, "stale": 1, "ok": 2}


def _gdelt_term(name: str) -> str:
    return f'"{name}"' if (" " in name or "-" in name) else name


def _resolve_country(value: str) -> tuple[str, str | None]:
    """(search name, iso3 or None) for a country given as a name or code.

    Resolves the 22-country table's ISO3 codes, the ISO2 codes ``dossier``
    maps onto it (IR, UA, ...), its names and its keywords.
    """
    raw = (value or "").strip()
    upper = raw.upper()
    iso3 = upper if upper in TIER1_COUNTRIES else _ISO2_TO_ISO3.get(upper)
    if iso3 in TIER1_COUNTRIES:
        return TIER1_COUNTRIES[iso3]["name"], iso3
    lower = raw.lower()
    for code, info in TIER1_COUNTRIES.items():
        if lower == info["name"].lower() or lower in (k.lower() for k in info["keywords"]):
            return info["name"], code
    return raw, None


def _looks_like_code(value: str) -> bool:
    """A 2- or 3-letter input: searched verbatim, GDELT would count the
    letters ("GBR", "fra"), not the country, and reject 2-letter terms."""
    raw = (value or "").strip()
    return raw.isascii() and raw.isalpha() and 2 <= len(raw) <= 3


async def _gdelt_timeline(fetcher, query: str, mode: str, days: int) -> dict | None:
    """One GDELT timeline call (the fetcher spaces live GDELT calls).

    When the live call fails the fetcher may return an expired cached copy,
    marked ``_stale`` with ``_stale_age_seconds``; ``_call_outcome`` reads it.
    """
    digest = hashlib.sha1(f"{mode}|{days}|{query}".encode()).hexdigest()[:16]
    cache_key = f"news_signals:gdelt:{mode}:{days}:{digest}"
    data = await fetcher.get_json(
        news._GDELT_DOC_URL,
        source="gdelt",
        cache_key=cache_key,
        cache_ttl=1800,
        params={"query": query, "mode": mode, "timespan": f"{days}d", "format": "json"},
        timeout=news.GDELT_TIMEOUT,
    )
    if not isinstance(data, dict) or not isinstance(data.get("timeline"), list):
        return None
    return data


def _call_outcome(data: dict | None) -> tuple[str, float | None]:
    """(status, age in seconds of the expired copy when status is "stale")."""
    if data is None:
        return "failed", None
    if data.get("_stale"):
        age = data.get("_stale_age_seconds")
        return "stale", float(age) if isinstance(age, (int, float)) else None
    return "ok", None


def _age_text(age: float | None) -> str:
    return f"{age / 3600:.1f} h old" if age is not None else "age unknown"


def _series_by_day(data: dict) -> tuple[dict[str, list[dict]], str | None]:
    """Group a timeline's first series into UTC days (YYYY-MM-DD)."""
    resolution = (data.get("query_details") or {}).get("date_resolution")
    days: dict[str, list[dict]] = defaultdict(list)
    series = data["timeline"][0]["data"] if data["timeline"] else []
    for point in series:
        stamp = str(point.get("date", ""))
        if len(stamp) < 8 or not isinstance(point.get("value"), (int, float)):
            continue
        days[f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}"].append(point)
    return days, resolution


def _hours_of(points: list[dict]) -> set[str]:
    """The UTC hours ("00".."23") a day's buckets fall in."""
    return {str(p["date"])[9:11] for p in points if len(str(p["date"])) >= 11}


async def fetch_hostility_timeline(
    fetcher,
    country: str,
    other: str | None = None,
    days: int = 14,
    now: datetime | None = None,
) -> dict:
    """Daily coverage and hostile-coverage counts for a country or pair.

    Six GDELT calls: matching-article volume, one volume per hostility
    category (the base query AND that category's terms), and average
    tone. A call that fails, or that only an expired cached copy answers,
    leaves its column null for every day; ``column_status`` and
    ``data_gaps`` name it (with the copy's age). A day GDELT did not return
    is null in every column, and a day one series lacks is null in that
    column. None of these is ever reported as zero. An unresolved 2- or
    3-letter input is refused before any call: it reads as a country code.
    """
    now = now or datetime.now(timezone.utc)
    days = max(1, min(int(days), _MAX_DAYS))
    parties = []
    for value in [country] + ([other] if other else []):
        name, iso3 = _resolve_country(value)
        parties.append({"input": value, "name": name, "iso3": iso3})

    def _refused(error: str) -> dict:
        return {
            "parties": parties,
            "days_requested": days,
            "days": [],
            "error": error,
            "source": "gdelt-doc-2.0",
            "timestamp": _iso(now),
        }

    # An empty or one-letter input would send GDELT empty or one-letter
    # queries (nine requests with the retry pass) and could open its breaker.
    short = [p["input"] for p in parties if len("".join(str(p["input"] or "").split())) < 2]
    if short:
        return _refused(
            "; ".join(f"country {c!r} is too short to search" for c in short)
            + "; pass a country name, or an ISO3 or ISO2 code from the "
            f"{len(TIER1_COUNTRIES)}-country table"
        )
    codes = [p["input"] for p in parties if p["iso3"] is None and _looks_like_code(p["input"])]
    if codes:
        return _refused(
            "; ".join(
                f"'{c}' looks like a country code but is not in the "
                f"{len(TIER1_COUNTRIES)}-country table"
                for c in codes
            )
            + "; pass the country's name instead (names are searched verbatim)"
        )

    base_query = " ".join(_gdelt_term(p["name"]) for p in parties)
    queries = {"articles": base_query}
    for cat, terms in GDELT_HOSTILITY_TERMS.items():
        queries[cat] = f"{base_query} ({' OR '.join(terms)})"

    data_gaps: list[str] = []
    for p in parties:
        if p["iso3"] is None:
            data_gaps.append(
                f"'{p['input']}' is not in the country table; searched GDELT for it verbatim"
            )

    jobs = [(col, q, "timelinevolraw") for col, q in queries.items()]
    jobs.append(("tone", base_query, "timelinetone"))
    fetched: dict[str, dict] = {}
    status: dict[str, str] = {}
    stale_age: dict[str, float | None] = {}

    def _record(col: str, data: dict | None) -> None:
        outcome, age = _call_outcome(data)
        if col in status and _STATUS_RANK[outcome] < _STATUS_RANK[status[col]]:
            return
        status[col] = outcome
        if outcome == "ok":
            fetched[col] = data
            stale_age.pop(col, None)
        elif outcome == "stale":
            stale_age[col] = age

    for col, query, mode in jobs:
        _record(col, await _gdelt_timeline(fetcher, query, mode, days))
    # One spaced retry pass over failed and expired columns: measured
    # 2026-10-17, about half of GDELT calls drew a 429 even 30 s apart, and
    # most succeeded on retry. A retry that fails again means GDELT is still
    # refusing, so the pass stops there rather than queue more requests.
    pending = [job for job in jobs if status[job[0]] != "ok"]
    for i, (col, query, mode) in enumerate(pending):
        rest = [c for c, _q, _m in pending[i:]]
        if not fetcher.breaker.is_available("gdelt"):
            data_gaps.append(
                "retry pass skipped: the GDELT circuit breaker is open after repeated "
                "failures; not retried: " + ", ".join(rest)
            )
            break
        await asyncio.sleep(_GDELT_RETRY_WAIT_S)
        _record(col, await _gdelt_timeline(fetcher, query, mode, days))
        if status[col] != "ok":
            if rest[1:]:
                data_gaps.append(
                    f"retry pass stopped after {col} failed again; not retried: "
                    + ", ".join(rest[1:])
                )
            break

    for col, _query, mode in jobs:
        kind = "tone" if mode == "timelinetone" else "volume"
        if status[col] == "stale":
            data_gaps.append(
                f"{col}: GDELT {kind} timeline call failed and only an expired cached copy "
                f"({_age_text(stale_age[col])}) was available; column is null, not shown "
                "as current counts"
            )
        elif status[col] == "failed":
            data_gaps.append(f"{col}: GDELT {kind} timeline fetch failed; column is null")

    by_series: dict[str, dict[str, list[dict]]] = {}
    resolution: str | None = None
    for col, _query, _mode in jobs:
        if col in fetched:
            series, res = _series_by_day(fetched[col])
            by_series[col] = series
            resolution = resolution or res
    tone_days = by_series.pop("tone", None)
    any_stale = "stale" in status.values()

    base_out = {
        "parties": parties,
        "days_requested": days,
        "queries": dict(queries, tone=base_query),
        "column_status": {col: status[col] for col, _q, _m in jobs},
        "stale_age_seconds": {
            col: stale_age[col] for col, _q, _m in jobs if status[col] == "stale"
        },
        "source": "gdelt-doc-2.0",
        "timestamp": _iso(now),
    }
    if not by_series and tone_days is None:
        out = dict(base_out)
        if any_stale:
            n_stale = sum(1 for s in status.values() if s == "stale")
            error = (
                f"Every GDELT timeline call failed; {n_stale} of {len(jobs)} columns came "
                "back only as an expired cached copy, which is never shown as current counts"
            )
        else:
            error = "Every GDELT timeline call failed (rate limited, API error, or malformed response)"
        out.update(
            {
                "days": [],
                "error": error,
                "degraded": True,
                "reason": "gdelt_stale" if any_stale else "gdelt_fetch_failed",
                "data_gaps": data_gaps,
            }
        )
        return out

    subdaily = resolution in _SUBDAILY_RESOLUTIONS
    if not subdaily:
        data_gaps.append(
            f"GDELT date_resolution {resolution!r} is not one this server has measured; "
            "partial-day detection is off"
        )
    volume_cols = [c for c in _VOLUME_COLUMNS if c in by_series]
    # The series that gives a row's buckets_returned and hours_covered: the
    # base series, or the first category series that answered when the base
    # call failed. partial itself is judged per column.
    judge_col = volume_cols[0] if volume_cols else None
    if subdaily and judge_col is None:
        data_gaps.append("no volume timeline answered, so partial days are not flagged")
    elif subdaily and judge_col != "articles":
        data_gaps.append(
            f"buckets_returned and hours_covered are judged from the {judge_col} series "
            "(the base articles call failed); partial is judged per column "
            "(coverage_by_column)"
        )
    all_series = dict(by_series)
    if tone_days is not None:
        all_series["tone"] = tone_days

    today = now.date()
    rows = []
    missing_days: list[str] = []
    col_missing: dict[str, list[str]] = defaultdict(list)
    for offset in range(days - 1, -1, -1):
        day = (today - timedelta(days=offset)).isoformat()
        points = {c: series.get(day) or [] for c, series in all_series.items()}
        returned = [c for c, pts in points.items() if pts]
        if not returned:
            missing_days.append(day)
        else:
            for c in all_series:
                if not points[c]:
                    col_missing[c].append(day)
        # Today's UTC day has not ended: its totals are not comparable with
        # a full day's, whatever GDELT returned for the hours so far.
        row: dict[str, Any] = {"date": day, "in_progress": day == today.isoformat()}
        row["buckets_returned"] = len(points[judge_col]) if judge_col else None
        hours = {c: _hours_of(points[c]) for c in volume_cols}
        if subdaily and judge_col is not None:
            # A column is partial on a day when some elapsed hour has no
            # bucket at all, or the day has none (missing data, never a
            # quiet hour: GDELT sends zero buckets). Today counts only wholly
            # elapsed hours: GDELT's newest bucket lags the clock.
            elapsed = {f"{h:02d}" for h in range(now.hour if row["in_progress"] else 24)}
            coverage: dict[str, dict] = {}
            partial_columns: list[str] = []
            hours_missing: set[str] = set()
            for c in volume_cols:
                gone = elapsed - hours[c]
                col_partial = not points[c] or bool(gone)
                coverage[c] = {
                    "buckets_returned": len(points[c]),
                    "hours_covered": len(hours[c]),
                    "partial": col_partial,
                }
                if col_partial:
                    partial_columns.append(c)
                    hours_missing |= gone
            row["hours_covered"] = len(hours[judge_col])
            row["partial"] = bool(partial_columns)
            row["partial_columns"] = partial_columns
            if partial_columns:
                row["hours_missing"] = sorted(hours_missing)
            row["coverage_by_column"] = coverage
        base_points = points.get("articles")
        if base_points:
            row["articles"] = int(sum(p["value"] for p in base_points))
            norms = [p.get("norm") for p in base_points]
            row["all_monitored_articles"] = (
                int(sum(norms)) if all(isinstance(n, (int, float)) for n in norms) else None
            )
        else:
            row["articles"] = None
        hostile: dict[str, int | None] = {}
        for cat in GDELT_HOSTILITY_TERMS:
            # GDELT returns explicit zero buckets (measured 2026-10-17: a
            # category series carried the base series' exact buckets,
            # zeros included), so a day with no buckets is missing data.
            pts = points.get(cat)
            hostile[cat] = int(sum(p["value"] for p in pts)) if pts else None
        row["hostile"] = hostile
        if row["articles"]:
            # A count over hours the base count does not cover is not a share.
            row["hostile_share"] = {
                cat: (
                    round(v / row["articles"], 4)
                    if v is not None and not (subdaily and hours[cat] != hours["articles"])
                    else None
                )
                for cat, v in hostile.items()
            }
        row["tone"] = _day_tone(points.get("tone"), base_points)
        rows.append(row)

    if missing_days:
        data_gaps.append(
            f"GDELT returned no buckets for {len(missing_days)} of {days} requested days "
            f"({missing_days[0]} .. {missing_days[-1]}); those days are null, not zero"
        )
    for c in all_series:
        if col_missing.get(c):
            gone_days = col_missing[c]
            data_gaps.append(
                f"{c}: GDELT returned no buckets on {len(gone_days)} of {days} requested "
                f"days that other series covered ({gone_days[0]} .. {gone_days[-1]}); "
                "null there, not zero"
            )

    out = dict(base_out)
    out.update(
        {
            "days": rows,
            "date_resolution": resolution,
            "method": (
                "GDELT DOC 2.0 timelinevolraw: 'articles' counts articles GDELT "
                "matched to the base query per bucket, summed to UTC days; each "
                "hostile column counts articles matching the base query AND any of "
                "that category's terms (GDELT_HOSTILITY_TERMS, listed in 'queries'). "
                "This is GDELT's keyword match over the text it indexes, not a "
                "headline classifier and not a model: it cannot tell who threatens "
                "whom, negation, or quotation, and one article can count in several "
                "categories. 'all_monitored_articles' is GDELT's per-bucket total "
                "('norm'). tone is GDELT timelinetone average tone, weighted by "
                "the day's per-bucket article counts when those are available. "
                "Unmeasured: the bucket resolution GDELT uses for spans over 7 "
                "days, and whether it holds buckets before 2026-10-15T10Z (a 7-day "
                "call returned none earlier). Every probe of either drew HTTP 429 "
                "on 2026-10-17 (timespan=14d at 16:42Z, 16:51:16Z and 16:51:55Z; "
                "Oct 11 to 13 by start and end date at 16:52:15Z and 16:52:53Z); "
                "30 days was not probed."
            ),
            "definitions": {
                "partial": (
                    "a volume column (articles or a hostility category) has no bucket "
                    "for at least one elapsed hour of that UTC day, or none at all (for "
                    "today, only wholly elapsed hours count). Judged per column: "
                    "partial_columns names them, coverage_by_column gives each column's "
                    "buckets and hours, and hours_missing is the union of their missing "
                    "hours. buckets_returned and hours_covered come from the base "
                    "series, or the first category series that answered when the base "
                    "call failed. Tone is not judged. Absent when it cannot be judged"
                ),
                "in_progress": (
                    "the row is today's UTC day, which has not ended; its totals "
                    "are not comparable with a full day's"
                ),
                "hostile_share": (
                    "a category's count over the day's articles; null when either is "
                    "null or, with hourly or finer buckets, when the two series cover "
                    "different hours of the day"
                ),
                "column_status": (
                    "per GDELT call: ok (a live answer or a cache entry within its "
                    "30-minute TTL), stale (the live call failed and only an expired "
                    "cached copy was available: the column is null, and "
                    "stale_age_seconds gives the copy's age), failed (nothing usable: "
                    "the column is null)"
                ),
                "null": "not measured (call failed, expired copy, or day not returned), never zero",
            },
            "data_gaps": data_gaps,
        }
    )
    if any(s != "ok" for s in status.values()):
        out["degraded"] = True
        out["reason"] = "gdelt_stale" if any_stale else "gdelt_partial"
    return out


def _day_tone(tone_points: list[dict] | None, base_points: list[dict] | None) -> float | None:
    if not tone_points:
        return None
    weights = {p["date"]: p["value"] for p in (base_points or [])}
    if weights:
        num = sum(p["value"] * weights.get(p["date"], 0) for p in tone_points)
        den = sum(weights.get(p["date"], 0) for p in tone_points)
        if den > 0:
            return round(num / den, 4)
    return round(sum(p["value"] for p in tone_points) / len(tone_points), 4)


# ---------------------------------------------------------------------------
# Situations
# ---------------------------------------------------------------------------


async def fetch_situations(
    fetcher,
    hours: float = 24,
    sort: str = "covered",
    kind: str = "all",
    limit: int = 20,
    min_articles: int = 2,
    now: datetime | None = None,
) -> dict:
    """Rank situations between countries and inside a country.

    A headline naming two or more countries counts toward each pair it
    names ("between"); a headline naming exactly one counts toward that
    country ("inside"). Hostile = the headline matches any hostility
    category. When no feed covers both windows, change fields are null
    and ``change_measured`` is false; a "change" sort then falls back to
    coverage and ``sort_applied`` says so.
    """
    now = now or datetime.now(timezone.utc)
    hours = _clamp_hours(hours)
    sort = sort if sort in _SORTS else "covered"
    kind = kind if kind in ("all", "between", "inside") else "all"
    limit = max(1, int(limit))
    min_articles = max(1, int(min_articles))
    base = {
        "situations": [],
        "count": 0,
        "sort": sort,
        "kind": kind,
        "limit": limit,
        "source": "rss-aggregator+news-signals",
        "timestamp": _iso(now),
    }
    feed = await news.fetch_news_feed(fetcher, limit=_ALL_ITEMS)
    # Every feed failed: nothing to rank. A partial outage (degraded, no
    # error) is ranked over the feeds that answered, and says so.
    if feed.get("error"):
        return _degraded_feed_response(feed, base)

    win = _Windows(feed, hours, now)
    measured = win.change_measured

    def _blank() -> dict:
        return {
            "articles": 0,
            "hostile": 0,
            "by_category": {c: 0 for c in HOSTILITY_CATEGORIES},
            "outlets": set(),
            "current_comparable": 0,
            "prior": 0,
            "headlines": [],
        }

    acc: dict[tuple[str, tuple[str, ...]], dict] = defaultdict(_blank)

    def _keys(countries: set[str]) -> list[tuple[str, tuple[str, ...]]]:
        if len(countries) >= 2:
            return [("between", pair) for pair in combinations(sorted(countries), 2)]
        if len(countries) == 1:
            return [("inside", (next(iter(countries)),))]
        return []

    for row in win.current:
        countries = _headline_countries(row.get("title") or "")
        keys = _keys(countries)
        if not keys:
            continue
        hostility = classify_hostility(row.get("title") or "")
        comparable = row.get("feed_name") in win.comparable_feeds
        for key in keys:
            s = acc[key]
            s["articles"] += 1
            if comparable:
                s["current_comparable"] += 1
            if hostility:
                s["hostile"] += 1
                for cat in hostility:
                    s["by_category"][cat] += 1
            if row.get("feed_name"):
                s["outlets"].add(news.outlet_for(row["feed_name"]))
            if len(s["headlines"]) < 3:
                s["headlines"].append(
                    {
                        "title": row.get("title", ""),
                        "outlet": row.get("feed_name", ""),
                        "link": row.get("link", ""),
                        "published": row.get("published"),
                        "hostility": sorted(hostility),
                    }
                )

    for row in win.prior:
        if row.get("feed_name") not in win.comparable_feeds:
            continue
        for key in _keys(_headline_countries(row.get("title") or "")):
            acc[key]["prior"] += 1

    situations = []
    for (skind, codes), s in acc.items():
        if kind != "all" and skind != kind:
            continue
        if s["articles"] < min_articles:
            continue
        situations.append(
            {
                "kind": skind,
                "key": "-".join(codes),
                "countries": [{"iso3": c, "name": _country_name(c)} for c in codes],
                "articles": s["articles"],
                "hostile_articles": s["hostile"],
                "hostile_share": round(s["hostile"] / s["articles"], 3),
                "by_category": s["by_category"],
                "outlet_count": len(s["outlets"]),
                **(
                    _change_fields(s["current_comparable"], s["prior"])
                    if measured
                    else _UNMEASURED_CHANGE
                ),
                "change_measured": measured,
                "sample_headlines": s["headlines"],
            }
        )

    sort_applied = sort if (sort != "change" or measured) else "covered"
    if sort_applied == "hostile":
        situations.sort(key=lambda x: (x["hostile_articles"], x["hostile_share"], x["articles"]), reverse=True)
    elif sort_applied == "change":
        situations.sort(key=lambda x: (x["change"], x["articles"]), reverse=True)
    else:
        situations.sort(key=lambda x: (x["articles"], x["hostile_articles"]), reverse=True)
    situations = situations[:limit]

    data_gaps = win.gaps(feed) + win.change_gaps("change")
    if sort_applied != sort:
        data_gaps.append(
            "sort=change was asked for but change was not measured; ranked by coverage instead"
        )
    out = dict(base)
    out.update(
        {
            "situations": situations,
            "count": len(situations),
            "sort_applied": sort_applied,
            "change_measured": measured,
            "window": win.describe(),
            "min_articles": min_articles,
            "definitions": {
                "between": "a headline naming two or more countries counts once toward each pair it names",
                "inside": (
                    "a headline naming exactly one country; it may describe that "
                    "country acting abroad against a party the headline does not name"
                ),
                "covered": "articles in the current window",
                "hostile": (
                    "headlines matching any hostility category; sort orders by "
                    "hostile_articles, then hostile_share"
                ),
                "change": _CHANGE_DEFINITION,
            },
            "method": (
                "RSS headlines from every configured feed; countries from "
                "entities.extract_entities (country names, demonyms, capitals, "
                "leader names mapped to their country) over its 22-country table, "
                "so countries outside it never appear; a link seen twice counts "
                "once. Hostility: " + HOSTILITY_METHOD
            ),
            "coverage": win.coverage(feed),
            **_feed_marker(feed),
            "data_gaps": data_gaps,
        }
    )
    return out


# ---------------------------------------------------------------------------
# Top stories and attention
# ---------------------------------------------------------------------------


def _attention_counts(rows: list[dict], comparable: set[str] | None) -> dict:
    counts: dict[tuple[str, str], int] = defaultdict(int)
    for row in rows:
        if comparable is not None and row.get("feed_name") not in comparable:
            continue
        ents = extract_entities(row.get("title") or "")["entities"]
        for iso3 in _countries_of(ents):
            counts[("country", iso3)] += 1
        for o in ents["organizations"]:
            counts[("organization", o["name"])] += 1
    return counts


async def fetch_top_stories(
    fetcher,
    hours: float = 24,
    min_outlets: int = 3,
    limit: int = 10,
    attention_limit: int = 10,
    threshold: float = 0.25,
    now: datetime | None = None,
) -> dict:
    """Stories carried by at least ``min_outlets`` distinct outlets.

    Clusters the current window's headlines (Jaccard, as in
    ``intel_news_clusters``), counts distinct publishers per cluster
    (sister feeds count once), and ranks by outlet count, then article
    count. ``attention`` ranks named countries and organizations by
    growth in mentions against the prior window; when no feed covers both
    windows it is empty and ``attention_change_measured`` is false.
    """
    now = now or datetime.now(timezone.utc)
    hours = _clamp_hours(hours)
    min_outlets = max(1, int(min_outlets))
    limit = max(1, int(limit))
    attention_limit = max(1, int(attention_limit))
    base = {
        "stories": [],
        "count": 0,
        "attention": [],
        "min_outlets": min_outlets,
        "limit": limit,
        "attention_limit": attention_limit,
        "source": "rss-aggregator+news-signals",
        "timestamp": _iso(now),
    }
    feed = await news.fetch_news_feed(fetcher, limit=_ALL_ITEMS)
    # As in fetch_situations: only a total outage stops the ranking.
    if feed.get("error"):
        return _degraded_feed_response(feed, base)

    win = _Windows(feed, hours, now)
    rows = win.current
    # Clustering compares headline pairs: seconds of CPU over a 7 d window.
    # In a worker thread it leaves the caller's event loop serving other
    # requests, and a caller's timeout can fire while it runs.
    raw = await asyncio.to_thread(cluster_articles, rows, similarity_threshold=threshold)

    stories = []
    for c in raw:
        members = [rows[i] for i in c["member_indices"]]
        outlets = sorted(
            {news.outlet_for(m["feed_name"]) for m in members if m.get("feed_name")}
        )
        if len(outlets) < min_outlets:
            continue
        freq: dict[str, int] = defaultdict(int)
        for m in members:
            for w in _tokenize(m.get("title") or ""):
                freq[w] += 1
        stamps = [m["_ts"] for m in members]
        stories.append(
            {
                "headline": c["representative"].get("title", ""),
                "outlet_count": len(outlets),
                "outlets": outlets,
                "articles": len(members),
                "keywords": sorted(freq, key=lambda w: (-freq[w], w))[:8],
                "first_seen": _iso(min(stamps)),
                "last_seen": _iso(max(stamps)),
                "items": [
                    {
                        "title": m.get("title", ""),
                        "outlet": m.get("feed_name", ""),
                        "link": m.get("link", ""),
                        "published": m.get("published"),
                    }
                    for m in members[:5]
                ],
            }
        )
    stories.sort(key=lambda s: (s["outlet_count"], s["articles"], s["last_seen"]), reverse=True)
    stories = stories[:limit]

    # With no comparable feed every growth would be 0 - 0: the list stays
    # empty and attention_change_measured says why, never "nothing grew".
    attention = []
    if win.change_measured:
        cur = _attention_counts(win.current, win.comparable_feeds)
        prior = _attention_counts(win.prior, win.comparable_feeds)
        for key in set(cur) | set(prior):
            etype, ident = key
            entry: dict[str, Any] = {"type": etype, "name": ident}
            if etype == "country":
                entry = {"type": etype, "name": _country_name(ident), "iso3": ident}
            entry.update(_change_fields(cur.get(key, 0), prior.get(key, 0)))
            attention.append(entry)
        attention = [a for a in attention if a["change"] > 0]
        attention.sort(key=lambda a: (a["change"], a["current_comparable"]), reverse=True)
        attention = attention[:attention_limit]

    out = dict(base)
    out.update(
        {
            "stories": stories,
            "count": len(stories),
            "attention": attention,
            "attention_change_measured": win.change_measured,
            "window": win.describe(),
            "definitions": {
                "outlet": (
                    "a publisher; sister feeds of one publisher (news.FEED_OUTLETS, "
                    "e.g. BBC World and BBC Mundo) count once"
                ),
                "story": (
                    f"a cluster of current-window headlines with Jaccard title "
                    f"similarity >= {threshold} to its first member"
                ),
                "attention": (
                    "countries (named directly or through a leader) and "
                    "organizations named in headlines, ranked by growth: "
                    + _CHANGE_DEFINITION + " Only entities that grew are listed."
                ),
            },
            "method": (
                "RSS headlines in the current window, clustered by title token "
                "overlap; entity names from entities.extract_entities. Clustering "
                "is lexical: one event told in different words can split into "
                "several stories, and headlines in different languages never merge."
            ),
            "coverage": win.coverage(feed),
            **_feed_marker(feed),
            "data_gaps": win.gaps(feed) + win.change_gaps("attention"),
        }
    )
    return out
