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
did not return is null, not a quiet day; a feed that cannot reach back
across the comparison window, or that came back as an expired cached
copy, is excluded from change figures and named.

Gaps / not covered:
- Hostility categories are keyword heuristics (GDELT's own keyword match
  for the timeline, ``classifier.classify_hostility`` over headlines for
  situations). Neither parses who acts on whom, negation or idiom.
- Country detection uses ``entities.extract_entities`` (the 22-country
  TIER1 table plus leader names); countries outside it (France, Egypt,
  Saudi Arabia, ...) are invisible to ``fetch_situations`` and to the
  attention ranking. The hostility timeline searches any name verbatim.
- RSS feeds keep different amounts of history; change figures use only
  feeds whose oldest retained item predates the prior window, so a feed
  that keeps ten items never fakes a surge, but it also never counts.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from itertools import combinations
from typing import Any

from ..config.countries import TIER1_COUNTRIES
from ..sources import news
from .classifier import HOSTILITY_CATEGORIES, HOSTILITY_METHOD, classify_hostility
from .clustering import _tokenize, cluster_articles
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

    def gaps(self, feed: dict) -> list[str]:
        out: list[str] = []
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


_CHANGE_DEFINITION = (
    "change = articles in the current window minus articles in the prior "
    "window of equal length, both counted only over comparable feeds (feeds "
    "whose oldest retained item is at or before the prior window's start). "
    "change_ratio = (current_comparable + 1) / (prior + 1). 'change' sort "
    "orders by change, then by current articles."
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

# GDELT asks for at most one request per 5 s per client (its 429 body,
# measured 2026-10-17). The shared Fetcher has no GDELT floor, so this
# module spaces its own live calls; cache hits are not delayed.
_GDELT_SPACING_S = 5.5
_GDELT_RETRY_WAIT_S = 10.0
_gdelt_last_call = 0.0

# The DOC API serves roughly the last three months.
_MAX_DAYS = 90

# GDELT ``date_resolution`` values seen live 2026-10-17: "15m" (timespan=3d)
# and "hour" (7d). A day's completeness is judged by the hours its buckets
# cover, not by bucket count: in 15m mode GDELT returned 79 of 96
# quarter-hours for 2026-10-16 yet covered all 24 hours, with the same
# daily total (732) as hourly mode. Other resolutions are unmeasured.
_SUBDAILY_RESOLUTIONS = {"15m", "hour"}


def _gdelt_term(name: str) -> str:
    return f'"{name}"' if (" " in name or "-" in name) else name


def _resolve_country(value: str) -> tuple[str, str | None]:
    """(search name, iso3 or None) for a country given as ISO3 or name."""
    raw = (value or "").strip()
    if raw.upper() in TIER1_COUNTRIES:
        iso3 = raw.upper()
        return TIER1_COUNTRIES[iso3]["name"], iso3
    lower = raw.lower()
    for iso3, info in TIER1_COUNTRIES.items():
        if lower == info["name"].lower() or lower in (k.lower() for k in info["keywords"]):
            return info["name"], iso3
    return raw, None


async def _gdelt_timeline(fetcher, query: str, mode: str, days: int) -> dict | None:
    """One GDELT timeline call, spaced from this module's previous one."""
    global _gdelt_last_call
    digest = hashlib.sha1(f"{mode}|{days}|{query}".encode()).hexdigest()[:16]
    cache_key = f"news_signals:gdelt:{mode}:{days}:{digest}"
    live_call = fetcher.cache.get(cache_key) is None and fetcher.breaker.is_available("gdelt")
    if live_call:
        wait = _GDELT_SPACING_S - (time.monotonic() - _gdelt_last_call)
        if wait > 0:
            await asyncio.sleep(wait)
        _gdelt_last_call = time.monotonic()
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
    tone. A call that fails leaves its column null for every day and is
    listed in ``data_gaps``; a day GDELT did not return is null in every
    column. Neither is ever reported as zero.
    """
    now = now or datetime.now(timezone.utc)
    days = max(1, min(int(days), _MAX_DAYS))
    name_a, iso_a = _resolve_country(country)
    parties = [{"input": country, "name": name_a, "iso3": iso_a}]
    base_query = _gdelt_term(name_a)
    if other:
        name_b, iso_b = _resolve_country(other)
        parties.append({"input": other, "name": name_b, "iso3": iso_b})
        base_query = f"{base_query} {_gdelt_term(name_b)}"

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
    for col, query, mode in jobs:
        data = await _gdelt_timeline(fetcher, query, mode, days)
        if data is not None:
            fetched[col] = data
    # One spaced retry for failed calls: measured 2026-10-17, about half of
    # GDELT calls drew a 429 even 30 s apart, and most succeeded on retry.
    for col, query, mode in jobs:
        if col in fetched or not fetcher.breaker.is_available("gdelt"):
            continue
        await asyncio.sleep(_GDELT_RETRY_WAIT_S)
        data = await _gdelt_timeline(fetcher, query, mode, days)
        if data is not None:
            fetched[col] = data

    by_series: dict[str, dict[str, list[dict]]] = {}
    resolution: str | None = None
    for col, _query, mode in jobs:
        if col not in fetched:
            kind = "tone" if mode == "timelinetone" else "volume"
            data_gaps.append(f"{col}: GDELT {kind} timeline fetch failed; column is null")
            continue
        series, res = _series_by_day(fetched[col])
        by_series[col] = series
        resolution = resolution or res
    tone_days = by_series.pop("tone", None)

    base_out = {
        "parties": parties,
        "days_requested": days,
        "queries": dict(queries, tone=base_query),
        "source": "gdelt-doc-2.0",
        "timestamp": _iso(now),
    }
    if not by_series and tone_days is None:
        out = dict(base_out)
        out.update(
            {
                "days": [],
                "error": "Every GDELT timeline call failed (rate limited, API error, or malformed response)",
                "degraded": True,
                "reason": "gdelt_fetch_failed",
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
    today = now.date()
    rows = []
    missing_days: list[str] = []
    for offset in range(days - 1, -1, -1):
        day = (today - timedelta(days=offset)).isoformat()
        base_points = by_series["articles"].get(day) if "articles" in by_series else None
        # Today's UTC day has not ended: its totals are not comparable with
        # a full day's, whatever GDELT returned for the hours so far.
        row: dict[str, Any] = {"date": day, "in_progress": day == today.isoformat()}
        returned = None
        if "articles" in by_series:
            returned = len(base_points or [])
        row["buckets_returned"] = returned
        if subdaily and returned is not None:
            # partial = some elapsed hour of the day has no bucket at all
            # (missing data). Today counts only wholly elapsed hours, so the
            # current one may be absent: GDELT's newest bucket lags the clock.
            hours = {
                str(p["date"])[9:11] for p in base_points or [] if len(str(p["date"])) >= 11
            }
            elapsed = now.hour if row["in_progress"] else 24
            missing_hours = sorted({f"{h:02d}" for h in range(elapsed)} - hours)
            row["hours_covered"] = len(hours)
            row["partial"] = bool(hours) and bool(missing_hours)
            if row["partial"]:
                row["hours_missing"] = missing_hours
        if "articles" in by_series:
            if not base_points:
                row["articles"] = None
                missing_days.append(day)
            else:
                row["articles"] = int(sum(p["value"] for p in base_points))
                norms = [p.get("norm") for p in base_points]
                row["all_monitored_articles"] = (
                    int(sum(norms)) if all(isinstance(n, (int, float)) for n in norms) else None
                )
        else:
            row["articles"] = None
        hostile: dict[str, int | None] = {}
        for cat in GDELT_HOSTILITY_TERMS:
            if cat not in by_series:
                hostile[cat] = None
                continue
            # GDELT returns explicit zero buckets (measured 2026-10-17: a
            # category series carried the base series' exact buckets,
            # zeros included), so a day with no buckets is missing data.
            pts = by_series[cat].get(day)
            hostile[cat] = int(sum(p["value"] for p in pts)) if pts else None
        row["hostile"] = hostile
        if row["articles"]:
            row["hostile_share"] = {
                cat: (round(v / row["articles"], 4) if v is not None else None)
                for cat, v in hostile.items()
            }
        row["tone"] = _day_tone(
            (tone_days or {}).get(day) if tone_days is not None else None,
            base_points,
        )
        rows.append(row)

    if missing_days:
        data_gaps.append(
            f"GDELT returned no buckets for {len(missing_days)} of {days} requested days "
            f"({missing_days[0]} .. {missing_days[-1]}); those days are null, not zero"
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
                "the day's per-bucket article counts when those are available."
            ),
            "definitions": {
                "partial": (
                    "GDELT returned no bucket for at least one elapsed hour of that "
                    "UTC day (for today, only wholly elapsed hours count); "
                    "hours_covered says how many hours had one"
                ),
                "in_progress": (
                    "the row is today's UTC day, which has not ended; its totals "
                    "are not comparable with a full day's"
                ),
                "null": "not measured (call failed or day not returned), never zero",
            },
            "data_gaps": data_gaps,
        }
    )
    if len(by_series) < len(queries) or tone_days is None:
        out["degraded"] = True
        out["reason"] = "gdelt_partial"
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
    category.
    """
    now = now or datetime.now(timezone.utc)
    hours = _clamp_hours(hours)
    sort = sort if sort in _SORTS else "covered"
    kind = kind if kind in ("all", "between", "inside") else "all"
    base = {
        "situations": [],
        "count": 0,
        "sort": sort,
        "kind": kind,
        "source": "rss-aggregator+news-signals",
        "timestamp": _iso(now),
    }
    feed = await news.fetch_news_feed(fetcher, limit=_ALL_ITEMS)
    if feed.get("degraded") or feed.get("error"):
        return _degraded_feed_response(feed, base)

    win = _Windows(feed, hours, now)

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
                **_change_fields(s["current_comparable"], s["prior"]),
                "sample_headlines": s["headlines"],
            }
        )

    if sort == "hostile":
        situations.sort(key=lambda x: (x["hostile_articles"], x["hostile_share"], x["articles"]), reverse=True)
    elif sort == "change":
        situations.sort(key=lambda x: (x["change"], x["articles"]), reverse=True)
    else:
        situations.sort(key=lambda x: (x["articles"], x["hostile_articles"]), reverse=True)
    situations = situations[:limit]

    out = dict(base)
    out.update(
        {
            "situations": situations,
            "count": len(situations),
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
            "data_gaps": win.gaps(feed),
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
    growth in mentions against the prior window.
    """
    now = now or datetime.now(timezone.utc)
    hours = _clamp_hours(hours)
    min_outlets = max(1, int(min_outlets))
    base = {
        "stories": [],
        "count": 0,
        "attention": [],
        "min_outlets": min_outlets,
        "source": "rss-aggregator+news-signals",
        "timestamp": _iso(now),
    }
    feed = await news.fetch_news_feed(fetcher, limit=_ALL_ITEMS)
    if feed.get("degraded") or feed.get("error"):
        return _degraded_feed_response(feed, base)

    win = _Windows(feed, hours, now)
    rows = win.current
    raw = cluster_articles(rows, similarity_threshold=threshold)

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

    cur = _attention_counts(win.current, win.comparable_feeds)
    prior = _attention_counts(win.prior, win.comparable_feeds)
    attention = []
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
            "data_gaps": win.gaps(feed),
        }
    )
    return out
