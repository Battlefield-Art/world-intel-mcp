"""News vs market reaction: same-session moves of what the news names.

For recent story clusters (the RSS feed of ``sources/news.py`` grouped by
``analysis/clustering.cluster_articles``), the companies and countries
their headlines name (``analysis/entities.extract_entities``) are mapped
to a listing: a company to the ticker in the entity table, confirmed
against SEC's ``company_tickers.json`` for US listings; a country to its
primary index (``config/exchanges.COUNTRY_INDEX_TICKERS``, the table
``intel_country_stocks`` uses). The move on the story's market session is
then read from Yahoo v8 daily bars.

Company rows: the stock's close-to-close return on the assigned session,
the benchmark's return over the same two dates (SPY for US listings, the
exchange country's primary index otherwise) and the excess return
(simple difference, not beta-adjusted). Country rows: the index's return
on the session and a z-score against its own preceding session returns.

Session assignment: the story time is the earliest ``published`` among
the cluster's articles, converted to the listing's exchange timezone.
Before that exchange's regular close on a trading day -> that session;
at or after the close -> the next session; on a day with no session
(weekend or holiday, read from the listing's own daily bars) -> the next
session. A session that has not closed yet is ``pending``, never a move.

This describes co-movement on the same session, not causation, and is
not a trading signal. A failed quote is a data gap, never a 0% move.

Gaps / not covered:
- One close time of day per listing, from Yahoo's current trading period,
  is applied to every day: past early-close days count as full days, and
  on an early-close day its shortened close is applied to every day.
- Excess return is a simple difference, not beta- or sector-adjusted.
- Admitting an ambiguous company name by its feed category is a stated
  heuristic; a corroborating term in the headline is the stronger signal.
- A weekday missing from a country index's bars is read as a holiday;
  for companies a day the benchmark traded but the stock did not is a
  data gap instead, but country rows have no such cross-check.
- Story times are only as good as the feeds' timestamps and their parsing
  in ``sources/news.py``; one later than the collection time is reported
  as a data gap, but a wrong time in the past cannot be detected here.
- Headlines only (titles, not summaries) are matched; the entity table
  covers a curated set of companies, not every listed name.
"""

from __future__ import annotations

import asyncio
import html
import logging
import re
import statistics
from datetime import date, datetime, time, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..config.exchanges import COUNTRY_INDEX_TICKERS
from ..fetcher import Fetcher
from ..sources import news
from ..sources.markets import _YAHOO_CHART_URL
from ..sources.sec_edgar import _SEC_HEADERS, _TICKERS_URL
from . import clustering, entities

logger = logging.getLogger("world-intel-mcp.analysis.news_market")

US_BENCHMARK = "SPY"
BARS_RANGE = "3mo"
BARS_TTL = 900
ZSCORE_WINDOW = 20
ZSCORE_MIN = 10
# A story stamped more than this far past the collection time has a bad
# feed timestamp, not a scoop.
FUTURE_TOLERANCE = timedelta(minutes=5)

# Yahoo exchange suffix -> ISO-3 of the exchange's country, so a foreign
# listing is benchmarked against the index intel_country_stocks maps.
SUFFIX_COUNTRY: dict[str, str] = {
    "L": "GBR", "DE": "DEU", "F": "DEU", "AS": "NLD", "SW": "CHE",
    "MI": "ITA", "MC": "ESP", "ST": "SWE", "SR": "SAU", "KS": "KOR",
    "KQ": "KOR", "T": "JPN", "HK": "HKG", "SS": "CHN", "SZ": "CHN",
    "TW": "TWN", "NS": "IND", "BO": "IND", "TO": "CAN", "AX": "AUS",
    "SA": "BRA", "JO": "ZAF", "MX": "MEX", "SI": "SGP", "TA": "ISR",
}

# Entity-table company keywords that are also ordinary words or name more
# than one listed company: (reason, corroborating terms, feed categories).
# Each is kept only when the story's headlines carry a corroborating term
# or one of its articles comes from a feed category where the company
# sense dominates (and the name is not only hyphen-joined); otherwise it
# is excluded with the reason.
AMBIGUOUS_COMPANIES: dict[str, tuple[str, tuple[str, ...], tuple[str, ...]]] = {
    "apple": (
        "'apple' is also the fruit",
        ("iphone", "ipad", "macbook", "app store", "tim cook", "apple inc", "airpods", "ios"),
        ("technology", "finance"),
    ),
    "amazon": (
        "'amazon' is also the river and rainforest",
        ("aws", "amazon web services", "bezos", "jassy", "amazon.com", "alexa", "prime video"),
        ("technology", "finance"),
    ),
    "meta": (
        "'meta' is also a common word and prefix",
        ("facebook", "instagram", "whatsapp", "zuckerberg", "meta platforms"),
        ("technology", "finance"),
    ),
    "shell": (
        "'shell' is also an artillery round, a casing and a shell company",
        ("shell plc", "royal dutch shell", "shell oil", "sawan"),
        ("energy",),
    ),
    "bp": (
        # Finance feeds are not context here: they quote basis points.
        "'bp' is also basis points and blood pressure",
        ("bp plc", "british petroleum", "bp's"),
        ("energy",),
    ),
    "alphabet": (
        "'alphabet' is also a common word",
        ("google", "waymo", "pichai", "alphabet inc"),
        ("technology", "finance"),
    ),
    "chevron": (
        "'chevron' is also an insignia and a US legal doctrine",
        ("chevron corp", "chevron's", "oil", "refinery", "crude", "wirth"),
        ("energy",),
    ),
    "samsung": (
        "'samsung' names several separately listed companies",
        ("samsung electronics", "galaxy", "exynos", "hbm", "chip", "semiconductor"),
        (),
    ),
}

# A corroborating term may carry a plural "s" ("foldable iPhones").
_CORROBORATOR_RE: dict[str, re.Pattern[str]] = {
    kw: re.compile(r"\b(?:" + "|".join(re.escape(t) for t in terms) + r")s?\b")
    for kw, (_reason, terms, _cats) in AMBIGUOUS_COMPANIES.items()
}

METHOD = {
    "story_time": (
        "Earliest `published` timestamp among the cluster's articles (UTC, as "
        "the RSS feeds state it)."
    ),
    "session_rule": (
        "The story time is converted to the listing's exchange timezone "
        "(Yahoo exchangeTimezoneName). Published before that exchange's "
        "regular close on a trading day: that day's session. Published at or "
        "after the close: the next session. Published on a day with no session "
        "(weekend or holiday, read from the listing's own daily bars): the next "
        "session. The close time of day is Yahoo's currentTradingPeriod.regular.end "
        "applied to every day, so early-close days are not modelled."
    ),
    "return": (
        "Close-to-close: adjusted close of the assigned session over the adjusted "
        "close of the prior session in the listing's own daily bars, minus 1. A "
        "story that landed mid-session shares the window with trading before it."
    ),
    "benchmark": (
        f"US listings (no exchange suffix): {US_BENCHMARK}. Other listings: the "
        "primary index of the exchange's country in the table intel_country_stocks "
        "uses. The benchmark is measured over the same two dates; excess = stock "
        "return - benchmark return, a simple difference, not beta-adjusted."
    ),
    "countries": (
        "Country rows measure the country's primary index on the assigned session, "
        f"with a z-score against the index's previous up-to-{ZSCORE_WINDOW} session "
        f"returns (null with fewer than {ZSCORE_MIN})."
    ),
    "entities": (
        "Companies and countries named in the cluster's headlines (titles only) by "
        "analysis/entities.py. Company names that are also common words "
        "(apple, amazon, meta, shell, bp, alphabet, chevron, samsung) are excluded "
        "unless the headlines carry a corroborating term, or an article comes from a "
        "feed category where the company sense dominates and the name stands on its "
        "own (a hyphen-joined use such as 'meta-analysis' does not count); each kept "
        "one says which."
    ),
}

INTERPRETATION = (
    "Same-session co-movement only. A move on the story's session does not show "
    "that the story caused it: other news, sector moves and market-wide events "
    "share the session. This is not a trading signal."
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Story time
# ---------------------------------------------------------------------------


def parse_published(value: Any) -> datetime | None:
    """Parse a feed item's ``published`` (ISO 8601 or RFC 822) to aware UTC."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError):
            return None
    if dt.tzinfo is None:
        # A naive timestamp has no stated zone; reading it as UTC would be
        # a guess, so the story time is unknown.
        return None
    return dt.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Daily bars
# ---------------------------------------------------------------------------


class Bars:
    """One listing's daily bars keyed by session date in its exchange zone."""

    def __init__(self, symbol: str, tz: ZoneInfo, close_time: time, closes: dict[date, float]):
        self.symbol = symbol
        self.tz = tz
        self.close_time = close_time
        self.closes = closes
        self.dates = sorted(closes)

    @property
    def tz_name(self) -> str:
        return self.tz.key


def parse_bars(symbol: str, data: Any) -> tuple[Bars | None, str | None]:
    """Build ``Bars`` from a Yahoo v8 chart payload, or say why not."""
    if not isinstance(data, dict):
        return None, f"{symbol}: Yahoo chart returned no data (request failed or symbol unknown)"
    chart = data.get("chart") or {}
    results = chart.get("result")
    if not results:
        err = chart.get("error") or {}
        detail = err.get("description") or err.get("code") or "empty result"
        return None, f"{symbol}: Yahoo chart error ({detail})"
    try:
        result = results[0]
        meta = result.get("meta") or {}
        tz = ZoneInfo(meta["exchangeTimezoneName"])
        end = meta["currentTradingPeriod"]["regular"]["end"]
        close_time = datetime.fromtimestamp(end, tz).time()
        stamps = result.get("timestamp") or []
        indicators = result.get("indicators") or {}
        adj = (indicators.get("adjclose") or [{}])[0].get("adjclose")
        raw = (indicators.get("quote") or [{}])[0].get("close")
    except (KeyError, IndexError, TypeError, AttributeError, ZoneInfoNotFoundError) as exc:
        return None, f"{symbol}: unexpected Yahoo chart structure ({type(exc).__name__}: {exc})"
    values = adj if isinstance(adj, list) and len(adj) == len(stamps) else raw
    if not isinstance(values, list) or len(values) != len(stamps):
        return None, f"{symbol}: Yahoo chart has no close series matching its timestamps"
    closes: dict[date, float] = {}
    for ts, value in zip(stamps, values):
        # A null close is a missing bar, never a zero price.
        if ts is None or value is None:
            continue
        # A later bar on the same local date (Yahoo's live bar) replaces it.
        closes[datetime.fromtimestamp(ts, tz).date()] = float(value)
    if not closes:
        return None, f"{symbol}: Yahoo chart has no usable closes"
    return Bars(symbol, tz, close_time, closes), None


async def fetch_bars(fetcher: Fetcher, symbol: str) -> tuple[Bars | None, str | None]:
    data = await fetcher.get_json(
        _YAHOO_CHART_URL.format(symbol=symbol),
        source="yahoo-finance",
        cache_key=f"news_market:bars:{symbol}",
        cache_ttl=BARS_TTL,
        params={"range": BARS_RANGE, "interval": "1d"},
        yahoo_rate_limit=True,
    )
    return parse_bars(symbol, data)


# ---------------------------------------------------------------------------
# Session assignment and returns
# ---------------------------------------------------------------------------


def assign_session(story_utc: datetime, bars: Bars, now_utc: datetime) -> dict:
    """Map a story time to the listing's session under the stated rule.

    Returns a dict with ``status`` ("assigned" or "pending"), and when
    assigned: ``session_date``, ``basis``, ``published_local``.
    """
    local = story_utc.astimezone(bars.tz)
    after_close = local.time() >= bars.close_time
    candidate = local.date() + timedelta(days=1) if after_close else local.date()
    out: dict[str, Any] = {
        "published_local": local.isoformat(),
        "after_close": after_close,
        "candidate_date": candidate.isoformat(),
    }
    if candidate < bars.dates[0]:
        out["status"] = "out_of_range"
        out["reason"] = (
            f"story session {candidate.isoformat()} predates the first {bars.symbol} "
            f"bar in range ({bars.dates[0].isoformat()})"
        )
        return out
    session = next((d for d in bars.dates if d >= candidate), None)
    if session is None:
        out["status"] = "pending"
        out["reason"] = (
            f"no {bars.symbol} session on or after {candidate.isoformat()} yet "
            f"(last bar {bars.dates[-1].isoformat()})"
        )
        return out
    now_local = now_utc.astimezone(bars.tz)
    if session == now_local.date() and now_local.time() < bars.close_time:
        out["status"] = "pending"
        out["session_date"] = session.isoformat()
        out["reason"] = f"the {session.isoformat()} session has not closed yet; its move is not final"
        return out
    if session > candidate:
        basis = "next_session_non_trading_day"
    elif after_close:
        basis = "next_session_after_close"
    else:
        basis = "same_session"
    out.update({"status": "assigned", "session_date": session.isoformat(), "basis": basis})
    return out


# assign_session status -> row status when no session was assigned.
_UNASSIGNED = {"pending": "pending", "out_of_range": "data_gap"}


def session_return(bars: Bars, session: date) -> tuple[date | None, float | None]:
    """Close-to-close return of ``session`` against the prior bar."""
    idx = bars.dates.index(session)
    if idx == 0:
        return None, None
    prior = bars.dates[idx - 1]
    return prior, bars.closes[session] / bars.closes[prior] - 1.0


def window_return(bars: Bars, start: date, end: date) -> float | None:
    """Return over two exact dates, or None when either bar is missing."""
    if start not in bars.closes or end not in bars.closes:
        return None
    return bars.closes[end] / bars.closes[start] - 1.0


def trailing_zscore(bars: Bars, session: date, value: float) -> tuple[float | None, int]:
    """Z-score of ``value`` against the series' preceding session returns."""
    idx = bars.dates.index(session)
    rets = [
        bars.closes[bars.dates[i]] / bars.closes[bars.dates[i - 1]] - 1.0
        for i in range(max(1, idx - ZSCORE_WINDOW), idx)
    ]
    if len(rets) < ZSCORE_MIN:
        return None, len(rets)
    sd = statistics.stdev(rets)
    if sd == 0:
        return None, len(rets)
    return round((value - statistics.fmean(rets)) / sd, 2), len(rets)


def _pct(x: float) -> float:
    return round(x * 100.0, 4)


def benchmark_for(symbol: str) -> tuple[str | None, str | None]:
    """Benchmark symbol for a listing, or the reason there is none."""
    if "." not in symbol:
        return US_BENCHMARK, None
    suffix = symbol.rsplit(".", 1)[1].upper()
    iso3 = SUFFIX_COUNTRY.get(suffix)
    if iso3 is None:
        return None, f"no benchmark mapped for exchange suffix .{suffix}"
    index = COUNTRY_INDEX_TICKERS.get(iso3)
    if index is None:
        return None, f"no primary index mapped for {iso3} (exchange suffix .{suffix})"
    return index, None


def measure_company(story_utc: datetime, bars: Bars, bench: Bars, now_utc: datetime) -> dict:
    """Stock, benchmark and excess return on the story's session."""
    if bench.tz_name != bars.tz_name:
        return {
            "status": "data_gap",
            "gap": (
                f"benchmark {bench.symbol} trades in {bench.tz_name}, "
                f"{bars.symbol} in {bars.tz_name}; session dates do not align"
            ),
        }
    sess = assign_session(story_utc, bars, now_utc)
    out: dict[str, Any] = {k: sess[k] for k in ("published_local", "session_date") if k in sess}
    if sess["status"] != "assigned":
        out.update({"status": _UNASSIGNED[sess["status"]], "gap": sess["reason"]})
        return out
    session = date.fromisoformat(sess["session_date"])
    candidate = date.fromisoformat(sess["candidate_date"])
    # A day the benchmark traded but the stock series lacks is missing
    # data in the stock series, not a holiday: the session would be wrong.
    skipped = [d for d in bench.dates if candidate <= d < session]
    if skipped:
        out.update({
            "status": "data_gap",
            "gap": (
                f"{bench.symbol} traded on {skipped[0].isoformat()} but the {bars.symbol} "
                "series has no bar for it; the session cannot be assigned"
            ),
        })
        return out
    out["session_basis"] = sess["basis"]
    prior, stock_ret = session_return(bars, session)
    if prior is None or stock_ret is None:
        out.update({"status": "data_gap", "gap": f"no {bars.symbol} bar before {session.isoformat()} in range"})
        return out
    out["prior_session_date"] = prior.isoformat()
    bench_ret = window_return(bench, prior, session)
    if bench_ret is None:
        out.update({
            "status": "data_gap",
            "stock_return_pct": _pct(stock_ret),
            "gap": (
                f"benchmark {bench.symbol} has no bar for {prior.isoformat()} or "
                f"{session.isoformat()}"
            ),
        })
        return out
    out.update({
        "status": "measured",
        "stock_return_pct": _pct(stock_ret),
        "benchmark_return_pct": _pct(bench_ret),
        "excess_return_pct": _pct(stock_ret - bench_ret),
    })
    return out


def measure_country(story_utc: datetime, bars: Bars, now_utc: datetime) -> dict:
    """Index return on the story's session and its trailing z-score."""
    sess = assign_session(story_utc, bars, now_utc)
    out: dict[str, Any] = {k: sess[k] for k in ("published_local", "session_date") if k in sess}
    if sess["status"] != "assigned":
        out.update({"status": _UNASSIGNED[sess["status"]], "gap": sess["reason"]})
        return out
    session = date.fromisoformat(sess["session_date"])
    out["session_basis"] = sess["basis"]
    prior, ret = session_return(bars, session)
    if prior is None or ret is None:
        out.update({"status": "data_gap", "gap": f"no {bars.symbol} bar before {session.isoformat()} in range"})
        return out
    z, n = trailing_zscore(bars, session, ret)
    out.update({
        "status": "measured",
        "prior_session_date": prior.isoformat(),
        "index_return_pct": _pct(ret),
        "zscore": z,
        "trailing_sessions": n,
    })
    return out


# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------


def _a(phrase: str) -> str:
    """Phrase with its indefinite article ("an energy", "a technology")."""
    return ("an " if phrase[:1] in "aeiou" else "a ") + phrase


def classify_companies(
    text: str, categories: frozenset[str] | set[str] = frozenset()
) -> tuple[list[dict], list[dict], list[dict]]:
    """Split the companies a text names into (mapped, unmapped, excluded).

    Uses ``entities.extract_entities`` and keeps one row per ticker.
    ``categories`` are the feed categories of the story's articles.
    """
    lowered = text.lower()
    mapped: list[dict] = []
    unmapped: list[dict] = []
    excluded: list[dict] = []
    seen: set[str] = set()
    for comp in entities.extract_entities(text)["entities"]["companies"]:
        keyword = str(comp.get("name", "")).lower()
        entity: dict[str, Any] = {"type": "company", "name": comp.get("name"), "sector": comp.get("sector")}
        ambiguous = AMBIGUOUS_COMPANIES.get(keyword)
        if ambiguous:
            reason, terms, context = ambiguous
            term = _CORROBORATOR_RE[keyword].search(lowered)
            feed_cats = sorted(set(context) & set(categories))
            # "meta-analysis", "apple-picking": a hyphen-joined use is the
            # common word, so a feed category alone does not admit it.
            standalone = any(
                m.group(1) is None
                for m in re.finditer(r"\b" + re.escape(keyword) + r"\b(-)?", lowered)
            )
            if term:
                entity["admitted_by"] = f"corroborating term '{term.group(0)}'"
            elif feed_cats and standalone:
                entity["admitted_by"] = f"article from {_a(feed_cats[0])} feed"
            else:
                if feed_cats:
                    where = (
                        f"; the {feed_cats[0]} feed does not count because '{keyword}' "
                        "only appears hyphen-joined"
                    )
                elif context:
                    where = f" and no article from {_a(' or '.join(context))} feed"
                else:
                    where = ""
                excluded.append({
                    "entity": entity,
                    "reason": (
                        f"{reason}; no corroborating term ({', '.join(terms)}) in the "
                        f"headlines{where}"
                    ),
                })
                continue
        ticker = comp.get("ticker")
        if not ticker:
            unmapped.append({
                "entity": entity,
                "reason": "no public listing in the entity table (private or unlisted)",
            })
            continue
        if ticker in seen:
            continue
        seen.add(ticker)
        mapped.append({**entity, "ticker": ticker})
    return mapped, unmapped, excluded


def classify_countries(text: str) -> tuple[list[dict], list[dict]]:
    """Split the countries a text names into (mapped to an index, unmapped)."""
    mapped: list[dict] = []
    unmapped: list[dict] = []
    for c in entities.extract_entities(text)["entities"]["countries"]:
        entity = {"type": "country", "iso3": c["iso3"], "name": c["name"]}
        index = COUNTRY_INDEX_TICKERS.get(c["iso3"])
        if index is None:
            unmapped.append({
                "entity": entity,
                "reason": (
                    f"no primary index mapped for {c['iso3']} in the table "
                    "intel_country_stocks uses (it covers mega and major exchanges)"
                ),
            })
            continue
        mapped.append({**entity, "index": index})
    return mapped, unmapped


# ---------------------------------------------------------------------------
# SEC registrant confirmation
# ---------------------------------------------------------------------------


async def _sec_registrants(fetcher: Fetcher) -> dict[str, dict] | None:
    """Ticker -> {cik, name} from SEC's company_tickers.json, or None."""
    data = await fetcher.get_json(
        _TICKERS_URL,
        source="sec-edgar",
        cache_key="sec:company_tickers",
        cache_ttl=86400,
        headers=_SEC_HEADERS,
    )
    if not isinstance(data, dict):
        return None
    out: dict[str, dict] = {}
    for entry in data.values():
        if not isinstance(entry, dict) or not entry.get("ticker"):
            continue
        try:
            cik = str(int(entry["cik_str"])).zfill(10)
        except (KeyError, TypeError, ValueError):
            continue
        out.setdefault(str(entry["ticker"]).upper(), {"cik": cik, "name": entry.get("title", "")})
    return out or None


# ---------------------------------------------------------------------------
# Composite
# ---------------------------------------------------------------------------


def _clean_title(title: Any) -> str:
    # Feeds leave HTML entities in titles ("Apple&#8217;s"); curly
    # apostrophes become straight so "BP’s" matches "bp's".
    return html.unescape(str(title or "")).replace("’", "'")


def _story(items: list[dict], indices: list[int]) -> dict:
    members = [items[i] for i in indices]
    stamps = [t for t in (parse_published(m.get("published")) for m in members) if t is not None]
    first = min(stamps) if stamps else None
    rep = members[0]
    return {
        "headline": _clean_title(rep.get("title")),
        "link": rep.get("link", ""),
        "articles": len(members),
        "outlets": len({m.get("feed_name") for m in members if m.get("feed_name")}),
        "first_published": first.strftime("%Y-%m-%dT%H:%M:%SZ") if first else None,
        "_time": first,
        "_text": " ".join(_clean_title(m.get("title")) for m in members),
        "_categories": frozenset(m["category"] for m in members if m.get("category")),
    }


def _public(story: dict) -> dict:
    return {k: v for k, v in story.items() if not k.startswith("_")}


async def fetch_news_market_reaction(
    fetcher: Fetcher,
    category: str | None = None,
    limit: int = 150,
    threshold: float = 0.25,
    min_cluster_size: int = 2,
    include_countries: bool = True,
    max_symbols: int = 40,
    now: datetime | None = None,
) -> dict:
    """Same-session market moves of the companies and countries in the news."""
    now_utc = (now or _utc_now()).astimezone(timezone.utc)
    limit = max(1, min(int(limit), 300))
    min_cluster_size = max(1, int(min_cluster_size))
    max_symbols = max(1, min(int(max_symbols), 60))

    feed = await news.fetch_news_feed(fetcher, category=category, limit=limit)
    base = {
        "category": category,
        "method": dict(METHOD),
        "interpretation": INTERPRETATION,
        "source": "rss-aggregator + yahoo-finance + sec-edgar",
        "timestamp": now_utc.isoformat(),
    }
    if feed.get("error"):
        return {**base, "error": feed["error"]}
    items = feed.get("items") or []
    if not items:
        return {**base, "error": "news feed returned no items; nothing to measure (feeds down or empty)"}

    clusters = [
        c for c in clustering.cluster_articles(items, similarity_threshold=threshold)
        if c["member_count"] >= min_cluster_size
    ]

    data_gaps: list[str] = []
    unmapped: list[dict] = []
    excluded: list[dict] = []
    # (story, entity, symbol, kind)
    pending_rows: list[tuple[dict, dict, str, str]] = []
    stories_with_entities = 0
    for c in clusters:
        story = _story(items, c["member_indices"])
        companies, c_unmapped, c_excluded = classify_companies(story["_text"], story["_categories"])
        countries, k_unmapped = classify_countries(story["_text"]) if include_countries else ([], [])
        if companies or countries or c_unmapped or c_excluded or k_unmapped:
            stories_with_entities += 1
        for row in c_unmapped + k_unmapped:
            unmapped.append({"story": story["headline"], **row})
        for row in c_excluded:
            excluded.append({"story": story["headline"], **row})
        for comp in companies:
            pending_rows.append((story, comp, comp["ticker"], "company"))
        for ctry in countries:
            pending_rows.append((story, ctry, ctry["index"], "country"))

    # Symbols in first-seen order (largest clusters first), benchmarks
    # alongside their stocks, capped so one call stays bounded.
    wanted: list[str] = []
    bench_of: dict[str, tuple[str | None, str | None]] = {}
    for _story_, _entity, symbol, kind in pending_rows:
        needed = [symbol]
        if kind == "company":
            bench_of[symbol] = benchmark_for(symbol)
            if bench_of[symbol][0]:
                needed.append(bench_of[symbol][0])
        for s in needed:
            if s not in wanted:
                wanted.append(s)
    fetch_list = wanted[:max_symbols]
    over_budget = set(wanted[max_symbols:])
    if over_budget:
        data_gaps.append(
            f"symbol budget {max_symbols} reached; not fetched: {', '.join(sorted(over_budget))}"
        )

    fetched = await asyncio.gather(*(fetch_bars(fetcher, s) for s in fetch_list))
    bars: dict[str, Bars] = {}
    for symbol, (b, err) in zip(fetch_list, fetched):
        if b is not None:
            bars[symbol] = b
        elif err:
            data_gaps.append(err)

    registrants: dict[str, dict] | None = None
    us_tickers = [s for s, (bench, _r) in bench_of.items() if bench == US_BENCHMARK]
    if us_tickers:
        registrants = await _sec_registrants(fetcher)
        if registrants is None:
            data_gaps.append("SEC company_tickers.json unavailable; US registrants not confirmed")

    company_rows: list[dict] = []
    country_rows: list[dict] = []
    for story, entity, symbol, kind in pending_rows:
        row: dict[str, Any] = {"story": _public(story), "entity": entity, "symbol": symbol}
        if kind == "company":
            bench_sym, bench_reason = bench_of[symbol]
            row["benchmark"] = bench_sym
            if bench_sym == US_BENCHMARK and registrants is not None:
                row["registrant"] = registrants.get(symbol.upper())
                if row["registrant"] is None:
                    row["registrant_note"] = "ticker not found in SEC company_tickers.json"
        b = bars.get(symbol)
        if story["_time"] is None:
            row.update({"status": "data_gap", "gap": "no parseable published time in the cluster"})
        elif story["_time"] > now_utc + FUTURE_TOLERANCE:
            row.update({
                "status": "data_gap",
                "gap": (
                    f"story timestamp {story['first_published']} is later than the collection "
                    f"time {now_utc.strftime('%Y-%m-%dT%H:%M:%SZ')} (a timestamp error in the "
                    "feed or in its parsing)"
                ),
            })
        elif b is None:
            row.update({
                "status": "data_gap",
                "gap": f"no daily bars for {symbol}" + (" (symbol budget)" if symbol in over_budget else ""),
            })
        elif kind == "country":
            row["exchange_timezone"] = b.tz_name
            row.update(measure_country(story["_time"], b, now_utc))
        else:
            row["exchange_timezone"] = b.tz_name
            if bench_sym is None:
                row.update({"status": "data_gap", "gap": bench_reason})
            elif bench_sym not in bars:
                row.update({
                    "status": "data_gap",
                    "gap": f"no daily bars for benchmark {bench_sym}"
                    + (" (symbol budget)" if bench_sym in over_budget else ""),
                })
            else:
                row.update(measure_company(story["_time"], b, bars[bench_sym], now_utc))
        (company_rows if kind == "company" else country_rows).append(row)

    def _count(rows: list[dict], status: str) -> int:
        return sum(1 for r in rows if r.get("status") == status)

    return {
        **base,
        "company_reactions": company_rows,
        "country_reactions": country_rows,
        "unmapped": unmapped,
        "excluded": excluded,
        "summary": {
            "news_items": len(items),
            "stories_considered": len(clusters),
            "stories_with_entities": stories_with_entities,
            "min_cluster_size": min_cluster_size,
            "companies_measured": _count(company_rows, "measured"),
            "companies_pending": _count(company_rows, "pending"),
            "companies_data_gap": _count(company_rows, "data_gap"),
            "countries_measured": _count(country_rows, "measured"),
            "countries_pending": _count(country_rows, "pending"),
            "countries_data_gap": _count(country_rows, "data_gap"),
            "unmapped": len(unmapped),
            "excluded": len(excluded),
            "symbols_fetched": len(bars),
        },
        "data_gaps": data_gaps,
    }
