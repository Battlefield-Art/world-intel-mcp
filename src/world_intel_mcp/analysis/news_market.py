"""News vs market reaction: same-session moves of what the news names.

For recent story clusters (the RSS feed of ``sources/news.py`` grouped by
``analysis/clustering.cluster_articles``), the companies and countries
their headlines name (``analysis/entities.extract_entities``) are mapped
to a listing: a company to the ticker in the entity table (a US ticker is
annotated with its registrant when SEC's ``company_tickers.json`` lists
it); a country to its primary index (``config/exchanges.COUNTRY_INDEX_TICKERS``,
the table ``intel_country_stocks`` uses). The move on the story's market
session is then read from Yahoo v8 daily bars.

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
session. A session that has not closed yet, or whose last bar Yahoo has
not finalized (its regularMarketTime is before the close), is
``pending``, never a move.

This describes co-movement on the same session, not causation, and is
not a trading signal. A failed quote, or an expired cached chart served
because Yahoo failed, is a data gap, never a 0% move.

Gaps / not covered:
- One close time of day per listing, from Yahoo's current trading period,
  is applied to every day: past early-close days count as full days, and
  on an early-close day its shortened close is applied to every day. A
  data_gaps line says so when that close differs from the zone's standard
  close in ``STANDARD_CLOSE``; zones missing from that table get no check.
- A bar counts as final once Yahoo's regularMarketTime reaches the close.
  Last prints after the stated close are common (measured 2026-10-15/16:
  ^NSEI 1 minute, ^TWII 3, ^FTSE 5, 2222.SR 20, ^MXX 26, ^GDAXI and BA.L
  30 minutes after it), so a chart fetched in that window counts as final
  before its last print. A listing whose last trade came before the close
  stays pending until the next bar arrives.
- Yahoo's regular period for Seoul listings ends 15:00, but the closing
  print came at 15:30 (005930.KS, 2026-10-16): those rows use 15:00 and
  carry the nonstandard-close data_gaps line.
- Excess return is a simple difference, not beta- or sector-adjusted.
- Admitting an ambiguous company name by its feed is a stated heuristic,
  and the phrases that mark the other sense are a short list; a
  corroborating term in the headline is the stronger signal.
- A weekday missing from a country index's bars is read as a holiday;
  for companies a day the benchmark traded but the stock did not is a
  data gap instead, but country rows have no such cross-check.
- Story times are read from ``sources/news.py`` as it reports them; one
  later than the collection time is reported as a data gap, but a wrong
  time in the past cannot be detected here.
- Headlines only (titles, not summaries) are matched; the entity table
  covers a curated set of companies, not every listed name.
"""

from __future__ import annotations

import asyncio
import functools
import html
import logging
import re
import statistics
from datetime import date, datetime, time, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, NamedTuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..config.countries import TIER1_COUNTRIES
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
# Stories whose earliest article is older than this are left out; the
# upper limit is about the 3-month bar range.
MAX_AGE_HOURS = 168
MAX_AGE_LIMIT = 2160
# A Jaccard threshold of 0 would merge every pair of headlines.
THRESHOLD_MIN = 0.01

# Regular close on a full trading day per exchange timezone, measured on
# the 2026-10-16 sessions (2026-10-15 for Riyadh): Yahoo's
# currentTradingPeriod.regular.end for the zone's listings, except
# Asia/Seoul, where Yahoo's period ends 15:00 but the closing print of
# 005930.KS (regularMarketTime) came at 15:30:05. Only used to flag a
# close that differs. America/Mexico_City is left out: its hours may move
# with US daylight saving time, and only one season was measured.
STANDARD_CLOSE: dict[str, time] = {
    "America/New_York": time(16, 0),
    "Europe/London": time(16, 30),
    "Europe/Berlin": time(17, 30),
    "Asia/Riyadh": time(15, 0),
    "Asia/Seoul": time(15, 30),
    "Asia/Shanghai": time(15, 0),
    "Asia/Taipei": time(13, 30),
    "Asia/Kolkata": time(15, 30),
}

# Yahoo exchange suffix -> ISO-3 of the exchange's country, so a foreign
# listing is benchmarked against the index intel_country_stocks maps.
SUFFIX_COUNTRY: dict[str, str] = {
    "L": "GBR", "DE": "DEU", "F": "DEU", "AS": "NLD", "SW": "CHE",
    "MI": "ITA", "MC": "ESP", "ST": "SWE", "SR": "SAU", "KS": "KOR",
    "KQ": "KOR", "T": "JPN", "HK": "HKG", "SS": "CHN", "SZ": "CHN",
    "TW": "TWN", "NS": "IND", "BO": "IND", "TO": "CAN", "AX": "AUS",
    "SA": "BRA", "JO": "ZAF", "MX": "MEX", "SI": "SGP", "TA": "ISR",
}

# Finance feeds whose headlines are mostly company news; FT World,
# Bloomberg and Zero Hedge also carry world and macro stories.
_COMPANY_DESKS = ("CNBC", "MarketWatch", "WSJ Markets")


class Ambiguous(NamedTuple):
    """An entity-table company keyword that is also an ordinary word or
    names more than one listed company."""

    reason: str
    # Terms that settle the company sense when the headlines carry one.
    terms: tuple[str, ...]
    # Feed category -> the feeds in it where the company sense dominates
    # (None: every feed in the category).
    context: dict[str, tuple[str, ...] | None]
    # Phrases (regex) that mark the other sense; one excludes the name
    # whatever else admits it.
    other_sense: tuple[str, ...] = ()


# Each is kept only when the story's headlines carry a corroborating term
# or one of its articles comes from a context feed and the name is not
# only hyphen-joined, and no other-sense phrase appears; otherwise it is
# excluded with the reason.
AMBIGUOUS_COMPANIES: dict[str, Ambiguous] = {
    "apple": Ambiguous(
        "'apple' is also the fruit",
        ("iphone", "ipad", "macbook", "app store", "tim cook", "apple inc", "airpods", "ios"),
        {"technology": None, "finance": _COMPANY_DESKS},
        (r"big apple", r"apple hospitality",
         r"apple (?:orchards?|growers?|harvests?|cider|pies?|juice|trees?)"),
    ),
    "amazon": Ambiguous(
        "'amazon' is also the river and rainforest",
        ("aws", "amazon web services", "bezos", "jassy", "amazon.com", "alexa", "prime video"),
        {"technology": None, "finance": _COMPANY_DESKS},
        (r"pan-amazon\w*", r"(?:brazilian|peruvian|colombian|ecuadorian|bolivian) amazon",
         r"amazon (?:rainforest|basin|river|region|forests?|fires?)", r"rainforests?", r"deforestation"),
    ),
    "meta": Ambiguous(
        "'meta' is also a common word and prefix",
        ("facebook", "instagram", "whatsapp", "zuckerberg", "meta platforms"),
        {"technology": None, "finance": _COMPANY_DESKS},
        # "meta-analysis" is left to the hyphen rule, so a corroborating
        # term still admits "Meta trims ... after a meta-analysis".
        (r"meta analys[ie]s",),
    ),
    "shell": Ambiguous(
        "'shell' is also an artillery round, a casing and a shell company",
        ("shell plc", "royal dutch shell", "shell oil", "sawan"),
        {"energy": None},
        (r"shell (?:fire|hits?|strikes?|struck|lands?|landed|explodes|exploded"
         r"|compan(?:y|ies)|corporations?)", r"artillery", r"mortars?"),
    ),
    "bp": Ambiguous(
        # Finance feeds are not context here: they quote basis points.
        "'bp' is also basis points and blood pressure",
        ("bp plc", "british petroleum", "bp's"),
        {"energy": None},
        (r"\d+(?:\.\d+)? ?bps?", r"basis points?", r"blood pressure"),
    ),
    "alphabet": Ambiguous(
        "'alphabet' is also a common word",
        ("google", "waymo", "pichai", "alphabet inc"),
        {"technology": None, "finance": _COMPANY_DESKS},
        (r"alphabet soup", r"(?:phonetic|latin|cyrillic|greek|arabic|hebrew) alphabet"),
    ),
    "chevron": Ambiguous(
        "'chevron' is also an insignia and a US legal doctrine",
        # Not "oil", "crude", "refinery" or "chevron's": a headline on the
        # doctrine can carry each of them.
        ("chevron corp", "chevron corporation", "wirth", "hess"),
        # Only the oil and gas trade feeds: the energy category's policy
        # feeds can mean the doctrine.
        {"energy": ("Oil Price", "Rigzone")},
        (r"chevron (?:ruling|doctrine|deference|decision|precedent)", r"post-chevron",
         r"(?:overturn\w*|overrul\w*|scrapp\w*|demise of) chevron", r"loper bright"),
    ),
    "samsung": Ambiguous(
        "'samsung' names several separately listed companies",
        ("samsung electronics", "galaxy", "exynos", "hbm", "chip", "semiconductor"),
        {},
    ),
}

# A corroborating term may carry a plural "s" ("foldable iPhones").
_CORROBORATOR_RE: dict[str, re.Pattern[str]] = {
    kw: re.compile(r"\b(?:" + "|".join(re.escape(t) for t in amb.terms) + r")s?\b")
    for kw, amb in AMBIGUOUS_COMPANIES.items()
}
_OTHER_SENSE_RE: dict[str, re.Pattern[str]] = {
    kw: re.compile(r"\b(?:" + "|".join(amb.other_sense) + r")\b")
    for kw, amb in AMBIGUOUS_COMPANIES.items()
    if amb.other_sense
}

# A country index row needs the country's name: a demonym, capital or
# code also names airlines, oceans and food ("American Airlines",
# "Indian Ocean", "Tex-Mex"), and some longer names contain the
# country's name without meaning it.
COUNTRY_NAMES: dict[str, tuple[str, ...]] = {"USA": ("United States", "USA")}
NOT_THE_COUNTRY: dict[str, tuple[str, ...]] = {
    "USA": ("united states steel", "usa today", "team usa"),
    "CHN": ("south china sea", "east china sea"),
    "MEX": ("new mexico", "gulf of mexico", "mexico city"),
    "TWN": ("taiwan semiconductor",),
}

METHOD = {
    "story_time": (
        "Earliest `published` timestamp among the cluster's articles, as sources/news.py "
        "reports it, read as UTC (a time without a zone counts as unknown). An article "
        "time later than the collection time makes the story a data gap. The headline and "
        "link are the earliest article's; last_published and span_hours show how far the "
        "cluster spreads, and a cluster whose first and last articles map to different "
        "sessions of a listing is a data gap for that listing. Stories whose earliest "
        "article is older than max_age_hours are left out and counted in the summary."
    ),
    "session_rule": (
        "The story time is converted to the listing's exchange timezone "
        "(Yahoo exchangeTimezoneName). Published before that exchange's "
        "regular close on a trading day: that day's session. Published at or "
        "after the close: the next session. Published on a day with no session "
        "(weekend or holiday, read from the listing's own daily bars): the next "
        "session. The close time of day is Yahoo's currentTradingPeriod.regular.end "
        "applied to every day, so early-close days are not modelled; each row gives "
        "the close it used (close_time_used), and a data_gaps line names a listing whose "
        "close differs from its zone's standard close. A listing's last bar counts only "
        "once Yahoo's regularMarketTime is at or after that day's close; until then the "
        "row is pending."
    ),
    "return": (
        "Close-to-close: adjusted close of the assigned session over the adjusted "
        "close of the prior session in the listing's own daily bars, minus 1 (raw "
        "closes, with a data_gaps line, when the chart has no adjusted closes). A bar "
        "Yahoo fills for a day without trading (open = high = low = close = the "
        "previous close, volume 0) is dropped, so that day counts as no session. A "
        "story that landed mid-session shares the window with trading before it."
    ),
    "benchmark": (
        f"US listings (no exchange suffix): {US_BENCHMARK}. Other listings: the "
        "primary index of the exchange's country in the table intel_country_stocks "
        "uses. The benchmark is measured over the same two dates; excess = stock "
        "return - benchmark return, a simple difference, not beta-adjusted. A session "
        "the benchmark traded that the stock series lacks, inside that window or where "
        "the story's session would be, makes the row a data gap."
    ),
    "countries": (
        "Country rows measure the country's primary index on the assigned session, "
        f"with a z-score against the index's previous up-to-{ZSCORE_WINDOW} session "
        f"returns (null with fewer than {ZSCORE_MIN})."
    ),
    "entities": (
        "Companies and countries named in the cluster's headlines (titles only) by "
        "analysis/entities.py. Company names that are also common words "
        "(apple, amazon, meta, shell, bp, alphabet, chevron, samsung) are kept only "
        "with a corroborating term in the headlines, or an article from a feed where the "
        "company sense dominates: technology feeds, and the CNBC, MarketWatch and WSJ "
        "Markets finance feeds, for apple, amazon, meta and alphabet; energy feeds for "
        "shell and bp; Oil Price and Rigzone for chevron; none for samsung. A feed counts "
        "only when the name stands on its own, not hyphen-joined ('meta-analysis', "
        "'Pan-Amazon'). A phrase marking the other sense ('big apple', 'amazon "
        "rainforest', 'chevron ruling', 'shell fire', '25 bp') excludes the name whatever "
        "else admits it. Each kept name says what admitted it and each excluded one why. "
        "A country gets an index row only when the headlines use its name (for the United "
        "States also 'USA'); one named only by a demonym, capital or code, or inside a "
        "longer name such as 'New Mexico' or 'Gulf of Mexico', is listed as excluded with "
        "the reason."
    ),
}

INTERPRETATION = (
    "Same-session co-movement only. A move on the story's session does not show "
    "that the story caused it: other news, sector moves and market-wide events "
    "share the session. This is not a trading signal."
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _age(data: dict) -> str:
    """How old the fetcher's expired fallback copy is, for a gap line."""
    age = data.get("_stale_age_seconds")
    return f"{age:.0f} s old" if isinstance(age, (int, float)) else "of unknown age"


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

    def __init__(
        self,
        symbol: str,
        tz: ZoneInfo,
        close_time: time,
        closes: dict[date, float],
        market_time: datetime | None = None,
        notes: list[str] | None = None,
    ):
        self.symbol = symbol
        self.tz = tz
        self.close_time = close_time
        self.closes = closes
        self.dates = sorted(closes)
        # Yahoo's regularMarketTime: the last bar is final only once the
        # last regular-market print is at or after that day's close.
        self.market_time = market_time
        self.last_final = market_time is not None and market_time >= datetime.combine(
            self.dates[-1], close_time, tz
        )
        # Data-gap lines about this series (raw closes, dropped bars, a
        # nonstandard close).
        self.notes = list(notes or [])

    @property
    def tz_name(self) -> str:
        return self.tz.key


def _placeholder(quote: dict, i: int, prev_close: float | None) -> bool:
    """A bar Yahoo fills for a day without trading: open, high, low and
    close all equal the previous close, and no volume."""
    if prev_close is None:
        return False
    for key in ("open", "high", "low", "close"):
        seq = quote.get(key)
        if not isinstance(seq, list) or i >= len(seq) or seq[i] != prev_close:
            return False
    volume = quote.get("volume")
    return not (isinstance(volume, list) and i < len(volume) and volume[i])


def parse_bars(symbol: str, data: Any) -> tuple[Bars | None, str | None]:
    """Build ``Bars`` from a Yahoo v8 chart payload, or say why not."""
    if not isinstance(data, dict):
        return None, f"{symbol}: Yahoo chart returned no data (request failed or symbol unknown)"
    if data.get("_stale"):
        # The fetcher's expired last-known-good copy after a failed
        # request: an old chart is never read as the current one.
        return None, f"{symbol}: Yahoo unavailable; a cached chart {_age(data)} was not used"
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
        rmt = meta.get("regularMarketTime")
        market_time = datetime.fromtimestamp(rmt, tz) if isinstance(rmt, (int, float)) else None
        stamps = result.get("timestamp") or []
        indicators = result.get("indicators") or {}
        adj = (indicators.get("adjclose") or [{}])[0].get("adjclose")
        quote = (indicators.get("quote") or [{}])[0]
        raw = quote.get("close")
    except (KeyError, IndexError, TypeError, AttributeError, ZoneInfoNotFoundError) as exc:
        return None, f"{symbol}: unexpected Yahoo chart structure ({type(exc).__name__}: {exc})"
    notes: list[str] = []
    raw_ok = isinstance(raw, list) and len(raw) == len(stamps)
    if isinstance(adj, list) and len(adj) == len(stamps):
        values = adj
    else:
        values = raw
        if raw_ok:
            notes.append(
                f"{symbol}: no adjusted closes in the Yahoo chart; raw closes used, so a "
                "dividend or split in range distorts its returns"
            )
    if not isinstance(values, list) or len(values) != len(stamps):
        return None, f"{symbol}: Yahoo chart has no close series matching its timestamps"
    closes: dict[date, float] = {}
    filled: list[str] = []
    prev_close = None
    for i, (ts, value) in enumerate(zip(stamps, values)):
        # A null close is a missing bar, never a zero price.
        if ts is None or value is None:
            continue
        day = datetime.fromtimestamp(ts, tz).date()
        if _placeholder(quote, i, prev_close):
            filled.append(day.isoformat())
            continue
        if raw_ok and raw[i] is not None:
            prev_close = raw[i]
        # A later bar on the same local date (Yahoo's live bar) replaces it.
        closes[day] = float(value)
    if not closes:
        return None, f"{symbol}: Yahoo chart has no usable closes"
    if filled:
        what = (
            "a day without trading, not a 0% session"
            if len(filled) == 1
            else "days without trading, not 0% sessions"
        )
        notes.append(
            f"{symbol}: placeholder bar{'s' if len(filled) > 1 else ''} dropped for "
            f"{', '.join(filled)} (open = high = low = close = the previous close, volume 0): {what}"
        )
    standard = STANDARD_CLOSE.get(tz.key)
    if standard is not None and close_time != standard:
        notes.append(
            f"{symbol}: Yahoo's current regular close, {close_time.isoformat(timespec='minutes')} "
            f"{tz.key}, differs from the standard {standard.isoformat(timespec='minutes')} for that "
            "zone; it is applied to every day in range, so a story published between the two "
            "times may map to the wrong session"
        )
    return Bars(symbol, tz, close_time, closes, market_time, notes), None


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


def _not_final(bars: Bars) -> str:
    last = bars.dates[-1].isoformat()
    if bars.market_time is None:
        return f"the {last} {bars.symbol} bar may not be final: Yahoo gave no regularMarketTime"
    return (
        f"the {last} {bars.symbol} bar is not final: Yahoo's last regular-market time is "
        f"{bars.market_time.isoformat()}, before the {bars.close_time.isoformat(timespec='minutes')} close"
    )


def assign_session(story_utc: datetime, bars: Bars, now_utc: datetime) -> dict:
    """Map a story time to the listing's session under the stated rule.

    Returns a dict with ``status`` ("assigned", "pending" or
    "out_of_range"), and when assigned: ``session_date``, ``basis``,
    ``published_local``. A pending result says why in ``pending``:
    "no_session_yet", "in_progress" or "not_final".
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
        out.update({"status": "pending", "pending": "no_session_yet"})
        out["reason"] = (
            f"no {bars.symbol} session on or after {candidate.isoformat()} yet "
            f"(last bar {bars.dates[-1].isoformat()})"
        )
        return out
    now_local = now_utc.astimezone(bars.tz)
    if session == now_local.date() and now_local.time() < bars.close_time:
        out.update({"status": "pending", "pending": "in_progress", "session_date": session.isoformat()})
        out["reason"] = f"the {session.isoformat()} session has not closed yet; its move is not final"
        return out
    if session == bars.dates[-1] and not bars.last_final:
        out.update({"status": "pending", "pending": "not_final", "session_date": session.isoformat()})
        out["reason"] = _not_final(bars)
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


def _span_gap(first_utc: datetime, last_utc: datetime | None, bars: Bars, now_utc: datetime,
              session: date) -> str | None:
    """Why a cluster cannot be tied to one session of this listing, if so."""
    if last_utc is None or last_utc == first_utc:
        return None
    later = assign_session(last_utc, bars, now_utc).get("session_date")
    if later == session.isoformat():
        return None
    other = later or f"a session after {bars.dates[-1].isoformat()}"
    return (
        f"the story's articles run from {_stamp(first_utc)} to {_stamp(last_utc)}, which map "
        f"to different {bars.symbol} sessions ({session.isoformat()} and {other}); the move "
        "cannot be tied to one session"
    )


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


def _missing_bar(bench: Bars, bars: Bars, day: date) -> str:
    return f"{bench.symbol} traded on {day.isoformat()} but the {bars.symbol} series has no bar for it"


def measure_company(
    story_utc: datetime,
    bars: Bars,
    bench: Bars,
    now_utc: datetime,
    story_last_utc: datetime | None = None,
) -> dict:
    """Stock, benchmark and excess return on the story's session.

    ``story_last_utc`` is the cluster's latest article time; a cluster
    whose first and last articles map to different sessions is a gap.
    """
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
    candidate = date.fromisoformat(sess["candidate_date"])
    if sess["status"] != "assigned":
        if sess.get("pending") == "no_session_yet":
            # The benchmark already closed a session the stock series
            # lacks: missing stock data, not a session still to come.
            closed = [d for d in bench.dates if d >= candidate and (d < bench.dates[-1] or bench.last_final)]
            if closed:
                out.update({
                    "status": "data_gap",
                    "gap": _missing_bar(bench, bars, closed[0]) + "; the session cannot be assigned",
                })
                return out
        out.update({"status": _UNASSIGNED[sess["status"]], "gap": sess["reason"]})
        return out
    session = date.fromisoformat(sess["session_date"])
    # A day the benchmark traded but the stock series lacks is missing
    # data in the stock series, not a holiday: the session would be wrong.
    skipped = [d for d in bench.dates if candidate <= d < session]
    if skipped:
        out.update({
            "status": "data_gap",
            "gap": _missing_bar(bench, bars, skipped[0]) + "; the session cannot be assigned",
        })
        return out
    out["session_basis"] = sess["basis"]
    span = _span_gap(story_utc, story_last_utc, bars, now_utc, session)
    if span:
        out.update({"status": "data_gap", "gap": span})
        return out
    prior, stock_ret = session_return(bars, session)
    if prior is None or stock_ret is None:
        out.update({"status": "data_gap", "gap": f"no {bars.symbol} bar before {session.isoformat()} in range"})
        return out
    out["prior_session_date"] = prior.isoformat()
    # The same gap one step back: a benchmark session between the stock's
    # prior bar and the session would make this a multi-session return.
    between = [d for d in bench.dates if prior < d < session]
    if between:
        out.update({
            "status": "data_gap",
            "gap": (
                _missing_bar(bench, bars, between[0]) + f"; the {prior.isoformat()} to "
                f"{session.isoformat()} return would span more than one session"
            ),
        })
        return out
    if session == bench.dates[-1] and not bench.last_final:
        out.update({"status": "pending", "gap": _not_final(bench)})
        return out
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


def measure_country(
    story_utc: datetime, bars: Bars, now_utc: datetime, story_last_utc: datetime | None = None
) -> dict:
    """Index return on the story's session and its trailing z-score."""
    sess = assign_session(story_utc, bars, now_utc)
    out: dict[str, Any] = {k: sess[k] for k in ("published_local", "session_date") if k in sess}
    if sess["status"] != "assigned":
        out.update({"status": _UNASSIGNED[sess["status"]], "gap": sess["reason"]})
        return out
    session = date.fromisoformat(sess["session_date"])
    out["session_basis"] = sess["basis"]
    span = _span_gap(story_utc, story_last_utc, bars, now_utc, session)
    if span:
        out.update({"status": "data_gap", "gap": span})
        return out
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


def _either(names: tuple[str, ...] | list[str], word: str = "or") -> str:
    """"A", "A or B", "A, B or C"."""
    names = list(names)
    return names[0] if len(names) == 1 else f"{', '.join(names[:-1])} {word} {names[-1]}"


def _ambiguity(keyword: str, amb: Ambiguous, lowered: str,
               feeds: frozenset[tuple[str, str]]) -> tuple[str | None, str]:
    """(what admits the name, or None) and, when it is excluded, why."""
    term = _CORROBORATOR_RE[keyword].search(lowered)
    other = _OTHER_SENSE_RE[keyword].search(lowered) if keyword in _OTHER_SENSE_RE else None
    in_context = sorted((cat, name) for cat, name in feeds if cat in amb.context)
    counting = [(cat, name) for cat, name in in_context if amb.context[cat] is None or name in amb.context[cat]]
    # "meta-analysis", "Pan-Amazon": a hyphen-joined use is the common
    # word, so a feed alone does not admit it.
    standalone = re.search(r"(?<!-)\b" + re.escape(keyword) + r"\b(?!-)", lowered) is not None
    if term:
        by = f"corroborating term '{term.group(0)}'"
    elif counting and standalone:
        cat, name = counting[0]
        by = f"article from {_a(cat)} feed" + (f" ({name})" if name else "")
    else:
        by = None
    if by and other is None:
        return by, ""
    clauses = [amb.reason]
    if other is not None:
        clauses.append(f"'{other.group(0)}' in the headlines points to the other sense")
    if by:
        clauses.append(f"that outweighs the {by}")
    else:
        if counting:
            where = (
                f"; the {counting[0][0]} feed does not count because '{keyword}' only appears "
                "hyphen-joined"
            )
        elif in_context:
            cat = in_context[0][0]
            names = sorted({name for c, name in in_context if c == cat and name})
            where = f"; {cat} articles count only from {_either(amb.context[cat])}"
            if names:
                where += f", and this story's are from {_either(names, 'and')}"
        elif amb.context:
            where = f" and no article from {_a(' or '.join(amb.context))} feed"
        else:
            where = ""
        clauses.append(f"no corroborating term ({', '.join(amb.terms)}) in the headlines{where}")
    return None, "; ".join(clauses)


def classify_companies(
    text: str,
    categories: frozenset[str] | set[str] = frozenset(),
    feeds: frozenset[tuple[str, str]] | set[tuple[str, str]] | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Split the companies a text names into (mapped, unmapped, excluded).

    Uses ``entities.extract_entities`` and keeps one row per ticker.
    ``feeds`` are the story's (category, feed name) pairs; with only
    ``categories`` no feed name is known, so a category whose context is
    limited to named feeds (finance for apple) admits nothing.
    """
    lowered = text.lower()
    feed_pairs = frozenset(feeds) if feeds is not None else frozenset((c, "") for c in categories)
    mapped: list[dict] = []
    unmapped: list[dict] = []
    excluded: list[dict] = []
    seen: set[str] = set()
    for comp in entities.extract_entities(text)["entities"]["companies"]:
        keyword = str(comp.get("name", "")).lower()
        entity: dict[str, Any] = {"type": "company", "name": comp.get("name"), "sector": comp.get("sector")}
        amb = AMBIGUOUS_COMPANIES.get(keyword)
        if amb:
            admitted_by, reason = _ambiguity(keyword, amb, lowered, feed_pairs)
            if admitted_by is None:
                excluded.append({"entity": entity, "reason": reason})
                continue
            entity["admitted_by"] = admitted_by
        ticker = comp.get("ticker")
        if not ticker:
            unmapped.append({"entity": entity, "reason": "no ticker in the entity table"})
            continue
        if ticker in seen:
            continue
        seen.add(ticker)
        mapped.append({**entity, "ticker": ticker})
    return mapped, unmapped, excluded


@functools.lru_cache(maxsize=None)
def _country_forms(iso3: str) -> re.Pattern[str]:
    """Every form the entity matcher accepts for a country, plus the longer
    names in NOT_THE_COUNTRY, longest first so those win over the name."""
    info = TIER1_COUNTRIES[iso3]
    forms = {kw.lower() for kw in info["keywords"]} | {info["name"].lower(), iso3.lower()}
    forms |= set(NOT_THE_COUNTRY.get(iso3, ()))
    parts = []
    for form in sorted(forms, key=len, reverse=True):
        esc = re.escape(form)
        # The entity matcher's plural rule ("Americans").
        if len(form) >= 5 and form.isalpha():
            esc += "s?"
        parts.append(esc)
    return re.compile(r"\b(?:" + "|".join(parts) + r")\b")


def classify_countries(text: str, excluded: list[dict] | None = None) -> tuple[list[dict], list[dict]]:
    """Split the countries a text names into (mapped to an index, unmapped).

    A country with an index that the text names only by a demonym,
    capital or code, or only inside a longer name (``NOT_THE_COUNTRY``),
    gets no index row: it goes to ``excluded`` with the reason when a list
    is given, else to the unmapped list, so it is never dropped.
    """
    lowered = text.lower()
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
        names = COUNTRY_NAMES.get(c["iso3"], (c["name"],))
        found = list(dict.fromkeys(m.group(0) for m in _country_forms(c["iso3"]).finditer(lowered)))
        if any(form in {n.lower() for n in names} for form in found):
            mapped.append({**entity, "index": index})
            continue
        forms = ", ".join(f"'{form}'" for form in found) or "a form other than its name"
        (unmapped if excluded is None else excluded).append({
            "entity": entity,
            "reason": (
                f"named only as {forms}, not by the country's name ({' or '.join(names)}); "
                "an index row needs the name itself"
            ),
        })
    return mapped, unmapped


# ---------------------------------------------------------------------------
# SEC registrant annotation
# ---------------------------------------------------------------------------


async def _sec_registrants(fetcher: Fetcher, notes: list[str] | None = None) -> dict[str, dict] | None:
    """Ticker -> {cik, name} from SEC's company_tickers.json, or None.

    An expired copy the fetcher served because SEC failed is not used;
    the reason goes to ``notes`` when a list is given.
    """
    data = await fetcher.get_json(
        _TICKERS_URL,
        source="sec-edgar",
        cache_key="sec:company_tickers",
        cache_ttl=86400,
        headers=_SEC_HEADERS,
    )
    if not isinstance(data, dict):
        return None
    if data.get("_stale"):
        if notes is not None:
            notes.append(
                f"SEC company_tickers.json unavailable; a cached copy {_age(data)} was not used, "
                "so US registrants are not confirmed"
            )
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


def _stamp(dt: datetime | None) -> str | None:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None


def _story(items: list[dict], indices: list[int]) -> dict:
    members = [items[i] for i in indices]
    stamped = []
    for m in members:
        t = parse_published(m.get("published"))
        if t is not None:
            stamped.append((t, m))
    stamped.sort(key=lambda pair: pair[0])
    first, last = (stamped[0][0], stamped[-1][0]) if stamped else (None, None)
    # Headline and link come from the article whose time is the story time.
    rep = stamped[0][1] if stamped else members[0]
    return {
        "headline": _clean_title(rep.get("title")),
        "link": rep.get("link", ""),
        "articles": len(members),
        "outlets": len({m.get("feed_name") for m in members if m.get("feed_name")}),
        "first_published": _stamp(first),
        "last_published": _stamp(last),
        "span_hours": round((last - first).total_seconds() / 3600, 2) if stamped else None,
        "_time": first,
        "_last": last,
        "_text": " ".join(_clean_title(m.get("title")) for m in members),
        "_categories": frozenset(m["category"] for m in members if m.get("category")),
        "_feeds": frozenset(
            (m["category"], str(m.get("feed_name") or "")) for m in members if m.get("category")
        ),
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
    max_age_hours: float = MAX_AGE_HOURS,
    now: datetime | None = None,
) -> dict:
    """Same-session market moves of the companies and countries in the news."""
    now_utc = (now or _utc_now()).astimezone(timezone.utc)
    limit = max(1, min(int(limit), 300))
    threshold = max(THRESHOLD_MIN, min(float(threshold), 1.0))
    min_cluster_size = max(1, int(min_cluster_size))
    max_symbols = max(1, min(int(max_symbols), 60))
    max_age_hours = max(1.0, min(float(max_age_hours), float(MAX_AGE_LIMIT)))

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
    cutoff = now_utc - timedelta(hours=max_age_hours)
    stories: list[dict] = []
    dropped_by_age = 0
    for c in clusters:
        story = _story(items, c["member_indices"])
        if story["_time"] is not None and story["_time"] < cutoff:
            dropped_by_age += 1
            continue
        stories.append(story)

    data_gaps: list[str] = []
    unmapped: list[dict] = []
    excluded: list[dict] = []
    # (story, entity, symbol, kind)
    pending_rows: list[tuple[dict, dict, str, str]] = []
    stories_with_entities = 0
    for story in stories:
        companies, c_unmapped, c_excluded = classify_companies(
            story["_text"], story["_categories"], feeds=story["_feeds"]
        )
        k_excluded: list[dict] = []
        countries, k_unmapped = (
            classify_countries(story["_text"], excluded=k_excluded) if include_countries else ([], [])
        )
        if companies or countries or c_unmapped or c_excluded or k_unmapped or k_excluded:
            stories_with_entities += 1
        for row in c_unmapped + k_unmapped:
            unmapped.append({"story": story["headline"], **row})
        for row in c_excluded + k_excluded:
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
            data_gaps.extend(b.notes)
        elif err:
            data_gaps.append(err)

    registrants: dict[str, dict] | None = None
    us_tickers = [s for s, (bench, _r) in bench_of.items() if bench == US_BENCHMARK]
    if us_tickers:
        sec_notes: list[str] = []
        registrants = await _sec_registrants(fetcher, sec_notes)
        if registrants is None:
            data_gaps.extend(sec_notes or ["SEC company_tickers.json unavailable; US registrants not confirmed"])

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
        elif story["_last"] > now_utc + FUTURE_TOLERANCE:
            row.update({
                "status": "data_gap",
                "gap": (
                    f"story timestamp {story['last_published']} is later than the collection "
                    f"time {_stamp(now_utc)} (a timestamp error in the feed or in its parsing)"
                ),
            })
        elif b is None:
            row.update({
                "status": "data_gap",
                "gap": f"no daily bars for {symbol}" + (" (symbol budget)" if symbol in over_budget else ""),
            })
        elif kind == "country":
            row["exchange_timezone"] = b.tz_name
            row["close_time_used"] = b.close_time.isoformat(timespec="minutes")
            row.update(measure_country(story["_time"], b, now_utc, story["_last"]))
        else:
            row["exchange_timezone"] = b.tz_name
            row["close_time_used"] = b.close_time.isoformat(timespec="minutes")
            if bench_sym is None:
                row.update({"status": "data_gap", "gap": bench_reason})
            elif bench_sym not in bars:
                row.update({
                    "status": "data_gap",
                    "gap": f"no daily bars for benchmark {bench_sym}"
                    + (" (symbol budget)" if bench_sym in over_budget else ""),
                })
            else:
                row.update(measure_company(story["_time"], b, bars[bench_sym], now_utc, story["_last"]))
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
            "stories_considered": len(stories),
            "stories_dropped_by_age": dropped_by_age,
            "max_age_hours": max_age_hours,
            "threshold": threshold,
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
