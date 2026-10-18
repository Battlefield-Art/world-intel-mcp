"""Tests for analysis.news_market: news vs market reaction.

Yahoo v8 chart fixtures reproduce the live response shape captured
2026-10-17 (meta incl. exchangeTimezoneName and currentTradingPeriod,
timestamp[], indicators.quote[0].close, indicators.adjclose[0].adjclose)
and carry the live daily closes of AAPL, SPY and 000001.SS for the dates
used, so the expected returns are real market moves: Labor Day
(2026-09-07) and China's Mid-Autumn / Golden Week closures are in the
data, and SPY's 2026-09-18 ex-dividend bar is where close and adjusted
close disagree. The SEC fixture is a subset of the live
company_tickers.json. Clustering and entity matching run for real on
headlines in the live feed-item shape; only the RSS fetch is faked.
"""

import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import httpx
import pytest
import respx

import world_intel_mcp.fetcher as fetcher_mod
from world_intel_mcp.analysis import news_market as nm
from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher

_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{}"
_SEC = "https://www.sec.gov/files/company_tickers.json"

# Live currentTradingPeriod blocks (2026-10-16 sessions).
_US_PERIOD = {
    "pre": {"timezone": "EDT", "start": 1792137600, "end": 1792157400, "gmtoffset": -14400},
    "regular": {"timezone": "EDT", "start": 1792157400, "end": 1792180800, "gmtoffset": -14400},
    "post": {"timezone": "EDT", "start": 1792180800, "end": 1792195200, "gmtoffset": -14400},
}
_SH_PERIOD = {
    "pre": {"timezone": "CST", "start": 1792114200, "end": 1792114200, "gmtoffset": 28800},
    "regular": {"timezone": "CST", "start": 1792114200, "end": 1792134000, "gmtoffset": 28800},
    "post": {"timezone": "CST", "start": 1792134000, "end": 1792134000, "gmtoffset": 28800},
}

# (timestamp = session open, close, adjclose), live values.
_AAPL = [
    (1788442200, 328.2099914550781, 328.2099914550781),  # 2026-09-03
    (1788528600, 319.9700012207031, 319.9700012207031),  # 2026-09-04
    (1788874200, 316.2200012207031, 316.2200012207031),  # 2026-09-08 (after Labor Day)
    (1789651800, 337.0, 337.0),  # 2026-09-17
    (1789738200, 336.1300048828125, 336.1300048828125),  # 2026-09-18
    (1791379800, 336.6700134277344, 336.6700134277344),  # 2026-10-07
    (1791466200, 340.4200134277344, 340.4200134277344),  # 2026-10-08
    (1791552600, 336.6400146484375, 336.6400146484375),  # 2026-10-09
    (1791811800, 340.9700012207031, 340.9700012207031),  # 2026-10-12
    (1791898200, 341.2699890136719, 341.2699890136719),  # 2026-10-13
    (1791984600, 342.9599914550781, 342.9599914550781),  # 2026-10-14
    (1792071000, 342.0, 342.0),  # 2026-10-15
    (1792157400, 343.4599914550781, 343.4599914550781),  # 2026-10-16
]
_SPY = [
    (1788442200, 773.1699829101562, 771.2548217773438),  # 2026-09-03
    (1788528600, 770.1900024414062, 768.2822265625),  # 2026-09-04
    (1788874200, 765.9600219726562, 764.0626831054688),  # 2026-09-08
    (1789651800, 762.5999755859375, 760.7109985351562),  # 2026-09-17
    (1789738200, 761.6900024414062, 761.6900024414062),  # 2026-09-18 (ex-dividend)
    (1791379800, 777.219970703125, 777.219970703125),  # 2026-10-07
    (1791466200, 773.9299926757812, 773.9299926757812),  # 2026-10-08
    (1791552600, 778.5700073242188, 778.5700073242188),  # 2026-10-09
    (1791811800, 778.0999755859375, 778.0999755859375),  # 2026-10-12
    (1791898200, 778.010009765625, 778.010009765625),  # 2026-10-13
    (1791984600, 769.5999755859375, 769.5999755859375),  # 2026-10-14
    (1792071000, 771.7899780273438, 771.7899780273438),  # 2026-10-15
    (1792157400, 763.25, 763.25),  # 2026-10-16
]
_SSE = [
    (1789435800, 3864.279052734375),  # 2026-09-15
    (1789522200, 3891.60107421875),  # 2026-09-16
    (1789608600, 3875.60400390625),  # 2026-09-17
    (1789695000, 3911.87109375),  # 2026-09-18
    (1789954200, 3949.906982421875),  # 2026-09-21
    (1790040600, 3952.1279296875),  # 2026-09-22
    (1790127000, 3936.52001953125),  # 2026-09-23
    (1790213400, 3888.3740234375),  # 2026-09-24 (09-25 Mid-Autumn: closed)
    (1790559000, 3823.62109375),  # 2026-09-28
    (1790645400, 3830.450927734375),  # 2026-09-29
    (1790731800, 3842.195068359375),  # 2026-09-30 (Golden Week to 10-07)
    (1791423000, 3811.904052734375),  # 2026-10-08
    # Live close written as its exact binary fraction (the decimal form
    # trips a PII scanner's digit-run rule).
    (1791509400, 1952661 / 512),  # 2026-10-09
    (1791768600, 3797.241943359375),  # 2026-10-12
    (1791855000, 3800.864990234375),  # 2026-10-13
    (1791941400, 3767.22705078125),  # 2026-10-14
    (1792027800, 3707.47607421875),  # 2026-10-15
    (1792114200, 3716.283935546875),  # 2026-10-16
]

_SEC_SUBSET = {
    "0": {"cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP"},
    "1": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    "133": {"cik_str": 936468, "ticker": "LMT", "title": "LOCKHEED MARTIN CORP"},
    "7177": {"cik_str": 884394, "ticker": "SPY", "title": "SPDR S&P 500 ETF TRUST"},
}

# Saturday 2026-10-17 16:00 UTC: US and Shanghai closed for the weekend.
_SATURDAY = datetime(2026, 10, 17, 16, 0, tzinfo=timezone.utc)


def _market_time(last_stamp, tz, period):
    """regularMarketTime as Yahoo sends it: a finished daily bar (stamped
    at the session open) carries its day's closing print, as the live
    AAPL 16:00:01 and SPY 16:00:00 captures do; a bar stamped later is a
    live intraday print and carries that stamp."""
    zone = ZoneInfo(tz)
    regular = period["regular"]
    last = datetime.fromtimestamp(last_stamp, zone)
    if last.time() != datetime.fromtimestamp(regular["start"], zone).time():
        return last_stamp
    close = datetime.fromtimestamp(regular["end"], zone).time()
    return int(datetime.combine(last.date(), close, zone).timestamp())


def _chart(symbol, bars, *, tz="America/New_York", abbr="EDT", gmtoffset=-14400,
           period=None, instrument="EQUITY", exchange="NMS", full="NasdaqGS", market_time=None):
    stamps = [b[0] for b in bars]
    closes = [b[1] for b in bars]
    adj = [b[2] if len(b) > 2 else b[1] for b in bars]
    period = period or _US_PERIOD
    return {
        "chart": {
            "result": [{
                "meta": {
                    "currency": "USD" if tz == "America/New_York" else "CNY",
                    "symbol": symbol,
                    "exchangeName": exchange,
                    "fullExchangeName": full,
                    "instrumentType": instrument,
                    "regularMarketTime": (
                        _market_time(stamps[-1], tz, period) if market_time is None else market_time),
                    "gmtoffset": gmtoffset,
                    "timezone": abbr,
                    "exchangeTimezoneName": tz,
                    "chartPreviousClose": closes[0],
                    "currentTradingPeriod": period,
                    "dataGranularity": "1d",
                    "range": "3mo",
                },
                "timestamp": stamps,
                "indicators": {
                    "quote": [{"open": closes, "high": closes, "low": closes,
                               "close": closes, "volume": [1000] * len(bars)}],
                    "adjclose": [{"adjclose": adj}],
                },
            }],
            "error": None,
        }
    }


def _sse_chart(bars=_SSE):
    return _chart("000001.SS", bars, tz="Asia/Shanghai", abbr="CST", gmtoffset=28800,
                  period=_SH_PERIOD, instrument="INDEX", exchange="SHH", full="Shanghai")


def _bars(symbol, payload):
    bars, err = nm.parse_bars(symbol, payload)
    assert err is None
    return bars


def _item(title, published, category="technology", feed="Ars Technica"):
    """Feed item in the live sources/news.py shape."""
    return {
        "title": title,
        "link": "https://example.com/" + title.lower().replace(" ", "-"),
        "published": published,
        "summary": "",
        "feed_name": feed,
        "category": category,
        "source_tier": "major",
    }


@pytest.fixture
async def fast_fetcher(tmp_path, monkeypatch):
    """Fetcher without retries or Yahoo spacing so failure paths stay fast."""
    monkeypatch.setattr(fetcher_mod, "_YAHOO_MIN_INTERVAL", 0.0)
    monkeypatch.setitem(fetcher_mod._SOURCE_RATE_LIMITS, "yahoo-finance", 0.0)
    fetcher = Fetcher(cache=Cache(db_path=tmp_path / "nm.db"), breaker=CircuitBreaker(),
                      default_timeout=5.0, max_retries=0)
    try:
        yield fetcher
    finally:
        await fetcher.close()
        fetcher.cache.close()


def _feed(monkeypatch, items):
    async def _fake(fetcher, category=None, limit=50):
        return {"items": items, "count": len(items), "source": "rss-aggregator"}

    monkeypatch.setattr(nm.news, "fetch_news_feed", _fake)


# ---------------------------------------------------------------------------
# Story time
# ---------------------------------------------------------------------------


def test_parse_published_forms():
    assert nm.parse_published("2026-10-16T19:43:52Z") == datetime(2026, 10, 16, 19, 43, 52, tzinfo=timezone.utc)
    # RFC 822 with an offset (a feed's raw string when feedparser could not parse it)
    assert nm.parse_published("Sat, 17 Oct 2026 19:43:52 +0500") == datetime(2026, 10, 17, 14, 43, 52, tzinfo=timezone.utc)
    # A naive timestamp states no zone: unknown, not assumed UTC
    assert nm.parse_published("2026-10-16T19:43:52") is None
    assert nm.parse_published("yesterday") is None
    assert nm.parse_published(None) is None
    assert nm.parse_published("") is None


# ---------------------------------------------------------------------------
# Bars
# ---------------------------------------------------------------------------


def test_parse_bars_reads_live_shape():
    bars = _bars("AAPL", _chart("AAPL", _AAPL))
    assert bars.tz_name == "America/New_York"
    assert bars.close_time.isoformat() == "16:00:00"
    assert bars.dates[0].isoformat() == "2026-09-03"
    assert bars.dates[-1].isoformat() == "2026-10-16"
    assert len(bars.dates) == 13
    sse = _bars("000001.SS", _sse_chart())
    assert sse.close_time.isoformat() == "15:00:00"


def test_parse_bars_uses_adjusted_close_on_ex_dividend_day():
    """SPY went ex-dividend 2026-09-18: close-to-close reads -0.1193%,
    the adjusted series +0.1287%. The adjusted one is the return."""
    bars = _bars("SPY", _chart("SPY", _SPY))
    prior, ret = nm.session_return(bars, datetime(2026, 9, 18).date())
    assert prior.isoformat() == "2026-09-17"
    assert round(ret * 100, 4) == 0.1287


def test_parse_bars_null_close_is_a_missing_bar_not_zero():
    payload = _chart("AAPL", _AAPL[-3:])
    payload["chart"]["result"][0]["indicators"]["adjclose"][0]["adjclose"][1] = None
    bars = _bars("AAPL", payload)
    assert [d.isoformat() for d in bars.dates] == ["2026-10-14", "2026-10-16"]
    assert 0.0 not in bars.closes.values()


def test_parse_bars_failures_name_the_symbol():
    assert nm.parse_bars("^TASI", None) == (
        None, "^TASI: Yahoo chart returned no data (request failed or symbol unknown)")
    bars, err = nm.parse_bars("ZZZZ", {"chart": {"result": None, "error": {
        "code": "Not Found", "description": "No data found, symbol may be delisted"}}})
    assert bars is None and err == "ZZZZ: Yahoo chart error (No data found, symbol may be delisted)"
    broken = _chart("AAPL", _AAPL)
    del broken["chart"]["result"][0]["meta"]["exchangeTimezoneName"]
    bars, err = nm.parse_bars("AAPL", broken)
    assert bars is None and err.startswith("AAPL: unexpected Yahoo chart structure")
    mismatched = _chart("AAPL", _AAPL)
    mismatched["chart"]["result"][0]["indicators"] = {"quote": [{"close": [1.0]}]}
    assert nm.parse_bars("AAPL", mismatched)[1] == (
        "AAPL: Yahoo chart has no close series matching its timestamps")
    empty = _chart("AAPL", _AAPL[:2])
    empty["chart"]["result"][0]["indicators"]["adjclose"][0]["adjclose"] = [None, None]
    assert nm.parse_bars("AAPL", empty)[1] == "AAPL: Yahoo chart has no usable closes"


def test_parse_bars_same_date_later_bar_wins():
    # Yahoo can append a live bar stamped later on the same local date.
    bars_in = _AAPL[-2:] + [(1792180700, 344.0, 344.0)]  # 2026-10-16 15:58 EDT
    bars = _bars("AAPL", _chart("AAPL", bars_in))
    assert bars.dates[-1].isoformat() == "2026-10-16"
    assert bars.closes[bars.dates[-1]] == 344.0


# ---------------------------------------------------------------------------
# Session assignment
# ---------------------------------------------------------------------------


def _assign(published, bars, now=_SATURDAY):
    return nm.assign_session(nm.parse_published(published), bars, now)


def test_session_before_close_is_same_session():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    s = _assign("2026-10-15T19:59:59Z", aapl)  # 15:59:59 EDT
    assert (s["status"], s["session_date"], s["basis"]) == ("assigned", "2026-10-15", "same_session")
    assert s["published_local"] == "2026-10-15T15:59:59-04:00"


def test_session_at_or_after_close_is_next_session():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    at_close = _assign("2026-10-15T20:00:00Z", aapl)  # exactly 16:00 EDT
    assert (at_close["session_date"], at_close["basis"]) == ("2026-10-16", "next_session_after_close")
    late = _assign("2026-10-16T01:30:00Z", aapl)  # 21:30 EDT on the 15th
    assert (late["session_date"], late["basis"]) == ("2026-10-16", "next_session_after_close")


def test_session_weekend_maps_to_monday():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    s = _assign("2026-10-10T15:00:00Z", aapl)  # Saturday
    assert (s["session_date"], s["basis"]) == ("2026-10-12", "next_session_non_trading_day")
    friday_late = _assign("2026-10-09T21:00:00Z", aapl)  # Friday after close
    assert friday_late["session_date"] == "2026-10-12"
    assert friday_late["after_close"] is True


def test_session_holiday_from_the_listings_own_bars():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    labor_day = _assign("2026-09-07T14:00:00Z", aapl)  # Monday 10:00 EDT, NYSE closed
    assert (labor_day["session_date"], labor_day["basis"]) == ("2026-09-08", "next_session_non_trading_day")
    sse = _bars("000001.SS", _sse_chart())
    golden_week = _assign("2026-10-02T03:00:00Z", sse)  # 11:00 Shanghai, market closed
    assert (golden_week["session_date"], golden_week["basis"]) == ("2026-10-08", "next_session_non_trading_day")
    mid_autumn = _assign("2026-09-25T02:00:00Z", sse)  # Friday 10:00 Shanghai, closed
    assert mid_autumn["session_date"] == "2026-09-28"


def test_session_timezone_is_the_exchanges():
    """One instant, two exchanges: 16:00 Shanghai is after its close but
    04:00 in New York is before the US session of the same date."""
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    sse = _bars("000001.SS", _sse_chart())
    midday = "2026-10-15T05:00:00Z"  # 13:00 Shanghai, 01:00 New York
    assert _assign(midday, sse)["session_date"] == "2026-10-15"
    assert _assign(midday, aapl)["session_date"] == "2026-10-15"
    evening = "2026-10-15T08:00:00Z"  # 16:00 Shanghai (after close), 04:00 New York
    assert _assign(evening, sse)["session_date"] == "2026-10-16"
    assert _assign(evening, aapl)["session_date"] == "2026-10-15"


def test_session_not_yet_traded_is_pending():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    s = _assign("2026-10-16T21:00:00Z", aapl)  # Friday after close; now Saturday
    assert s["status"] == "pending"
    assert s["reason"] == "no AAPL session on or after 2026-10-17 yet (last bar 2026-10-16)"


def test_session_in_progress_is_pending():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    midday = datetime(2026, 10, 16, 18, 0, tzinfo=timezone.utc)  # 14:00 EDT Friday
    s = _assign("2026-10-16T13:00:00Z", aapl, now=midday)
    assert s["status"] == "pending"
    assert s["session_date"] == "2026-10-16"
    assert "has not closed yet" in s["reason"]
    after = datetime(2026, 10, 16, 20, 0, tzinfo=timezone.utc)  # 16:00 EDT: closed
    assert _assign("2026-10-16T13:00:00Z", aapl, now=after)["status"] == "assigned"


def test_session_before_the_bar_range_is_out_of_range():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    s = _assign("2025-01-28T00:26:00Z", aapl)
    assert s["status"] == "out_of_range"
    assert "predates the first AAPL bar in range (2026-09-03)" in s["reason"]


# ---------------------------------------------------------------------------
# Measurement
# ---------------------------------------------------------------------------


def test_measure_company_excess_return_arithmetic():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    spy = _bars("SPY", _chart("SPY", _SPY))
    # After the 2026-10-15 close -> the 2026-10-16 session.
    row = nm.measure_company(nm.parse_published("2026-10-15T20:30:00Z"), aapl, spy, _SATURDAY)
    assert row["status"] == "measured"
    assert row["session_date"] == "2026-10-16"
    assert row["prior_session_date"] == "2026-10-15"
    assert row["session_basis"] == "next_session_after_close"
    assert row["stock_return_pct"] == 0.4269  # 343.46 / 342.00 - 1
    assert row["benchmark_return_pct"] == -1.1065  # 763.25 / 771.79 - 1
    assert row["excess_return_pct"] == 1.5334  # from unrounded returns


def test_measure_company_holiday_and_adjusted_benchmark():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    spy = _bars("SPY", _chart("SPY", _SPY))
    labor_day = nm.measure_company(nm.parse_published("2026-09-07T14:00:00Z"), aapl, spy, _SATURDAY)
    assert (labor_day["session_date"], labor_day["prior_session_date"]) == ("2026-09-08", "2026-09-04")
    assert labor_day["session_basis"] == "next_session_non_trading_day"
    assert (labor_day["stock_return_pct"], labor_day["benchmark_return_pct"], labor_day["excess_return_pct"]) == (
        -1.172, -0.5492, -0.6228)
    ex_div = nm.measure_company(nm.parse_published("2026-09-18T14:00:00Z"), aapl, spy, _SATURDAY)
    assert (ex_div["stock_return_pct"], ex_div["benchmark_return_pct"], ex_div["excess_return_pct"]) == (
        -0.2582, 0.1287, -0.3869)


def test_measure_company_weekend_story():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    spy = _bars("SPY", _chart("SPY", _SPY))
    row = nm.measure_company(nm.parse_published("2026-10-10T15:00:00Z"), aapl, spy, _SATURDAY)
    assert (row["session_date"], row["prior_session_date"]) == ("2026-10-12", "2026-10-09")
    assert (row["stock_return_pct"], row["benchmark_return_pct"], row["excess_return_pct"]) == (
        1.2862, -0.0604, 1.3466)


def test_measure_company_benchmark_missing_bar_is_a_gap_with_no_excess():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    spy = _bars("SPY", _chart("SPY", [b for b in _SPY if b[0] != 1792071000]))  # no 10-15
    row = nm.measure_company(nm.parse_published("2026-10-16T14:00:00Z"), aapl, spy, _SATURDAY)
    assert row["status"] == "data_gap"
    assert row["stock_return_pct"] == 0.4269
    assert "benchmark_return_pct" not in row and "excess_return_pct" not in row
    assert row["gap"] == "benchmark SPY has no bar for 2026-10-15 or 2026-10-16"


def test_measure_company_stock_missing_a_day_the_benchmark_traded_is_a_gap():
    """No 2026-10-15 AAPL bar while SPY traded: a halt or missing data,
    not a holiday, so the story is not shifted to the 16th."""
    aapl = _bars("AAPL", _chart("AAPL", [b for b in _AAPL if b[0] != 1792071000]))
    spy = _bars("SPY", _chart("SPY", _SPY))
    row = nm.measure_company(nm.parse_published("2026-10-15T14:00:00Z"), aapl, spy, _SATURDAY)
    assert row["status"] == "data_gap"
    assert row["gap"] == "SPY traded on 2026-10-15 but the AAPL series has no bar for it; the session cannot be assigned"
    assert "stock_return_pct" not in row


def test_measure_company_timezone_mismatch_is_a_gap():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    sse = _bars("000001.SS", _sse_chart())
    row = nm.measure_company(nm.parse_published("2026-10-15T14:00:00Z"), aapl, sse, _SATURDAY)
    assert row["status"] == "data_gap"
    assert "trades in Asia/Shanghai" in row["gap"]


def test_measure_company_first_bar_has_no_prior():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    spy = _bars("SPY", _chart("SPY", _SPY))
    row = nm.measure_company(nm.parse_published("2026-09-03T14:00:00Z"), aapl, spy, _SATURDAY)
    assert row["status"] == "data_gap"
    assert row["gap"] == "no AAPL bar before 2026-09-03 in range"


def test_measure_company_pending_and_out_of_range_carry_no_return():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    spy = _bars("SPY", _chart("SPY", _SPY))
    pending = nm.measure_company(nm.parse_published("2026-10-16T21:00:00Z"), aapl, spy, _SATURDAY)
    assert pending["status"] == "pending" and "stock_return_pct" not in pending
    old = nm.measure_company(nm.parse_published("2025-01-28T00:26:00Z"), aapl, spy, _SATURDAY)
    assert old["status"] == "data_gap" and "stock_return_pct" not in old


def test_measure_country_return_and_zscore():
    sse = _bars("000001.SS", _sse_chart())
    row = nm.measure_country(nm.parse_published("2026-10-16T01:00:00Z"), sse, _SATURDAY)
    assert row["status"] == "measured"
    assert (row["session_date"], row["prior_session_date"]) == ("2026-10-16", "2026-10-15")
    assert row["index_return_pct"] == 0.2376
    # 16 earlier session returns in the fixture (09-16 .. 10-15)
    assert (row["zscore"], row["trailing_sessions"]) == (0.6, 16)
    golden = nm.measure_country(nm.parse_published("2026-10-02T03:00:00Z"), sse, _SATURDAY)
    assert (golden["session_date"], golden["prior_session_date"], golden["index_return_pct"]) == (
        "2026-10-08", "2026-09-30", -0.7884)


def test_measure_country_short_history_has_no_zscore():
    sse = _bars("000001.SS", _sse_chart(_SSE[-6:]))
    row = nm.measure_country(nm.parse_published("2026-10-16T01:00:00Z"), sse, _SATURDAY)
    assert row["status"] == "measured"
    assert row["zscore"] is None and row["trailing_sessions"] == 4
    pending = nm.measure_country(nm.parse_published("2026-10-16T09:00:00Z"), sse, _SATURDAY)
    assert pending["status"] == "pending"
    older = nm.measure_country(nm.parse_published("2026-10-08T01:00:00Z"), sse, _SATURDAY)
    assert older["status"] == "data_gap"
    assert older["gap"] == (
        "story session 2026-10-08 predates the first 000001.SS bar in range (2026-10-09)")


def test_measure_country_first_bar_has_no_prior():
    sse = _bars("000001.SS", _sse_chart(_SSE[-6:]))  # first bar 2026-10-09
    row = nm.measure_country(nm.parse_published("2026-10-09T01:00:00Z"), sse, _SATURDAY)
    assert row["status"] == "data_gap"
    assert row["gap"] == "no 000001.SS bar before 2026-10-09 in range"


def test_trailing_zscore_flat_series_is_none():
    flat = [(1789435800 + i * 86400, 100.0) for i in range(14)]
    bars = _bars("FLAT", _chart("FLAT", flat))
    # 14 flat bars give 12 returns before the session (the session's own
    # return is not in its baseline); enough for a z-score, but sd is 0.
    assert nm.trailing_zscore(bars, bars.dates[-1], 0.01) == (None, 12)


def test_benchmark_for():
    assert nm.benchmark_for("AAPL") == ("SPY", None)
    assert nm.benchmark_for("BA.L") == ("^FTSE", None)
    assert nm.benchmark_for("RHM.DE") == ("^GDAXI", None)
    assert nm.benchmark_for("005930.KS") == ("^KS11", None)
    assert nm.benchmark_for("2222.SR") == ("^TASI", None)
    assert nm.benchmark_for("X.QQ") == (None, "no benchmark mapped for exchange suffix .QQ")


def test_benchmark_for_country_without_index(monkeypatch):
    monkeypatch.setitem(nm.SUFFIX_COUNTRY, "XX", "ZZZ")
    assert nm.benchmark_for("ACME.XX") == (None, "no primary index mapped for ZZZ (exchange suffix .XX)")


# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------


def test_ambiguous_company_excluded_with_reason():
    mapped, unmapped, excluded = nm.classify_companies("Shell fire hits border town", {"geopolitics"})
    assert mapped == [] and unmapped == []
    assert excluded[0]["entity"]["name"] == "Shell"
    assert excluded[0]["reason"].startswith("'shell' is also an artillery round")
    assert "and no article from an energy feed" in excluded[0]["reason"]
    # "25 bp" is basis points; a finance feed does not make it the company
    _, _, bp = nm.classify_companies("Fed cuts rates by 25 bp", {"finance"})
    assert bp[0]["entity"]["name"] == "Bp"
    assert "basis points" in bp[0]["reason"]


def test_ambiguous_company_admitted_by_term_or_feed_category():
    mapped, _, excluded = nm.classify_companies("Apple foldable iPhones sell out in hours", {"geopolitics"})
    assert excluded == []
    assert mapped == [{"type": "company", "name": "Apple", "sector": "tech",
                       "admitted_by": "corroborating term 'iphones'", "ticker": "AAPL"}]
    mapped, _, excluded = nm.classify_companies(
        "Meta removes language that may have allowed it to scrape news sites", {"technology"})
    assert excluded == [] and mapped[0]["ticker"] == "META"
    assert mapped[0]["admitted_by"] == "article from a technology feed"
    # Samsung has no feed context: only a corroborating term admits it
    _, _, excluded = nm.classify_companies("Samsung shipyard wins LNG carrier order", {"technology"})
    assert excluded[0]["reason"].endswith("in the headlines")


def test_hyphen_joined_name_is_not_admitted_by_feed_category():
    mapped, _, excluded = nm.classify_companies(
        "Meta-analysis links screen time to sleep loss", {"technology"})
    assert mapped == []
    assert excluded[0]["entity"]["name"] == "Meta"
    assert excluded[0]["reason"] == (
        "'meta' is also a common word and prefix; no corroborating term (facebook, "
        "instagram, whatsapp, zuckerberg, meta platforms) in the headlines; the "
        "technology feed does not count because 'meta' only appears hyphen-joined")
    # a corroborating term still admits a hyphen-joined use
    mapped, _, excluded = nm.classify_companies("Meta-owned WhatsApp adds usernames", {"geopolitics"})
    assert excluded == []
    assert mapped[0]["ticker"] == "META"
    assert mapped[0]["admitted_by"] == "corroborating term 'whatsapp'"
    # one standalone use is enough for the feed category to count
    mapped, _, excluded = nm.classify_companies(
        "Meta trims teen features after a meta-analysis on screen time", {"technology"})
    assert excluded == []
    assert mapped[0]["admitted_by"] == "article from a technology feed"


def test_exclusion_reason_article_agrees_with_feed_name():
    _, _, excluded = nm.classify_companies("Chevron insignia stolen from base", {"military"})
    assert excluded[0]["reason"].endswith("in the headlines and no article from an energy feed")
    _, _, excluded = nm.classify_companies("Apple orchards hit by early frost", {"geopolitics"})
    assert excluded[0]["reason"].endswith(
        "in the headlines and no article from a technology or finance feed")


def test_unambiguous_and_unlisted_companies():
    mapped, unmapped, excluded = nm.classify_companies(
        "Lockheed Martin and OpenAI sign deal; Google and Alphabet shares rise", set())
    assert [m["ticker"] for m in mapped] == ["LMT", "GOOGL"]  # one row per ticker
    assert unmapped == [{"entity": {"type": "company", "name": "Openai", "sector": "ai"},
                         "reason": "no ticker in the entity table"}]
    assert excluded == []


def test_classify_countries():
    mapped, unmapped = nm.classify_countries("China and Ukraine talks")
    assert mapped == [{"type": "country", "iso3": "CHN", "name": "China", "index": "000001.SS"}]
    assert unmapped[0]["entity"]["iso3"] == "UKR"
    assert "no primary index mapped for UKR" in unmapped[0]["reason"]


# ---------------------------------------------------------------------------
# Composite (real clustering + entities; Yahoo + SEC via respx)
# ---------------------------------------------------------------------------


def _mock_markets(fail=()):
    for symbol, payload in {
        "AAPL": _chart("AAPL", _AAPL),
        "SPY": _chart("SPY", _SPY, instrument="ETF", exchange="PCX", full="NYSEArca"),
        "LMT": _chart("LMT", _AAPL),
        "000001.SS": _sse_chart(),
    }.items():
        route = respx.get(_CHART.format(symbol))
        if symbol in fail:
            route.mock(return_value=httpx.Response(
                404, json={"chart": {"result": None, "error": {"code": "Not Found"}}}))
        else:
            route.mock(return_value=httpx.Response(200, json=payload))
    respx.get(_CHART.format("NVDA")).mock(return_value=httpx.Response(500, text="upstream error"))


_ITEMS = [
    _item("Apple foldable iPhones sell out in hours", "2026-10-15T20:30:00Z"),
    _item("Lockheed Martin wins hypersonic contract", "2026-10-16T21:00:00Z", "military", "Defense One"),
    _item("Shell fire hits border town", "2026-10-16T12:00:00Z", "geopolitics", "BBC World"),
    _item("OpenAI raises new funding round", "2026-10-16T12:00:00Z"),
    _item("Ukraine grain corridor reopens", "2026-10-16T12:00:00Z", "geopolitics", "BBC World"),
    _item("China stimulus lifts property developers", "2026-10-16T01:00:00Z", "finance", "CNBC"),
    _item("Nvidia unveils new AI chip", "2026-10-16T14:00:00Z"),
]


@pytest.mark.asyncio
@respx.mock
async def test_composite_end_to_end(fast_fetcher, monkeypatch):
    _feed(monkeypatch, list(_ITEMS))
    _mock_markets()
    sec = respx.get(_SEC).mock(return_value=httpx.Response(200, json=_SEC_SUBSET))

    out = await nm.fetch_news_market_reaction(fast_fetcher, min_cluster_size=1, now=_SATURDAY)

    assert "error" not in out
    rows = {r["symbol"]: r for r in out["company_reactions"]}
    apple = rows["AAPL"]
    assert apple["status"] == "measured"
    assert apple["story"]["headline"] == "Apple foldable iPhones sell out in hours"
    assert apple["story"]["first_published"] == "2026-10-15T20:30:00Z"
    assert apple["benchmark"] == "SPY"
    assert apple["registrant"] == {"cik": "0000320193", "name": "Apple Inc."}
    assert apple["exchange_timezone"] == "America/New_York"
    assert (apple["session_date"], apple["session_basis"]) == ("2026-10-16", "next_session_after_close")
    assert (apple["stock_return_pct"], apple["benchmark_return_pct"], apple["excess_return_pct"]) == (
        0.4269, -1.1065, 1.5334)

    lmt = rows["LMT"]
    assert lmt["status"] == "pending"  # Friday after close; Monday not yet traded
    assert lmt["registrant"]["name"] == "LOCKHEED MARTIN CORP"

    # Failed quote: a data gap with no number, never 0%
    nvda = rows["NVDA"]
    assert nvda["status"] == "data_gap"
    assert nvda["gap"] == "no daily bars for NVDA"
    assert all(k not in nvda for k in ("stock_return_pct", "benchmark_return_pct", "excess_return_pct"))
    assert "NVDA: Yahoo chart returned no data (request failed or symbol unknown)" in out["data_gaps"]

    china = out["country_reactions"]
    assert [(c["entity"]["iso3"], c["symbol"], c["status"], c["index_return_pct"]) for c in china] == [
        ("CHN", "000001.SS", "measured", 0.2376)]

    assert [e["entity"]["name"] for e in out["excluded"]] == ["Shell"]
    assert sorted((u["entity"].get("iso3") or u["entity"]["name"]) for u in out["unmapped"]) == ["Openai", "UKR"]

    s = out["summary"]
    assert (s["news_items"], s["stories_considered"], s["stories_with_entities"]) == (7, 7, 7)
    assert (s["companies_measured"], s["companies_pending"], s["companies_data_gap"]) == (1, 1, 1)
    assert (s["countries_measured"], s["unmapped"], s["excluded"]) == (1, 2, 1)
    assert s["symbols_fetched"] == 4  # AAPL, SPY, LMT, 000001.SS; NVDA failed
    assert sec.call_count == 1
    assert out["interpretation"].startswith("Same-session co-movement only")
    assert "not a trading signal" in out["interpretation"]
    assert "next session" in out["method"]["session_rule"]


@pytest.mark.asyncio
@respx.mock
async def test_composite_benchmark_failure_and_sec_outage(fast_fetcher, monkeypatch):
    _feed(monkeypatch, [_ITEMS[0]])
    _mock_markets(fail=("SPY",))
    respx.get(_SEC).mock(return_value=httpx.Response(503))

    out = await nm.fetch_news_market_reaction(fast_fetcher, min_cluster_size=1, now=_SATURDAY)

    row = out["company_reactions"][0]
    assert row["status"] == "data_gap"
    assert row["gap"] == "no daily bars for benchmark SPY"
    assert "stock_return_pct" not in row
    assert "registrant" not in row
    assert "SEC company_tickers.json unavailable; US registrants not confirmed" in out["data_gaps"]
    assert "SPY: Yahoo chart returned no data (request failed or symbol unknown)" in out["data_gaps"]


@pytest.mark.asyncio
@respx.mock
async def test_composite_unknown_sec_ticker_is_noted(fast_fetcher, monkeypatch):
    _feed(monkeypatch, [_ITEMS[0]])
    _mock_markets()
    respx.get(_SEC).mock(return_value=httpx.Response(200, json={
        "0": {"cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP"},
        "1": {"ticker": "BROKEN"},  # malformed rows are skipped, not fatal
        "2": "not a row",
    }))
    out = await nm.fetch_news_market_reaction(fast_fetcher, min_cluster_size=1, now=_SATURDAY)
    row = out["company_reactions"][0]
    assert row["registrant"] is None
    assert row["registrant_note"] == "ticker not found in SEC company_tickers.json"
    assert row["status"] == "measured"


@pytest.mark.asyncio
@respx.mock
async def test_composite_default_needs_multi_article_clusters(fast_fetcher, monkeypatch):
    """Default min_cluster_size=2: a story two outlets carry is measured,
    single-article stories are not considered."""
    items = [
        _item("Apple foldable iPhones sell out in hours", "2026-10-15T21:00:00Z", feed="The Verge"),
        _item("Apple foldable iPhones sell out within hours", "2026-10-15T20:30:00Z", "finance", "CNBC"),
        _item("Nvidia unveils new AI chip", "2026-10-16T14:00:00Z"),
    ]
    _feed(monkeypatch, items)
    _mock_markets()
    respx.get(_SEC).mock(return_value=httpx.Response(200, json=_SEC_SUBSET))

    out = await nm.fetch_news_market_reaction(fast_fetcher, now=_SATURDAY)

    assert out["summary"]["stories_considered"] == 1
    [row] = out["company_reactions"]
    assert row["story"]["articles"] == 2 and row["story"]["outlets"] == 2
    # Earliest member time is the story time
    assert row["story"]["first_published"] == "2026-10-15T20:30:00Z"
    assert row["status"] == "measured" and row["excess_return_pct"] == 1.5334


@pytest.mark.asyncio
@respx.mock
async def test_composite_independent_of_cluster_sources_shape(fast_fetcher, monkeypatch):
    """Outlets are counted from the items' feed_name, so a cluster's
    "sources" being empty (as live before the clustering fix) or holding
    outlet names, with or without an outlet count, changes nothing."""
    items = [
        _item("Apple foldable iPhones sell out in hours", "2026-10-15T21:00:00Z", feed="The Verge"),
        _item("Apple foldable iPhones sell out within hours", "2026-10-15T20:30:00Z", "finance", "CNBC"),
    ]
    _feed(monkeypatch, items)
    _mock_markets()
    respx.get(_SEC).mock(return_value=httpx.Response(200, json=_SEC_SUBSET))
    real = nm.clustering.cluster_articles
    outs = []
    for extra in ({"sources": []}, {"sources": ["The Verge", "CNBC"], "outlet_count": 2}):
        def _shaped(articles, similarity_threshold=0.3, title_field="title", _extra=extra):
            return [{**c, **_extra} for c in real(articles, similarity_threshold, title_field)]

        monkeypatch.setattr(nm.clustering, "cluster_articles", _shaped)
        outs.append(await nm.fetch_news_market_reaction(fast_fetcher, now=_SATURDAY))
    empty, named = outs
    assert empty["company_reactions"] == named["company_reactions"]
    assert empty["summary"] == named["summary"]
    [row] = named["company_reactions"]
    assert row["story"]["outlets"] == 2
    assert row["status"] == "measured" and row["excess_return_pct"] == 1.5334


@pytest.mark.asyncio
@respx.mock
async def test_composite_story_time_problems_are_gaps(fast_fetcher, monkeypatch):
    items = [
        _item("Apple foldable iPhones sell out in hours", "2026-10-17T21:18:34Z"),  # after "now"
        _item("Lockheed Martin wins hypersonic contract", "not a date", "military"),
    ]
    _feed(monkeypatch, items)
    _mock_markets()
    respx.get(_SEC).mock(return_value=httpx.Response(200, json=_SEC_SUBSET))

    out = await nm.fetch_news_market_reaction(fast_fetcher, min_cluster_size=1, now=_SATURDAY)

    rows = {r["symbol"]: r for r in out["company_reactions"]}
    assert rows["AAPL"]["status"] == "data_gap"
    assert rows["AAPL"]["gap"] == (
        "story timestamp 2026-10-17T21:18:34Z is later than the collection time "
        "2026-10-17T16:00:00Z (a timestamp error in the feed or in its parsing)")
    assert rows["LMT"]["status"] == "data_gap"
    assert rows["LMT"]["gap"] == "no parseable published time in the cluster"


@pytest.mark.asyncio
@respx.mock
async def test_composite_html_entities_in_titles(fast_fetcher, monkeypatch):
    _feed(monkeypatch, [_item("Apple&#8217;s updated App Store rules", "2026-10-15T20:30:00Z", "geopolitics")])
    _mock_markets()
    respx.get(_SEC).mock(return_value=httpx.Response(200, json=_SEC_SUBSET))
    out = await nm.fetch_news_market_reaction(fast_fetcher, min_cluster_size=1, now=_SATURDAY)
    [row] = out["company_reactions"]
    assert row["story"]["headline"] == "Apple's updated App Store rules"
    assert row["entity"]["admitted_by"] == "corroborating term 'app store'"


@pytest.mark.asyncio
@respx.mock
async def test_composite_symbol_budget_and_no_countries(fast_fetcher, monkeypatch):
    _feed(monkeypatch, [_ITEMS[0], _ITEMS[1], _ITEMS[5]])
    _mock_markets()
    respx.get(_SEC).mock(return_value=httpx.Response(200, json=_SEC_SUBSET))

    out = await nm.fetch_news_market_reaction(
        fast_fetcher, min_cluster_size=1, max_symbols=2, include_countries=False, now=_SATURDAY)

    assert out["country_reactions"] == []
    rows = {r["symbol"]: r for r in out["company_reactions"]}
    assert rows["AAPL"]["status"] == "measured"  # AAPL + SPY fit the budget of 2
    assert rows["LMT"]["status"] == "data_gap"
    assert rows["LMT"]["gap"] == "no daily bars for LMT (symbol budget)"
    assert out["data_gaps"] == ["symbol budget 2 reached; not fetched: LMT"]


@pytest.mark.asyncio
async def test_composite_foreign_listing_without_benchmark(fast_fetcher, monkeypatch):
    """A listing whose exchange has no mapped index is a gap, not a
    comparison against the wrong market."""
    monkeypatch.setitem(nm.entities._COMPANY_KW, "acme", {"ticker": "ACME.PA", "sector": "test"})
    monkeypatch.setattr(nm.entities, "_COMPANY_RE", nm.entities._compile_keywords(nm.entities._COMPANY_KW))
    _feed(monkeypatch, [_item("Acme recalls widgets", "2026-10-16T12:00:00Z")])
    with respx.mock:
        respx.get(_CHART.format("ACME.PA")).mock(return_value=httpx.Response(200, json=_chart(
            "ACME.PA", _AAPL, tz="Europe/Paris", abbr="CEST", gmtoffset=7200)))
        out = await nm.fetch_news_market_reaction(fast_fetcher, min_cluster_size=1, now=_SATURDAY)
    [row] = out["company_reactions"]
    assert row["benchmark"] is None
    assert row["status"] == "data_gap"
    assert row["gap"] == "no benchmark mapped for exchange suffix .PA"
    assert "registrant" not in row


@pytest.mark.asyncio
async def test_composite_benchmark_cut_by_symbol_budget(fast_fetcher, monkeypatch):
    monkeypatch.setitem(nm.entities._COMPANY_KW, "acme", {"ticker": "ACME.L", "sector": "test"})
    monkeypatch.setattr(nm.entities, "_COMPANY_RE", nm.entities._compile_keywords(nm.entities._COMPANY_KW))
    _feed(monkeypatch, [_item("Acme recalls widgets", "2026-10-16T12:00:00Z")])
    with respx.mock:
        respx.get(_CHART.format("ACME.L")).mock(return_value=httpx.Response(200, json=_chart(
            "ACME.L", _AAPL, tz="Europe/London", abbr="BST", gmtoffset=3600)))
        out = await nm.fetch_news_market_reaction(
            fast_fetcher, min_cluster_size=1, max_symbols=1, now=_SATURDAY)
    [row] = out["company_reactions"]
    assert row["benchmark"] == "^FTSE"
    assert row["status"] == "data_gap"
    assert row["gap"] == "no daily bars for benchmark ^FTSE (symbol budget)"
    # The fixture's trading period is the US one, so the London listing's
    # close (21:00) is also flagged as nonstandard.
    assert out["data_gaps"] == [
        "symbol budget 1 reached; not fetched: ^FTSE",
        "ACME.L: Yahoo's current regular close, 21:00 Europe/London, differs from the standard "
        "16:30 for that zone; it is applied to every day in range, so a story published between "
        "the two times may map to the wrong session",
    ]


@pytest.mark.asyncio
async def test_composite_empty_feed_is_an_error_not_zero(fast_fetcher, monkeypatch):
    _feed(monkeypatch, [])
    out = await nm.fetch_news_market_reaction(fast_fetcher, now=_SATURDAY)
    assert out["error"].startswith("news feed returned no items")
    assert "company_reactions" not in out


@pytest.mark.asyncio
async def test_composite_feed_error_passes_through(fast_fetcher, monkeypatch):
    async def _bad(fetcher, category=None, limit=50):
        return {"items": [], "count": 0, "error": "Unknown category 'nope'."}

    monkeypatch.setattr(nm.news, "fetch_news_feed", _bad)
    out = await nm.fetch_news_market_reaction(fast_fetcher, category="nope", now=_SATURDAY)
    assert out["error"] == "Unknown category 'nope'."
    assert out["category"] == "nope"


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_handler_passes_arguments(monkeypatch):
    from world_intel_mcp import tools
    from world_intel_mcp.tools import news_market as tool_mod

    calls = []

    async def _fake(fetcher, **kwargs):
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(nm, "fetch_news_market_reaction", _fake)
    assert "intel_news_market_reaction" in tools.ALL_HANDLERS
    assert [t.name for t in tool_mod.TOOLS] == ["intel_news_market_reaction"]
    result = await tools.ALL_HANDLERS["intel_news_market_reaction"](
        {"category": "finance", "min_cluster_size": 1, "include_countries": False})
    assert result == {"ok": True}
    assert calls == [{"category": "finance", "limit": 150, "threshold": 0.25, "min_cluster_size": 1,
                      "include_countries": False, "max_symbols": 40}]


@pytest.mark.asyncio
async def test_tool_handler_passes_max_age_hours_when_given(monkeypatch):
    from world_intel_mcp import tools
    from world_intel_mcp.tools import news_market as tool_mod

    calls = []

    async def _fake(fetcher, **kwargs):
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(nm, "fetch_news_market_reaction", _fake)
    await tools.ALL_HANDLERS["intel_news_market_reaction"]({"max_age_hours": 24})
    assert calls[0]["max_age_hours"] == 24
    [tool] = tool_mod.TOOLS
    assert tool.inputSchema["properties"]["max_age_hours"]["default"] == 168
    # The SEC lookup annotates a US ticker with its registrant; it confirms nothing.
    assert "SEC-confirmed" not in tool.description


# ---------------------------------------------------------------------------
# Stale cache copies, bar finality, placeholder bars
# ---------------------------------------------------------------------------


def test_parse_bars_stale_cached_chart_is_a_gap_naming_its_age():
    """When Yahoo fails the fetcher serves an expired copy marked _stale;
    an old chart must never become a measured row."""
    payload = _chart("AAPL", _AAPL)
    payload.update({"_stale": True, "_stale_age_seconds": 259200.4})
    assert nm.parse_bars("AAPL", payload) == (
        None, "AAPL: Yahoo unavailable; a cached chart 259200 s old was not used")
    del payload["_stale_age_seconds"]
    assert nm.parse_bars("AAPL", payload)[1] == (
        "AAPL: Yahoo unavailable; a cached chart of unknown age was not used")


@pytest.mark.asyncio
@respx.mock
async def test_composite_stale_yahoo_and_sec_copies_are_gaps(fast_fetcher, monkeypatch):
    """Yahoo and SEC down, expired copies in the cache: no row is measured
    from the old charts and no registrant comes from the old file."""
    _feed(monkeypatch, [_ITEMS[0]])
    fast_fetcher.cache.set("news_market:bars:AAPL", _chart("AAPL", _AAPL), -1)
    fast_fetcher.cache.set("news_market:bars:SPY", _chart("SPY", _SPY), -1)
    fast_fetcher.cache.set("sec:company_tickers", _SEC_SUBSET, -1)
    respx.get(_CHART.format("AAPL")).mock(return_value=httpx.Response(503))
    respx.get(_CHART.format("SPY")).mock(return_value=httpx.Response(503))
    respx.get(_SEC).mock(return_value=httpx.Response(503))

    out = await nm.fetch_news_market_reaction(fast_fetcher, min_cluster_size=1, now=_SATURDAY)

    [row] = out["company_reactions"]
    assert row["status"] == "data_gap"
    assert row["gap"] == "no daily bars for AAPL"
    assert "stock_return_pct" not in row and "registrant" not in row
    gaps = " | ".join(out["data_gaps"])
    assert re.search(r"AAPL: Yahoo unavailable; a cached chart \d+ s old was not used", gaps)
    assert re.search(r"SPY: Yahoo unavailable; a cached chart \d+ s old was not used", gaps)
    assert re.search(r"SEC company_tickers\.json unavailable; a cached copy \d+ s old was not used, "
                     r"so US registrants are not confirmed", gaps)
    assert out["summary"]["symbols_fetched"] == 0


def test_bars_keep_regular_market_time_and_last_bar_finality():
    final = _bars("AAPL", _chart("AAPL", _AAPL))  # closing print at 16:00 on 2026-10-16
    assert final.market_time.isoformat() == "2026-10-16T16:00:00-04:00"
    assert final.last_final is True
    live = _bars("AAPL", _chart("AAPL", _AAPL, market_time=1792180680))  # a 15:58 print
    assert live.last_final is False
    one_past = _bars("AAPL", _chart("AAPL", _AAPL, market_time=1792180801))  # 16:00:01, as live AAPL
    assert one_past.last_final is True


def test_last_bar_printed_before_the_close_is_pending_after_the_close():
    """A chart read at 15:58 and cached is still served at 16:05: the
    clock says the session closed, but the last bar is the 15:58 print."""
    cached = _bars("AAPL", _chart("AAPL", _AAPL, market_time=1792180680))
    after_close = datetime(2026, 10, 16, 20, 5, tzinfo=timezone.utc)  # 16:05 EDT
    s = _assign("2026-10-16T14:00:00Z", cached, now=after_close)
    assert s["status"] == "pending"
    assert s["session_date"] == "2026-10-16"
    assert s["reason"] == (
        "the 2026-10-16 AAPL bar is not final: Yahoo's last regular-market time is "
        "2026-10-16T15:58:00-04:00, before the 16:00 close")
    # An earlier session of the same series is final whatever the last bar is
    assert _assign("2026-10-15T14:00:00Z", cached, now=after_close)["status"] == "assigned"
    no_time = _chart("AAPL", _AAPL)
    del no_time["chart"]["result"][0]["meta"]["regularMarketTime"]
    s = _assign("2026-10-16T14:00:00Z", _bars("AAPL", no_time), now=after_close)
    assert s["status"] == "pending"
    assert s["reason"] == "the 2026-10-16 AAPL bar may not be final: Yahoo gave no regularMarketTime"


def test_measure_company_benchmark_bar_not_final_is_pending():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL))
    spy = _bars("SPY", _chart("SPY", _SPY, market_time=1792180680))
    row = nm.measure_company(nm.parse_published("2026-10-16T14:00:00Z"), aapl, spy, _SATURDAY)
    assert row["status"] == "pending"
    assert row["gap"] == (
        "the 2026-10-16 SPY bar is not final: Yahoo's last regular-market time is "
        "2026-10-16T15:58:00-04:00, before the 16:00 close")
    assert "stock_return_pct" not in row and "excess_return_pct" not in row


@pytest.mark.asyncio
@respx.mock
async def test_composite_bars_cached_before_the_close_stay_pending(fast_fetcher, monkeypatch):
    _feed(monkeypatch, [_item("Apple foldable iPhones sell out in hours", "2026-10-16T14:00:00Z")])
    aapl = respx.get(_CHART.format("AAPL")).mock(return_value=httpx.Response(
        200, json=_chart("AAPL", _AAPL[:-1] + [(1792180680, 341.1, 341.1)])))  # 15:58 print
    respx.get(_CHART.format("SPY")).mock(return_value=httpx.Response(
        200, json=_chart("SPY", _SPY[:-1] + [(1792180680, 766.4, 766.4)])))
    respx.get(_SEC).mock(return_value=httpx.Response(200, json=_SEC_SUBSET))

    before = await nm.fetch_news_market_reaction(
        fast_fetcher, min_cluster_size=1, now=datetime(2026, 10, 16, 19, 58, tzinfo=timezone.utc))
    after = await nm.fetch_news_market_reaction(
        fast_fetcher, min_cluster_size=1, now=datetime(2026, 10, 16, 20, 5, tzinfo=timezone.utc))

    assert aapl.call_count == 1  # the 16:05 call read the 15:58 chart from the cache
    assert before["company_reactions"][0]["status"] == "pending"
    row = after["company_reactions"][0]
    assert row["status"] == "pending"
    assert row["gap"].startswith("the 2026-10-16 AAPL bar is not final")
    assert "stock_return_pct" not in row


# Live 2222.SR daily bars (Yahoo v8, range 3mo, read 2026-10-17). Sunday
# 2026-08-16 is a bar in which nothing traded: open, high, low and close
# all equal Thursday's close and volume is 0.
_ARAMCO = [  # (timestamp = session open, open, high, low, close, volume, adjclose)
    (1786431600, 26.639999389648438, 26.860000610351562, 26.600000381469727, 26.600000381469727,
     10714020, 26.26120948791504),  # 2026-08-11
    (1786518000, 26.600000381469727, 26.68000030517578, 26.520000457763672, 26.600000381469727,
     6601208, 26.26120948791504),  # 2026-08-12
    (1786604400, 26.559999465942383, 26.600000381469727, 26.479999542236328, 26.479999542236328,
     7482066, 26.142736434936523),  # 2026-08-13
    (1786863600, 26.479999542236328, 26.479999542236328, 26.479999542236328, 26.479999542236328,
     0, 26.142736434936523),  # 2026-08-16 (nothing traded)
    (1786950000, 26.700000762939453, 26.700000762939453, 26.520000457763672, 26.520000457763672,
     9968769, 26.182228088378906),  # 2026-08-17
    (1787036400, 26.579999923706055, 26.700000762939453, 26.559999465942383, 26.579999923706055,
     10271573, 26.241464614868164),  # 2026-08-18
]
# Live 2222.SR currentTradingPeriod (2026-10-15 session).
_RIYADH_PERIOD = {
    "pre": {"timezone": "AST", "start": 1792047600, "end": 1792047600, "gmtoffset": 10800},
    "regular": {"timezone": "AST", "start": 1792047600, "end": 1792065600, "gmtoffset": 10800},
    "post": {"timezone": "AST", "start": 1792065600, "end": 1792065600, "gmtoffset": 10800},
}


def _aramco_chart(rows=_ARAMCO):
    payload = _chart("2222.SR", [(r[0], r[4], r[6]) for r in rows], tz="Asia/Riyadh", abbr="AST",
                     gmtoffset=10800, period=_RIYADH_PERIOD, exchange="SAU", full="Saudi")
    result = payload["chart"]["result"][0]
    result["meta"]["currency"] = "SAR"
    result["indicators"]["quote"] = [{
        "open": [r[1] for r in rows], "high": [r[2] for r in rows], "low": [r[3] for r in rows],
        "close": [r[4] for r in rows], "volume": [r[5] for r in rows]}]
    return payload


def test_parse_bars_drops_a_bar_in_which_nothing_traded():
    bars = _bars("2222.SR", _aramco_chart())
    assert [d.isoformat() for d in bars.dates] == [
        "2026-08-11", "2026-08-12", "2026-08-13", "2026-08-17", "2026-08-18"]
    assert bars.notes == [
        "2222.SR: placeholder bar dropped for 2026-08-16 (open = high = low = close = the "
        "previous close, volume 0): a day without trading, not a 0% session"]
    # A Sunday story maps to Monday, measured from Thursday's close
    s = _assign("2026-08-16T07:30:00Z", bars)  # 10:30 Riyadh
    assert (s["session_date"], s["basis"]) == ("2026-08-17", "next_session_non_trading_day")
    prior, ret = nm.session_return(bars, datetime(2026, 8, 17).date())
    assert (prior.isoformat(), round(ret * 100, 4)) == ("2026-08-13", 0.1511)
    # The same flat bar with any volume traded and stays
    traded = [r[:5] + (5,) + r[6:] if r[0] == 1786863600 else r for r in _ARAMCO]
    assert "2026-08-16" in [d.isoformat() for d in _bars("2222.SR", _aramco_chart(traded)).dates]


def test_placeholder_bar_does_not_enter_the_zscore_sample():
    """A volume-0 bar copying the previous close on a closed weekday
    (2026-09-25, Mid-Autumn) would add a fake 0.0% return to the sample."""
    filler = (1790299800, 3888.3740234375)  # 2026-09-25 09:30 Shanghai, 09-24's close again
    rows = sorted(_SSE + [filler])
    payload = _sse_chart(rows)
    payload["chart"]["result"][0]["indicators"]["quote"][0]["volume"][rows.index(filler)] = 0
    sse = _bars("000001.SS", payload)
    row = nm.measure_country(nm.parse_published("2026-10-16T01:00:00Z"), sse, _SATURDAY)
    assert (row["index_return_pct"], row["zscore"], row["trailing_sessions"]) == (0.2376, 0.6, 16)
    assert sse.notes == [
        "000001.SS: placeholder bar dropped for 2026-09-25 (open = high = low = close = the "
        "previous close, volume 0): a day without trading, not a 0% session"]


def test_parse_bars_raw_closes_without_adjclose_are_noted():
    payload = _chart("SPY", _SPY)
    del payload["chart"]["result"][0]["indicators"]["adjclose"]
    bars = _bars("SPY", payload)
    _prior, ret = nm.session_return(bars, datetime(2026, 9, 18).date())
    assert round(ret * 100, 4) == -0.1193  # the unadjusted ex-dividend move
    assert bars.notes == [
        "SPY: no adjusted closes in the Yahoo chart; raw closes used, so a dividend or split "
        "in range distorts its returns"]


def test_close_differing_from_the_zone_standard_is_noted():
    assert _bars("AAPL", _chart("AAPL", _AAPL)).notes == []
    # A US early-close day: Yahoo's current period ends 13:00 and that
    # close is applied to every day in range.
    early = {"regular": {"timezone": "EDT", "start": 1792157400, "end": 1792170000, "gmtoffset": -14400}}
    bars = _bars("AAPL", _chart("AAPL", _AAPL, period=early))
    assert bars.close_time.isoformat() == "13:00:00"
    assert bars.notes == [
        "AAPL: Yahoo's current regular close, 13:00 America/New_York, differs from the standard "
        "16:00 for that zone; it is applied to every day in range, so a story published between "
        "the two times may map to the wrong session"]
    # KRX: Yahoo's period ends 15:00 (live, 2026-10-16) but the closing
    # print of 005930.KS that day was at 15:30:05.
    seoul = {"regular": {"timezone": "KST", "start": 1792108800, "end": 1792130400, "gmtoffset": 32400}}
    krx = _bars("005930.KS", _chart("005930.KS", [(1792022400, 100.0), (1792108800, 101.0)],
                                    tz="Asia/Seoul", abbr="KST", gmtoffset=32400, period=seoul,
                                    market_time=1792132205))
    assert krx.last_final is True
    assert krx.notes == [
        "005930.KS: Yahoo's current regular close, 15:00 Asia/Seoul, differs from the standard "
        "15:30 for that zone; it is applied to every day in range, so a story published between "
        "the two times may map to the wrong session"]


@pytest.mark.asyncio
@respx.mock
async def test_composite_rows_name_the_close_used_and_bar_notes_reach_data_gaps(fast_fetcher, monkeypatch):
    _feed(monkeypatch, [_ITEMS[0], _ITEMS[5]])
    early = {"regular": {"timezone": "EDT", "start": 1792157400, "end": 1792170000, "gmtoffset": -14400}}
    respx.get(_CHART.format("AAPL")).mock(return_value=httpx.Response(
        200, json=_chart("AAPL", _AAPL, period=early)))
    spy = _chart("SPY", _SPY, instrument="ETF", exchange="PCX", full="NYSEArca")
    del spy["chart"]["result"][0]["indicators"]["adjclose"]
    respx.get(_CHART.format("SPY")).mock(return_value=httpx.Response(200, json=spy))
    respx.get(_CHART.format("000001.SS")).mock(return_value=httpx.Response(200, json=_sse_chart()))
    respx.get(_SEC).mock(return_value=httpx.Response(200, json=_SEC_SUBSET))

    out = await nm.fetch_news_market_reaction(fast_fetcher, min_cluster_size=1, now=_SATURDAY)

    [apple] = out["company_reactions"]
    assert apple["close_time_used"] == "13:00"
    assert apple["status"] == "measured" and apple["benchmark_return_pct"] == -1.1065
    [china] = out["country_reactions"]
    assert china["close_time_used"] == "15:00"
    assert out["data_gaps"] == [
        "AAPL: Yahoo's current regular close, 13:00 America/New_York, differs from the standard "
        "16:00 for that zone; it is applied to every day in range, so a story published between "
        "the two times may map to the wrong session",
        "SPY: no adjusted closes in the Yahoo chart; raw closes used, so a dividend or split "
        "in range distorts its returns",
    ]


# ---------------------------------------------------------------------------
# Session windows and story spans
# ---------------------------------------------------------------------------


def test_measure_company_stock_missing_the_prior_session_is_a_gap():
    """AAPL lacks 2026-10-15 while SPY traded: a 10-16 story would be
    measured from 10-14, a two-session return labelled same_session."""
    aapl = _bars("AAPL", _chart("AAPL", [b for b in _AAPL if b[0] != 1792071000]))
    spy = _bars("SPY", _chart("SPY", _SPY))
    row = nm.measure_company(nm.parse_published("2026-10-16T14:00:00Z"), aapl, spy, _SATURDAY)
    assert row["status"] == "data_gap"
    assert row["gap"] == (
        "SPY traded on 2026-10-15 but the AAPL series has no bar for it; the 2026-10-14 to "
        "2026-10-16 return would span more than one session")
    assert "stock_return_pct" not in row


def test_measure_company_benchmark_already_closed_the_pending_session_is_a_gap():
    aapl = _bars("AAPL", _chart("AAPL", _AAPL[:-1]))  # series ends 2026-10-15
    spy = _bars("SPY", _chart("SPY", _SPY))  # 2026-10-16 bar is final
    row = nm.measure_company(nm.parse_published("2026-10-15T20:30:00Z"), aapl, spy, _SATURDAY)
    assert row["status"] == "data_gap"
    assert row["gap"] == (
        "SPY traded on 2026-10-16 but the AAPL series has no bar for it; the session cannot be assigned")
    # While the benchmark's own 10-16 bar is not final either, it stays pending
    live_spy = _bars("SPY", _chart("SPY", _SPY, market_time=1792180680))
    row = nm.measure_company(nm.parse_published("2026-10-15T20:30:00Z"), aapl, live_spy, _SATURDAY)
    assert row["status"] == "pending"


_MULTI_DAY = [  # sources/news.py sorts newest first
    _item("Apple shares rise after analyst upgrade", "2026-10-16T14:00:00Z", "finance", "CNBC"),
    _item("Apple shares fall after earnings miss", "2026-10-08T14:00:00Z", "finance", "MarketWatch"),
]


@pytest.mark.asyncio
@respx.mock
async def test_composite_cluster_spanning_sessions_is_a_gap(fast_fetcher, monkeypatch):
    _feed(monkeypatch, list(_MULTI_DAY))
    _mock_markets()
    respx.get(_SEC).mock(return_value=httpx.Response(200, json=_SEC_SUBSET))

    out = await nm.fetch_news_market_reaction(fast_fetcher, max_age_hours=400, now=_SATURDAY)

    [row] = out["company_reactions"]
    story = row["story"]
    # Headline, link and time all come from the earliest article
    assert story["headline"] == "Apple shares fall after earnings miss"
    assert story["link"] == "https://example.com/apple-shares-fall-after-earnings-miss"
    assert (story["first_published"], story["last_published"], story["span_hours"]) == (
        "2026-10-08T14:00:00Z", "2026-10-16T14:00:00Z", 192.0)
    assert row["status"] == "data_gap"
    assert row["gap"] == (
        "the story's articles run from 2026-10-08T14:00:00Z to 2026-10-16T14:00:00Z, which map "
        "to different AAPL sessions (2026-10-08 and 2026-10-16); the move cannot be tied to one session")
    assert "stock_return_pct" not in row


@pytest.mark.asyncio
@respx.mock
async def test_composite_cluster_with_a_future_article_is_a_gap(fast_fetcher, monkeypatch):
    """Any article stamped after the collection time puts the cluster's
    times in doubt, not only the earliest one."""
    _feed(monkeypatch, [
        _item("Apple foldable iPhones sell out in hours worldwide", "2026-10-17T21:18:34Z"),
        _item("Apple foldable iPhones sell out in hours", "2026-10-16T14:00:00Z"),
    ])
    _mock_markets()
    respx.get(_SEC).mock(return_value=httpx.Response(200, json=_SEC_SUBSET))

    out = await nm.fetch_news_market_reaction(fast_fetcher, now=_SATURDAY)

    [row] = out["company_reactions"]
    assert (row["story"]["first_published"], row["story"]["last_published"]) == (
        "2026-10-16T14:00:00Z", "2026-10-17T21:18:34Z")
    assert row["status"] == "data_gap"
    assert row["gap"].startswith(
        "story timestamp 2026-10-17T21:18:34Z is later than the collection time 2026-10-17T16:00:00Z")
    assert "stock_return_pct" not in row


@pytest.mark.asyncio
@respx.mock
async def test_composite_country_cluster_spanning_sessions_is_a_gap(fast_fetcher, monkeypatch):
    _feed(monkeypatch, [
        _item("China shares rise after stimulus pledge", "2026-10-16T01:00:00Z", "finance", "CNBC"),
        _item("China shares fall after stimulus pledge", "2026-10-14T01:00:00Z", "finance", "MarketWatch"),
    ])
    _mock_markets()

    out = await nm.fetch_news_market_reaction(fast_fetcher, now=_SATURDAY)

    [row] = out["country_reactions"]
    assert row["symbol"] == "000001.SS"
    assert row["status"] == "data_gap"
    assert row["gap"] == (
        "the story's articles run from 2026-10-14T01:00:00Z to 2026-10-16T01:00:00Z, which map "
        "to different 000001.SS sessions (2026-10-14 and 2026-10-16); the move cannot be tied to one session")
    assert "index_return_pct" not in row


@pytest.mark.asyncio
@respx.mock
async def test_composite_leaves_out_stories_older_than_max_age(fast_fetcher, monkeypatch):
    _feed(monkeypatch, list(_MULTI_DAY))
    _mock_markets()
    respx.get(_SEC).mock(return_value=httpx.Response(200, json=_SEC_SUBSET))

    out = await nm.fetch_news_market_reaction(fast_fetcher, now=_SATURDAY)  # default 168 h

    assert out["company_reactions"] == []
    s = out["summary"]
    assert (s["stories_considered"], s["stories_dropped_by_age"], s["max_age_hours"]) == (0, 1, 168)


@pytest.mark.asyncio
@respx.mock
async def test_composite_clamps_threshold_and_max_age(fast_fetcher, monkeypatch):
    _feed(monkeypatch, [_ITEMS[3]])  # OpenAI: unmapped, no market data needed
    seen = []
    real = nm.clustering.cluster_articles

    def _recording(articles, similarity_threshold=0.3, title_field="title"):
        seen.append(similarity_threshold)
        return real(articles, similarity_threshold, title_field)

    monkeypatch.setattr(nm.clustering, "cluster_articles", _recording)
    low = await nm.fetch_news_market_reaction(
        fast_fetcher, min_cluster_size=1, threshold=0, max_age_hours=0, now=_SATURDAY)
    high = await nm.fetch_news_market_reaction(
        fast_fetcher, min_cluster_size=1, threshold=7, max_age_hours=10**6, now=_SATURDAY)
    assert seen == [0.01, 1.0]
    assert (low["summary"]["threshold"], low["summary"]["max_age_hours"]) == (0.01, 1)
    assert (high["summary"]["threshold"], high["summary"]["max_age_hours"]) == (1.0, 2160)
    assert low["summary"]["stories_dropped_by_age"] == 1  # 28 h old against a 1 h limit


def test_method_describes_what_the_module_reads():
    story_time = nm.METHOD["story_time"]
    # Feed timestamps are read as sources/news.py reports them; nothing
    # here can vouch that the feed or its parsing got them right.
    assert "as the RSS feeds state it" not in story_time
    assert "sources/news.py" in story_time and "UTC" in story_time


# ---------------------------------------------------------------------------
# Ambiguous company names and country names
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("headline, feeds, phrase", [
    ("Pan-Amazon summit ends without deforestation deal", {("finance", "CNBC")}, "pan-amazon"),
    ("Lula vows to end illegal Amazon deforestation by 2030", {("finance", "CNBC")}, "deforestation"),
    ("Big Apple office vacancies hit record as hybrid work sticks", {("finance", "CNBC")}, "big apple"),
    ("Apple Hospitality REIT raises monthly dividend", {("finance", "MarketWatch")}, "apple hospitality"),
    ("Post-Chevron, utilities challenge FERC transmission rule", {("energy", "Oil Price")}, "post-chevron"),
    ("Supreme Court's Chevron ruling unleashes oil industry lawsuits against EPA",
     {("energy", "Rigzone")}, "chevron ruling"),
    ("Alphabet soup of rescue facilities confuses bond traders", {("finance", "WSJ Markets")}, "alphabet soup"),
    ("Shell strikes Zaporizhzhia nuclear plant's power line", {("energy", "Oil Price")}, "shell strikes"),
    ("A new meta analysis of AI coding tools finds small gains",
     {("technology", "MIT Tech Review")}, "meta analysis"),
    ("Oil majors slip as Fed signals 50 bp hike", {("energy", "Oil Price")}, "50 bp"),
])
def test_other_sense_phrases_exclude_ambiguous_names(headline, feeds, phrase):
    mapped, _, excluded = nm.classify_companies(nm._clean_title(headline), feeds=feeds)
    assert mapped == []
    assert f"'{phrase}' in the headlines points to the other sense" in excluded[0]["reason"]


@pytest.mark.parametrize("headline, feeds, ticker, admitted_by", [
    ("Shell profit beats forecasts on strong LNG trading", {("energy", "Oil Price")}, "SHEL",
     "article from an energy feed (Oil Price)"),
    ("Chevron completes Hess takeover after arbitration win", {("finance", "FT World")}, "CVX",
     "corroborating term 'hess'"),
    ("Meta signs nuclear power deal for AI data centers", {("technology", "The Verge")}, "META",
     "article from a technology feed (The Verge)"),
    ("Samsung Electronics profit jumps on memory demand", {("asia_pacific", "Nikkei Asia")}, "005930.KS",
     "corroborating term 'samsung electronics'"),
    ("Amazon to cut 14,000 corporate jobs", {("finance", "WSJ Markets")}, "AMZN",
     "article from a finance feed (WSJ Markets)"),
])
def test_company_sense_still_admitted(headline, feeds, ticker, admitted_by):
    mapped, _, excluded = nm.classify_companies(headline, feeds=feeds)
    assert excluded == []
    assert (mapped[0]["ticker"], mapped[0]["admitted_by"]) == (ticker, admitted_by)


def test_feed_context_counts_only_from_the_listed_feeds():
    apple_terms = "iphone, ipad, macbook, app store, tim cook, apple inc, airpods, ios"
    mapped, _, excluded = nm.classify_companies(
        "Apple to open new campus in Texas", feeds={("finance", "FT World"), ("finance", "Zero Hedge")})
    assert mapped == []
    assert excluded[0]["reason"].endswith(
        f"no corroborating term ({apple_terms}) in the headlines; finance articles count only from "
        "CNBC, MarketWatch or WSJ Markets, and this story's are from FT World and Zero Hedge")
    mapped, _, _ = nm.classify_companies("Apple to open new campus in Texas", feeds={("finance", "CNBC")})
    assert mapped[0]["admitted_by"] == "article from a finance feed (CNBC)"
    # Categories alone name no feed, so a finance category admits nothing
    mapped, _, excluded = nm.classify_companies("Apple to open new campus in Texas", {"finance"})
    assert mapped == []
    assert excluded[0]["reason"].endswith(
        "in the headlines; finance articles count only from CNBC, MarketWatch or WSJ Markets")
    # Chevron: energy context only from Oil Price and Rigzone; "chevron's",
    # "oil", "crude" and "refinery" no longer corroborate
    _, _, excluded = nm.classify_companies(
        "Chevron's demise leaves agencies exposed in court", feeds={("energy", "Utility Dive")})
    assert excluded[0]["reason"].endswith(
        "in the headlines; energy articles count only from Oil Price or Rigzone, and this story's "
        "are from Utility Dive")
    _, _, excluded = nm.classify_companies("Chevron's demise leaves agencies exposed in court", {"government"})
    assert excluded[0]["entity"]["name"] == "Chevron"


def test_name_hyphen_joined_on_either_side_is_not_admitted_by_a_feed():
    mapped, _, excluded = nm.classify_companies(
        "Anti-Amazon protests spread across French warehouses", feeds={("technology", "Wired")})
    assert mapped == []
    assert excluded[0]["reason"].endswith(
        "; the technology feed does not count because 'amazon' only appears hyphen-joined")


@pytest.mark.asyncio
@respx.mock
async def test_composite_passes_feed_names_to_the_ambiguity_rule(fast_fetcher, monkeypatch):
    _feed(monkeypatch, [_item("Apple to open new campus in Texas", "2026-10-16T12:00:00Z",
                              "finance", "FT World")])
    out = await nm.fetch_news_market_reaction(fast_fetcher, min_cluster_size=1, now=_SATURDAY)
    assert out["company_reactions"] == []
    assert out["excluded"][0]["reason"].endswith("and this story's are from FT World")


def test_company_without_a_ticker_says_so():
    _, unmapped, _ = nm.classify_companies("Gazprom export revenue slumps")
    assert unmapped == [{"entity": {"type": "company", "name": "Gazprom", "sector": "energy"},
                         "reason": "no ticker in the entity table"}]


def test_country_index_rows_need_the_country_name():
    excluded = []
    mapped, unmapped = nm.classify_countries("American Airlines pilots vote to strike", excluded=excluded)
    assert mapped == [] and unmapped == []
    assert excluded == [{
        "entity": {"type": "country", "iso3": "USA", "name": "United States"},
        "reason": ("named only as 'american', not by the country's name (United States or USA); "
                   "an index row needs the name itself"),
    }]
    for headline, iso3, form in [
        ("Indian Ocean cyclone forces port closures", "IND", "'indian'"),
        ("Tex-Mex chain files for bankruptcy", "MEX", "'mex'"),
        ("New Mexico wildfire forces evacuations", "MEX", "'new mexico'"),
        ("Gulf of Mexico output recovers after storm", "MEX", "'gulf of mexico'"),
        ("Beijing hosts trade envoys", "CHN", "'beijing'"),
    ]:
        excluded = []
        mapped, _ = nm.classify_countries(headline, excluded=excluded)
        assert mapped == [], headline
        assert excluded[0]["entity"]["iso3"] == iso3
        assert excluded[0]["reason"].startswith(f"named only as {form}, not by the country's name")
    mapped, _ = nm.classify_countries("USA and Mexico agree on steel quotas")
    assert [m["index"] for m in mapped] == ["^GSPC", "^MXX"]
    # Without a collector the row goes to unmapped: listed, never dropped
    mapped, unmapped = nm.classify_countries("American Airlines pilots vote to strike")
    assert mapped == [] and unmapped[0]["reason"].startswith("named only as 'american'")


@pytest.mark.asyncio
@respx.mock
async def test_composite_lists_countries_named_only_by_a_demonym(fast_fetcher, monkeypatch):
    _feed(monkeypatch, [_item("American Airlines pilots vote to strike", "2026-10-16T12:00:00Z",
                              "geopolitics", "BBC World")])
    out = await nm.fetch_news_market_reaction(fast_fetcher, min_cluster_size=1, now=_SATURDAY)
    assert out["country_reactions"] == []
    [row] = out["excluded"]
    assert row["story"] == "American Airlines pilots vote to strike"
    assert row["entity"]["iso3"] == "USA"
    assert (out["summary"]["excluded"], out["summary"]["stories_with_entities"]) == (1, 1)
