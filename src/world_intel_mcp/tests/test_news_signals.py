"""Tests for analysis/news_signals.py and the hostility classifier.

GDELT fixtures are the live ``timelinevolraw`` / ``timelinetone`` shape
captured 2026-10-17 for the query ``Iran Israel`` (timespan 7d): hourly
buckets 2026-10-15T10Z .. 2026-10-17T15Z, each volume bucket carrying
``value`` (matching articles) and ``norm`` (all articles GDELT monitored).
The base, sanction and tone values below are the captured values; the
threat / force / criticism series reuse the captured bucket dates with
values derived from the base series (no live capture of those three).

RSS items use the live ``fetch_news_feed`` item shape (title, link,
published, summary, feed_name, category, source_tier).

Gaps / not covered: GDELT resolutions other than "hour" and "15m" (both
seen live 2026-10-17; 14- and 30-day requests drew 429 on every attempt,
so whether long spans answer in "day" buckets is unmeasured, and the 15m
test's values are synthetic); real RSS feed depth, which changes hour to
hour and decides which feeds are comparable.
"""

import asyncio as asyncio_mod
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx

from world_intel_mcp import fetcher as fetcher_mod
from world_intel_mcp.analysis import news_signals as ns
from world_intel_mcp.analysis.classifier import classify_hostility
from world_intel_mcp.sources import news

# ---------------------------------------------------------------------------
# Live GDELT capture (2026-10-17, query "Iran Israel", timespan 7d)
# ---------------------------------------------------------------------------

_T0 = datetime(2026, 10, 15, 10, tzinfo=timezone.utc)
_BASE = [(27, 5098), (19, 3390), (39, 5841), (46, 7762), (50, 12055), (50, 9595), (39, 7777), (18, 5031), (30, 4316), (42, 6521), (49, 10247), (41, 8548), (26, 5548), (17, 3544), (7, 4271), (21, 4579), (23, 4146), (1, 769), (49, 7947), (35, 6915), (44, 7664), (60, 12786), (46, 9761), (40, 7951), (60, 10619), (33, 7248), (30, 4446), (35, 7831), (19, 7133), (5, 2634), (33, 8123), (4, 1571), (15, 5152), (21, 5898), (16, 3995), (44, 5931), (40, 5153), (51, 7765), (45, 5962), (20, 1878), (9, 1700), (3, 1387), (45, 4971), (72, 7503), (33, 3328), (24, 3531), (32, 4220), (53, 6763), (50, 4567), (25, 4010), (58, 8293), (52, 6437), (16, 2958), (19, 4944)]
_SANCTION = [2, 3, 2, 3, 3, 4, 3, 1, 2, 3, 4, 6, 3, 1, 0, 4, 1, 0, 5, 3, 6, 9, 6, 7, 6, 4, 1, 5, 2, 0, 5, 1, 1, 3, 1, 1, 2, 6, 5, 1, 1, 0, 29, 38, 17, 14, 19, 14, 17, 9, 12, 14, 2, 6]
_TONE = [-3.4429, -2.766, -4.1908, -3.338, -3.5459, -2.8294, -3.0918, -3.1102, -3.8054, -2.9113, -3.0198, -3.1901, -2.7817, -2.5914, -2.1026, -1.8877, -2.3993, -3.8345, -3.065, -1.5778, -3.2848, -2.2168, -2.4771, -2.5812, -2.2363, -1.314, -3.168, -2.3672, -1.35, -3.4801, -2.7073, -3.8555, -1.9097, -3.1912, -3.1254, -3.4842, -3.5892, -3.7607, -3.535, -2.9224, -3.7986, -3.6808, -3.323, -3.0679, -3.3748, -3.1541, -3.5608, -3.3522, -3.8718, -4.0552, -3.4406, -4.0055, -4.2382, -4.1623]
# Captured day totals (summed independently of the module under test).
_DAY_ARTICLES = {"2026-10-15": 493, "2026-10-16": 732, "2026-10-17": 556}
_DAY_NORM = {"2026-10-15": 95273, "2026-10-16": 150288, "2026-10-17": 72452}
_DAY_SANCTION = {"2026-10-15": 40, "2026-10-16": 79, "2026-10-17": 198}
_NOW_GDELT = datetime(2026, 10, 17, 15, 30, tzinfo=timezone.utc)


def _stamps() -> list[str]:
    return [(_T0 + timedelta(hours=i)).strftime("%Y%m%dT%H%M%SZ") for i in range(len(_BASE))]


def _volraw(title: str, values: list[int]) -> dict:
    return {
        "query_details": {"title": title, "date_resolution": "hour"},
        "timeline": [
            {
                "series": "Article Count",
                "data": [
                    {"date": d, "value": v, "norm": n}
                    for d, v, (_, n) in zip(_stamps(), values, _BASE)
                ],
            }
        ],
    }


def _tone(title: str) -> dict:
    return {
        "query_details": {"title": title, "date_resolution": "hour"},
        "timeline": [
            {
                "series": "Average Tone",
                "data": [{"date": d, "value": v} for d, v in zip(_stamps(), _TONE)],
            }
        ],
    }


_DERIVED = {
    "threat": [v // 5 for v, _ in _BASE],
    "force": [v // 2 for v, _ in _BASE],
    "criticism": [v // 10 for v, _ in _BASE],
}

# Opening words of the live 429 body (the rest names a personal contact
# address, left out of the fixture; the module never reads the body).
_RATE_LIMIT_BODY = "Please limit requests to one every 5 seconds"


def _category_of(query: str) -> str:
    for cat, terms in ns.GDELT_HOSTILITY_TERMS.items():
        if f"({' OR '.join(terms)})" in query:
            return cat
    return "articles"


def _gdelt_router(fail: set[str] | None = None, fail_times: dict | None = None):
    """respx side effect answering by mode and category; ``fail`` columns
    always 429, ``fail_times`` columns 429 that many times first."""
    fail = fail or set()
    remaining = dict(fail_times or {})
    calls: list[tuple[str, str]] = []

    def _route(request: httpx.Request) -> httpx.Response:
        mode = request.url.params["mode"]
        query = request.url.params["query"]
        col = "tone" if mode == "timelinetone" else _category_of(query)
        calls.append((col, query))
        if col in fail:
            return httpx.Response(429, text=_RATE_LIMIT_BODY)
        if remaining.get(col, 0) > 0:
            remaining[col] -= 1
            return httpx.Response(429, text=_RATE_LIMIT_BODY)
        if col == "tone":
            return httpx.Response(200, json=_tone(query))
        if col == "articles":
            return httpx.Response(200, json=_volraw(query, [v for v, _ in _BASE]))
        if col == "sanction":
            return httpx.Response(200, json=_volraw(query, _SANCTION))
        return httpx.Response(200, json=_volraw(query, _DERIVED[col]))

    return _route, calls


@pytest.fixture
def _fast(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _no_sleep(*args, **kwargs) -> None:
        return None

    # Also silences the fetcher's per-source GDELT floor, which sleeps
    # through asyncio.sleep.
    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    monkeypatch.setattr(ns, "_GDELT_RETRY_WAIT_S", 0.0)


# ---------------------------------------------------------------------------
# Hostility timeline
# ---------------------------------------------------------------------------


@respx.mock
async def test_hostility_timeline_daily_values_from_live_shape(fetcher, _fast) -> None:
    route, calls = _gdelt_router()
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(side_effect=route)

    r = await ns.fetch_hostility_timeline(fetcher, "IRN", "Israel", days=5, now=_NOW_GDELT)

    assert r["parties"][0] == {"input": "IRN", "name": "Iran", "iso3": "IRN"}
    assert r["parties"][1]["iso3"] == "ISR"
    assert r["queries"]["articles"] == "Iran Israel"
    assert r["queries"]["sanction"].startswith("Iran Israel (sanction OR sanctions OR")
    assert r["date_resolution"] == "hour"
    assert "degraded" not in r
    assert sorted(c for c, _ in calls) == sorted(
        ["articles", "threat", "sanction", "force", "criticism", "tone"]
    )

    days = {d["date"]: d for d in r["days"]}
    assert list(days) == ["2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16", "2026-10-17"]
    for day, n in _DAY_ARTICLES.items():
        assert days[day]["articles"] == n
        assert days[day]["all_monitored_articles"] == _DAY_NORM[day]
        assert days[day]["hostile"]["sanction"] == _DAY_SANCTION[day]
    # The sanction jump on 10-17 is in the captured data.
    assert days["2026-10-17"]["hostile_share"]["sanction"] == round(198 / 556, 4)
    assert days["2026-10-16"]["hostile"]["force"] == sum(v // 2 for v, _ in _BASE[14:38])
    # 10-15 starts at 10:00 (14 of 24 hours); 10-17 has 16 of the 16
    # hours elapsed at 15:30.
    assert days["2026-10-15"]["partial"] is True
    assert days["2026-10-15"]["buckets_returned"] == 14
    assert days["2026-10-16"]["partial"] is False
    assert days["2026-10-17"]["partial"] is False
    # Tone weighted by hourly article counts, hand-checked against the
    # capture: -2.6654 for 2026-10-16.
    assert days["2026-10-16"]["tone"] == -2.6654


@respx.mock
async def test_hostility_timeline_days_gdelt_did_not_return_are_null_not_zero(
    fetcher, _fast
) -> None:
    route, _ = _gdelt_router()
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(side_effect=route)

    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=5, now=_NOW_GDELT)
    days = {d["date"]: d for d in r["days"]}
    for day in ("2026-10-13", "2026-10-14"):
        assert days[day]["articles"] is None
        assert days[day]["hostile"] == {c: None for c in ns.GDELT_HOSTILITY_TERMS}
        assert days[day]["tone"] is None
        assert days[day]["buckets_returned"] == 0
    assert any("no buckets for 2 of 5" in g for g in r["data_gaps"])


@respx.mock
async def test_hostility_timeline_outage_is_not_a_peaceful_timeline(fetcher, _fast) -> None:
    route, _ = _gdelt_router(fail={"articles", "threat", "sanction", "force", "criticism", "tone"})
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(side_effect=route)

    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=5, now=_NOW_GDELT)
    assert r["degraded"] is True
    assert r["reason"] == "gdelt_fetch_failed"
    assert "error" in r
    assert r["days"] == []
    for col in ("articles", "threat", "sanction", "force", "criticism", "tone"):
        assert any(g.startswith(f"{col}:") for g in r["data_gaps"]), col


@respx.mock
async def test_hostility_timeline_one_failed_category_is_null_for_every_day(
    fetcher, _fast
) -> None:
    route, _ = _gdelt_router(fail={"force"})
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(side_effect=route)

    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    assert r["degraded"] is True
    assert r["reason"] == "gdelt_partial"
    assert any(g.startswith("force:") for g in r["data_gaps"])
    for d in r["days"]:
        assert d["hostile"]["force"] is None
        assert d["hostile_share"]["force"] is None
        assert d["hostile"]["sanction"] == _DAY_SANCTION[d["date"]]
        assert d["articles"] == _DAY_ARTICLES[d["date"]]


@respx.mock
async def test_hostility_timeline_retries_a_rate_limited_call_once(fetcher, _fast) -> None:
    # The Fetcher makes 3 attempts per call; the 4th request is this
    # module's retry pass.
    route, calls = _gdelt_router(fail_times={"sanction": 3})
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(side_effect=route)

    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    assert "degraded" not in r
    assert [c for c, _ in calls].count("sanction") == 4
    assert {d["date"]: d["hostile"]["sanction"] for d in r["days"]} == _DAY_SANCTION


@respx.mock
async def test_hostility_timeline_failed_base_never_reports_zero_articles(
    fetcher, _fast
) -> None:
    route, _ = _gdelt_router(fail={"articles"})
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(side_effect=route)

    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    assert all(d["articles"] is None for d in r["days"])
    assert all("hostile_share" not in d for d in r["days"])
    # Tone falls back to an unweighted mean when volumes are missing.
    d16 = next(d for d in r["days"] if d["date"] == "2026-10-16")
    assert d16["tone"] == round(sum(_TONE[14:38]) / 24, 4)


@respx.mock
async def test_hostility_timeline_failed_base_still_flags_partial_days(fetcher, _fast) -> None:
    # Live 2026-10-17 17:20Z: only the threat call answered, and its 10-15
    # count (hours 10-23 only) carried no partial flag, because partial
    # days were judged from the failed base series alone.
    route, _ = _gdelt_router(fail={"articles"})
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(side_effect=route)

    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    days = {d["date"]: d for d in r["days"]}
    assert days["2026-10-15"]["hostile"]["threat"] == sum(v // 5 for v, _ in _BASE[:14])
    assert days["2026-10-15"]["partial"] is True
    assert days["2026-10-15"]["hours_missing"] == [f"{h:02d}" for h in range(10)]
    assert days["2026-10-16"]["partial"] is False
    assert {d: v["buckets_returned"] for d, v in days.items()} == {
        "2026-10-15": 14,
        "2026-10-16": 24,
        "2026-10-17": 16,
    }
    assert any("judged from the threat series" in g for g in r["data_gaps"])


@respx.mock
async def test_hostility_timeline_tone_only_leaves_partial_days_unjudged(fetcher, _fast) -> None:
    # Volume calls answer 200 without a timeline (a failed call that does
    # not trip the breaker, so the tone call still runs).
    route, _ = _gdelt_router()

    def _volume_malformed(request: httpx.Request) -> httpx.Response:
        if request.url.params["mode"] == "timelinevolraw":
            return httpx.Response(200, json={"query_details": {"date_resolution": "hour"}})
        return route(request)

    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(side_effect=_volume_malformed)

    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    assert r["degraded"] is True
    assert all("partial" not in d and d["buckets_returned"] is None for d in r["days"])
    tone = {d["date"]: d["tone"] for d in r["days"]}
    assert tone["2026-10-16"] == round(sum(_TONE[14:38]) / 24, 4)
    assert any("partial days are not flagged" in g for g in r["data_gaps"])


@respx.mock
async def test_hostility_timeline_unknown_resolution_is_disclosed(fetcher, _fast) -> None:
    payload = _volraw("Iran", [v for v, _ in _BASE])
    payload["query_details"]["date_resolution"] = "15 minutes"
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(
        return_value=httpx.Response(200, json=payload)
    )
    r = await ns.fetch_hostility_timeline(fetcher, "Iran", days=3, now=_NOW_GDELT)
    assert any("'15 minutes'" in g for g in r["data_gaps"])
    assert all("partial" not in d for d in r["days"])


@respx.mock
async def test_hostility_timeline_fifteen_minute_buckets_judged_by_hours_covered(
    fetcher, _fast
) -> None:
    # Live 2026-10-17, timespan=3d: date_resolution "15m", and GDELT
    # omitted quarter-hours (79 of 96 on 10-16) while still covering every
    # hour, with the same daily total as hourly mode. Missing quarter-hours
    # are not a partial day; a missing hour is. Shape as captured, values
    # synthetic: 10-15 starts 10:00; 10-16 skips every 5th quarter-hour;
    # 10-17 skips 03:00-03:59 entirely and runs to 15:15.
    start = datetime(2026, 10, 15, 10, tzinfo=timezone.utc)
    stamps = []
    for i in range(56 + 96 + 62):
        t = start + timedelta(minutes=15 * i)
        if t.date().isoformat() == "2026-10-16" and i % 5 == 0:
            continue
        if t.date().isoformat() == "2026-10-17" and t.hour == 3:
            continue
        stamps.append(t.strftime("%Y%m%dT%H%M%SZ"))
    payload = {
        "query_details": {"title": "Iran", "date_resolution": "15m"},
        "timeline": [{"series": "Article Count",
                      "data": [{"date": d, "value": 2, "norm": 100} for d in stamps]}],
    }
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(
        return_value=httpx.Response(200, json=payload)
    )
    r = await ns.fetch_hostility_timeline(fetcher, "Iran", days=3, now=_NOW_GDELT)
    days = {d["date"]: d for d in r["days"]}
    assert r["date_resolution"] == "15m"
    assert not any("not one this server has measured" in g for g in r["data_gaps"])
    got = {d: (v["buckets_returned"], v["hours_covered"], v["partial"]) for d, v in days.items()}
    assert got == {
        "2026-10-15": (56, 14, True),
        "2026-10-16": (77, 24, False),
        # 15:30: hours 00-14 have wholly elapsed and 03 is missing; the
        # in-progress hour 15 does not make up for it.
        "2026-10-17": (58, 15, True),
    }
    assert days["2026-10-15"]["hours_missing"] == [f"{h:02d}" for h in range(10)]
    assert days["2026-10-17"]["hours_missing"] == ["03"]
    assert "hours_missing" not in days["2026-10-16"]
    assert days["2026-10-16"]["articles"] == 154


def test_country_resolution_and_query_quoting() -> None:
    assert ns._resolve_country("irn") == ("Iran", "IRN")
    assert ns._resolve_country("Iranian") == ("Iran", "IRN")
    assert ns._resolve_country("usa") == ("United States", "USA")
    assert ns._resolve_country("Atlantis") == ("Atlantis", None)
    assert ns._gdelt_term("United States") == '"United States"'
    assert ns._gdelt_term("Iran") == "Iran"


@respx.mock
async def test_hostility_timeline_unknown_country_searched_verbatim_and_flagged(
    fetcher, _fast
) -> None:
    route, calls = _gdelt_router()
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(side_effect=route)
    r = await ns.fetch_hostility_timeline(fetcher, "Saudi Arabia", "USA", days=3, now=_NOW_GDELT)
    assert r["queries"]["articles"] == '"Saudi Arabia" "United States"'
    assert any("'Saudi Arabia' is not in the country table" in g for g in r["data_gaps"])


@respx.mock
async def test_hostility_timeline_marks_today_in_progress(fetcher, _fast) -> None:
    route, _ = _gdelt_router()
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(side_effect=route)
    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    flags = {d["date"]: d["in_progress"] for d in r["days"]}
    assert flags == {"2026-10-15": False, "2026-10-16": False, "2026-10-17": True}
    assert "in_progress" in r["definitions"]


@respx.mock
async def test_hostility_timeline_clamps_days_and_names_unreturned_days(fetcher, _fast) -> None:
    route, calls = _gdelt_router()
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(side_effect=route)
    r = await ns.fetch_hostility_timeline(fetcher, "Iran", days=500, now=_NOW_GDELT)
    assert r["days_requested"] == 90
    assert len(r["days"]) == 90
    assert all(req.url.params["timespan"] == "90d" for req, _ in respx.calls)
    # The capture holds 3 days; the other 87 are null and named.
    assert any("no buckets for 87 of 90" in g for g in r["data_gaps"])
    assert sum(1 for d in r["days"] if d["articles"] is None) == 87


@respx.mock
async def test_hostility_timeline_malformed_payload_is_a_failed_call(fetcher, _fast) -> None:
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(
        return_value=httpx.Response(200, json={"timeline": "unavailable"})
    )
    r = await ns.fetch_hostility_timeline(fetcher, "Iran", days=3, now=_NOW_GDELT)
    assert r["reason"] == "gdelt_fetch_failed"
    assert r["days"] == []


def test_series_by_day_skips_malformed_buckets() -> None:
    days, resolution = ns._series_by_day(
        {
            "query_details": {"date_resolution": "hour"},
            "timeline": [
                {
                    "data": [
                        {"date": "2026", "value": 3},
                        {"date": "20261016T010000Z", "value": "n/a"},
                        {"date": "20261016T020000Z", "value": 4},
                        {"date": "20261017T000000Z", "value": 0},
                    ]
                }
            ],
        }
    )
    assert resolution == "hour"
    assert {d: [p["value"] for p in pts] for d, pts in days.items()} == {
        "2026-10-16": [4],
        "2026-10-17": [0],
    }
    assert ns._series_by_day({"timeline": []}) == ({}, None)


@respx.mock
async def test_hostility_timeline_spaces_live_calls_and_not_cache_hits(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    waits: list[float] = []

    async def _record(delay, *args, **kwargs) -> None:
        waits.append(delay)

    monkeypatch.setattr(asyncio_mod, "sleep", _record)
    route, calls = _gdelt_router()
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(side_effect=route)

    await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    # Six live calls: the first goes at once, each later one waits out
    # GDELT's one-per-5-s floor.
    assert len(calls) == 6
    assert len(waits) == 5
    assert all(4.5 < w <= fetcher_mod._SOURCE_RATE_LIMITS["gdelt"] for w in waits)

    again = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    assert len(calls) == 6 and len(waits) == 5
    assert again["days"][1]["articles"] == _DAY_ARTICLES["2026-10-16"]


@respx.mock
async def test_gdelt_search_and_timeline_share_one_rate_limit(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Every GDELT caller goes through the fetcher's per-source floor, so in
    # one process an intel_gdelt_search call and the timeline's calls cannot
    # burst together (GDELT's 429 text: "one every 5 seconds"). Across
    # processes the floor holds through shared_floor (test_gdelt_floor.py).
    waits: list[float] = []

    async def _record(delay, *args, **kwargs) -> None:
        waits.append(delay)

    monkeypatch.setattr(asyncio_mod, "sleep", _record)
    route, _ = _gdelt_router()
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(side_effect=route)

    assert fetcher_mod._SOURCE_RATE_LIMITS["gdelt"] == 5.0
    await news.fetch_gdelt_search(fetcher, "Iran")
    assert waits == []
    await ns.fetch_hostility_timeline(fetcher, "Iran", days=3, now=_NOW_GDELT)
    # All six timeline calls wait, the first one included: it follows the
    # search.
    assert len(waits) == 6
    assert all(4.5 < w <= 5.0 for w in waits)


def test_parse_ts_rejects_naive_and_garbage() -> None:
    assert ns._parse_ts("2026-10-17T10:00:00+02:00") == datetime(2026, 10, 17, 8, tzinfo=timezone.utc)
    assert ns._parse_ts("2026-10-17T10:00:00Z") == datetime(2026, 10, 17, 10, tzinfo=timezone.utc)
    assert ns._parse_ts("2026-10-17T10:00:00") is None
    assert ns._parse_ts("yesterday") is None
    assert ns._parse_ts(None) is None
    assert ns._parse_ts(1760695200) is None


# ---------------------------------------------------------------------------
# RSS fixtures (live item shape)
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 10, 17, 16, 0, tzinfo=timezone.utc)


def _item(title: str, feed: str, hours_ago: float | None, link: str | None = None) -> dict:
    published = None
    if hours_ago is not None:
        published = (_NOW - timedelta(hours=hours_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "title": title,
        "link": link or f"https://example.org/{feed.replace(' ', '-')}/{abs(hash(title))}",
        "published": published,
        "summary": "",
        "feed_name": feed,
        "category": "geopolitics",
        "source_tier": news.SOURCE_TIERS.get(feed, "unknown"),
    }


_SITUATION_ITEMS = [
    # BBC World: oldest item 76 h old, so it is comparable across 24 h windows.
    _item("Russia launches missile strikes on Kyiv as Ukraine war grinds on", "BBC World", 2),
    _item("Iran warns Israel against strikes on its nuclear sites", "BBC World", 3),
    _item("Russia and Ukraine open talks in Geneva", "BBC World", 30),
    _item("Chocolate festival opens in Bruges", "BBC World", 76),
    # NPR World keeps only 8 h of history: not comparable.
    _item("Russian drone attack hits Ukrainian grid", "NPR World", 1, link="https://npr.example/1"),
    _item("Russian drone attack hits Ukrainian grid", "NPR World", 1, link="https://npr.example/1"),
    _item("Quiet day at the zoo", "NPR World", 8),
    # Al Jazeera: comparable.
    _item("Mexico braces for strikes as budget protests spread", "Al Jazeera", 4),
    _item("Mexico unveils new budget plan", "Al Jazeera", 5),
    _item("Mexico budget vote delayed", "Al Jazeera", 26),
    _item("Old story", "Al Jazeera", 60),
    # Undated and future-stamped items are excluded and counted.
    _item("India and Pakistan trade fire", "Al Jazeera", None),
    _item("India and Pakistan trade fire again", "Al Jazeera", -5),
]


def _feed(items: list[dict], **extra) -> dict:
    out = {"items": items, "count": len(items), "feeds_ok": 3, "feeds_failed": [], "feeds_attempted": 3}
    out.update(extra)
    return out


def _patch_feed(monkeypatch: pytest.MonkeyPatch, payload: dict) -> None:
    async def _fake(fetcher, category=None, limit=50):
        return payload

    monkeypatch.setattr(news, "fetch_news_feed", _fake)


# ---------------------------------------------------------------------------
# Situations
# ---------------------------------------------------------------------------


async def test_situations_counts_pairs_inside_and_change(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_feed(monkeypatch, _feed(_SITUATION_ITEMS))
    r = await ns.fetch_situations(fetcher, min_articles=1, now=_NOW)

    by_key = {s["key"]: s for s in r["situations"]}
    rus_ukr = by_key["RUS-UKR"]
    assert rus_ukr["kind"] == "between"
    # BBC missile strike + NPR drone attack (the NPR duplicate link counts once).
    assert rus_ukr["articles"] == 2
    assert rus_ukr["hostile_articles"] == 2
    assert rus_ukr["by_category"]["force"] == 2
    assert rus_ukr["outlet_count"] == 2
    # Change uses comparable feeds only: BBC 1 now vs BBC 1 prior.
    assert (rus_ukr["current_comparable"], rus_ukr["prior"], rus_ukr["change"]) == (1, 1, 0)
    assert rus_ukr["change_ratio"] == 1.0

    irn_isr = by_key["IRN-ISR"]
    assert irn_isr["by_category"]["threat"] == 1
    assert irn_isr["by_category"]["force"] == 1

    mex = by_key["MEX"]
    assert mex["kind"] == "inside"
    assert mex["articles"] == 2
    # A labour strike is not force.
    assert mex["hostile_articles"] == 0
    assert (mex["current_comparable"], mex["prior"], mex["change"]) == (2, 1, 1)
    assert mex["change_ratio"] == 1.5

    assert "IND-PAK" not in by_key
    cov = r["coverage"]
    assert cov["undated_items"] == 1
    assert cov["future_dated_items"] == 1
    assert cov["feeds_not_comparable"] == ["NPR World"]
    assert cov["items_current_window_comparable"] == 4
    assert any("NPR World" in g and "excluded from change" in g for g in r["data_gaps"])
    assert r["window"]["prior"] == {"start": "2026-10-15T16:00:00Z", "end": "2026-10-16T16:00:00Z"}
    assert "change_ratio" in r["definitions"]["change"]


async def test_situations_sorts(fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_feed(monkeypatch, _feed(_SITUATION_ITEMS))

    async def _keys(**kw):
        r = await ns.fetch_situations(fetcher, min_articles=1, now=_NOW, **kw)
        return [s["key"] for s in r["situations"]]

    assert await _keys(sort="covered") == ["RUS-UKR", "MEX", "IRN-ISR"]
    assert await _keys(sort="hostile") == ["RUS-UKR", "IRN-ISR", "MEX"]
    assert await _keys(sort="change") == ["MEX", "IRN-ISR", "RUS-UKR"]
    assert await _keys(kind="inside") == ["MEX"]
    assert await _keys(kind="between", limit=1) == ["RUS-UKR"]
    default = await ns.fetch_situations(fetcher, now=_NOW)
    assert [s["key"] for s in default["situations"]] == ["RUS-UKR", "MEX"]


async def test_situations_leaders_map_to_countries_and_failed_feeds_are_named(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    items = [
        _item("Trump threatens Xi with new tariffs", "BBC World", 2),
        _item("Putin rebukes Zelenskyy over drone attack", "Al Jazeera", 3),
        _item("Israeli strike kills five in Gaza", "BBC World", 4),
        # NPR World keeps 30 h of history: not comparable, so its
        # prior-window item must not count as prior coverage.
        _item("Putin and Zelenskyy trade accusations", "NPR World", 1),
        _item("Russia and Ukraine trade prisoners", "NPR World", 30),
        _item("Archive item", "BBC World", 70),
        _item("Archive item", "Al Jazeera", 70),
    ]
    _patch_feed(monkeypatch, _feed(items, feeds_failed=["CFR", "DW News"]))
    r = await ns.fetch_situations(fetcher, min_articles=1, now=_NOW)
    by_key = {s["key"]: s for s in r["situations"]}

    usa_chn = by_key["CHN-USA"]
    assert usa_chn["kind"] == "between"
    assert usa_chn["by_category"] == {"threat": 1, "sanction": 1, "force": 0, "criticism": 0}

    rus_ukr = by_key["RUS-UKR"]
    assert rus_ukr["articles"] == 2
    assert rus_ukr["by_category"] == {"threat": 0, "sanction": 0, "force": 1, "criticism": 2}
    assert (rus_ukr["current_comparable"], rus_ukr["prior"], rus_ukr["change"]) == (1, 0, 1)

    assert by_key["ISR-PSE"]["by_category"]["force"] == 1
    assert r["coverage"]["feeds_failed"] == ["CFR", "DW News"]
    assert any(
        g.startswith("2 RSS feeds failed or timed out and are absent: CFR, DW News")
        for g in r["data_gaps"]
    )


async def test_situations_never_compare_a_feed_served_stale(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # BBC World is the only comparable feed carrying Russia and Ukraine in
    # both windows; served from an old cached copy, it would fake a drop.
    _patch_feed(monkeypatch, _feed(_SITUATION_ITEMS, feeds_stale=["BBC World"]))
    r = await ns.fetch_situations(fetcher, min_articles=1, now=_NOW)
    rus_ukr = next(s for s in r["situations"] if s["key"] == "RUS-UKR")
    assert rus_ukr["articles"] == 2
    assert (rus_ukr["current_comparable"], rus_ukr["prior"]) == (0, 0)
    assert r["coverage"]["feeds_stale"] == ["BBC World"]
    assert "BBC World" not in r["coverage"]["feeds_not_comparable"]
    assert r["coverage"]["feeds_comparable"] == 1
    assert any(g.startswith("1 feeds failed live and were served from an expired cache copy")
               and g.endswith("BBC World") for g in r["data_gaps"])


async def test_windows_out_of_range_are_clamped_not_silently_empty(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A negative window used to invert the windows and drop every item,
    # which read as "no situations".
    recent = _item("Russia and Ukraine swap prisoners after drone attack", "BBC World", 0.5)
    _patch_feed(monkeypatch, _feed(_SITUATION_ITEMS + [recent]))
    r = await ns.fetch_situations(fetcher, hours=-5, min_articles=1, now=_NOW)
    assert r["window"]["hours"] == 1.0
    assert r["window"]["current"] == {"start": "2026-10-17T15:00:00Z", "end": "2026-10-17T16:00:00Z"}
    # Only the 30-minute-old item is inside (15:00, 16:00]; the NPR item
    # stamped exactly 15:00 belongs to the prior window.
    assert [(s["key"], s["articles"]) for s in r["situations"]] == [("RUS-UKR", 1)]
    wide = await ns.fetch_top_stories(fetcher, hours=10_000, min_outlets=0, now=_NOW)
    assert wide["window"]["hours"] == 168.0
    assert wide["min_outlets"] == 1


async def test_situations_names_undated_items_by_feed(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    items = [
        _item("Iran warns Israel", "BBC World", 1),
        _item("Japan and China hold talks", "Nikkei Asia", None),
        _item("China exports rise", "Nikkei Asia", None),
    ]
    _patch_feed(monkeypatch, _feed(items))
    r = await ns.fetch_situations(fetcher, min_articles=1, now=_NOW)
    assert r["coverage"]["undated_items"] == 2
    assert any("2 items had no parseable publish time" in g and "Nikkei Asia 2" in g
               for g in r["data_gaps"])


async def test_situations_outage_is_not_a_quiet_world(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_feed(
        monkeypatch,
        {
            "items": [],
            "count": 0,
            "feeds_attempted": 119,
            "feeds_ok": 0,
            "feeds_failed": ["BBC World"],
            "error": "All 119 RSS feeds failed or timed out",
            "degraded": True,
            "reason": "all_feeds_failed",
        },
    )
    r = await ns.fetch_situations(fetcher, now=_NOW)
    assert r["situations"] == []
    assert r["degraded"] is True
    assert r["reason"] == "all_feeds_failed"
    assert r["error"] == "All 119 RSS feeds failed or timed out"
    assert r["data_gaps"]


# ---------------------------------------------------------------------------
# Top stories
# ---------------------------------------------------------------------------

_POPE = [
    ("Pope Leo leaves hospital after lung cancer surgery", "Al Jazeera"),
    ("Smiling pope returns to Vatican after cancer surgery", "Dawn Pakistan"),
    ("Pope Leo XIV leaves hospital days after lung cancer surgery", "NPR World"),
    ("Pope discharged from hospital days after lung cancer surgery", "BBC World"),
    ("Pope Leo discharged from hospital after cancer operation", "The Guardian World"),
    ("Vatican says Pope Leo to be released from the hospital after lung cancer surgery", "Times of Israel"),
    ("Pope Leo leaves hospital 4 days after lung cancer surgery", "SCMP"),
    ("Pope Leo leaves hospital after lung cancer surgery, Vatican says", "BBC Mundo"),
]


def _top_items() -> list[dict]:
    items = [_item(t, f, 1 + i * 0.5) for i, (t, f) in enumerate(_POPE)]
    # Comparable history for two feeds, and an attention signal: Yemen
    # twice now and never before in those feeds; UN once in each window.
    items += [
        _item("Saudi strikes on Yemen kill 12, Houthis say", "BBC World", 2.5),
        _item("Yemen ceasefire talks stall", "Al Jazeera", 3.5),
        _item("UN warns of famine", "BBC World", 4.5),
        _item("UN budget talks resume", "BBC World", 30),
        _item("Archive item", "BBC World", 70),
        _item("Archive item two", "Al Jazeera", 70),
    ]
    return items


async def test_top_stories_ranks_by_distinct_outlets(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_feed(monkeypatch, _feed(_top_items()))
    r = await ns.fetch_top_stories(fetcher, min_outlets=3, now=_NOW)
    assert r["count"] == 1
    story = r["stories"][0]
    assert story["articles"] == 8
    # BBC World and BBC Mundo are one outlet.
    assert story["outlet_count"] == 7
    assert "BBC" in story["outlets"] and "BBC Mundo" not in story["outlets"]
    assert story["first_seen"] == "2026-10-17T11:30:00Z"
    assert story["last_seen"] == "2026-10-17T15:00:00Z"
    assert "hospital" in story["keywords"]

    stricter = await ns.fetch_top_stories(fetcher, min_outlets=8, now=_NOW)
    assert stricter["stories"] == []
    assert "degraded" not in stricter


async def test_top_stories_attention_growth(fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_feed(monkeypatch, _feed(_top_items()))
    r = await ns.fetch_top_stories(fetcher, now=_NOW)
    top = r["attention"][0]
    assert top == {
        "type": "country",
        "name": "Yemen",
        "iso3": "YEM",
        "current_comparable": 2,
        "prior": 0,
        "change": 2,
        "change_ratio": 3.0,
    }
    # UN: 1 now, 1 before: no growth, not listed.
    assert all(a["name"] != "UN" for a in r["attention"])


async def test_top_stories_attention_counts_a_leader_for_their_country(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    items = _top_items() + [_item("Trump threatens Xi with new tariffs", "BBC World", 1.5)]
    _patch_feed(monkeypatch, _feed(items))
    r = await ns.fetch_top_stories(fetcher, now=_NOW)
    grew = {a.get("iso3"): a["change"] for a in r["attention"] if a["type"] == "country"}
    assert grew["USA"] == 1 and grew["CHN"] == 1
    assert "through a leader" in r["definitions"]["attention"]


async def test_top_stories_outage_is_not_a_slow_news_day(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_feed(
        monkeypatch,
        {"items": [], "count": 0, "error": "All 3 RSS feeds failed or timed out",
         "degraded": True, "reason": "all_feeds_failed", "feeds_failed": ["A", "B", "C"]},
    )
    r = await ns.fetch_top_stories(fetcher, now=_NOW)
    assert r["stories"] == [] and r["attention"] == []
    assert r["degraded"] is True
    assert r["reason"] == "all_feeds_failed"


# ---------------------------------------------------------------------------
# Hostility classifier (headline heuristic)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "title,expected",
    [
        # Live headlines, 2026-10-17.
        ("Saudi strikes on Yemen kill 12, Houthis say", {"force"}),
        ("Trump waives new Russia sanctions as Ukraine ties slump", {"sanction"}),
        ("Belgium condemns Israeli settler attack on European diplomats in West Bank", {"criticism", "force"}),
        ("Freeland Warns US Alliances Are Fraying", {"threat"}),
        ("Pope Leo leaves hospital after lung cancer surgery", set()),
        # Patterns pinned against idiom.
        ("Warsaw court issues warrant", set()),
        ("Raiders beat Chiefs in overtime", set()),
        ("Chelsea attacker signs new deal", set()),
        ("Mexico braces for strikes as budget protests spread", set()),
        ("Israeli strike kills five in Gaza", {"force"}),
        ("Russia launches new offensive near Kharkiv", {"force"}),
        ("Senator apologizes for offensive remarks", set()),
        ("China and US escalate trade war", {"sanction"}),
        ("Trade warning from IMF", set()),
        ("Price war hits airline stocks", set()),
        ("War of words over Arctic claims", set()),
        ("Apple blames weak demand for slump", set()),
        ("Putin and Zelenskyy trade accusations", {"criticism"}),
    ],
)
def test_classify_hostility(title: str, expected: set) -> None:
    assert set(classify_hostility(title)) == expected


def test_classify_hostility_returns_matched_keywords() -> None:
    assert classify_hostility("Iran warns Israel against strikes on its nuclear sites") == {
        "threat": ["warns"],
        "force": ["strikes on"],
    }
    assert classify_hostility("") == {}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

from click.testing import CliRunner  # noqa: E402

from world_intel_mcp import cli  # noqa: E402


@pytest.fixture
def _cli_env(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "cli-cache.db"))
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.setattr(cli, "_fetcher", None)


def _invoke(*args: str):
    return CliRunner().invoke(cli.main, list(args), catch_exceptions=False)


def _fake(payload, calls: list | None = None):
    async def _f(*args, **kwargs):
        if calls is not None:
            calls.append(kwargs)
        return payload

    return _f


def test_cli_hostility_renders_null_as_na_never_zero(
    _cli_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list = []
    payload = {
        "parties": [{"name": "Iran"}, {"name": "Israel"}],
        "days_requested": 2,
        "date_resolution": "hour",
        "days": [
            {"date": "2026-10-16", "articles": 732, "partial": False, "tone": -2.6654,
             "hostile": {"threat": 140, "sanction": 79, "force": None, "criticism": 70}},
            {"date": "2026-10-17", "articles": None, "partial": True, "tone": None,
             "hostile": {"threat": None, "sanction": None, "force": None, "criticism": None}},
        ],
        "data_gaps": ["force: GDELT volume timeline fetch failed; column is null"],
    }
    monkeypatch.setattr(ns, "fetch_hostility_timeline", _fake(payload, calls))
    result = _invoke("hostility", "Iran", "Israel", "--days", "2")
    assert result.exit_code == 0
    assert calls == [{"country": "Iran", "other": "Israel", "days": 2}]
    assert "Iran / Israel" in result.output
    row16 = next(line for line in result.output.splitlines() if "2026-10-16" in line)
    assert row16.split()[1:] == ["732", "140", "79", "n/a", "70", "-2.6654"]
    row17 = next(line for line in result.output.splitlines() if "2026-10-17" in line)
    assert " 0 " not in row17 and row17.count("n/a") == 6
    assert "2026-10-17 *" in result.output
    assert "Gap:" in result.output and "force" in result.output


def test_cli_hostility_marks_the_day_in_progress(
    _cli_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = {
        "parties": [{"name": "Iran"}],
        "days_requested": 1,
        "date_resolution": "hour",
        "days": [
            {"date": "2026-10-17", "articles": 556, "partial": False, "in_progress": True,
             "tone": -3.5253, "hostile": {"threat": 205, "sanction": 198, "force": 460,
                                          "criticism": 61}},
        ],
        "data_gaps": [],
    }
    monkeypatch.setattr(ns, "fetch_hostility_timeline", _fake(payload))
    result = _invoke("hostility", "Iran", "-d", "1")
    assert result.exit_code == 0
    row = next(line for line in result.output.splitlines() if "2026-10-17" in line)
    assert row.split() == ["2026-10-17", "~", "556", "205", "198", "460", "61", "-3.5253"]
    assert "~ day in progress" in result.output


def test_cli_hostility_outage_prints_error(_cli_env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ns, "fetch_hostility_timeline",
        _fake({"error": "Every GDELT timeline call failed", "days": [], "degraded": True}),
    )
    result = _invoke("hostility", "Iran")
    assert result.exit_code == 0
    assert "Error:" in result.output
    assert "Every GDELT timeline call failed" in result.output


def test_cli_situations_passes_options_and_escapes(
    _cli_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list = []
    payload = {
        "window": {"current": {"start": "2026-10-16T16:00:00Z", "end": "2026-10-17T16:00:00Z"}},
        "situations": [
            {"kind": "between", "countries": [{"name": "Russia"}, {"name": "United States"}],
             "articles": 13, "hostile_articles": 12, "change": 1, "outlet_count": 12,
             "sample_headlines": [{"title": "Trump waives [/] Russia sanctions"}]},
        ],
        "data_gaps": [],
    }
    monkeypatch.setattr(ns, "fetch_situations", _fake(payload, calls))
    result = _invoke("situations", "--sort", "hostile", "--kind", "between", "-n", "5", "-H", "12")
    assert result.exit_code == 0
    assert calls == [{"hours": 12.0, "sort": "hostile", "kind": "between", "limit": 5}]
    assert "Russia / United States" in result.output
    assert "Trump waives [/] Russia sanctions" in result.output
    assert "+1" in result.output


def test_cli_top_stories_renders_stories_and_attention(
    _cli_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list = []
    payload = {
        "stories": [{"headline": "Pope Leo leaves [hospital]", "outlet_count": 7, "articles": 8}],
        "attention": [{"name": "Yemen", "type": "country", "current_comparable": 4,
                       "prior": 0, "change": 4}],
        "data_gaps": ["32 feeds keep no history"],
    }
    monkeypatch.setattr(ns, "fetch_top_stories", _fake(payload, calls))
    result = _invoke("top-stories", "-m", "4", "-n", "3")
    assert result.exit_code == 0
    assert calls == [{"hours": 24.0, "min_outlets": 4, "limit": 3}]
    assert "Pope Leo leaves [hospital]" in result.output
    assert "Yemen" in result.output and "+4" in result.output
    assert "32 feeds keep no history" in result.output


def test_cli_top_stories_json_output_is_raw(_cli_env, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"stories": [], "attention": [], "degraded": True, "error": "All 119 RSS feeds failed"}
    monkeypatch.setattr(ns, "fetch_top_stories", _fake(payload))
    result = _invoke("--json-output", "top-stories")
    assert result.exit_code == 0
    assert '"degraded": true' in result.output


# ---------------------------------------------------------------------------
# Review fixes (cloud review of 3c0ebff, 2026-10-17): expired GDELT copies,
# per-column partial days, unmeasured change, retry spacing, odd inputs.
# ---------------------------------------------------------------------------

import sqlite3  # noqa: E402
import time  # noqa: E402
from collections import Counter  # noqa: E402

from world_intel_mcp.analysis import clustering  # noqa: E402

_COLUMNS = ("articles", "threat", "sanction", "force", "criticism", "tone")
_GDELT_URL = r".*api\.gdeltproject\.org.*"


def _expire_gdelt_copies(fetcher, age_s: float) -> int:
    """Turn every cached GDELT timeline into an expired copy fetched
    ``age_s`` ago: what the cache holds when no collector evicts it."""
    now = time.time()
    conn = sqlite3.connect(str(fetcher.cache.db_path))
    try:
        cur = conn.execute(
            "UPDATE cache SET expires_at = ?, created_at = ? WHERE key LIKE ?",
            (now - 1, now - age_s, "news_signals:gdelt:%"),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def _column(request: httpx.Request) -> str:
    if request.url.params["mode"] == "timelinetone":
        return "tone"
    return _category_of(request.url.params["query"])


def _volraw_dropping(title: str, values: list[int], drop) -> dict:
    """The live volume shape without the buckets whose stamp ``drop`` picks."""
    payload = _volraw(title, values)
    series = payload["timeline"][0]
    series["data"] = [p for p in series["data"] if not drop(p["date"])]
    return payload


@respx.mock
async def test_hostility_timeline_expired_copies_are_never_shown_as_current(
    fetcher, _fast
) -> None:
    # Review H1: every live call fails, so the fetcher serves each column's
    # expired cached copy (marked _stale). The timeline used to return those
    # copies as current counts with no gap, marker or degraded flag.
    route, _ = _gdelt_router()
    down = {"on": False}

    def _answer(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503) if down["on"] else route(request)

    respx.get(url__regex=_GDELT_URL).mock(side_effect=_answer)
    first = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    assert first["column_status"] == {c: "ok" for c in _COLUMNS}
    assert first["stale_age_seconds"] == {}
    assert _expire_gdelt_copies(fetcher, age_s=72 * 3600) == 6
    down["on"] = True

    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    assert r["degraded"] is True
    assert r["reason"] == "gdelt_stale"
    assert "expired cached copy" in r["error"]
    assert r["days"] == []
    assert r["column_status"] == {c: "stale" for c in _COLUMNS}
    assert sorted(r["stale_age_seconds"]) == sorted(_COLUMNS)
    assert all(abs(a - 72 * 3600) < 120 for a in r["stale_age_seconds"].values())
    for col in _COLUMNS:
        assert any(
            g.startswith(f"{col}:") and "expired cached copy (72.0 h old)" in g
            for g in r["data_gaps"]
        ), col


@respx.mock
async def test_hostility_timeline_one_expired_column_is_null_and_named(fetcher, _fast) -> None:
    route, _ = _gdelt_router()
    force_down = {"on": False}

    def _answer(request: httpx.Request) -> httpx.Response:
        if force_down["on"] and _column(request) == "force":
            return httpx.Response(503)
        return route(request)

    respx.get(url__regex=_GDELT_URL).mock(side_effect=_answer)
    await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    _expire_gdelt_copies(fetcher, age_s=26 * 3600)
    force_down["on"] = True

    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    assert r["degraded"] is True
    assert r["reason"] == "gdelt_stale"
    assert "error" not in r
    assert r["column_status"] == dict({c: "ok" for c in _COLUMNS}, force="stale")
    assert list(r["stale_age_seconds"]) == ["force"]
    for d in r["days"]:
        assert d["hostile"]["force"] is None
        assert d["hostile_share"]["force"] is None
        assert d["articles"] == _DAY_ARTICLES[d["date"]]
        assert d["hostile"]["sanction"] == _DAY_SANCTION[d["date"]]
    assert any(
        g.startswith("force:") and "expired cached copy (26.0 h old)" in g for g in r["data_gaps"]
    )


@respx.mock
async def test_hostility_timeline_retry_pass_replaces_an_expired_copy(fetcher, _fast) -> None:
    # The initial force call falls back to its expired copy; the retry pass
    # asks again and the fresh answer (different values) replaces it.
    route, _ = _gdelt_router()
    second = {"on": False, "force_429s": 3}
    fresh_force = [v * 3 for v in _DERIVED["force"]]

    def _answer(request: httpx.Request) -> httpx.Response:
        if second["on"] and _column(request) == "force":
            if second["force_429s"] > 0:
                second["force_429s"] -= 1
                return httpx.Response(429, text=_RATE_LIMIT_BODY)
            return httpx.Response(200, json=_volraw(request.url.params["query"], fresh_force))
        return route(request)

    respx.get(url__regex=_GDELT_URL).mock(side_effect=_answer)
    await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    _expire_gdelt_copies(fetcher, age_s=26 * 3600)
    second["on"] = True

    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    assert "degraded" not in r
    assert r["column_status"]["force"] == "ok"
    assert r["stale_age_seconds"] == {}
    d16 = next(d for d in r["days"] if d["date"] == "2026-10-16")
    assert d16["hostile"]["force"] == sum(fresh_force[14:38])


@respx.mock
async def test_hostility_timeline_partial_category_column_is_flagged_per_column(
    fetcher, _fast
) -> None:
    # Review M1 (C): the base series covers all of 10-16 but the force
    # series returned only 00-11Z; the half-day force count sat next to
    # full-day counts with partial False.
    route, _ = _gdelt_router()

    def _answer(request: httpx.Request) -> httpx.Response:
        if _column(request) == "force":
            return httpx.Response(200, json=_volraw_dropping(
                request.url.params["query"], _DERIVED["force"],
                lambda d: d.startswith("20261016") and int(d[9:11]) >= 12,
            ))
        return route(request)

    respx.get(url__regex=_GDELT_URL).mock(side_effect=_answer)
    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    days = {d["date"]: d for d in r["days"]}
    d16 = days["2026-10-16"]
    assert d16["hostile"]["force"] == sum(_DERIVED["force"][14:26])
    assert d16["partial"] is True
    assert d16["partial_columns"] == ["force"]
    assert d16["hours_missing"] == [f"{h:02d}" for h in range(12, 24)]
    assert d16["coverage_by_column"]["force"] == {
        "buckets_returned": 12, "hours_covered": 12, "partial": True,
    }
    assert d16["coverage_by_column"]["articles"] == {
        "buckets_returned": 24, "hours_covered": 24, "partial": False,
    }
    # A half-day count over a full-day count is not a share.
    assert d16["hostile_share"]["force"] is None
    assert d16["hostile_share"]["sanction"] == round(79 / 732, 4)
    assert days["2026-10-17"]["partial"] is False
    assert days["2026-10-17"]["partial_columns"] == []
    # 10-15 starts at 10:00 in every series: partial everywhere, and the
    # shares compare like with like.
    assert days["2026-10-15"]["partial_columns"] == ["articles", "threat", "sanction", "force", "criticism"]
    assert days["2026-10-15"]["hostile_share"]["force"] is not None


@respx.mock
async def test_hostility_timeline_day_missing_from_the_judging_series_is_not_complete(
    fetcher, _fast
) -> None:
    # Review M1 (A): with the base call failed, the threat series judges
    # partial days, and it has no buckets for 10-16. That day read partial
    # False with hours_covered 0 (surviving mutant M22), and the other
    # columns' counts carried no gap.
    route, _ = _gdelt_router(fail={"articles"})

    def _answer(request: httpx.Request) -> httpx.Response:
        if _column(request) == "threat":
            return httpx.Response(200, json=_volraw_dropping(
                request.url.params["query"], _DERIVED["threat"],
                lambda d: d.startswith("20261016"),
            ))
        return route(request)

    respx.get(url__regex=_GDELT_URL).mock(side_effect=_answer)
    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    d16 = next(d for d in r["days"] if d["date"] == "2026-10-16")
    assert d16["hostile"]["threat"] is None
    assert d16["hostile"]["sanction"] == _DAY_SANCTION["2026-10-16"]
    assert d16["partial"] is True
    assert d16["partial_columns"] == ["threat"]
    assert d16["coverage_by_column"]["threat"] == {
        "buckets_returned": 0, "hours_covered": 0, "partial": True,
    }
    assert d16["coverage_by_column"]["sanction"]["partial"] is False
    assert any(
        g.startswith("threat:") and "2026-10-16" in g and "not zero" in g for g in r["data_gaps"]
    )


@respx.mock
async def test_hostility_timeline_failed_base_still_names_days_no_series_returned(
    fetcher, _fast
) -> None:
    # The "no buckets" gap used to be computed from the base series only,
    # so with the base call failed, unreturned days went unnamed.
    route, _ = _gdelt_router(fail={"articles"})
    respx.get(url__regex=_GDELT_URL).mock(side_effect=route)
    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=5, now=_NOW_GDELT)
    days = {d["date"]: d for d in r["days"]}
    assert days["2026-10-13"]["hostile"] == {c: None for c in ns.GDELT_HOSTILITY_TERMS}
    assert days["2026-10-13"]["partial"] is True
    assert any("no buckets for 2 of 5" in g for g in r["data_gaps"])


@respx.mock
async def test_hostility_timeline_retry_requests_are_spaced_by_the_floor(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Review M3: attempts inside one call went 1 s and 2 s apart, and the
    # timeline's retry pass added three more such requests per failed
    # column. Every GDELT request now follows the previous one by at least
    # GDELT's 5 s floor (sleeps recorded, not slept).
    events: list[tuple[str, float]] = []

    async def _record(delay, *args, **kwargs) -> None:
        events.append(("sleep", delay))

    monkeypatch.setattr(asyncio_mod, "sleep", _record)
    route, calls = _gdelt_router(fail={"sanction"})

    def _answer(request: httpx.Request) -> httpx.Response:
        events.append(("request", 0.0))
        return route(request)

    respx.get(url__regex=_GDELT_URL).mock(side_effect=_answer)
    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    assert Counter(c for c, _ in calls)["sanction"] == 6
    assert r["column_status"]["sanction"] == "failed"
    gaps: list[float] = []
    waited: float | None = None
    for kind, delay in events:
        if kind == "request":
            if waited is not None:
                gaps.append(waited)
            waited = 0.0
        elif waited is not None:
            waited += delay
    assert len(gaps) == 10
    assert min(gaps) > 4.5, gaps


@respx.mock
async def test_hostility_timeline_retry_pass_stops_when_a_retry_fails_again(
    fetcher, _fast
) -> None:
    route, calls = _gdelt_router(fail={"sanction", "force"})
    respx.get(url__regex=_GDELT_URL).mock(side_effect=route)
    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    counts = Counter(c for c, _ in calls)
    # sanction: three attempts, then three more on the retry pass, which
    # then stops: GDELT is still refusing, so force is not asked again.
    assert counts["sanction"] == 6
    assert counts["force"] == 3
    assert r["column_status"]["force"] == "failed"
    assert any("retry pass stopped" in g and "force" in g for g in r["data_gaps"])


def test_country_resolution_maps_two_letter_codes() -> None:
    assert ns._resolve_country("IR") == ("Iran", "IRN")
    assert ns._resolve_country("ua") == ("Ukraine", "UKR")


@respx.mock
async def test_hostility_timeline_refuses_codes_it_cannot_name(fetcher, _fast) -> None:
    # Review L2: "GBR" or "FRA" used to be sent to GDELT as literal text,
    # which reads as low coverage; "UK" is too short for GDELT.
    route, calls = _gdelt_router()
    respx.get(url__regex=_GDELT_URL).mock(side_effect=route)
    for code in ("GBR", "UK", "fra"):
        r = await ns.fetch_hostility_timeline(fetcher, code, now=_NOW_GDELT)
        assert f"'{code}' looks like a country code" in r["error"]
        assert r["days"] == []
    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "DE", now=_NOW_GDELT)
    assert "'DE' looks like a country code" in r["error"]
    assert calls == []


_SHALLOW_ITEMS = [
    # Every feed keeps about 10 h of history, so none reaches back to a
    # 24 h prior window's start: nothing can be compared.
    _item("Russia launches missile strikes on Kyiv as Ukraine war grinds on", "BBC World", 2),
    _item("Russia and Ukraine trade prisoners", "BBC World", 10),
    _item("Russian drone attack hits Ukrainian grid", "Al Jazeera", 1),
    _item("Mexico unveils new budget plan", "Al Jazeera", 9),
    _item("Mexico budget vote delayed", "NPR World", 3),
]


async def test_situations_change_is_not_measured_when_no_feed_is_comparable(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Review M2: with no comparable feed, change read 0 and change_ratio 1.0
    # ("no change"), and the CLI printed +0.
    _patch_feed(monkeypatch, _feed(_SHALLOW_ITEMS))
    r = await ns.fetch_situations(fetcher, min_articles=1, now=_NOW)
    assert r["coverage"]["feeds_comparable"] == 0
    assert r["change_measured"] is False
    rus_ukr = next(s for s in r["situations"] if s["key"] == "RUS-UKR")
    assert rus_ukr["articles"] == 3
    assert rus_ukr["change_measured"] is False
    for key in ("current_comparable", "prior", "change", "change_ratio"):
        assert rus_ukr[key] is None, key
    assert any(g.startswith("change not measured") for g in r["data_gaps"])

    by_change = await ns.fetch_situations(fetcher, sort="change", min_articles=1, now=_NOW)
    assert by_change["sort"] == "change"
    assert by_change["sort_applied"] == "covered"
    assert [s["key"] for s in by_change["situations"]] == ["RUS-UKR", "MEX"]
    assert any("ranked by coverage instead" in g for g in by_change["data_gaps"])


async def test_situations_measured_change_says_so_and_flags_a_thin_sample(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_feed(monkeypatch, _feed(_SITUATION_ITEMS))
    r = await ns.fetch_situations(fetcher, sort="change", min_articles=1, now=_NOW)
    assert r["change_measured"] is True
    assert r["sort_applied"] == "change"
    assert all(s["change_measured"] is True for s in r["situations"])
    # 4 of the 6 current-window items come from comparable feeds.
    assert not any("thin sample" in g for g in r["data_gaps"])

    thin = _SITUATION_ITEMS + [
        _item(f"Russia and Ukraine story {i}", "NPR World", 1 + i / 10) for i in range(6)
    ]
    _patch_feed(monkeypatch, _feed(thin))
    r = await ns.fetch_situations(fetcher, min_articles=1, now=_NOW)
    assert any("4 of 12 current-window items" in g and "thin sample" in g for g in r["data_gaps"])


async def test_situations_item_exactly_at_the_prior_start_is_in_neither_window(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Review mutant M16: windows are (start, end], so an item stamped
    # exactly at the prior window's start belongs to neither window; it
    # still shows that its feed's history reaches back that far.
    items = [
        _item("Mexico unveils new budget plan", "Al Jazeera", 5),
        _item("Mexico budget vote delayed", "Al Jazeera", 48),
    ]
    _patch_feed(monkeypatch, _feed(items))
    r = await ns.fetch_situations(fetcher, min_articles=1, now=_NOW)
    mex = next(s for s in r["situations"] if s["key"] == "MEX")
    assert (mex["current_comparable"], mex["prior"], mex["change"]) == (1, 0, 1)
    assert r["coverage"]["feeds_comparable"] == 1
    assert r["coverage"]["items_prior_window"] == 0


async def test_top_stories_attention_is_not_measured_when_no_feed_is_comparable(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Review M2: an empty attention list read as "nothing grew".
    _patch_feed(monkeypatch, _feed(_SHALLOW_ITEMS))
    r = await ns.fetch_top_stories(fetcher, now=_NOW)
    assert r["attention"] == []
    assert r["attention_change_measured"] is False
    assert any(g.startswith("attention not measured") for g in r["data_gaps"])

    _patch_feed(monkeypatch, _feed(_top_items()))
    measured = await ns.fetch_top_stories(fetcher, now=_NOW)
    assert measured["attention_change_measured"] is True
    assert measured["attention"][0]["name"] == "Yemen"


async def test_situations_and_top_stories_proceed_on_a_partial_feed_outage(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Review M5: a partial outage now carries degraded/reason; it is not a
    # total outage, so rankings are built and the marker passed on.
    marker = {"degraded": True, "reason": "partial_feeds_failed", "feeds_ok": 1,
              "feeds_attempted": 3, "feeds_failed": ["CFR", "DW News"]}
    _patch_feed(monkeypatch, _feed(_SITUATION_ITEMS, **marker))
    r = await ns.fetch_situations(fetcher, min_articles=1, now=_NOW)
    assert r["situations"]
    assert "error" not in r
    assert (r["degraded"], r["reason"]) == (True, "partial_feeds_failed")
    assert any(g.startswith("only 1 of 3 RSS feeds answered live") for g in r["data_gaps"])

    _patch_feed(monkeypatch, _feed(_top_items(), **marker))
    t = await ns.fetch_top_stories(fetcher, now=_NOW)
    assert t["count"] == 1
    assert (t["degraded"], t["reason"]) == (True, "partial_feeds_failed")
    assert any(g.startswith("only 1 of 3 RSS feeds answered live") for g in t["data_gaps"])


async def test_situations_and_top_stories_clamp_limits_to_one(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Review L3: limit -1 dropped the last result and 0 returned nothing;
    # min_articles 0 divided by zero on a situation seen only in the prior
    # window.
    prior_only = _item("Iraq and Syria close their border", "BBC World", 30)
    _patch_feed(monkeypatch, _feed(_SITUATION_ITEMS + [prior_only]))
    for bad in (0, -1):
        r = await ns.fetch_situations(fetcher, limit=bad, min_articles=1, now=_NOW)
        assert [s["key"] for s in r["situations"]] == ["RUS-UKR"]
        assert r["limit"] == 1
    r = await ns.fetch_situations(fetcher, min_articles=0, now=_NOW)
    assert r["min_articles"] == 1
    assert "IRQ-SYR" not in {s["key"] for s in r["situations"]}

    _patch_feed(monkeypatch, _feed(_top_items()))
    t = await ns.fetch_top_stories(fetcher, limit=0, attention_limit=-1, min_outlets=1, now=_NOW)
    assert t["count"] == 1
    assert len(t["attention"]) == 1
    assert (t["limit"], t["attention_limit"]) == (1, 1)


def _blocking_clusterer(monkeypatch: pytest.MonkeyPatch, module) -> None:
    real = module.cluster_articles

    def _slow(*args, **kwargs):
        time.sleep(0.3)
        return real(*args, **kwargs)

    monkeypatch.setattr(module, "cluster_articles", _slow)


async def _ticks_during(coro) -> tuple[int, object]:
    """How often a 10 ms ticker ran while ``coro`` was awaited."""
    ticks = 0
    done = asyncio_mod.Event()

    async def _ticker() -> None:
        nonlocal ticks
        while not done.is_set():
            await asyncio_mod.sleep(0.01)
            ticks += 1

    task = asyncio_mod.create_task(_ticker())
    try:
        result = await coro
    finally:
        done.set()
        await task
    return ticks, result


async def test_top_stories_and_clusters_cluster_off_the_event_loop(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Review L1: clustering 3,500 titles took 6.4 s on the event loop the
    # collector and the MCP server share.
    _patch_feed(monkeypatch, _feed(_top_items()))
    _blocking_clusterer(monkeypatch, ns)
    ticks, r = await _ticks_during(ns.fetch_top_stories(fetcher, now=_NOW))
    assert r["count"] == 1
    assert ticks >= 10, ticks

    _blocking_clusterer(monkeypatch, clustering)
    ticks, c = await _ticks_during(clustering.fetch_news_clusters(fetcher))
    assert c["cluster_count"] >= 1
    assert ticks >= 10, ticks


def test_cli_hostility_marks_partial_and_stale_columns(
    _cli_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = {
        "parties": [{"name": "Iran"}, {"name": "Israel"}],
        "days_requested": 1,
        "date_resolution": "hour",
        "column_status": dict({c: "ok" for c in _COLUMNS}, criticism="stale"),
        "stale_age_seconds": {"criticism": 93600.0},
        "days": [
            {"date": "2026-10-16", "articles": 732, "partial": True, "partial_columns": ["force"],
             "tone": -2.6654,
             "hostile": {"threat": 140, "sanction": 79, "force": 120, "criticism": None}},
        ],
        "data_gaps": [],
    }
    monkeypatch.setattr(ns, "fetch_hostility_timeline", _fake(payload))
    result = _invoke("hostility", "Iran", "Israel", "-d", "1")
    assert result.exit_code == 0
    row = next(line for line in result.output.splitlines() if "2026-10-16" in line)
    assert row.split() == ["2026-10-16", "*", "732", "140", "79", "120*", "n/a", "-2.6654"]
    assert "criticism is an expired cached copy (26.0 h old)" in result.output


def test_cli_situations_prints_unmeasured_change_as_not_measured(
    _cli_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = {
        "window": {"current": {"start": "2026-10-16T16:00:00Z", "end": "2026-10-17T16:00:00Z"}},
        "change_measured": False,
        "situations": [
            {"kind": "between", "countries": [{"name": "Russia"}, {"name": "Ukraine"}],
             "articles": 3, "hostile_articles": 2, "change": None, "change_measured": False,
             "outlet_count": 2, "sample_headlines": [{"title": "Russia launches missile strikes"}]},
        ],
        "data_gaps": [],
    }
    monkeypatch.setattr(ns, "fetch_situations", _fake(payload))
    result = _invoke("situations")
    assert result.exit_code == 0
    assert "Change not measured" in result.output
    assert "+0" not in result.output


def test_cli_top_stories_prints_unmeasured_attention(
    _cli_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = {"stories": [], "attention": [], "attention_change_measured": False,
               "data_gaps": []}
    monkeypatch.setattr(ns, "fetch_top_stories", _fake(payload))
    result = _invoke("top-stories")
    assert result.exit_code == 0
    assert "Attention: not measured" in result.output


async def test_hostility_timeline_failed_retry_keeps_the_expired_copy_status(
    fetcher, monkeypatch: pytest.MonkeyPatch, _fast
) -> None:
    # The force call finds only an expired copy and its retry finds nothing
    # (the copy evicted in between): the column stays "stale" with the
    # copy's age instead of turning into a plain failure.
    calls: Counter = Counter()

    async def _timeline(fetcher, query, mode, days):
        col = "tone" if mode == "timelinetone" else _category_of(query)
        calls[col] += 1
        if col == "force":
            if calls[col] == 1:
                return dict(_volraw(query, _DERIVED["force"]), _stale=True, _stale_age_seconds=7200.0)
            return None
        if col == "tone":
            return _tone(query)
        if col == "articles":
            return _volraw(query, [v for v, _ in _BASE])
        if col == "sanction":
            return _volraw(query, _SANCTION)
        return _volraw(query, _DERIVED[col])

    monkeypatch.setattr(ns, "_gdelt_timeline", _timeline)
    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    assert calls["force"] == 2
    assert r["column_status"]["force"] == "stale"
    assert r["stale_age_seconds"] == {"force": 7200.0}
    assert all(d["hostile"]["force"] is None for d in r["days"])
    assert any(
        g.startswith("force:") and "expired cached copy (2.0 h old)" in g for g in r["data_gaps"]
    )


async def test_situations_half_comparable_items_is_not_a_thin_sample(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The thin-sample note's edge: 4 comparable of 8 current-window items is
    # half, not under half.
    items = _SITUATION_ITEMS + [
        _item(f"Russia and Ukraine story {i}", "NPR World", 1 + i / 10) for i in range(2)
    ]
    _patch_feed(monkeypatch, _feed(items))
    r = await ns.fetch_situations(fetcher, min_articles=1, now=_NOW)
    assert r["coverage"]["items_current_window"] == 8
    assert r["coverage"]["items_current_window_comparable"] == 4
    assert not any("thin sample" in g for g in r["data_gaps"])
