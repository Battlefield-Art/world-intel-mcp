"""Tests for the news and convergence components of intel_aoi_escalation
(ROADMAP Phase 23, "Escalation news + convergence components").

Before this change ``fetch_aoi_escalation`` reported both components as
null unconditionally. These tests pin real values:

* news: the rise in the area's share of GDELT coverage, this week
  against last, for the reverse-geocoded place
  (``aoi.news_coverage_trend``);
* convergence: ``analysis/convergence.py`` over the in-fence events of
  four geolocated domains, scoped to the AOI's exact shape
  (``aoi.aoi_convergence``).

Each expected number is worked by hand in a comment next to its
assertion. No test reaches a live API: the autouse fixture turns every
network-bound source the escalation composes into an outage, and a test
that wants data stubs that domain or mocks its HTTP edge with respx. The
two respx tests replay responses captured live on 2026-10-17.
"""

import functools
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
import respx

from world_intel_mcp.analysis import aoi
from world_intel_mcp.analysis.aoi import AOIStore
from world_intel_mcp.sources import (
    aviation,
    conflict,
    geocode,
    military,
    news,
    seismology,
    wildfire,
)

# The real HTTP-backed functions, kept before any test patches them, for
# the respx tests that exercise them end to end.
_REAL_FETCH_PLACE_CONTEXT = geocode.fetch_place_context
_REAL_FETCH_GDELT_SEARCH = news.fetch_gdelt_search


async def _outage(*args, **kwargs) -> dict:
    return {"error": "domain not stubbed by this test"}


@pytest.fixture(autouse=True)
def _hermetic_domains(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every source the escalation composes defaults to an outage, so a
    forgotten stub shows up as a data gap instead of a live request."""
    for module, attr in (
        (geocode, "fetch_place_context"),
        (seismology, "fetch_earthquakes"),
        (military, "fetch_military_flights"),
        (conflict, "fetch_acled_events"),
        (wildfire, "fetch_wildfires"),
        (aviation, "fetch_domestic_flights"),
        (news, "fetch_gdelt_search"),
    ):
        monkeypatch.setattr(module, attr, _outage)


@pytest.fixture
def store(tmp_path: Path) -> AOIStore:
    s = AOIStore(tmp_path / "test_aoi_escalation.db")
    yield s
    s.close()


# Pittsburgh, the README worked example: a 50 km circle.
_PGH_LAT, _PGH_LON, _PGH_RADIUS_KM = 40.4406, -79.9959, 50.0
_PGH_QUERY = '("Pittsburgh" OR "Allegheny County")'
# ~7 km from the centre (inside); Detroit area, ~330 km (outside).
_NEAR = (40.50, -80.00)
_FAR = (42.33, -83.05)

# A square around Pittsburgh, and a point inside its bounding circle but
# outside the square (same geometry as test_aoi.py's polygon tests).
_PGH_SQUARE = [[39.5, -81.0], [39.5, -79.0], [41.3, -79.0], [41.3, -81.0]]
_IN_SQUARE = (40.4, -80.0)
_IN_CIRCLE_NOT_SQUARE = (40.4, -78.7)

# A 20 km wide corridor along latitude 40 from -81 to -79. Its bounding
# circle (centroid 40.0, -80.0; radius ~95 km) contains the off-route
# point, ~67 km from the route and so outside the 10 km half-width.
_ROUTE = [[40.0, -81.0], [40.0, -79.0]]
_ON_ROUTE = (40.0, -80.0)
_OFF_ROUTE = (40.6, -80.0)

_SIX = ["baseline", "news", "military", "conflict", "social_unrest", "convergence"]

_PLACE = {
    "place": "Pittsburgh",
    "county": "Allegheny County",
    "state": "Pennsylvania",
    "country_code": "us",
    "display_name": "Pittsburgh, Allegheny County, Pennsylvania, United States",
    "terms": ["Pittsburgh", "Allegheny County"],
    "source": "nominatim-reverse",
}

# geocode.fetch_place_context's answer for a point with no named place.
_UNNAMED = "No named place at 40.4406, -79.9959 (open water or unnamed area)."

# GDELT's documented degraded answer from sources/news.py.
_GDELT_DOWN = "GDELT fetch failed (rate limited, API error, or malformed response)"

# Test clock: windows are the 7 days ending with the step containing it.
_NOW = datetime(2026, 10, 17, 16, 30, tzinfo=timezone.utc)
_PRIOR_START = datetime(2026, 10, 4, tzinfo=timezone.utc)  # prior: Oct 4-10


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _steps(start: datetime, step: timedelta, counts: list[tuple[int, int]]) -> list:
    """GDELT ``timelinevolraw`` points: (matching articles, all monitored
    articles) per step from ``start``."""
    return [
        {
            "date": (start + step * i).strftime("%Y%m%dT%H%M%SZ"),
            "value": value,
            "norm": norm,
        }
        for i, (value, norm) in enumerate(counts)
    ]


def _timeline(points: list[dict]) -> list[dict]:
    return [{"series": "Article Count", "data": points}]


def _two_weeks(prior: list[tuple[int, int]], current: list[tuple[int, int]]) -> list:
    """Daily timeline: ``prior`` for Oct 4-10, ``current`` for Oct 11-17."""
    return _timeline(_steps(_PRIOR_START, timedelta(days=1), prior + current))


_FLAT_WEEKS = _two_weeks([(10, 1000)] * 7, [(10, 1000)] * 7)  # change 0.0
_RISING_WEEKS = _two_weeks([(10, 1000)] * 7, [(15, 1000)] * 7)  # change +50.0


def _quake(point: tuple[float, float]) -> dict:
    return {"latitude": point[0], "longitude": point[1], "magnitude": 4.6}


def _aircraft(point: tuple[float, float], icao: str) -> dict:
    return {"latitude": point[0], "longitude": point[1], "icao24": icao}


def _battle(point: tuple[float, float], fatalities: int = 0) -> dict:
    return {
        "event_type": "Battles",
        "latitude": point[0],
        "longitude": point[1],
        "fatalities": fatalities,
    }


def _protest(point: tuple[float, float]) -> dict:
    return {
        "event_type": "Protests",
        "latitude": point[0],
        "longitude": point[1],
        "fatalities": 0,
    }


def _fire(point: tuple[float, float]) -> dict:
    return {"lat": point[0], "lon": point[1], "fire_count": 3, "max_frp": 10.0}


def _install(
    monkeypatch: pytest.MonkeyPatch,
    *,
    quakes: list | str | None = None,
    aircraft: list | str | None = None,
    acled: list | str | None = None,
    fires: list | str | None = None,
    place: dict | None = _PLACE,
    timeline: list | None = None,
    gdelt: dict | Exception | None = None,
    now: datetime = _NOW,
) -> dict:
    """Stub the escalation's sources. A list is that domain's events, a
    str its fetch error, None leaves the autouse outage. GDELT answers
    ``timeline`` unless ``gdelt`` overrides it with a result dict or an
    exception to raise. The news trend's clock is pinned to ``now``.
    Returns the recorded geocode and GDELT calls."""
    calls: dict[str, list] = {"geocode": [], "gdelt": []}

    def _answer(value, key):
        return {"error": value} if isinstance(value, str) else {key: value}

    if quakes is not None:

        async def _quakes(fetcher, **kwargs):
            return _answer(quakes, "earthquakes")

        monkeypatch.setattr(seismology, "fetch_earthquakes", _quakes)
    if aircraft is not None:

        async def _military(fetcher, bbox=None):
            return _answer(aircraft, "aircraft")

        monkeypatch.setattr(military, "fetch_military_flights", _military)
    if acled is not None:

        async def _acled(fetcher, **kwargs):
            return _answer(acled, "events")

        monkeypatch.setattr(conflict, "fetch_acled_events", _acled)
    if fires is not None:

        async def _fires(fetcher, region=None):
            if isinstance(fires, str):
                return {"error": fires}
            return {"fires_by_region": {region: {"top_clusters": fires}}}

        monkeypatch.setattr(wildfire, "fetch_wildfires", _fires)
    if place is not None:

        async def _place(fetcher, lat, lon):
            calls["geocode"].append((lat, lon))
            return place

        monkeypatch.setattr(geocode, "fetch_place_context", _place)

    async def _gdelt(fetcher, **kwargs):
        calls["gdelt"].append(kwargs)
        if isinstance(gdelt, Exception):
            raise gdelt
        if gdelt is not None:
            return gdelt
        return {"timeline": timeline or [], "source": "gdelt"}

    monkeypatch.setattr(news, "fetch_gdelt_search", _gdelt)
    monkeypatch.setattr(
        aoi, "news_coverage_trend", functools.partial(aoi.news_coverage_trend, now=now)
    )
    return calls


def _near_and_far() -> dict:
    """One event per domain near Pittsburgh and one in Detroit. Inside
    the fence: 1 quake, 1 aircraft, 1 protest, 1 fire, all in grid cell
    (40, -80). Outside: the same four domains in cell (42, -84), which
    would form a second convergent cell if scoping leaked."""
    return {
        "quakes": [_quake(_NEAR), _quake(_FAR)],
        "aircraft": [_aircraft(_NEAR, "a1"), _aircraft(_FAR, "a2")],
        "acled": [_protest(_NEAR), _battle(_FAR, fatalities=2)],
        "fires": [_fire(_NEAR), _fire(_FAR)],
    }


def _all_at(point: tuple[float, float]) -> dict:
    """One quake, one aircraft, one protest and one fire at ``point``."""
    return {
        "quakes": [_quake(point)],
        "aircraft": [_aircraft(point, "a1")],
        "acled": [_protest(point)],
        "fires": [_fire(point)],
    }


# ---------------------------------------------------------------------------
# news_coverage_trend: the news component's input
# ---------------------------------------------------------------------------


def test_news_trend_scores_the_rise_in_coverage_share() -> None:
    trend = aoi.news_coverage_trend(_RISING_WEEKS, now=_NOW)

    assert "error" not in trend
    assert trend["window_days"] == 7
    assert trend["step_seconds"] == 86400
    assert trend["latest_step"] == "2026-10-17T00:00:00Z"
    # _NOW (Oct 17 16:30) is inside the Oct 17 step, so the current
    # window is Oct 11-17 and the prior one Oct 4-10.
    assert trend["current_window"] == {
        "start": "2026-10-11T00:00:00Z",
        "end": "2026-10-18T00:00:00Z",
        "articles": 105,  # 7 x 15
        "monitored_articles": 7000,  # 7 x 1000
        "steps_with_data": 7,
        "steps": 7,
        "share_per_10k": 150.0,  # 105 / 7000 x 10^4
    }
    assert trend["prior_window"] == {
        "start": "2026-10-04T00:00:00Z",
        "end": "2026-10-11T00:00:00Z",
        "articles": 70,
        "monitored_articles": 7000,
        "steps_with_data": 7,
        "steps": 7,
        "share_per_10k": 100.0,  # 70 / 7000 x 10^4
    }
    # expected = prior share 70/7000 x current volume 7000 = 70;
    # change = 100 x (105 - 70) / 70 = +50%.
    assert trend["expected_articles"] == 70.0
    assert trend["change_pct"] == 50.0
    assert trend["score_input"] == 50.0
    assert "note" not in trend
    assert "max(expected, 20)" in trend["method"]


def test_news_trend_measures_share_not_raw_volume() -> None:
    """Raw matches rise 80% (70 -> 126) only because GDELT monitored 80%
    more articles (7000 -> 12600): the share is flat, so no rise."""
    trend = aoi.news_coverage_trend(
        _two_weeks([(10, 1000)] * 7, [(18, 1800)] * 7), now=_NOW
    )
    assert trend["current_window"]["articles"] == 126
    assert trend["current_window"]["monitored_articles"] == 12600
    # expected = 70/7000 x 12600 = 126; change = 0.
    assert trend["expected_articles"] == 126.0
    assert trend["change_pct"] == 0.0
    assert trend["score_input"] == 0.0
    assert trend["prior_window"]["share_per_10k"] == 100.0
    assert trend["current_window"]["share_per_10k"] == 100.0


def test_news_trend_fall_scores_zero_not_negative() -> None:
    trend = aoi.news_coverage_trend(
        _two_weeks([(20, 1000)] * 7, [(10, 1000)] * 7), now=_NOW
    )
    # expected = 140/7000 x 7000 = 140; change = 100 x (70 - 140) / 140.
    assert trend["expected_articles"] == 140.0
    assert trend["change_pct"] == -50.0
    assert trend["score_input"] == 0.0


def test_news_trend_floor_keeps_a_few_stories_from_reading_as_a_surge() -> None:
    """Nothing last week and 5 stories this week: the percent change is
    taken against the 20-article floor, not against an expected 0."""
    trend = aoi.news_coverage_trend(
        _two_weeks([(0, 1000)] * 7, [(5, 1000)] + [(0, 1000)] * 6), now=_NOW
    )
    assert trend["expected_articles"] == 0.0
    # 100 x (5 - 0) / max(0, 20) = 25.
    assert trend["change_pct"] == 25.0
    assert trend["score_input"] == 25.0


def test_news_trend_partly_filled_latest_day_weighs_by_its_intake() -> None:
    """The current day is half ingested (5 of a normal 10 matches, 500 of
    1000 monitored). Raw counts would read 70 -> 65 as a fall; the share
    is unchanged."""
    trend = aoi.news_coverage_trend(
        _two_weeks([(10, 1000)] * 7, [(10, 1000)] * 6 + [(5, 500)]), now=_NOW
    )
    assert trend["current_window"]["articles"] == 65
    assert trend["current_window"]["monitored_articles"] == 6500
    # expected = 70/7000 x 6500 = 65.
    assert trend["expected_articles"] == 65.0
    assert trend["change_pct"] == 0.0


def test_news_trend_names_missing_steps_when_intake_still_balances() -> None:
    points = _steps(_PRIOR_START, timedelta(days=1), [(10, 1000)] * 14)
    del points[3]  # GDELT has nothing for Oct 7
    trend = aoi.news_coverage_trend(_timeline(points), now=_NOW)

    assert trend["prior_window"]["steps_with_data"] == 6
    assert trend["prior_window"]["monitored_articles"] == 6000
    # 6000 >= 0.5 x 7000, so comparable: expected = 60/6000 x 7000 = 70.
    assert trend["expected_articles"] == 70.0
    assert trend["change_pct"] == 0.0
    assert trend["note"] == (
        "GDELT has no data for 1 of the 14 time steps compared; each "
        "window's share covers the steps it has."
    )


def test_news_trend_refuses_a_stale_feed() -> None:
    """A series that stopped on Oct 9 is compared over the week that
    actually ends now (no data), not slid back to its last two weeks."""
    start = datetime(2026, 9, 26, tzinfo=timezone.utc)
    trend = aoi.news_coverage_trend(
        _timeline(_steps(start, timedelta(days=1), [(10, 1000)] * 14)), now=_NOW
    )
    assert trend["latest_step"] == "2026-10-09T00:00:00Z"
    assert trend["current_window"]["start"] == "2026-10-11T00:00:00Z"
    assert trend["current_window"]["steps_with_data"] == 0
    assert trend["error"] == (
        "GDELT intake too uneven to compare: it monitored 6,000 articles in "
        "the prior 7 days (6 of 7 steps with data) and 0 in the latest 7 "
        "(0 of 7), so a comparison would measure GDELT's ingestion gaps, not "
        "the news"
    )
    assert "score_input" not in trend
    assert "change_pct" not in trend


def test_news_trend_hourly_steps() -> None:
    """Short GDELT timespans come back hourly: windows are 168 steps."""
    start = datetime(2026, 10, 3, 17, tzinfo=timezone.utc)
    counts = [(1, 100)] * 168 + [(2, 100)] * 168  # ends Oct 17 16:00
    trend = aoi.news_coverage_trend(
        _timeline(_steps(start, timedelta(hours=1), counts)), now=_NOW
    )
    assert trend["step_seconds"] == 3600
    assert trend["latest_step"] == "2026-10-17T16:00:00Z"
    assert trend["current_window"]["start"] == "2026-10-10T17:00:00Z"
    assert trend["current_window"]["end"] == "2026-10-17T17:00:00Z"
    assert trend["current_window"]["steps"] == 168
    assert trend["current_window"]["steps_with_data"] == 168
    # expected = 168/16800 x 16800 = 168; change = 100 x (336 - 168) / 168.
    assert trend["expected_articles"] == 168.0
    assert trend["change_pct"] == 100.0
    assert trend["score_input"] == 100.0


@pytest.mark.parametrize(
    "timeline, starts_with",
    [
        (None, "GDELT returned no coverage timeline"),
        ([], "GDELT returned no coverage timeline"),
        ([{"series": "Article Count", "data": []}], "GDELT returned no coverage"),
        (
            _timeline([{"date": "20261017T000000Z", "value": "12", "norm": 900}]),
            "GDELT timeline point is malformed",
        ),
        (
            _timeline([{"date": "20261017T000000Z", "value": 3, "norm": -1}]),
            "GDELT timeline point is malformed",
        ),
        (
            _timeline([{"date": "2026-10-17", "value": 3, "norm": 900}]),
            "GDELT timeline point is malformed",
        ),
        (
            _timeline([{"date": "20261017T000000Z", "value": 3, "norm": 900}]),
            "GDELT timeline holds a single time step",
        ),
        (
            _timeline(
                _steps(_PRIOR_START, timedelta(hours=7), [(1, 100)] * 60)
            ),
            "GDELT timeline step of 25200 s does not divide a day",
        ),
        (
            _timeline(_steps(_PRIOR_START, timedelta(days=2), [(1, 100)] * 9)),
            "GDELT timeline step of 172800 s does not divide a day",
        ),
    ],
)
def test_news_trend_unusable_timeline_is_an_error_not_a_zero(
    timeline, starts_with
) -> None:
    trend = aoi.news_coverage_trend(timeline, now=_NOW)
    assert trend["error"].startswith(starts_with)
    assert "score_input" not in trend


# ---------------------------------------------------------------------------
# aoi_convergence: the convergence component's input
# ---------------------------------------------------------------------------


def _scoped(**domains) -> dict:
    """``aoi_convergence`` input: each domain's items, or {"error"} for
    a str. Domains not given answered with no events."""
    out = {}
    for domain in ("earthquakes", "military_flights", "conflict_events", "wildfires"):
        value = domains.get(domain, [])
        out[domain] = {"error": value} if isinstance(value, str) else {"items": value}
    return out


def test_convergence_quiet_area_with_every_input_answered_is_a_measured_zero() -> (
    None
):
    result = aoi.aoi_convergence(_scoped(earthquakes=[_quake(_NEAR)]))

    assert result["score"] == 0.0
    assert result["lower_bound"] is False
    assert result["convergent_cells"] == 0
    assert result["cells"] == []
    assert result["events_by_domain"] == {
        "earthquakes": 1,
        "military_flights": 0,
        "conflict_events": 0,
        "wildfires": 0,
    }
    assert result["inputs_used"] == [
        "earthquakes",
        "military_flights",
        "conflict_events",
        "wildfires",
    ]
    assert result["inputs_failed"] == []
    assert "error" not in result


def test_convergence_cell_score_is_domains_times_one_plus_sqrt_events() -> None:
    result = aoi.aoi_convergence(
        _scoped(
            earthquakes=[_quake(_NEAR)],
            military_flights=[_aircraft(_NEAR, "a1")],
            conflict_events=[_battle(_NEAR), _protest(_NEAR)],
            wildfires=[_fire(_NEAR)],
        )
    )
    # 5 events from 4 domains in cell (40, -80): 4 x (1 + sqrt 5) = 12.944.
    assert result["score"] == 12.94
    assert result["lower_bound"] is False
    assert result["cells"] == [
        {
            "lat": 40.5,
            "lon": -79.5,
            "event_count": 5,
            "signal_types": [
                "conflict_events",
                "earthquakes",
                "military_flights",
                "wildfires",
            ],
            "type_count": 4,
            "total_weight": 5.0,
            "convergence_score": 12.94,
        }
    ]


def test_convergence_with_a_failed_input_is_a_lower_bound() -> None:
    result = aoi.aoi_convergence(
        _scoped(
            military_flights=[_aircraft(_NEAR, "a1")],
            conflict_events=[_battle(_NEAR), _battle(_NEAR)],
            wildfires="FIRMS unavailable",
        )
    )
    # 3 events from 2 domains: 2 x (1 + sqrt 3) = 5.464.
    assert result["score"] == 5.46
    assert result["lower_bound"] is True
    assert result["inputs_failed"] == ["wildfires"]
    assert result["inputs_used"] == [
        "earthquakes",
        "military_flights",
        "conflict_events",
    ]
    assert "error" not in result


def test_convergence_with_a_failed_input_and_no_cell_is_null_not_zero() -> None:
    result = aoi.aoi_convergence(
        _scoped(conflict_events=[_battle(_NEAR)], wildfires="FIRMS unavailable")
    )
    assert result["score"] is None
    assert result["error"] == (
        "no convergent cell among earthquakes, military_flights, "
        "conflict_events, but wildfires answered only partly or not at all, "
        "so quiet cannot be told from missing"
    )


def test_convergence_partial_input_counts_as_incomplete() -> None:
    scoped = _scoped(conflict_events=[_battle(_NEAR)])
    scoped["military_flights"] = {
        "items": [_aircraft(_NEAR, "a1")],
        "partial_errors": ["box 2: adsb.lol timed out"],
    }
    quiet = aoi.aoi_convergence(scoped)
    assert quiet["score"] is None
    assert quiet["inputs_partial"] == ["military_flights"]
    assert "military_flights answered only partly or not at all" in quiet["error"]

    scoped["conflict_events"]["items"].append(_battle(_NEAR))
    busy = aoi.aoi_convergence(scoped)
    # 2 battles + 1 aircraft: 2 x (1 + sqrt 3) = 5.464, a lower bound.
    assert busy["score"] == 5.46
    assert busy["lower_bound"] is True


def test_convergence_with_no_input_answered_is_an_error() -> None:
    result = aoi.aoi_convergence(
        _scoped(
            earthquakes="USGS down",
            military_flights="adsb.lol down",
            conflict_events="ACLED down",
            wildfires="FIRMS down",
        )
    )
    assert result["score"] is None
    assert result["error"] == "no convergence input domain answered"
    assert result["inputs_used"] == []


def test_convergence_without_a_domain_never_exceeds_the_full_score() -> None:
    """The lower-bound claim on the real code path: over seeded random
    event sets spread across four grid cells, dropping any one domain
    never raises the strongest cell, and a score computed without it is
    flagged as a lower bound."""
    rng = random.Random(20261017)
    makers = {
        "earthquakes": lambda p: _quake(p),
        "military_flights": lambda p: _aircraft(p, "x"),
        "conflict_events": lambda p: _battle(p),
        "wildfires": lambda p: _fire(p),
    }
    checked = 0
    for _ in range(300):
        full = _scoped(
            **{
                domain: [
                    make((rng.uniform(40.0, 41.99), rng.uniform(-81.0, -79.01)))
                    for _ in range(rng.randint(0, 4))
                ]
                for domain, make in makers.items()
            }
        )
        full_score = aoi.aoi_convergence(full)["score"]
        assert full_score is not None
        for missing in makers:
            partial = aoi.aoi_convergence({**full, missing: {"error": "down"}})
            if partial["score"] is None:
                assert partial["error"]
                continue
            checked += 1
            assert partial["lower_bound"] is True
            assert partial["score"] <= full_score
    assert checked >= 200  # the property was exercised, not vacuous


# ---------------------------------------------------------------------------
# fetch_aoi_escalation end to end (sources stubbed)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_escalation_scores_news_and_convergence(
    fetcher, store: AOIStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    aoi.define_aoi(store, "Pittsburgh", _PGH_LAT, _PGH_LON, _PGH_RADIUS_KM)
    calls = _install(monkeypatch, timeline=_RISING_WEEKS, **_near_and_far())

    result = await aoi.fetch_aoi_escalation(fetcher, store, "Pittsburgh")

    # news: share +50% -> min(20, 0.2 x 50) = 10.0
    # military: 1 aircraft in the fence -> 1.0
    # conflict: the only in-fence ACLED event is a protest -> 0.0
    # social_unrest: 1 protest x 0.4 -> 0.4
    # convergence: 4 domains x (1 + sqrt 4) = 12.0 -> min(8, 2 x 12) = 8.0
    assert result["components"] == {
        "baseline": 0.0,
        "news": 10.0,
        "military": 1.0,
        "conflict": 0.0,
        "social_unrest": 0.4,
        "convergence": 8.0,
    }
    # 19.4 of 100 possible points.
    assert result["score"] == 19.4
    assert result["level"] == "watch"
    assert result["trend_signal"] == "active"
    assert result["computed_from"] == _SIX
    assert result["zero_filled_components"] == []
    assert result["unavailable_components"] == []
    assert result["data_gaps"] == []
    assert result["source"] == "aoi-escalation"
    assert result["aoi"]["kind"] == "circle"

    # The geo-scoped query, as a coverage timeline, sent once.
    assert calls["geocode"] == [(_PGH_LAT, _PGH_LON)]
    assert calls["gdelt"] == [{"query": _PGH_QUERY, "mode": "timelinevolraw"}]
    assert result["news_scoping"]["basis"] == "geocoded"
    assert result["news_scoping"]["place"] == "Pittsburgh"
    assert result["news_signal"]["query"] == _PGH_QUERY
    assert result["news_signal"]["change_pct"] == 50.0
    assert result["news_signal"]["expected_articles"] == 70.0

    convergence = result["convergence_signal"]
    assert convergence["score"] == 12.0
    assert convergence["lower_bound"] is False
    # Only the Pittsburgh cell: the Detroit events are outside the fence.
    assert convergence["convergent_cells"] == 1
    assert convergence["cells"][0]["lat"] == 40.5
    assert convergence["cells"][0]["lon"] == -79.5
    assert convergence["events_by_domain"] == {
        "earthquakes": 1,
        "military_flights": 1,
        "conflict_events": 1,
        "wildfires": 1,
    }


def _quiet_but_convergent() -> dict:
    """2 quakes and 1 fire in one cell, no aircraft or ACLED events:
    convergence 2 x (1 + sqrt 3) = 5.46 -> component 8.0, every other
    measured component 0.0, so the score is easy to read."""
    return {
        "quakes": [_quake(_NEAR), _quake(_NEAR)],
        "aircraft": [],
        "acled": [],
        "fires": [_fire(_NEAR)],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "gdelt, gap",
    [
        (
            {"error": _GDELT_DOWN, "degraded": True, "timeline": [], "count": 0},
            f"News: {_GDELT_DOWN}.",
        ),
        (httpx.ConnectError("connection refused"), "News: connection refused."),
    ],
)
async def test_escalation_failed_news_query_is_a_gap_not_a_zero(
    fetcher, store: AOIStore, monkeypatch: pytest.MonkeyPatch, gdelt, gap
) -> None:
    aoi.define_aoi(store, "Pittsburgh", _PGH_LAT, _PGH_LON, _PGH_RADIUS_KM)
    _install(monkeypatch, gdelt=gdelt, **_quiet_but_convergent())

    result = await aoi.fetch_aoi_escalation(fetcher, store, "Pittsburgh")

    assert result["components"]["news"] is None
    assert result["unavailable_components"] == ["news"]
    assert result["computed_from"] == [c for c in _SIX if c != "news"]
    assert result["data_gaps"] == [gap]
    # Renormalized over the 80 points measured: 8.0 / 80 x 100 = 10.0.
    assert result["score"] == 10.0
    assert result["news_signal"]["query"] == _PGH_QUERY
    assert "score_input" not in result["news_signal"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failing, error, zero_filled, gap",
    [
        (
            "acled",
            "ACLED down",
            ["conflict", "social_unrest"],
            "Conflict events: ACLED down",
        ),
        (
            "aircraft",
            "adsb.lol and OpenSky both unavailable",
            ["military"],
            "Military flights: adsb.lol and OpenSky both unavailable",
        ),
    ],
)
async def test_escalation_names_components_zero_filled_for_want_of_data(
    fetcher,
    store: AOIStore,
    monkeypatch: pytest.MonkeyPatch,
    failing,
    error,
    zero_filled,
    gap,
) -> None:
    """score_hotspot takes no null for military, conflict or social
    unrest, so a failed military or ACLED fetch still enters the score
    as 0. The response names those zeros instead of letting them pass
    as measured quiet."""
    aoi.define_aoi(store, "Pittsburgh", _PGH_LAT, _PGH_LON, _PGH_RADIUS_KM)
    inputs = _quiet_but_convergent()
    inputs[failing] = error
    _install(monkeypatch, timeline=_FLAT_WEEKS, **inputs)

    result = await aoi.fetch_aoi_escalation(fetcher, store, "Pittsburgh")

    assert result["zero_filled_components"] == zero_filled
    assert all(result["components"][c] == 0.0 for c in zero_filled)
    assert set(zero_filled) <= set(result["computed_from"])
    assert gap in result["data_gaps"]


@pytest.mark.asyncio
async def test_escalation_flat_news_week_is_a_measured_zero(
    fetcher, store: AOIStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same inputs with GDELT answering a flat share: news is a real 0.0
    and counts, so the score is 8.0 of 100 instead of 10.0 of 80."""
    aoi.define_aoi(store, "Pittsburgh", _PGH_LAT, _PGH_LON, _PGH_RADIUS_KM)
    _install(monkeypatch, timeline=_FLAT_WEEKS, **_quiet_but_convergent())

    result = await aoi.fetch_aoi_escalation(fetcher, store, "Pittsburgh")

    assert result["components"]["news"] == 0.0
    assert result["components"]["convergence"] == 8.0
    assert result["computed_from"] == _SIX
    assert result["score"] == 8.0
    assert result["data_gaps"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "place, reason",
    [
        (
            # An error that already names reverse geocoding is not prefixed again.
            {"error": "Reverse geocoding unavailable: Nominatim down", "place": None},
            "Reverse geocoding unavailable: Nominatim down",
        ),
        (
            {"place": None, "terms": [], "note": _UNNAMED},
            _UNNAMED,
        ),
    ],
)
async def test_escalation_unplaced_area_sends_no_name_mention_query(
    fetcher, store: AOIStore, monkeypatch: pytest.MonkeyPatch, place, reason
) -> None:
    """Without a geocoded place the brief falls back to the AOI's name;
    the escalation does not, because mentions of a label are not
    coverage of the area."""
    aoi.define_aoi(store, "Pittsburgh", _PGH_LAT, _PGH_LON, _PGH_RADIUS_KM)
    calls = _install(
        monkeypatch, place=place, timeline=_RISING_WEEKS, **_quiet_but_convergent()
    )

    result = await aoi.fetch_aoi_escalation(fetcher, store, "Pittsburgh")

    assert calls["gdelt"] == []
    assert result["components"]["news"] is None
    assert result["news_scoping"] == {
        "basis": "unavailable",
        "query": None,
        "reason": reason,
    }
    assert result["news_signal"]["query"] is None
    assert result["data_gaps"] == [
        f"News: not scored: the area could not be placed ({reason}), and "
        "counting mentions of the AOI's own name would not measure coverage "
        "of the area."
    ]


@pytest.mark.asyncio
async def test_escalation_geocoder_exception_is_a_gap_not_a_crash(
    fetcher, store: AOIStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    aoi.define_aoi(store, "Pittsburgh", _PGH_LAT, _PGH_LON, _PGH_RADIUS_KM)
    calls = _install(monkeypatch, timeline=_RISING_WEEKS, **_quiet_but_convergent())

    async def _raises(fetcher, lat, lon):
        raise RuntimeError("geocoder exploded")

    monkeypatch.setattr(geocode, "fetch_place_context", _raises)

    result = await aoi.fetch_aoi_escalation(fetcher, store, "Pittsburgh")

    assert calls["gdelt"] == []
    assert result["components"]["news"] is None
    assert result["news_scoping"] == {
        "basis": "unavailable",
        "query": None,
        "reason": "geocoder exploded",
    }
    assert result["data_gaps"] == [
        "News: the news query could not be built: geocoder exploded."
    ]
    # Convergence 8.0 of the 80 points measured: 8.0 / 80 x 100 = 10.0.
    assert result["score"] == 10.0


@pytest.mark.asyncio
async def test_escalation_names_gdelt_steps_with_no_data(
    fetcher, store: AOIStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """News is still scored when the intake balances, and the missing
    GDELT step is named as a gap."""
    points = _steps(_PRIOR_START, timedelta(days=1), [(10, 1000)] * 14)
    del points[3]  # GDELT has nothing for Oct 7
    aoi.define_aoi(store, "Pittsburgh", _PGH_LAT, _PGH_LON, _PGH_RADIUS_KM)
    _install(monkeypatch, timeline=_timeline(points), **_quiet_but_convergent())

    result = await aoi.fetch_aoi_escalation(fetcher, store, "Pittsburgh")

    # Share flat (60/6000 vs 70/7000): news 0.0, measured; 8.0 of 100.
    assert result["components"]["news"] == 0.0
    assert result["computed_from"] == _SIX
    assert result["score"] == 8.0
    assert result["data_gaps"] == [
        "News: GDELT has no data for 1 of the 14 time steps compared; each "
        "window's share covers the steps it has."
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "point, convergence, military",
    [
        (_IN_CIRCLE_NOT_SQUARE, 0.0, 0.0),  # in the bounding circle only
        (_IN_SQUARE, 8.0, 1.0),  # positive control
    ],
)
async def test_escalation_convergence_is_scoped_to_the_polygon(
    fetcher,
    store: AOIStore,
    monkeypatch: pytest.MonkeyPatch,
    point,
    convergence,
    military,
) -> None:
    aoi.define_polygon_aoi(store, "PGH Square", _PGH_SQUARE)
    _install(monkeypatch, timeline=_FLAT_WEEKS, **_all_at(point))

    result = await aoi.fetch_aoi_escalation(fetcher, store, "PGH Square")

    assert result["aoi"]["kind"] == "polygon"
    assert result["components"]["convergence"] == convergence
    assert result["components"]["military"] == military
    # Every input answered, so an empty fence is a measured 0.0.
    assert result["computed_from"] == _SIX
    expected_events = 1 if convergence else 0
    assert result["convergence_signal"]["events_by_domain"] == {
        "earthquakes": expected_events,
        "military_flights": expected_events,
        "conflict_events": expected_events,
        "wildfires": expected_events,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "point, convergence",
    [
        (_OFF_ROUTE, 0.0),  # in the bounding circle, 67 km off the route
        (_ON_ROUTE, 8.0),  # positive control
    ],
)
async def test_escalation_convergence_is_scoped_to_the_corridor(
    fetcher, store: AOIStore, monkeypatch: pytest.MonkeyPatch, point, convergence
) -> None:
    aoi.define_corridor_aoi(store, "Route 40", _ROUTE, width_km=20.0)
    _install(monkeypatch, timeline=_FLAT_WEEKS, **_all_at(point))

    result = await aoi.fetch_aoi_escalation(fetcher, store, "Route 40")

    assert result["aoi"]["kind"] == "corridor"
    assert result["components"]["convergence"] == convergence
    assert result["convergence_signal"]["convergent_cells"] == (1 if convergence else 0)


@pytest.mark.asyncio
async def test_escalation_convergence_without_wildfires_is_a_lower_bound(
    fetcher, store: AOIStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    aoi.define_aoi(store, "Pittsburgh", _PGH_LAT, _PGH_LON, _PGH_RADIUS_KM)
    _install(
        monkeypatch,
        timeline=_FLAT_WEEKS,
        quakes=[],
        aircraft=[_aircraft(_NEAR, "a1")],
        acled=[_battle(_NEAR), _battle(_NEAR)],
        fires="NASA_FIRMS_API_KEY not set",
    )

    result = await aoi.fetch_aoi_escalation(fetcher, store, "Pittsburgh")

    # 2 battles + 1 aircraft: 2 x (1 + sqrt 3) = 5.46 -> min(8, 10.9) = 8.0.
    assert result["convergence_signal"]["score"] == 5.46
    assert result["convergence_signal"]["lower_bound"] is True
    assert result["components"]["convergence"] == 8.0
    # military 1.0 + conflict 2 x 0.5 = 1.0 + convergence 8.0 = 10.0 of 100.
    assert result["score"] == 10.0
    assert result["data_gaps"] == [
        "Wildfires: NASA_FIRMS_API_KEY not set",
        "Convergence: computed without full wildfires data; 5.46 is a lower "
        "bound (the missing events could only raise it).",
    ]


@pytest.mark.asyncio
async def test_escalation_convergence_without_wildfires_and_no_cell_is_null(
    fetcher, store: AOIStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    aoi.define_aoi(store, "Pittsburgh", _PGH_LAT, _PGH_LON, _PGH_RADIUS_KM)
    _install(
        monkeypatch,
        timeline=_FLAT_WEEKS,
        quakes=[],
        aircraft=[],
        acled=[_battle(_NEAR)],
        fires="NASA_FIRMS_API_KEY not set",
    )

    result = await aoi.fetch_aoi_escalation(fetcher, store, "Pittsburgh")

    assert result["components"]["convergence"] is None
    assert result["unavailable_components"] == ["convergence"]
    assert result["computed_from"] == [c for c in _SIX if c != "convergence"]
    assert result["data_gaps"] == [
        "Wildfires: NASA_FIRMS_API_KEY not set",
        "Convergence: not scored; no convergent cell among earthquakes, "
        "military flights, conflict events, but wildfires answered only partly "
        "or not at all, so quiet cannot be told from missing.",
    ]
    # conflict 1 x 0.5 = 0.5 of the 92 points measured:
    # 0.5 / 92 x 100 = 0.54, which rounds to 0.5.
    assert result["score"] == 0.5


# ---------------------------------------------------------------------------
# Real geocode + GDELT paths, replaying responses captured live
# ---------------------------------------------------------------------------

# Captured live 2026-10-17T16:57:53Z: GET
# https://api.gdeltproject.org/api/v2/doc/doc with the parameters
# fetch_gdelt_search sends for this AOI (query ("Pittsburgh" OR
# "Allegheny County"), mode timelinevolraw, maxrecords 50, format json,
# no timespan). GDELT answered daily steps from 2026-07-25 to 2026-10-17
# (every step at T000000Z, shortened to its day here). 16 of those 85 days
# are absent and Oct 6, 8 and 10 hold a sliver of a normal day's volume:
# an ingestion outage, not quiet news.
_LIVE_PGH_DAYS = [
    ("20260725", 122, 92281),
    ("20260726", 97, 98797),
    ("20260727", 302, 137794),
    ("20260728", 402, 193271),
    ("20260729", 210, 162086),
    ("20260730", 176, 168455),
    ("20260731", 233, 149894),
    ("20260801", 129, 110262),
    ("20260802", 154, 103171),
    ("20260803", 288, 147538),
    ("20260804", 343, 157631),
    ("20260805", 310, 175970),
    ("20260806", 364, 159186),
    ("20260807", 247, 117971),
    ("20260808", 199, 114555),
    ("20260809", 178, 103098),
    ("20260810", 168, 115167),
    ("20260811", 255, 131138),
    ("20260812", 297, 140458),
    ("20260813", 308, 179907),
    ("20260814", 333, 194350),
    ("20260815", 185, 108259),
    ("20260816", 189, 103863),
    ("20260817", 232, 116203),
    ("20260818", 390, 169174),
    ("20260819", 344, 157610),
    ("20260820", 207, 171970),
    ("20260821", 573, 160721),
    ("20260822", 216, 98241),
    ("20260823", 100, 88414),
    ("20260824", 237, 117661),
    ("20260825", 315, 132186),
    ("20260826", 279, 179831),
    ("20260827", 333, 172877),
    ("20260828", 298, 125894),
    ("20260829", 164, 91911),
    ("20260830", 130, 79465),
    ("20260831", 248, 107519),
    ("20260901", 158, 116481),
    ("20260902", 174, 105316),
    ("20260903", 239, 120602),
    ("20260904", 276, 156481),
    ("20260905", 151, 102774),
    ("20260906", 156, 98645),
    ("20260907", 338, 118079),
    ("20260908", 124, 77318),
    ("20260909", 272, 158938),
    ("20260910", 443, 155062),
    ("20260911", 512, 154253),
    ("20260912", 184, 120863),
    ("20260913", 52, 29203),
    ("20260920", 172, 105485),
    ("20260921", 171, 108510),
    ("20260922", 437, 105779),
    ("20260923", 309, 149817),
    ("20260924", 441, 155942),
    ("20260925", 226, 58811),
    ("20260927", 207, 77392),
    ("20260928", 201, 121105),
    ("20260929", 164, 134919),
    ("20260930", 233, 111356),
    ("20261001", 491, 174731),
    ("20261002", 115, 61712),
    ("20261006", 1, 3958),
    ("20261008", 0, 678),
    ("20261010", 5, 1836),
    ("20261015", 92, 95273),
    ("20261016", 163, 150288),
    ("20261017", 96, 77358),
]
_LIVE_CAPTURED_AT = datetime(2026, 10, 17, 16, 57, 53, tzinfo=timezone.utc)
_LIVE_GDELT_BODY = {
    "query_details": {"title": _PGH_QUERY, "date_resolution": "day"},
    "timeline": [
        {
            "series": "Article Count",
            "data": [
                {"date": f"{day}T000000Z", "value": value, "norm": norm}
                for day, value, norm in _LIVE_PGH_DAYS
            ],
        }
    ],
}

# Captured live 2026-10-17T17:06:04Z: the Nominatim reverse lookup
# geocode.fetch_place_context makes for this AOI's centre (lat 40.44060,
# lon -79.99590, format jsonv2, zoom 10, addressdetails 1). Trimmed to
# the fields fetch_place_context reads plus the OSM identity of the match.
_LIVE_PGH_REVERSE = {
    "licence": "Data © OpenStreetMap contributors, ODbL 1.0. http://osm.org/copyright",
    "osm_type": "relation",
    "osm_id": 188553,
    "lat": "40.4406968",
    "lon": "-80.0025666",
    "category": "boundary",
    "type": "administrative",
    "addresstype": "city",
    "name": "Pittsburgh",
    "display_name": "Pittsburgh, Allegheny County, Pennsylvania, United States",
    "address": {
        "city": "Pittsburgh",
        "county": "Allegheny County",
        "state": "Pennsylvania",
        "ISO3166-2-lvl4": "US-PA",
        "country": "United States",
        "country_code": "us",
    },
}


def _live_edges(monkeypatch: pytest.MonkeyPatch, now: datetime) -> tuple:
    """Real geocode and GDELT functions behind respx routes serving the
    captures; the four event domains stubbed with 5 in-fence events from
    4 domains in one cell (1 quake, 2 aircraft, 1 protest, 1 fire)."""
    _install(
        monkeypatch,
        now=now,
        quakes=[_quake(_NEAR)],
        aircraft=[_aircraft(_NEAR, "a1"), _aircraft(_NEAR, "a2")],
        acled=[_protest(_NEAR)],
        fires=[_fire(_NEAR)],
    )
    monkeypatch.setattr(geocode, "fetch_place_context", _REAL_FETCH_PLACE_CONTEXT)
    monkeypatch.setattr(news, "fetch_gdelt_search", _REAL_FETCH_GDELT_SEARCH)
    nominatim = respx.get(
        url__regex=r"^https://nominatim\.openstreetmap\.org/reverse"
    ).mock(return_value=httpx.Response(200, json=_LIVE_PGH_REVERSE))
    gdelt = respx.get(
        url__regex=r"^https://api\.gdeltproject\.org/api/v2/doc/doc"
    ).mock(return_value=httpx.Response(200, json=_LIVE_GDELT_BODY))
    return nominatim, gdelt


@respx.mock
@pytest.mark.asyncio
async def test_live_capture_outage_week_is_refused_not_read_as_a_surge(
    fetcher, store: AOIStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """At capture time GDELT's prior week held 6 matching articles out of
    6,472 monitored (3 days with data) and the current week 351 of
    322,919. Raw counts would read a 58x surge; the escalation refuses the
    comparison and leaves news out of the score."""
    aoi.define_aoi(store, "Pittsburgh", _PGH_LAT, _PGH_LON, _PGH_RADIUS_KM)
    nominatim, gdelt = _live_edges(monkeypatch, _LIVE_CAPTURED_AT)

    result = await aoi.fetch_aoi_escalation(fetcher, store, "Pittsburgh")

    reverse = nominatim.calls.last.request.url.params
    assert (reverse["lat"], reverse["lon"]) == ("40.44060", "-79.99590")
    sent = gdelt.calls.last.request.url.params
    assert sent["query"] == _PGH_QUERY
    assert sent["mode"] == "timelinevolraw"
    assert gdelt.call_count == 1

    signal = result["news_signal"]
    assert signal["current_window"]["articles"] == 351  # 92 + 163 + 96
    assert signal["current_window"]["monitored_articles"] == 322919
    assert signal["current_window"]["steps_with_data"] == 3  # Oct 15-17
    assert signal["prior_window"]["articles"] == 6  # 1 + 0 + 5
    assert signal["prior_window"]["monitored_articles"] == 6472
    assert signal["prior_window"]["steps_with_data"] == 3  # Oct 6, 8, 10
    assert result["components"]["news"] is None
    assert result["data_gaps"] == [
        "News: GDELT intake too uneven to compare: it monitored 6,472 "
        "articles in the prior 7 days (3 of 7 steps with data) and 322,919 in "
        "the latest 7 (3 of 7), so a comparison would measure GDELT's "
        "ingestion gaps, not the news."
    ]
    # military 2.0 + unrest 0.4 + convergence 8.0 = 10.4 of the 80 points
    # measured: 10.4 / 80 x 100 = 13.0.
    assert result["score"] == 13.0
    assert result["computed_from"] == [c for c in _SIX if c != "news"]


@respx.mock
@pytest.mark.asyncio
async def test_live_capture_full_weeks_score_the_share_change(
    fetcher, store: AOIStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same capture read as of 2026-09-12 12:00 UTC, when both weeks
    are complete. Current Sep 6-12: 2,029 matches of 883,158 monitored;
    prior Aug 30-Sep 5: 1,376 of 788,638. expected = 1376/788638 x 883158
    = 1540.92; change = 100 x (2029 - 1540.92) / 1540.92 = +31.67%."""
    aoi.define_aoi(store, "Pittsburgh", _PGH_LAT, _PGH_LON, _PGH_RADIUS_KM)
    _live_edges(monkeypatch, datetime(2026, 9, 12, 12, tzinfo=timezone.utc))

    result = await aoi.fetch_aoi_escalation(fetcher, store, "Pittsburgh")

    signal = result["news_signal"]
    assert signal["current_window"]["start"] == "2026-09-06T00:00:00Z"
    assert signal["current_window"]["articles"] == 2029
    assert signal["current_window"]["monitored_articles"] == 883158
    assert signal["prior_window"]["start"] == "2026-08-30T00:00:00Z"
    assert signal["prior_window"]["articles"] == 1376
    assert signal["prior_window"]["monitored_articles"] == 788638
    assert signal["expected_articles"] == 1540.9
    assert signal["change_pct"] == 31.7
    assert signal["prior_window"]["share_per_10k"] == 17.45
    assert signal["current_window"]["share_per_10k"] == 22.97
    # news = min(20, 0.2 x 31.7) = 6.34 -> 6.3
    assert result["components"]["news"] == 6.3
    # military 2.0 + unrest 0.4 + news 6.34 + convergence 8.0 = 16.74 of 100.
    assert result["score"] == 16.7
    assert result["computed_from"] == _SIX
    assert result["data_gaps"] == []
