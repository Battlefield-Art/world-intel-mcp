"""Escalation inputs that must never read as quiet: real outages, expired
cached copies, global samples cut at their caps, cache entries shared by
different queries, and compact clusters split by the convergence grid.

Every test that names an upstream drives the real source functions
(geocode, news, seismology, military, wildfire, and ACLED's events fetch
with only its OAuth bearer stubbed) through the real Fetcher and Cache,
with each upstream answered by respx; nothing reaches a live API
(``assert_all_mocked``). An expired copy is made the way it happens in
production: one call caches a real answer, the entry is aged past its TTL
in the cache database, and the next call finds the upstream down.

Each expected number is worked by hand in a comment next to its
assertion. The news trend's clock is pinned to ``_NOW``; cache ages use
the real clock.
"""

import asyncio
import functools
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
import respx
from click.testing import CliRunner

import world_intel_mcp.fetcher as fetcher_mod
from world_intel_mcp import cli
from world_intel_mcp.analysis import aoi
from world_intel_mcp.analysis.aoi import AOIStore
from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import conflict, military, news, seismology, wildfire

# Pittsburgh, the README worked example: a 50 km circle.
_PGH = (40.4406, -79.9959, 50.0)
# ~6.6 km from the centre; unshifted grid cell (40, -80). A FIRMS fire
# here clusters to (40.5, -80.0), the same cell.
_IN = (40.5, -79.9)

# News trend clock: the current window is Oct 11-17 (Oct 17 is 0.75
# elapsed), the prior window Oct 4-10.
_NOW = datetime(2026, 10, 17, 18, 0, tzinfo=timezone.utc)
_DAY0 = datetime(2026, 10, 4, tzinfo=timezone.utc)

_UTC = "%Y-%m-%dT%H:%M:%SZ"

_NOMINATIM_PGH = {
    "osm_type": "relation",
    "osm_id": 188553,
    "name": "Pittsburgh",
    "display_name": "Pittsburgh, Allegheny County, Pennsylvania, United States",
    "address": {
        "city": "Pittsburgh",
        "county": "Allegheny County",
        "state": "Pennsylvania",
        "country": "United States",
        "country_code": "us",
    },
}
# Nominatim's own answer for a point it cannot name (open water).
_NOMINATIM_UNNAMED = {"error": "Unable to geocode"}

_URLS = {
    "nominatim": r"^https://nominatim\.openstreetmap\.org/reverse",
    "gdelt": r"^https://api\.gdeltproject\.org/api/v2/doc/doc",
    "usgs": r"^https://earthquake\.usgs\.gov/",
    "adsb": r"^https://api\.adsb\.lol/",
    "opensky": r"^https://opensky-network\.org/",
    "firms": r"^https://firms\.modaps\.eosdis\.nasa\.gov/",
    "acled": r"^https://acleddata\.com/api/acled/read",
}

_USGS_DOWN = "USGS earthquake fetch failed (API error, timeout, or malformed response)"
_FIRMS_ALL_DOWN = (
    "NASA FIRMS fetch failed for every requested region (API error, timeout, "
    "or open circuit; no cached copy)"
)
_GEOCODER_DOWN = (
    "Reverse geocoding unavailable: the Nominatim request failed (rate limit, "
    "upstream error, or network). See fetcher logs for the specific failure."
)
_MILITARY_DOWN = (
    "no military flight source answered (adsb.lol failed or returned no "
    "aircraft, and the OpenSky fallback failed)"
)


def _firms_region_down(region: str) -> str:
    return (
        f"NASA FIRMS gave no answer for {region} (API error, timeout, or open "
        "circuit) and no cached copy exists"
    )


def _stale_pattern(subject: str, age: str) -> str:
    return (
        rf"no fresh {subject} \(the request failed or its circuit breaker is "
        rf"open\); the only copy on hand is one this server cached {age} ago "
        r"\(at (\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ)\), so it is not scored as current"
    )


# ---------------------------------------------------------------------------
# Upstream answers
# ---------------------------------------------------------------------------


def _timeline(prior: list[tuple[int, int]], current: list[tuple[int, int]]) -> dict:
    """A GDELT DOC body: daily (matching, monitored) article counts for
    Oct 4-10 then Oct 11-17. It also carries an empty article list, so the
    same body answers the brief's artlist query."""
    points = [
        {
            "date": (_DAY0 + timedelta(days=i)).strftime("%Y%m%dT%H%M%SZ"),
            "value": value,
            "norm": norm,
        }
        for i, (value, norm) in enumerate(prior + current)
    ]
    return {"timeline": [{"series": "Article Count", "data": points}], "articles": []}


_FLAT = _timeline([(10, 1000)] * 7, [(10, 1000)] * 7)  # change 0.0
_RISING = _timeline([(10, 1000)] * 7, [(15, 1000)] * 7)  # change +50.0


def _usgs(points: list[tuple[float, float]], newest_ms: int | None = None) -> dict:
    """USGS GeoJSON, newest first, one minute apart."""
    newest_ms = newest_ms or int(time.time() * 1000) - 3_600_000
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "id": f"q{i}",
                "properties": {
                    "mag": 4.7,
                    "place": f"quake {i}",
                    "time": newest_ms - i * 60_000,
                },
                "geometry": {"coordinates": [lon, lat, 10.0]},
            }
            for i, (lat, lon) in enumerate(points)
        ],
    }


def _adsb(points: list[tuple[float, float]]) -> dict:
    return {
        "ac": [
            {"hex": f"ae{i:04x}", "flight": f"RCH{i}", "lat": lat, "lon": lon,
             "alt_baro": 20000}
            for i, (lat, lon) in enumerate(points)
        ]
    }


def _firms(points: list[tuple[float, float]]) -> str:
    header = "latitude,longitude,bright_ti4,confidence,frp,acq_date,acq_time,daynight,satellite"
    rows = [f"{lat},{lon},330.0,n,12.0,2026-10-17,0600,D,N" for lat, lon in points]
    return "\n".join([header, *rows]) + "\n"


def _acled(points: list[tuple[float, float]]) -> dict:
    """ACLED read API body; it sends coordinates and fatalities as strings."""
    return {
        "status": 200,
        "success": True,
        "count": len(points),
        "data": [
            {
                "event_id_cnty": f"T{i}",
                "event_date": "2026-10-16",
                "event_type": "Battles",
                "latitude": str(lat),
                "longitude": str(lon),
                "fatalities": "0",
                "location": f"site {i}",
                "country": "Testland",
            }
            for i, (lat, lon) in enumerate(points)
        ],
    }


def _response(spec) -> httpx.Response:
    if isinstance(spec, int):
        return httpx.Response(spec, text="upstream error")
    if isinstance(spec, str):
        return httpx.Response(200, text=spec)
    return httpx.Response(200, json=spec)


def _routes(router: respx.MockRouter, **specs) -> dict:
    """One route per upstream. A spec is a JSON body, a CSV string, an
    HTTP status, or a callable side effect. By default every upstream
    answers and nothing is in the area: Nominatim names Pittsburgh, GDELT
    is flat, adsb.lol is empty and OpenSky (its fallback) answers empty."""
    answers = {
        "nominatim": _NOMINATIM_PGH,
        "gdelt": _FLAT,
        "usgs": _usgs([]),
        "adsb": _adsb([]),
        "opensky": {"time": 0, "states": []},
        "firms": _firms([]),
        "acled": _acled([]),
    }
    assert set(specs) <= set(answers), set(specs) - set(answers)
    answers.update(specs)
    routes = {}
    for name, pattern in _URLS.items():
        route = router.get(url__regex=pattern)
        spec = answers[name]
        if callable(spec):
            route.mock(side_effect=spec)
        else:
            route.mock(return_value=_response(spec))
        routes[name] = route
    return routes


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------


class _Edge:
    """A real Cache, Fetcher (no retries) and AOIStore in a temp dir."""

    def __init__(self, tmp_path: Path) -> None:
        self.db = tmp_path / "cache.db"
        self.cache = Cache(db_path=self.db)
        self.fetcher = Fetcher(
            cache=self.cache,
            breaker=CircuitBreaker(),
            default_timeout=5.0,
            max_retries=0,
        )
        self.store = AOIStore(tmp_path / "aoi.db")

    async def aclose(self) -> None:
        await self.fetcher.close()
        self.store.close()
        self.cache.close()


def _hermetic(monkeypatch: pytest.MonkeyPatch) -> None:
    """No rate-limit sleeps, a FIRMS key, an ACLED bearer without the
    OAuth exchange, and the news trend's clock pinned to ``_NOW``."""

    async def _acled_bearer() -> str:
        return "respx-bearer"

    monkeypatch.setattr(fetcher_mod, "_SOURCE_RATE_LIMITS", {})
    monkeypatch.setenv("NASA_FIRMS_API_KEY", "respx-firms")
    monkeypatch.delenv("OPENSKY_CLIENT_ID", raising=False)
    monkeypatch.delenv("OPENSKY_CLIENT_SECRET", raising=False)
    monkeypatch.setattr(conflict, "_acled_get_token", _acled_bearer)
    monkeypatch.setattr(
        aoi, "news_coverage_trend", functools.partial(aoi.news_coverage_trend, now=_NOW)
    )


@pytest.fixture
async def edge(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _hermetic(monkeypatch)
    env = _Edge(tmp_path)
    yield env
    await env.aclose()


async def _escalate(env: _Edge, name: str = "Pittsburgh", **specs) -> tuple[dict, dict]:
    """``fetch_aoi_escalation`` behind ``_routes``; returns the result and
    each upstream's requests."""
    with respx.mock(assert_all_mocked=True, assert_all_called=False) as router:
        routes = _routes(router, **specs)
        result = await aoi.fetch_aoi_escalation(env.fetcher, env.store, name)
        requests = {k: [c.request for c in r.calls] for k, r in routes.items()}
    return result, requests


def _age_entry(db: Path, prefix: str, age_s: float, *, expire: bool) -> float:
    """Move the one cache entry whose key starts with ``prefix`` back by
    ``age_s`` seconds (and past its TTL when ``expire``); returns its new
    write time."""
    written = time.time() - age_s
    conn = sqlite3.connect(db)
    try:
        if expire:
            cur = conn.execute(
                "UPDATE cache SET created_at = ?, expires_at = ? "
                "WHERE substr(key, 1, length(?)) = ?",
                (written, time.time() - 1.0, prefix, prefix),
            )
        else:
            cur = conn.execute(
                "UPDATE cache SET created_at = ? WHERE substr(key, 1, length(?)) = ?",
                (written, prefix, prefix),
            )
        conn.commit()
        assert cur.rowcount == 1, f"{cur.rowcount} cache entries start with {prefix!r}"
    finally:
        conn.close()
    return written


def _epoch(stamp: str) -> float:
    return datetime.strptime(stamp, _UTC).replace(tzinfo=timezone.utc).timestamp()


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime(_UTC)


# ---------------------------------------------------------------------------
# Sources: an outage carries an error, never the shape of a quiet answer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_usgs_outage_is_an_error_not_a_quiet_planet(edge: _Edge) -> None:
    with respx.mock(assert_all_mocked=True) as router:
        router.get(url__regex=_URLS["usgs"]).mock(return_value=httpx.Response(503))
        result = await seismology.fetch_earthquakes(edge.fetcher, hours=72)

    assert result["error"] == _USGS_DOWN
    assert result["degraded"] is True
    assert result["reason"] == "usgs_fetch_failed"
    # The list keys keep their shape so callers that only iterate still run.
    assert result["earthquakes"] == []
    assert result["count"] == 0


@pytest.mark.asyncio
async def test_firms_region_outage_is_named_not_an_empty_region(edge: _Edge) -> None:
    with respx.mock(assert_all_mocked=True) as router:
        router.get(url__regex=_URLS["firms"]).mock(return_value=httpx.Response(503))
        result = await wildfire.fetch_wildfires(edge.fetcher, region="north_america")

    assert result["fires_by_region"]["north_america"] == {
        "count": 0,
        "top_clusters": [],
        "error": _firms_region_down("north_america"),
    }
    assert result["error"] == _FIRMS_ALL_DOWN
    assert result["degraded"] is True
    assert result["reason"] == "firms_fetch_failed"
    assert result["failed_regions"] == ["north_america"]
    assert result["total_fires"] == 0


@pytest.mark.asyncio
async def test_firms_failed_region_among_answered_ones_is_partial(edge: _Edge) -> None:
    def _by_region(request: httpx.Request) -> httpx.Response:
        if "/-25,35,45,72/" in request.url.path:  # europe
            return httpx.Response(503)
        return httpx.Response(200, text=_firms([(10.0, 10.0)]))

    with respx.mock(assert_all_mocked=True) as router:
        route = router.get(url__regex=_URLS["firms"]).mock(side_effect=_by_region)
        result = await wildfire.fetch_wildfires(edge.fetcher)

    assert route.call_count == 9
    assert "error" not in result
    assert result["degraded"] is True
    assert result["failed_regions"] == ["europe"]
    assert result["partial_errors"] == [_firms_region_down("europe")]
    assert result["fires_by_region"]["europe"]["error"] == _firms_region_down("europe")
    answered = {k: v for k, v in result["fires_by_region"].items() if k != "europe"}
    assert len(answered) == 8
    for region in answered.values():
        assert "error" not in region
        assert (region["count"], region["cluster_count"]) == (1, 1)
        assert len(region["top_clusters"]) == 1
    assert result["total_fires"] == 8


@pytest.mark.asyncio
async def test_firms_answer_says_how_many_clusters_it_cut(edge: _Edge) -> None:
    # 3 fires in one 0.5-degree cell plus 24 single fires in 24 others:
    # 25 clusters, of which the 20 largest are listed.
    fires = [_IN] * 3 + [(60.0, -140.0 + i) for i in range(24)]
    with respx.mock(assert_all_mocked=True) as router:
        router.get(url__regex=_URLS["firms"]).mock(
            return_value=httpx.Response(200, text=_firms(fires))
        )
        result = await wildfire.fetch_wildfires(edge.fetcher, region="north_america")

    region = result["fires_by_region"]["north_america"]
    assert region["count"] == 27
    assert region["cluster_count"] == 25
    assert len(region["top_clusters"]) == wildfire.TOP_CLUSTERS_PER_REGION == 20
    assert region["top_clusters"][0] == {
        "lat": 40.5, "lon": -80.0, "fire_count": 3, "max_frp": 12.0,
    }


# ---------------------------------------------------------------------------
# Sources: when upstream answered, and whether the answer is an expired copy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_cached_answer_reports_when_upstream_answered(edge: _Edge) -> None:
    with respx.mock(assert_all_mocked=True) as router:
        route = router.get(url__regex=_URLS["usgs"]).mock(
            return_value=httpx.Response(200, json=_usgs([_IN]))
        )
        before = int(time.time())
        fresh = await seismology.fetch_earthquakes(edge.fetcher, hours=72)
        written = _age_entry(edge.db, "seismology:", 120.0, expire=False)
        cached = await seismology.fetch_earthquakes(edge.fetcher, hours=72)

    assert before <= _epoch(fresh["fetched_at"]) <= time.time()
    # The second answer came from the live cache entry: its fetched_at is
    # when USGS answered, 2 minutes before it was served.
    assert route.call_count == 1
    assert cached["fetched_at"] == _iso(written)
    assert "_stale" not in cached


@pytest.mark.asyncio
async def test_usgs_expired_copy_is_marked_stale(edge: _Edge) -> None:
    with respx.mock(assert_all_mocked=True) as router:
        route = router.get(url__regex=_URLS["usgs"])
        route.mock(return_value=httpx.Response(200, json=_usgs([_IN])))
        await seismology.fetch_earthquakes(edge.fetcher, hours=72)
        written = _age_entry(edge.db, "seismology:", 7200.0, expire=True)
        route.mock(return_value=httpx.Response(503))
        result = await seismology.fetch_earthquakes(edge.fetcher, hours=72)

    assert route.call_count == 2
    assert result["count"] == 1
    assert result["_stale"] is True
    assert 7200.0 <= result["_stale_age_seconds"] < 7260.0
    assert abs(_epoch(result["fetched_at"]) - written) <= 2.0


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["artlist", "timelinevolraw"])
async def test_gdelt_expired_copy_is_marked_stale(edge: _Edge, mode: str) -> None:
    with respx.mock(assert_all_mocked=True) as router:
        route = router.get(url__regex=_URLS["gdelt"])
        route.mock(return_value=httpx.Response(200, json=_RISING))
        await news.fetch_gdelt_search(edge.fetcher, query="Pittsburgh", mode=mode)
        written = _age_entry(edge.db, "news:gdelt:", 72000.0, expire=True)
        route.mock(return_value=httpx.Response(429, text="limit"))
        result = await news.fetch_gdelt_search(edge.fetcher, query="Pittsburgh", mode=mode)

    assert route.call_count == 2
    assert result["_stale"] is True
    assert 72000.0 <= result["_stale_age_seconds"] < 72060.0
    assert abs(_epoch(result["fetched_at"]) - written) <= 2.0


@pytest.mark.asyncio
async def test_firms_expired_copy_is_marked_stale(edge: _Edge) -> None:
    with respx.mock(assert_all_mocked=True) as router:
        route = router.get(url__regex=_URLS["firms"])
        route.mock(return_value=httpx.Response(200, text=_firms([_IN])))
        await wildfire.fetch_wildfires(edge.fetcher, region="north_america")
        written = _age_entry(edge.db, "wildfire:csv:", 10800.0, expire=True)
        route.mock(return_value=httpx.Response(503))
        result = await wildfire.fetch_wildfires(edge.fetcher, region="north_america")

    assert route.call_count == 2
    region = result["fires_by_region"]["north_america"]
    assert region["count"] == 1
    assert region["_stale"] is True
    assert 10800.0 <= region["_stale_age_seconds"] < 10860.0
    assert region["fetched_at"] == _iso(written)
    assert result["_stale"] is True
    assert result["stale_regions"] == ["north_america"]
    assert "error" not in result


# ---------------------------------------------------------------------------
# Sources: one cache entry per distinct request
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gdelt_queries_that_reduce_to_one_ascii_string_get_their_own_answers(
    edge: _Edge,
) -> None:
    """'"Київ"' and '"Суми"' both reduce to the ASCII string '______'."""

    def _by_query(request: httpx.Request) -> httpx.Response:
        query = request.url.params["query"]
        return httpx.Response(
            200, json={"articles": [{"title": f"about {query}", "url": "https://example.org/a"}]}
        )

    with respx.mock(assert_all_mocked=True) as router:
        route = router.get(url__regex=_URLS["gdelt"]).mock(side_effect=_by_query)
        kyiv = await news.fetch_gdelt_search(edge.fetcher, query='"Київ"', mode="artlist")
        sumy = await news.fetch_gdelt_search(edge.fetcher, query='"Суми"', mode="artlist")

    assert route.call_count == 2
    assert [a["title"] for a in kyiv["articles"]] == ['about "Київ"']
    assert [a["title"] for a in sumy["articles"]] == ['about "Суми"']


@pytest.mark.asyncio
async def test_gdelt_answers_for_different_limits_do_not_share_a_cache_entry(
    edge: _Edge,
) -> None:
    def _by_limit(request: httpx.Request) -> httpx.Response:
        n = int(request.url.params["maxrecords"])
        return httpx.Response(
            200, json={"articles": [{"title": f"t{i}", "url": f"https://example.org/{i}"}
                                    for i in range(n)]}
        )

    with respx.mock(assert_all_mocked=True) as router:
        route = router.get(url__regex=_URLS["gdelt"]).mock(side_effect=_by_limit)
        ten = await news.fetch_gdelt_search(edge.fetcher, query="Pittsburgh", limit=10)
        fifty = await news.fetch_gdelt_search(edge.fetcher, query="Pittsburgh", limit=50)

    assert route.call_count == 2
    assert (ten["count"], fifty["count"]) == (10, 50)


@pytest.mark.asyncio
async def test_usgs_answers_for_different_limits_do_not_share_a_cache_entry(
    edge: _Edge,
) -> None:
    """A short list cached for one caller must not reach a caller that
    asked for 50 and reads a list under its cap as complete."""

    def _by_limit(request: httpx.Request) -> httpx.Response:
        n = int(request.url.params["limit"])
        return httpx.Response(200, json=_usgs([(-20.0 - i * 0.1, -70.0) for i in range(n)]))

    with respx.mock(assert_all_mocked=True) as router:
        route = router.get(url__regex=_URLS["usgs"]).mock(side_effect=_by_limit)
        short = await seismology.fetch_earthquakes(edge.fetcher, hours=72, limit=5)
        full = await seismology.fetch_earthquakes(edge.fetcher, hours=72, limit=50)

    assert route.call_count == 2
    assert (short["count"], full["count"]) == (5, 50)


@pytest.mark.asyncio
async def test_acled_answers_for_different_limits_do_not_share_a_cache_entry(
    edge: _Edge,
) -> None:
    def _by_limit(request: httpx.Request) -> httpx.Response:
        n = int(request.url.params["limit"])
        return httpx.Response(200, json=_acled([(0.1 * i, 30.0) for i in range(n)]))

    with respx.mock(assert_all_mocked=True) as router:
        route = router.get(url__regex=_URLS["acled"]).mock(side_effect=_by_limit)
        short = await conflict.fetch_acled_events(edge.fetcher, days=7, limit=15)
        full = await conflict.fetch_acled_events(edge.fetcher, days=7, limit=100)

    assert route.call_count == 2
    assert (short["count"], full["count"]) == (15, 100)


# ---------------------------------------------------------------------------
# Escalation: a real outage is a gap, never a measured quiet
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_escalation_usgs_outage_is_a_gap_and_blocks_a_quiet_convergence(
    edge: _Edge,
) -> None:
    """An aircraft and a battle in one cell: 2 events, under the 3 a cell
    needs, so with earthquakes missing quiet cannot be told from missing."""
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    result, requests = await _escalate(
        edge, usgs=503, adsb=_adsb([_IN]), acled=_acled([_IN])
    )

    assert len(requests["usgs"]) == 1
    assert result["data_gaps"] == [
        f"Earthquakes: {_USGS_DOWN}",
        "Convergence: not scored; no convergent cell among military flights, "
        "conflict events, wildfires, but earthquakes answered only partly or "
        "not at all, so quiet cannot be told from missing.",
    ]
    assert result["convergence_signal"]["inputs_failed"] == ["earthquakes"]
    assert result["components"]["convergence"] is None
    assert result["zero_filled_components"] == []
    # military 1.0 + conflict 0.5 = 1.5 of the 92 points measured: 1.63.
    assert result["score"] == 1.6
    # baseline 0 + news 20 + military 20 + conflict 20 + unrest 12 = 72 of
    # 92: 78.26.
    assert result["score_ceiling"] == 78.3


@pytest.mark.asyncio
async def test_escalation_military_outage_is_zero_filled_not_an_empty_sky(
    edge: _Edge,
) -> None:
    """adsb.lol and its OpenSky fallback both down: military.py answers
    ``source: "none"`` with an empty list, which is an outage. That answer
    now carries military.py's own error, so the gap names it; the generic
    _MILITARY_NO_SOURCE text is for a ``source: "none"`` answer without
    one."""
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    result, requests = await _escalate(
        edge,
        adsb=503,
        opensky=503,
        usgs=_usgs([_IN]),
        acled=_acled([_IN]),
        firms=_firms([_IN]),
    )

    assert (len(requests["adsb"]), len(requests["opensky"])) == (1, 1)
    assert result["data_gaps"] == [
        "Military flights: No military flight source available (adsb.lol and "
        "OpenSky both failed)",
        # quake + battle + fire in cell (40, -80): 3 x (1 + sqrt 3) = 8.2.
        "Convergence: computed without full military flights data; 8.2 is a "
        "lower bound (the missing events could only raise it).",
    ]
    assert aoi._MILITARY_NO_SOURCE == (
        "no military flight source answered (adsb.lol failed or returned no "
        "aircraft, and the OpenSky fallback failed)"
    )
    assert result["zero_filled_components"] == ["military"]
    assert result["lower_bound_components"] == ["convergence"]
    assert result["convergence_signal"]["inputs_failed"] == ["military_flights"]
    # The ceiling holds the zero-filled military component at 0:
    # news 20 + conflict 20 + unrest 12 + convergence 8 = 60 of 100.
    assert result["score_ceiling"] == 60.0


@pytest.mark.asyncio
async def test_escalation_firms_outage_is_a_gap_not_a_fireless_area(
    edge: _Edge,
) -> None:
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    result, requests = await _escalate(
        edge, firms=503, usgs=_usgs([_IN]), adsb=_adsb([_IN]), acled=_acled([_IN])
    )

    assert len(requests["firms"]) == 1
    assert result["data_gaps"] == [
        f"Wildfires: {_firms_region_down('north_america')}",
        "Convergence: computed without full wildfires data; 8.2 is a lower "
        "bound (the missing events could only raise it).",
    ]
    assert result["convergence_signal"]["inputs_failed"] == ["wildfires"]
    assert result["lower_bound_components"] == ["convergence"]


@pytest.mark.asyncio
async def test_escalation_names_a_failed_firms_region_next_to_an_answered_one(
    edge: _Edge,
) -> None:
    """A circle at 15.2 N straddles FIRMS's north_america box (from 15 N)
    and south_america box (to 15 N). south_america is down; the fire
    north_america lists is still counted, and the gap is named."""
    aoi.define_aoi(edge.store, "Border", 15.2, -80.0, 50.0)

    def _by_region(request: httpx.Request) -> httpx.Response:
        if "/-85,-60,-30,15/" in request.url.path:  # south_america
            return httpx.Response(503)
        return httpx.Response(200, text=_firms([(15.2, -80.0)]))

    result, requests = await _escalate(edge, "Border", firms=_by_region)

    assert len(requests["firms"]) == 2
    assert (
        f"Wildfires: partial coverage: {_firms_region_down('south_america')}"
        in result["data_gaps"]
    )
    signal = result["convergence_signal"]
    assert signal["events_by_domain"]["wildfires"] == 1
    assert signal["inputs_partial"] == ["wildfires"]


@pytest.mark.asyncio
async def test_brief_names_a_failed_firms_region_next_to_an_answered_one(
    edge: _Edge,
) -> None:
    aoi.define_aoi(edge.store, "Border", 15.2, -80.0, 50.0)

    def _by_region(request: httpx.Request) -> httpx.Response:
        if "/-85,-60,-30,15/" in request.url.path:  # south_america
            return httpx.Response(503)
        return httpx.Response(200, text=_firms([(15.2, -80.0)]))

    with respx.mock(assert_all_mocked=True, assert_all_called=False) as router:
        _routes(router, firms=_by_region)
        brief = await aoi.fetch_aoi_brief(edge.fetcher, edge.store, "Border")

    assert brief["counts"]["wildfires"] == 1
    assert (
        f"Wildfires: partial coverage: {_firms_region_down('south_america')}"
        in brief["data_gaps"]
    )


# ---------------------------------------------------------------------------
# Escalation: an expired cached copy is refused, with its age named
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_escalation_refuses_an_expired_usgs_copy(edge: _Edge) -> None:
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    events = {"usgs": _usgs([_IN]), "adsb": _adsb([_IN]), "acled": _acled([_IN])}
    first, _ = await _escalate(edge, **events)
    # Fresh: quake + aircraft + battle in one cell, 8.2, component 8.0.
    assert first["components"]["convergence"] == 8.0

    written = _age_entry(edge.db, "seismology:", 2 * 86400, expire=True)
    second, requests = await _escalate(edge, **{**events, "usgs": 503})

    assert len(requests["usgs"]) == 1
    gap = re.fullmatch(
        "Earthquakes: " + _stale_pattern("USGS answer", r"2\.0 days"),
        second["data_gaps"][0],
    )
    assert gap, second["data_gaps"]
    assert abs(_epoch(gap.group(1)) - written) <= 2.0
    assert second["data_gaps"][1:] == [
        "Convergence: not scored; no convergent cell among military flights, "
        "conflict events, wildfires, but earthquakes answered only partly or "
        "not at all, so quiet cannot be told from missing.",
    ]
    assert second["components"]["convergence"] is None
    assert second["convergence_signal"]["fetched_at"]["earthquakes"] is None


@pytest.mark.asyncio
async def test_escalation_refuses_an_expired_gdelt_copy_without_blaming_gdelt(
    edge: _Edge,
) -> None:
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    first, _ = await _escalate(edge, gdelt=_RISING)
    # Fresh: +50% share, news = 0.2 x 50 = 10.0.
    assert first["components"]["news"] == 10.0

    written = _age_entry(edge.db, "news:gdelt:", 20 * 3600, expire=True)
    second, requests = await _escalate(edge, gdelt=429)

    assert len(requests["gdelt"]) == 1
    assert second["components"]["news"] is None
    assert second["news_signal"]["fetched_at"] is None
    assert len(second["data_gaps"]) == 1
    gap = re.fullmatch(
        "News: " + _stale_pattern("GDELT answer", r"20\.0 h") + r"\.",
        second["data_gaps"][0],
    )
    assert gap, second["data_gaps"]
    assert abs(_epoch(gap.group(1)) - written) <= 2.0
    # The copy is ours; nothing says GDELT itself had no data.
    assert "GDELT returned" not in second["data_gaps"][0]
    assert "GDELT has no data" not in second["data_gaps"][0]


@pytest.mark.asyncio
async def test_escalation_refuses_an_expired_firms_copy(edge: _Edge) -> None:
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    events = {"usgs": _usgs([_IN]), "acled": _acled([_IN]), "firms": _firms([_IN])}
    first, _ = await _escalate(edge, **events)
    assert first["components"]["convergence"] == 8.0

    written = _age_entry(edge.db, "wildfire:csv:", 3 * 3600, expire=True)
    second, requests = await _escalate(edge, **{**events, "firms": 503})

    assert len(requests["firms"]) == 1
    gap = re.fullmatch(
        "Wildfires: " + _stale_pattern("NASA FIRMS answer for north_america", r"3\.0 h"),
        second["data_gaps"][0],
    )
    assert gap, second["data_gaps"]
    assert abs(_epoch(gap.group(1)) - written) <= 2.0
    assert second["components"]["convergence"] is None
    assert second["convergence_signal"]["inputs_failed"] == ["wildfires"]


@pytest.mark.asyncio
async def test_escalation_refuses_an_expired_acled_copy(edge: _Edge) -> None:
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    events = {"usgs": _usgs([_IN]), "adsb": _adsb([_IN]), "acled": _acled([_IN])}
    first, _ = await _escalate(edge, **events)
    assert first["components"]["conflict"] == 0.5

    _age_entry(edge.db, "conflict:acled:", 2 * 86400, expire=True)
    second, requests = await _escalate(edge, **{**events, "acled": 503})

    assert len(requests["acled"]) == 1
    assert re.fullmatch(
        "Conflict events: " + _stale_pattern("ACLED answer", r"2\.0 days"),
        second["data_gaps"][0],
    ), second["data_gaps"]
    assert second["zero_filled_components"] == ["conflict", "social_unrest"]
    assert second["components"]["conflict"] == 0.0
    assert second["components"]["convergence"] is None


# ---------------------------------------------------------------------------
# Escalation: each AOI's news is its own
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_escalation_news_for_two_non_latin_places_is_not_shared(
    edge: _Edge,
) -> None:
    aoi.define_aoi(edge.store, "Kyiv", 50.4501, 30.5234, 30.0)
    aoi.define_aoi(edge.store, "Sumy", 50.9077, 34.7981, 30.0)

    def _place(request: httpx.Request) -> httpx.Response:
        city = "Київ" if float(request.url.params["lat"]) < 50.7 else "Суми"
        return httpx.Response(200, json={"display_name": city, "address": {"city": city}})

    bodies = {
        '"Київ"': _timeline([(10, 1000)] * 7, [(10, 1000)] * 7),
        '"Суми"': _timeline([(10, 1000)] * 7, [(30, 1000)] * 7),
    }

    def _gdelt(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=bodies[request.url.params["query"]])

    kyiv, _ = await _escalate(edge, "Kyiv", nominatim=_place, gdelt=_gdelt)
    sumy, requests = await _escalate(edge, "Sumy", nominatim=_place, gdelt=_gdelt)

    # Sumy's timeline was fetched, not served from Kyiv's cache entry.
    assert [r.url.params["query"] for r in requests["gdelt"]] == ['"Суми"']
    assert kyiv["news_signal"]["change_pct"] == 0.0
    assert sumy["news_signal"]["query"] == '"Суми"'
    # expected 70 of 7,000 x 7,000 = 70; change = 100 x (210 - 70) / 70 = 200.
    assert sumy["news_signal"]["change_pct"] == 200.0
    assert sumy["components"]["news"] == 20.0


# ---------------------------------------------------------------------------
# Escalation: a global sample cut at its cap is partial coverage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_escalation_usgs_answer_at_its_cap_is_partial(edge: _Edge) -> None:
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    newest_ms = int(time.time() * 1000) - 3_600_000
    quakes = _usgs([_IN] + [(-20.0 - i * 0.5, -70.0) for i in range(49)], newest_ms)
    oldest = _iso((newest_ms - 49 * 60_000) / 1000)

    result, requests = await _escalate(
        edge, usgs=quakes, adsb=_adsb([_IN]), acled=_acled([_IN])
    )

    assert requests["usgs"][-1].url.params["limit"] == "50"
    assert result["data_gaps"] == [
        "Earthquakes: partial coverage: USGS answered with its cap of 50 M4.5+ "
        f"quakes worldwide, newest first, so only quakes since {oldest} are in; "
        "earlier ones in the 72 h window are missing",
        "Convergence: computed without full earthquakes data; 8.2 is a lower "
        "bound (the missing events could only raise it).",
    ]
    assert result["convergence_signal"]["inputs_partial"] == ["earthquakes"]
    assert result["lower_bound_components"] == ["convergence"]


@pytest.mark.asyncio
async def test_escalation_acled_answer_at_its_cap_is_partial(edge: _Edge) -> None:
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    battles = _acled([_IN] + [(0.5 * i, 30.0) for i in range(99)])

    result, requests = await _escalate(edge, acled=battles)

    assert requests["acled"][-1].url.params["limit"] == "100"
    assert result["data_gaps"] == [
        "Conflict events: partial coverage: ACLED answered with its cap of 100 "
        "events worldwide for 7 days, so in-area events outside that sample "
        "are missing",
        "Convergence: not scored; no convergent cell among earthquakes, "
        "military flights, conflict events, wildfires, but conflict events "
        "answered only partly or not at all, so quiet cannot be told from "
        "missing.",
    ]
    assert result["lower_bound_components"] == ["conflict", "social_unrest"]
    assert result["components"]["conflict"] == 0.5


@pytest.mark.asyncio
async def test_escalation_firms_region_cut_at_its_cap_is_partial(edge: _Edge) -> None:
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    fires = [_IN] * 3 + [(60.0, -140.0 + i) for i in range(24)]

    result, _ = await _escalate(edge, firms=_firms(fires))

    assert result["data_gaps"] == [
        "Wildfires: partial coverage: NASA FIRMS listed only the 20 largest of "
        "25 fire clusters for north_america, so smaller in-area clusters can "
        "be missing",
        "Convergence: not scored; no convergent cell among earthquakes, "
        "military flights, conflict events, wildfires, but wildfires answered "
        "only partly or not at all, so quiet cannot be told from missing.",
    ]
    assert result["convergence_signal"]["events_by_domain"]["wildfires"] == 1


# ---------------------------------------------------------------------------
# Escalation: per-domain windows and fetch times
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_escalation_names_each_inputs_window_and_fetch_time(edge: _Edge) -> None:
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    before = int(time.time())
    result, requests = await _escalate(
        edge,
        usgs=_usgs([_IN]),
        adsb=_adsb([_IN]),
        acled=_acled([_IN]),
        firms=_firms([_IN]),
    )
    after = time.time()

    signal = result["convergence_signal"]
    domains = {"earthquakes", "military_flights", "conflict_events", "wildfires"}
    assert set(signal["windows"]) == set(signal["fetched_at"]) == domains
    for domain in ("earthquakes", "conflict_events", "wildfires"):
        assert before <= _epoch(signal["fetched_at"][domain]) <= after
    # military.py does not report when it fetched; null, and the window says so.
    assert signal["fetched_at"]["military_flights"] is None
    assert "fetched_at is null" in signal["windows"]["military_flights"]
    assert before <= _epoch(result["news_signal"]["fetched_at"]) <= after
    # The window text states the day range the FIRMS request really asks for.
    assert requests["firms"][-1].url.path.endswith("/2")
    assert "day range of 2" in signal["windows"]["wildfires"]
    assert "72 h" in signal["windows"]["earthquakes"]
    assert "7 days" in signal["windows"]["conflict_events"]


def test_news_trend_states_how_far_into_its_current_step_it_is() -> None:
    trend = aoi.news_coverage_trend(_FLAT["timeline"], now=_NOW)

    assert trend["current_step_start"] == "2026-10-17T00:00:00Z"
    assert trend["current_step_elapsed"] == 0.75  # 18 of 24 hours


# ---------------------------------------------------------------------------
# Escalation: the convergence grid does not split a compact cluster
# ---------------------------------------------------------------------------


def _scoped(quakes=(), aircraft=(), battles=(), fires=()) -> dict:
    return {
        "earthquakes": {"items": [{"latitude": a, "longitude": o} for a, o in quakes]},
        "military_flights": {
            "items": [{"latitude": a, "longitude": o} for a, o in aircraft]
        },
        "conflict_events": {"items": [{"latitude": a, "longitude": o} for a, o in battles]},
        "wildfires": {"items": [{"lat": a, "lon": o} for a, o in fires]},
    }


@pytest.mark.asyncio
async def test_escalation_cluster_straddling_a_degree_line_converges(
    edge: _Edge,
) -> None:
    """Three domains within 2.6 km, astride lon -80 (Pittsburgh's centre
    is 0.0041 degrees east of it). The unshifted grid puts the quake in
    cell -81 and the other two in -80; the grid shifted half a degree in
    longitude holds all three."""
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    result, _ = await _escalate(
        edge,
        usgs=_usgs([(40.5, -80.01)]),
        adsb=_adsb([(40.5, -79.99)]),
        acled=_acled([(40.5, -79.98)]),
    )

    signal = result["convergence_signal"]
    assert signal["grid_offset_deg"] == {"lat": 0.0, "lon": 0.5}
    assert signal["cells"][0]["event_count"] == 3
    assert (signal["cells"][0]["lat"], signal["cells"][0]["lon"]) == (40.5, -80.0)
    # 3 domains x (1 + sqrt 3) = 8.2; component min(8, 16.4) = 8.0.
    assert signal["score"] == 8.2
    assert result["components"]["convergence"] == 8.0
    assert result["data_gaps"] == []


@pytest.mark.asyncio
async def test_escalation_cluster_straddling_the_antimeridian_converges(
    edge: _Edge,
) -> None:
    aoi.define_aoi(edge.store, "Dateline", -16.5, 179.9, 50.0)
    result, _ = await _escalate(
        edge,
        "Dateline",
        usgs=_usgs([(-16.5, 179.95)]),
        adsb=_adsb([(-16.5, -179.95)]),
        acled=_acled([(-16.5, 179.9)]),
    )

    signal = result["convergence_signal"]
    assert signal["events_by_domain"] == {
        "earthquakes": 1, "military_flights": 1, "conflict_events": 1, "wildfires": 0,
    }
    assert signal["cells"][0]["event_count"] == 3
    assert (signal["cells"][0]["lat"], signal["cells"][0]["lon"]) == (-16.5, 180.0)
    assert result["components"]["convergence"] == 8.0


def test_convergence_cluster_astride_a_grid_corner_converges() -> None:
    # Astride lat 41 and lon -80: only the grid shifted on both axes holds
    # all three; its cell centre is (41.0, -80.0).
    result = aoi.aoi_convergence(
        _scoped(quakes=[(40.99, -80.01)], aircraft=[(41.01, -79.99)], battles=[(41.0, -80.0)])
    )

    assert result["grid_offset_deg"] == {"lat": 0.5, "lon": 0.5}
    assert result["score"] == 8.2
    assert (result["cells"][0]["lat"], result["cells"][0]["lon"]) == (41.0, -80.0)
    assert result["lower_bound"] is False


def test_convergence_cluster_spanning_over_half_a_degree_can_still_split() -> None:
    """The stated limit: a 0.6-degree span (lon -80.55 to -79.95) is split
    on the unshifted grid (-81 | -80) and on the shifted one (-81 | -80
    after +0.5), so no cell holds 3 events."""
    result = aoi.aoi_convergence(
        _scoped(quakes=[(40.5, -80.55)], aircraft=[(40.5, -80.05)], battles=[(40.5, -79.95)])
    )

    assert result["convergent_cells"] == 0
    assert result["score"] == 0.0


# ---------------------------------------------------------------------------
# Escalation: the news scope is named where it bites
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_escalation_names_the_news_scope_of_a_large_area(edge: _Edge) -> None:
    aoi.define_aoi(edge.store, "Western PA", 40.4406, -79.9959, 200.0)
    result, _ = await _escalate(edge, "Western PA")

    assert result["data_gaps"] == [
        "News: scoped to Pittsburgh, Allegheny County, the place at the AOI's "
        "centre; the area reaches 200 km from its centre, so coverage of other "
        "places inside it is not counted."
    ]


@pytest.mark.asyncio
async def test_escalation_names_why_a_shape_centred_at_sea_has_no_news(
    edge: _Edge,
) -> None:
    aoi.define_polygon_aoi(
        edge.store, "Strait", [[26.0, 56.0], [26.0, 57.0], [27.0, 57.0], [27.0, 56.0]]
    )
    result, requests = await _escalate(edge, "Strait", nominatim=_NOMINATIM_UNNAMED)

    assert requests["gdelt"] == []
    assert result["components"]["news"] is None
    assert result["data_gaps"][-1] == (
        "News: the query is built from the place at the AOI's centre only, so a "
        "polygon whose centre cannot be placed gets no news score even where it "
        "covers named places."
    )


@pytest.mark.asyncio
async def test_escalation_geocoder_outage_reason_is_not_prefixed_twice(
    edge: _Edge,
) -> None:
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    result, requests = await _escalate(edge, nominatim=503)

    assert len(requests["nominatim"]) == 1
    assert result["news_scoping"] == {
        "basis": "unavailable",
        "query": None,
        "reason": _GEOCODER_DOWN,
    }
    assert result["data_gaps"] == [
        f"News: not scored: the area could not be placed ({_GEOCODER_DOWN}), and "
        "counting mentions of the AOI's own name would not measure coverage of "
        "the area."
    ]


# ---------------------------------------------------------------------------
# Escalation: what the score can reach, and the news gate's edges
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("specs", "ceiling"),
    [
        # Everything measured: 80 of 100 (the baseline is a structural 0).
        ({}, 80.0),
        # News unplaced: 60 of 80.
        ({"nominatim": _NOMINATIM_UNNAMED}, 75.0),
        # News unplaced and convergence unscored (USGS down, no cell): 52 of 72.
        ({"nominatim": _NOMINATIM_UNNAMED, "usgs": 503}, 72.2),
        # ACLED down: conflict and unrest zero-filled, convergence unscored:
        # news 20 + military 20 = 40 of 92.
        ({"acled": 503}, 43.5),
    ],
)
async def test_escalation_score_ceiling(edge: _Edge, specs: dict, ceiling: float) -> None:
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    result, _ = await _escalate(edge, **specs)

    assert result["score_ceiling"] == ceiling
    assert "baseline is a structural 0" in result["score_method"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("current_norm", "news"),
    [
        # 2,100 monitored against 7,000: 0.3 of the other window, refused.
        (300, None),
        # 3,500 against 7,000: exactly half, compared (flat share: 0.0).
        (500, 0.0),
    ],
)
async def test_escalation_news_intake_gate_boundary(
    edge: _Edge, current_norm: int, news: float | None
) -> None:
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    share_kept = [(current_norm // 100, current_norm)] * 7
    result, _ = await _escalate(edge, gdelt=_timeline([(10, 1000)] * 7, share_kept))

    assert result["components"]["news"] == news


@pytest.mark.asyncio
async def test_escalation_news_with_no_monitored_volume_is_a_gap_not_a_crash(
    edge: _Edge,
) -> None:
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    result, _ = await _escalate(edge, gdelt=_timeline([(0, 0)] * 7, [(0, 0)] * 7))

    assert result["components"]["news"] is None
    assert result["data_gaps"] == [
        "News: GDELT intake too uneven to compare: it monitored 0 articles in "
        "the prior 7 days (7 of 7 steps with data) and 0 in the latest 7 (7 of "
        "7), so a comparison would measure GDELT's ingestion gaps, not the news."
    ]


@pytest.mark.asyncio
async def test_escalation_falling_news_share_scores_zero_not_negative(
    edge: _Edge,
) -> None:
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    falling = _timeline([(20, 1000)] * 7, [(10, 1000)] * 7)
    result, _ = await _escalate(edge, gdelt=falling)

    # expected 140; change = 100 x (70 - 140) / 140 = -50.0.
    assert result["news_signal"]["change_pct"] == -50.0
    assert result["components"]["news"] == 0.0


@pytest.mark.asyncio
async def test_escalation_one_domain_alone_is_not_convergence(edge: _Edge) -> None:
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    result, _ = await _escalate(edge, adsb=_adsb([_IN, _IN, _IN]))

    assert result["components"]["military"] == 3.0
    # 3 aircraft in one cell are 1 domain, under the 2 a cell needs.
    assert result["convergence_signal"]["convergent_cells"] == 0
    assert result["components"]["convergence"] == 0.0


# ---------------------------------------------------------------------------
# Scoping helpers: an unrecognized answer is unknown, not empty
# ---------------------------------------------------------------------------

_PGH_ROW = {"name": "Pittsburgh", "lat": 40.4406, "lon": -79.9959, "radius_km": 50.0,
            "kind": "circle"}


@pytest.mark.parametrize("answer", [{}, {"count": 0, "source": "usgs"}])
def test_scoping_an_answer_without_its_list_is_unknown(answer: dict) -> None:
    assert aoi._scope_to_aoi(answer, "earthquakes", _PGH_ROW, "latitude", "longitude") == {
        "error": "the answer carried no earthquakes list, so what is inside the area "
        "is unknown"
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("answer", "error"),
    [
        # military.py's shape before it marked the outage (no error key).
        ({"aircraft": [], "count": 0, "source": "none"}, _MILITARY_DOWN),
        # The same shape with an outage marker added, as military.py answers now.
        (
            {"aircraft": [], "count": 0, "source": "none", "error": "both sources failed"},
            "both sources failed",
        ),
        ({"count": 0, "source": "adsb.lol"}, "the military flights answer carried no aircraft list"),
    ],
)
async def test_military_answer_without_a_source_or_list_is_an_outage(
    edge: _Edge, monkeypatch: pytest.MonkeyPatch, answer: dict, error: str
) -> None:
    async def _answer(fetcher, bbox=None) -> dict:
        return dict(answer)

    monkeypatch.setattr(military, "fetch_military_flights", _answer)
    result = await aoi._fetch_military_merged(edge.fetcher, ["39.9,-80.6,41.0,-79.4"])

    assert result == {"error": error}


@pytest.mark.asyncio
async def test_changes_skip_an_in_fence_item_whose_key_cannot_be_built(
    edge: _Edge, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A wildfire cluster whose lat is not a number reaches the diff (the
    scoping is patched to let it through): it is skipped, not a crash."""
    aoi.define_aoi(edge.store, "Pittsburgh", *_PGH)
    real_filter = aoi.filter_by_aoi

    def _leaky_filter(items, aoi_row, lat_key="latitude", lon_key="longitude"):
        kept = real_filter(items, aoi_row, lat_key, lon_key)
        if any("fire_count" in item for item in items):
            kept.append({"lat": "unplaceable", "lon": -79.9, "fire_count": 1,
                         "distance_km": 0.0})
        return kept

    monkeypatch.setattr(aoi, "filter_by_aoi", _leaky_filter)
    with respx.mock(assert_all_mocked=True, assert_all_called=False) as router:
        _routes(router, firms=_firms([_IN]))
        result = await aoi.fetch_aoi_changes(edge.fetcher, edge.store, "Pittsburgh")

    assert result["baseline"] is True
    assert result["counts"]["wildfires"] == 1


# ---------------------------------------------------------------------------
# CLI: a zero-filled component is not a measured 0
# ---------------------------------------------------------------------------


def test_cli_escalation_marks_zero_filled_and_lower_bound_components(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The CLI renders a real escalation result: both military sources
    down (zero-filled) and USGS at its cap (convergence a lower bound)."""
    _hermetic(monkeypatch)

    async def _measure() -> dict:
        env = _Edge(tmp_path)
        try:
            aoi.define_aoi(env.store, "Pittsburgh", *_PGH)
            quakes = _usgs([_IN] + [(-20.0 - i * 0.5, -70.0) for i in range(49)])
            result, _ = await _escalate(
                env,
                adsb=503,
                opensky=503,
                usgs=quakes,
                acled=_acled([_IN]),
                firms=_firms([_IN]),
            )
            return result
        finally:
            await env.aclose()

    result = asyncio.run(_measure())
    assert result["zero_filled_components"] == ["military"]
    assert result["lower_bound_components"] == ["convergence"]

    async def _canned(*args, **kwargs) -> dict:
        return result

    monkeypatch.setattr(cli.aoi_analysis, "fetch_aoi_escalation", _canned)
    run = CliRunner().invoke(
        cli.main, ["aoi", "escalation", "Pittsburgh"], catch_exceptions=False
    )
    out = " ".join(run.output.split())

    assert run.exit_code == 0
    # conflict 0.5 + convergence 8.0 = 8.5 of 100.
    assert "Pittsburgh escalation: 8.5 on a 0-100 scale" in out
    assert (
        "Highest score reachable with the components measured: 60.0 (an AOI has "
        "no curated baseline)" in out
    )
    assert (
        "Computed from: baseline, news, military, conflict, social_unrest, "
        "convergence" in out
    )
    assert "military not measured (fetch failed; scored as 0)" in out
    assert "convergence 8.0 (lower bound)" in out
    assert (
        "Military flights: No military flight source available (adsb.lol and "
        "OpenSky both failed)" in out
    )
