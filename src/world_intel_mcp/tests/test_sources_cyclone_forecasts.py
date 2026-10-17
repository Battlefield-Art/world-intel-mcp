"""Tests for opt-in tropical cyclone forecast tracks (sources/cyclone_forecasts.py).

Every upstream body is a live capture from 2026-10-17 ~16:45Z, stored
under ``fixtures/cyclones/``: NHC CurrentStorms.json with Hurricane
Trudy (EP212026, advisory 17), Trudy's forecast/advisory text (the
``<pre>`` body of MIATCMEP1.shtml), Trudy's 5-day cone KML, the JTWC RSS
feed, and the JTWC warning texts for Typhoon 29W (Champi) warning 13
and Tropical Cyclone 01P (Wati) warning 3. Two edits to the captures,
both semantics-free: the cone KML's ``<coordinates>`` whitespace is
re-wrapped (1,168 tuples, values byte-identical), and the JTWC texts
are stored with LF endings and served here with CRLF, as live.

Expected values were read off the products themselves and cross-checked
against an independent product from the same issuance: NHC's KMZ
forecast track for advisory 17 (same positions and winds at every
forecast hour; its times are local "CST" strings) and JTWC's JMV 3.0
``.tcw`` files (same tau/positions/winds; its AMP block lists 01P's
60 h BECOMING EXTRATROPICAL and 72 h EXTRATROPICAL).

Gaps / not covered: a Central Pacific (CPHC) forecast/advisory and a
cone that crosses the antimeridian (neither observed); NHC products for
potential tropical cyclones; a JTWC final warning (not observed, so the
shape of a warning with no forecast blocks is unknown and is reported as
unparsed); the JTWC 21E (Trudy) warning text body (only its failure path
is exercised here); live end-to-end MCP stdio calls.
"""

import asyncio as asyncio_mod
import io
import json
import math
import random
import struct
import threading
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import cyclone_forecasts as cf
from world_intel_mcp.sources.cyclones import fetch_cyclones
from world_intel_mcp.sources.jtwc import fetch_jtwc_cyclones

_FIXTURES = Path(__file__).parent / "fixtures" / "cyclones"

_CURRENT_STORMS_URL = "https://www.nhc.noaa.gov/CurrentStorms.json"
_TCM_URL = "https://www.nhc.noaa.gov/text/MIATCMEP1.shtml"
_CONE_URL = "https://www.nhc.noaa.gov/storm_graphics/api/EP212026_017adv_CONE.kmz"
_JTWC_RSS_URL = "https://www.metoc.navy.mil/jtwc/rss/jtwc.rss"
_JTWC_29W_URL = "https://www.metoc.navy.mil/jtwc/products/wp2926web.txt"
_JTWC_01P_URL = "https://www.metoc.navy.mil/jtwc/products/sh0127web.txt"
_JTWC_21E_URL = "https://www.metoc.navy.mil/jtwc/products/ep2126web.txt"


def _fixture(name: str) -> str:
    return (_FIXTURES / name).read_text(encoding="utf-8")


def _current_storms() -> dict:
    return json.loads(_fixture("nhc_current_storms_20261017.json"))


def _tcm_page() -> str:
    """The live page carries the product in its only <pre>, inside
    <div class="textbackground"><div class="textproduct">."""
    return (
        '<html><body><div class="textbackground"><div class="textproduct">\n'
        "  <pre>" + _fixture("nhc_tcm_ep212026_adv017.txt") + "  </pre>\n"
        "  </div></div></body></html>"
    )


def _kmz(
    kml: str | bytes,
    name: str = "ep212026_017adv_CONE.kml",
    method: int = zipfile.ZIP_DEFLATED,
) -> bytes:
    """Zip a KML the way the live CONE.kmz is packed (one deflated member,
    unless ``method`` names another compression)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", method) as zf:
        zf.writestr(name, kml)
    return buf.getvalue()


def _cone_kmz() -> bytes:
    return _kmz(_fixture("nhc_ep212026_017adv_CONE.kml"))


def _crlf(text: str) -> str:
    return text.replace("\n", "\r\n")


@pytest.fixture
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fetcher retries sleep between attempts; failure-path tests skip it."""

    async def _no_sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)


_FAR_NOW = datetime(2027, 3, 1, 12, 0, tzinfo=timezone.utc)


class _FrozenClock(datetime):
    """Stands in for cyclone_forecasts.datetime: ``now()`` returns ``at``."""

    at = _FAR_NOW

    @classmethod
    def now(cls, tz=None):
        return cls.at


@pytest.fixture(autouse=True)
def _far_now(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test here runs with "now" fixed at 2027-03-01T12:00Z, four
    and a half months from the 2026-10-17 captures. Day-only product times
    ("18/0000Z") then reach their expected dates only through the
    products' own date lines, so the tests exercise that anchoring and
    give the same result on any day the suite runs. A test that needs a
    different "now" sets ``_FrozenClock.at`` itself."""
    monkeypatch.setattr(cf, "datetime", _FrozenClock)


def _mock_nhc(
    current: dict | None = None,
    tcm: httpx.Response | None = None,
    cone: httpx.Response | None = None,
) -> dict:
    routes = {
        "current": respx.get(_CURRENT_STORMS_URL).mock(
            return_value=httpx.Response(200, json=current or _current_storms())
        ),
        "tcm": respx.get(_TCM_URL).mock(
            return_value=tcm
            or httpx.Response(
                200, text=_tcm_page(), headers={"content-type": "text/html; charset=UTF-8"}
            )
        ),
        "cone": respx.get(_CONE_URL).mock(
            return_value=cone
            or httpx.Response(
                200,
                content=_cone_kmz(),
                headers={"content-type": "application/vnd.google-earth.kmz"},
            )
        ),
    }
    return routes


# ---------------------------------------------------------------------------
# NHC: forecast track from the forecast/advisory text, cone from the KMZ
# ---------------------------------------------------------------------------

_TRUDY_POINTS = [
    # (tau, valid_time, lat, lon, max_wind_kt, gust_kt, status, section)
    (12, "2026-10-18T00:00:00Z", 17.7, -104.8, 90, 110, None, "forecast"),
    (24, "2026-10-18T12:00:00Z", 19.1, -105.7, 90, 110, None, "forecast"),
    (36, "2026-10-19T00:00:00Z", 20.1, -106.4, 85, 105, None, "forecast"),
    (48, "2026-10-19T12:00:00Z", 20.7, -107.0, 65, 80, None, "forecast"),
    (60, "2026-10-20T00:00:00Z", 20.9, -107.5, 45, 55, None, "forecast"),
    (72, "2026-10-20T12:00:00Z", 20.9, -108.2, 35, 45, None, "forecast"),
    (96, "2026-10-21T12:00:00Z", 21.7, -109.4, 25, 35, "POST-TROP/REMNT LOW", "outlook"),
    (120, "2026-10-22T12:00:00Z", None, None, None, None, "DISSIPATED", "outlook"),
]


def _as_tuples(points: list[dict]) -> list[tuple]:
    return [
        (
            p["tau_hours"],
            p["valid_time"],
            p["latitude"],
            p["longitude"],
            p["max_wind_kt"],
            p["gust_kt"],
            p["status"],
            p["section"],
        )
        for p in points
    ]


@respx.mock
@pytest.mark.asyncio
async def test_nhc_forecast_track_and_cone_from_live_trudy_advisory(
    fetcher: Fetcher,
) -> None:
    routes = _mock_nhc()

    result = await fetch_cyclones(fetcher, include_forecast=True)

    assert result["count"] == 1
    assert "degraded" not in result
    assert result["data_gaps"] == []
    trudy = result["storms"][0]
    assert trudy["id"] == "ep212026"
    # The warning-level fields are untouched by the opt-in.
    assert trudy["intensity_kt"] == 80
    assert trudy["pressure_mb"] == 979

    fc = trudy["forecast"]
    assert fc["agency"] == "NHC"
    assert fc["track_status"] == "ok"
    assert fc["track_url"] == _TCM_URL
    assert fc["advisory_number"] == "17"
    assert fc["issued"] == "2026-10-17T15:00:00Z"
    assert fc["initial"] == {
        "valid_time": "2026-10-17T15:00:00Z",
        "latitude": 16.6,
        "longitude": -103.9,
        "max_wind_kt": 80,
        "gust_kt": 100,
        "pressure_mb": 979,
    }
    assert _as_tuples(fc["points"]) == _TRUDY_POINTS

    assert fc["cone_status"] == "ok"
    assert fc["cone_url"] == _CONE_URL
    cone = fc["cone"]
    assert cone["advisory_number"] == "17"
    assert cone["forecast_period_hours"] == 120
    assert cone["source_vertex_count"] == 1167  # 1,168 tuples, ring closed
    assert cone["simplify_tolerance_deg"] == 0.01
    assert cone["vertex_count"] == len(cone["vertices"])
    assert 3 <= cone["vertex_count"] <= 64
    # [lat, lon] pairs, open ring, starting where NHC's ring starts.
    assert cone["vertices"][0] == [16.33344, -103.90001]
    assert cone["vertices"][0] != cone["vertices"][-1]

    assert routes["tcm"].call_count == 1
    assert routes["cone"].call_count == 1


@respx.mock
@pytest.mark.asyncio
async def test_nhc_cone_outline_stays_within_stated_tolerance(fetcher: Fetcher) -> None:
    """The simplified outline is a subset of NHC's vertices, in order,
    and every NHC vertex lies within the stated tolerance of it."""
    _mock_nhc()
    result = await fetch_cyclones(fetcher, include_forecast=True)
    cone = result["storms"][0]["forecast"]["cone"]

    source = cf.parse_cone_kml_ring(_fixture("nhc_ep212026_017adv_CONE.kml").encode())
    assert len(source) == 1167
    kept = [tuple(v) for v in cone["vertices"]]
    positions = [source.index(v) for v in kept]
    assert positions == sorted(positions)

    def seg_dist(p, a, b) -> float:
        (px, py), (ax, ay), (bx, by) = p, a, b
        dx, dy = bx - ax, by - ay
        if dx == 0 and dy == 0:
            return math.dist(p, a)
        t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
        return math.dist(p, (ax + t * dx, ay + t * dy))

    ring = kept + [kept[0]]
    worst = max(
        min(seg_dist(p, ring[i], ring[i + 1]) for i in range(len(ring) - 1))
        for p in source
    )
    assert worst <= cone["simplify_tolerance_deg"] + 1e-9


@respx.mock
@pytest.mark.asyncio
async def test_nhc_default_call_is_unchanged_and_fetches_no_products(
    fetcher: Fetcher,
) -> None:
    routes = _mock_nhc()

    result = await fetch_cyclones(fetcher)

    assert "forecast" not in result["storms"][0]
    assert "data_gaps" not in result
    assert routes["tcm"].called is False
    assert routes["cone"].called is False


@respx.mock
@pytest.mark.asyncio
async def test_nhc_quiet_season_with_forecast_is_not_degraded(fetcher: Fetcher) -> None:
    routes = _mock_nhc(current={"activeStorms": []})

    result = await fetch_cyclones(fetcher, include_forecast=True)

    assert result["count"] == 0
    assert result["data_gaps"] == []
    assert "degraded" not in result
    assert "error" not in result
    assert routes["tcm"].called is False


@respx.mock
@pytest.mark.asyncio
async def test_nhc_feed_down_with_forecast_keeps_feed_failure_marker(
    fetcher: Fetcher, no_sleep: None
) -> None:
    respx.get(_CURRENT_STORMS_URL).mock(return_value=httpx.Response(500))
    tcm = respx.get(_TCM_URL).mock(return_value=httpx.Response(200, text=_tcm_page()))

    result = await fetch_cyclones(fetcher, include_forecast=True)

    assert result["degraded"] is True
    assert result["reason"] == "nhc_fetch_failed"
    assert result["storms"] == []
    assert tcm.called is False


@respx.mock
@pytest.mark.asyncio
async def test_nhc_track_outage_is_named_never_an_empty_track(
    fetcher: Fetcher, no_sleep: None
) -> None:
    _mock_nhc(tcm=httpx.Response(503))

    result = await fetch_cyclones(fetcher, include_forecast=True)

    assert result["degraded"] is True
    assert "error" not in result  # the warnings themselves arrived
    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == "fetch_failed"
    assert "unavailable" in fc["track_error"]
    assert fc["points"] == []
    assert fc["initial"] is None
    # The cone is independent of the text product and still arrives.
    assert fc["cone_status"] == "ok"
    assert any("Trudy" in g and "track" in g for g in result["data_gaps"])


@respx.mock
@pytest.mark.asyncio
async def test_nhc_cone_outage_keeps_track_and_names_the_gap(
    fetcher: Fetcher, no_sleep: None
) -> None:
    _mock_nhc(cone=httpx.Response(500))

    result = await fetch_cyclones(fetcher, include_forecast=True)

    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == "ok"
    assert len(fc["points"]) == 8
    assert fc["cone"] is None
    assert fc["cone_status"] == "fetch_failed"
    assert "500" in fc["cone_error"]
    assert result["degraded"] is True
    assert any("Trudy" in g and "cone" in g for g in result["data_gaps"])


@respx.mock
@pytest.mark.asyncio
async def test_nhc_unlinked_cone_is_a_gap_not_a_failure(fetcher: Fetcher) -> None:
    current = _current_storms()
    current["activeStorms"][0]["trackCone"] = None
    routes = _mock_nhc(current=current)

    result = await fetch_cyclones(fetcher, include_forecast=True)

    fc = result["storms"][0]["forecast"]
    assert fc["cone_status"] == "not_linked"
    assert fc["cone"] is None
    assert "degraded" not in result
    assert any("no cone" in g for g in result["data_gaps"])
    assert routes["cone"].called is False


@respx.mock
@pytest.mark.asyncio
async def test_nhc_html_error_page_served_200_is_unparsed_not_empty(
    fetcher: Fetcher,
) -> None:
    _mock_nhc(
        tcm=httpx.Response(200, text="<html><body><h1>Not Found</h1></body></html>")
    )

    result = await fetch_cyclones(fetcher, include_forecast=True)

    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == "unparsed"
    assert fc["points"] == []
    assert result["degraded"] is True


@respx.mock
@pytest.mark.asyncio
async def test_nhc_lagging_advisory_page_is_flagged_and_not_kept(
    fetcher: Fetcher,
) -> None:
    """CurrentStorms.json can name advisory 018 while the bin-based text
    page still serves 017: return 017 labelled as such, mark the gap,
    and drop the cached page so the next call refetches it."""
    current = _current_storms()
    current["activeStorms"][0]["forecastAdvisory"]["advNum"] = "018"
    routes = _mock_nhc(current=current)

    result = await fetch_cyclones(fetcher, include_forecast=True)

    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == "stale_advisory"
    assert fc["advisory_number"] == "17"
    assert fc["expected_advisory_number"] == "018"
    assert len(fc["points"]) == 8
    assert result["degraded"] is True
    assert any("018" in g for g in result["data_gaps"])

    await fetch_cyclones(fetcher, include_forecast=True)
    assert routes["tcm"].call_count == 2


@respx.mock
@pytest.mark.asyncio
async def test_nhc_forecast_fan_out_is_capped_and_the_cap_is_named(
    fetcher: Fetcher,
) -> None:
    base = _current_storms()["activeStorms"][0]
    many = []
    for i in range(cf.MAX_FORECAST_STORMS + 2):
        storm = json.loads(json.dumps(base))
        storm["id"] = f"ep{30 + i:02d}2026"
        storm["name"] = f"Storm{i}"
        many.append(storm)
    routes = _mock_nhc(current={"activeStorms": many})

    result = await fetch_cyclones(fetcher, include_forecast=True)

    statuses = [s["forecast"]["track_status"] for s in result["storms"]]
    assert statuses[: cf.MAX_FORECAST_STORMS] == ["ok"] * cf.MAX_FORECAST_STORMS
    assert statuses[cf.MAX_FORECAST_STORMS :] == ["not_fetched_cap"] * 2
    assert routes["tcm"].call_count == cf.MAX_FORECAST_STORMS
    assert result["degraded"] is True
    assert sum("cap" in g for g in result["data_gaps"]) == 2


@respx.mock
@pytest.mark.asyncio
async def test_nhc_products_are_fetched_once_per_issuance(fetcher: Fetcher) -> None:
    """A second call for the same advisory reuses the text and the parsed
    cone (their cache keys carry the advisory number) instead of
    refetching NHC's products."""
    routes = _mock_nhc()

    first = await fetch_cyclones(fetcher, include_forecast=True)
    second = await fetch_cyclones(fetcher, include_forecast=True)

    fc = second["storms"][0]["forecast"]
    assert fc == first["storms"][0]["forecast"]
    assert _as_tuples(fc["points"]) == _TRUDY_POINTS
    assert fc["cone"]["source_vertex_count"] == 1167
    assert fc["cone"]["vertices"][0] == [16.33344, -103.90001]
    assert second["data_gaps"] == []
    assert routes["tcm"].call_count == 1
    assert routes["cone"].call_count == 1


@respx.mock
@pytest.mark.asyncio
async def test_nhc_storm_without_a_forecast_advisory_link_is_a_gap_not_a_failure(
    fetcher: Fetcher,
) -> None:
    current = _current_storms()
    current["activeStorms"][0]["forecastAdvisory"] = None
    routes = _mock_nhc(current=current)

    result = await fetch_cyclones(fetcher, include_forecast=True)

    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == "not_linked"
    assert fc["track_url"] is None
    assert fc["points"] == []
    assert fc["cone_status"] == "ok"  # the cone has its own link
    assert "degraded" not in result
    assert result["data_gaps"] == [
        "Trudy (ep212026): forecast track not_linked: "
        "CurrentStorms.json links no forecast/advisory for this storm"
    ]
    assert routes["tcm"].called is False


@respx.mock
@pytest.mark.asyncio
async def test_nhc_product_links_off_the_nhc_host_are_refused_not_fetched(
    fetcher: Fetcher,
) -> None:
    """Product links come from the feed: one that is not https on
    www.nhc.noaa.gov is never requested. A request would reach respx
    unmocked and read as fetch_failed, not refused_link."""
    current = _current_storms()
    storm = current["activeStorms"][0]
    storm["forecastAdvisory"]["url"] = "https://127.0.0.1:8080/text/MIATCMEP1.shtml"
    storm["trackCone"]["kmzFile"] = _CONE_URL.replace("https://", "http://")
    routes = _mock_nhc(current=current)

    result = await fetch_cyclones(fetcher, include_forecast=True)

    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == "refused_link"
    assert fc["points"] == []
    assert fc["cone_status"] == "refused_link"
    assert fc["cone"] is None
    assert result["degraded"] is True
    assert result["data_gaps"] == [
        "Trudy (ep212026): forecast track refused_link: forecast/advisory link "
        "'https://127.0.0.1:8080/text/MIATCMEP1.shtml' is not https on "
        "www.nhc.noaa.gov; not fetched",
        "Trudy (ep212026): no cone, cone KMZ link "
        "'http://www.nhc.noaa.gov/storm_graphics/api/EP212026_017adv_CONE.kmz' "
        "is not https on www.nhc.noaa.gov; not fetched",
    ]
    assert routes["tcm"].called is False
    assert routes["cone"].called is False


@pytest.mark.parametrize(
    ("url", "allowed"),
    [
        ("https://www.nhc.noaa.gov/text/MIATCMEP1.shtml", True),
        ("https://WWW.NHC.NOAA.GOV/text/MIATCMEP1.shtml", True),
        ("https://www.nhc.noaa.gov:443/text/MIATCMEP1.shtml", True),
        ("http://www.nhc.noaa.gov/text/MIATCMEP1.shtml", False),
        ("https://www.nhc.noaa.gov:8443/text/MIATCMEP1.shtml", False),
        ("https://www.nhc.noaa.gov:99999/text/MIATCMEP1.shtml", False),
        ("https://www.nhc.noaa.gov@169.254.169.254/latest", False),
        ("https://www.nhc.noaa.gov.example.net/text/MIATCMEP1.shtml", False),
        ("https://nhc.noaa.gov/text/MIATCMEP1.shtml", False),
        ("file:///etc/passwd", False),
        ("/text/MIATCMEP1.shtml", False),
    ],
)
def test_product_link_allowlist(url: str, allowed: bool) -> None:
    assert (cf._refused_link(url, cf._NHC_PRODUCT_HOSTS) is None) is allowed


# ---------------------------------------------------------------------------
# NHC parsers: direct
# ---------------------------------------------------------------------------


def test_cone_kmz_without_kml_member_is_refused() -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("readme.txt", "no kml here")
    with pytest.raises(ValueError, match="no .kml"):
        cf.parse_nhc_cone_kmz(buf.getvalue())


def test_cone_kmz_that_is_not_a_zip_is_refused() -> None:
    with pytest.raises(zipfile.BadZipFile):
        cf.parse_nhc_cone_kmz(b"<html>maintenance</html>")


def test_cone_with_more_than_one_ring_is_refused_not_trimmed() -> None:
    kml = _fixture("nhc_ep212026_017adv_CONE.kml")
    start = kml.index("<Placemark>")
    end = kml.index("</Placemark>") + len("</Placemark>")
    two = kml[:end] + kml[start:end] + kml[end:]
    assert two.count("<Polygon>") == 2
    with pytest.raises(ValueError, match="2 rings"):
        cf.parse_nhc_cone_kmz(_kmz(two))


def test_cone_kml_with_doctype_is_refused() -> None:
    kml = '<?xml version="1.0"?><!DOCTYPE kml [<!ENTITY x "y">]><kml/>'
    with pytest.raises(ValueError, match="DOCTYPE"):
        cf.parse_nhc_cone_kmz(_kmz(kml))


def _coords_kml(tokens: list[str]) -> str:
    """A minimal cone KML around one ring of KML "lon,lat,alt" tokens."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<kml xmlns="http://www.opengis.net/kml/2.2"><Placemark><Polygon>'
        "<outerBoundaryIs><LinearRing><coordinates>"
        + " ".join(tokens)
        + "</coordinates></LinearRing></outerBoundaryIs></Polygon></Placemark></kml>"
    )


def _ring_kml(vertices: int) -> str:
    """A closed ring of ``vertices`` distinct points in a minimal cone KML:
    a thin band, out along 15.00N and back along 15.05N, which simplifies
    to its four corners (a cone outline needs at least three vertices)."""
    half = vertices // 2
    out = [(15.0, -100 + i * 1e-4) for i in range(half)]
    back = [(15.05, -100 + i * 1e-4) for i in reversed(range(vertices - half))]
    points = [f"{lon:.4f},{lat:.4f},0" for lat, lon in out + back]
    return _coords_kml(points + points[:1])


def test_cone_ring_over_the_vertex_cap_is_refused_before_simplifying() -> None:
    """Douglas-Peucker is quadratic in the worst case; a ring past the cap
    is refused, one at the cap is simplified."""
    at_cap = cf.parse_nhc_cone_kmz(_kmz(_ring_kml(cf._CONE_MAX_VERTICES)))
    assert at_cap["source_vertex_count"] == cf._CONE_MAX_VERTICES
    with pytest.raises(ValueError, match="over the cap"):
        cf.parse_nhc_cone_kmz(_kmz(_ring_kml(cf._CONE_MAX_VERTICES + 1)))


def test_forecast_advisory_without_synoptic_line_keeps_times_drops_tau() -> None:
    text = _fixture("nhc_tcm_ep212026_adv017.txt").replace(
        "AT 17/1200Z CENTER WAS LOCATED NEAR 16.3N 103.7W\n", ""
    )
    parsed = cf.parse_nhc_forecast_advisory(text)
    assert parsed["points"][0]["valid_time"] == "2026-10-18T00:00:00Z"
    assert parsed["points"][0]["tau_hours"] is None


def test_day_only_times_resolve_across_month_and_year_ends() -> None:
    oct31 = datetime(2026, 10, 31, 21, 0, tzinfo=timezone.utc)
    assert cf.resolve_day_time(1, 0, 0, oct31) == datetime(2026, 11, 1, tzinfo=timezone.utc)
    nov1 = datetime(2026, 11, 1, 3, 0, tzinfo=timezone.utc)
    assert cf.resolve_day_time(31, 18, 0, nov1) == datetime(
        2026, 10, 31, 18, 0, tzinfo=timezone.utc
    )
    dec31 = datetime(2026, 12, 31, 12, 0, tzinfo=timezone.utc)
    assert cf.resolve_day_time(2, 12, 0, dec31) == datetime(
        2027, 1, 2, 12, 0, tzinfo=timezone.utc
    )


def test_simplify_ring_drops_collinear_vertices_only() -> None:
    square = [(0.0, 0.0), (0.5, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]
    assert cf.simplify_closed_ring(square, 0.01) == [
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 1.0),
        (0.0, 1.0),
    ]


# ---------------------------------------------------------------------------
# JTWC: forecast track from the warning text; JTWC links no cone
# ---------------------------------------------------------------------------

_CHAMPI_POINTS = [
    (12, "2026-10-18T00:00:00Z", 13.0, 143.7, 120, 145, None, "forecast"),
    (24, "2026-10-18T12:00:00Z", 13.5, 141.6, 135, 165, None, "forecast"),
    (36, "2026-10-19T00:00:00Z", 14.3, 139.5, 140, 170, None, "forecast"),
    (48, "2026-10-19T12:00:00Z", 15.3, 137.4, 135, 165, None, "extended_outlook"),
    (60, "2026-10-20T00:00:00Z", 16.3, 135.1, 135, 165, None, "extended_outlook"),
    (72, "2026-10-20T12:00:00Z", 17.3, 132.8, 135, 165, None, "extended_outlook"),
    (96, "2026-10-21T12:00:00Z", 19.4, 128.6, 125, 150, None, "long_range_outlook"),
    (120, "2026-10-22T12:00:00Z", 21.0, 125.9, 105, 130, None, "long_range_outlook"),
]

_WATI_POINTS = [
    (12, "2026-10-17T18:00:00Z", -14.9, -162.6, 70, 85, None, "forecast"),
    (24, "2026-10-18T06:00:00Z", -16.6, -161.4, 75, 90, None, "forecast"),
    (36, "2026-10-18T18:00:00Z", -18.5, -159.1, 65, 80, None, "forecast"),
    (48, "2026-10-19T06:00:00Z", -20.7, -155.7, 60, 75, None, "extended_outlook"),
    (60, "2026-10-19T18:00:00Z", -23.5, -150.9, 50, 65, "BECOMING EXTRATROPICAL", "extended_outlook"),
    (72, "2026-10-20T06:00:00Z", -26.3, -145.8, 45, 55, "EXTRATROPICAL", "extended_outlook"),
]


def _mock_jtwc(trudy: httpx.Response | None = None) -> dict:
    return {
        "rss": respx.get(_JTWC_RSS_URL).mock(
            return_value=httpx.Response(200, text=_fixture("jtwc_rss_20261017.xml"))
        ),
        "29W": respx.get(_JTWC_29W_URL).mock(
            return_value=httpx.Response(
                200, text=_crlf(_fixture("jtwc_wp2926web_wrn013.txt"))
            )
        ),
        "01P": respx.get(_JTWC_01P_URL).mock(
            return_value=httpx.Response(
                200, text=_crlf(_fixture("jtwc_sh0127web_wrn003.txt"))
            )
        ),
        "21E": respx.get(_JTWC_21E_URL).mock(
            return_value=trudy or httpx.Response(503)
        ),
    }


@respx.mock
@pytest.mark.asyncio
async def test_jtwc_forecast_tracks_from_live_warnings_and_named_failure(
    fetcher: Fetcher, no_sleep: None
) -> None:
    routes = _mock_jtwc()

    result = await fetch_jtwc_cyclones(fetcher, include_forecast=True)

    assert result["count"] == 3
    by_id = {s["id"]: s for s in result["storms"]}

    champi = by_id["29W"]["forecast"]
    assert champi["agency"] == "JTWC"
    assert champi["track_status"] == "ok"
    assert champi["track_url"] == _JTWC_29W_URL
    assert champi["warning_number"] == 13
    assert champi["issued"] == "2026-10-17T15:00:00Z"
    assert champi["initial"] == {
        "valid_time": "2026-10-17T12:00:00Z",
        "latitude": 12.6,
        "longitude": 146.1,
        "max_wind_kt": 120,
        "gust_kt": 145,
        "pressure_mb": 945,
    }
    assert _as_tuples(champi["points"]) == _CHAMPI_POINTS
    assert champi["cone"] is None
    assert champi["cone_status"] == "not_published"

    wati = by_id["01P"]["forecast"]
    assert wati["track_status"] == "ok"
    assert wati["warning_number"] == 3
    assert wati["issued"] == "2026-10-17T09:00:00Z"
    assert wati["initial"] == {
        "valid_time": "2026-10-17T06:00:00Z",
        "latitude": -13.2,
        "longitude": -163.2,
        "max_wind_kt": 60,
        "gust_kt": 75,
        "pressure_mb": 986,  # "IS 986 \r\nMB." wraps a line in the live text
    }
    assert _as_tuples(wati["points"]) == _WATI_POINTS

    trudy = by_id["21E"]["forecast"]
    assert trudy["track_status"] == "fetch_failed"
    assert trudy["points"] == []
    assert result["degraded"] is True
    assert "error" not in result
    assert any("Trudy" in g and "track" in g for g in result["data_gaps"])
    assert any("cone" in g for g in result["data_gaps"])
    assert routes["29W"].call_count == 1


@respx.mock
@pytest.mark.asyncio
async def test_jtwc_default_call_fetches_no_warning_texts(fetcher: Fetcher) -> None:
    routes = _mock_jtwc()

    result = await fetch_jtwc_cyclones(fetcher)

    assert result["count"] == 3
    assert all("forecast" not in s for s in result["storms"])
    assert "data_gaps" not in result
    assert not any(routes[k].called for k in ("29W", "01P", "21E"))


@respx.mock
@pytest.mark.asyncio
async def test_jtwc_quiet_basins_with_forecast_are_not_degraded(fetcher: Fetcher) -> None:
    quiet = (
        "<![CDATA[<ul><li><font color='red'>"
        "No Current Tropical Cyclone Warnings.</font></li></ul>]]>"
    )
    rss = _fixture("jtwc_rss_20261017.xml")
    for guid in ("NWPAC-NIO-WARNINGS", "EPAC-CPAC-WARNINGS", "SH-WARNINGS"):
        head, sep, tail = rss.partition(f"<guid>{guid}</guid>")
        start = head.rindex("<description>") + len("<description>")
        end = head.rindex("</description>")
        rss = head[:start] + quiet + head[end:] + sep + tail
    respx.get(_JTWC_RSS_URL).mock(return_value=httpx.Response(200, text=rss))
    text = respx.get(url__regex=r".*web\.txt").mock(return_value=httpx.Response(500))

    result = await fetch_jtwc_cyclones(fetcher, include_forecast=True)

    assert result["count"] == 0
    assert result["data_gaps"] == []
    assert "degraded" not in result
    assert text.called is False


@respx.mock
@pytest.mark.asyncio
async def test_jtwc_lagging_warning_text_is_flagged(fetcher: Fetcher) -> None:
    """The RSS says 21E warning #17; serve 29W's text (warning 13) at
    21E's URL to stand in for a text file lagging its feed entry."""
    _mock_jtwc(
        trudy=httpx.Response(200, text=_crlf(_fixture("jtwc_wp2926web_wrn013.txt")))
    )

    result = await fetch_jtwc_cyclones(fetcher, include_forecast=True)

    trudy = next(s for s in result["storms"] if s["id"] == "21E")["forecast"]
    assert trudy["track_status"] == "stale_warning"
    assert trudy["warning_number"] == 13
    assert trudy["expected_warning_number"] == 17
    assert result["degraded"] is True


@respx.mock
@pytest.mark.asyncio
async def test_jtwc_warning_texts_are_fetched_once_per_warning(
    fetcher: Fetcher, no_sleep: None
) -> None:
    routes = _mock_jtwc()

    await fetch_jtwc_cyclones(fetcher, include_forecast=True)
    second = await fetch_jtwc_cyclones(fetcher, include_forecast=True)

    by_id = {s["id"]: s["forecast"] for s in second["storms"]}
    assert _as_tuples(by_id["29W"]["points"]) == _CHAMPI_POINTS
    assert _as_tuples(by_id["01P"]["points"]) == _WATI_POINTS
    assert routes["29W"].call_count == 1
    assert routes["01P"].call_count == 1


def _unlinked_jtwc_storm(i: int) -> dict:
    return {
        "id": f"{i:02d}W",
        "name": f"Storm{i}",
        "warning_number": 1,
        "issued_at": "17/1500Z",
        "warning_text_url": None,
    }


@respx.mock
@pytest.mark.asyncio
async def test_jtwc_unlinked_text_is_a_gap_and_the_fan_out_cap_a_failure(
    fetcher: Fetcher,
) -> None:
    """A storm whose RSS item links no warning text is a coverage gap,
    not degradation; storms past the per-call cap are named and mark the
    response degraded. Neither makes a request (respx refuses any)."""
    one = [_unlinked_jtwc_storm(0)]
    gaps, degraded = await cf.attach_jtwc_forecasts(fetcher, one)
    assert one[0]["forecast"]["track_status"] == "not_linked"
    assert one[0]["forecast"]["cone_status"] == "not_published"
    assert degraded is False
    assert gaps[0] == (
        "Storm0 (00W): forecast track not_linked: "
        "the JTWC RSS item links no warning text for this storm"
    )
    assert gaps[1].startswith("JTWC links no cone of uncertainty")
    assert len(gaps) == 2

    many = [_unlinked_jtwc_storm(i) for i in range(cf.MAX_FORECAST_STORMS + 1)]
    gaps, degraded = await cf.attach_jtwc_forecasts(fetcher, many)
    statuses = [s["forecast"]["track_status"] for s in many]
    assert statuses == ["not_linked"] * cf.MAX_FORECAST_STORMS + ["not_fetched_cap"]
    assert many[-1]["forecast"]["cone_status"] == "not_published"
    assert degraded is True
    assert sum("per-call cap" in g for g in gaps) == 1


@respx.mock
@pytest.mark.asyncio
async def test_jtwc_warning_link_off_the_jtwc_host_is_refused_not_fetched(
    fetcher: Fetcher,
) -> None:
    storm = _unlinked_jtwc_storm(0)
    storm["warning_text_url"] = "https://www.metoc.navy.mil.example.net/jtwc/products/wp0026web.txt"
    gaps, degraded = await cf.attach_jtwc_forecasts(fetcher, [storm])
    assert storm["forecast"]["track_status"] == "refused_link"
    assert storm["forecast"]["points"] == []
    assert degraded is True
    assert gaps[0] == (
        "Storm0 (00W): forecast track refused_link: warning text link "
        "'https://www.metoc.navy.mil.example.net/jtwc/products/wp0026web.txt' "
        "is not https on www.metoc.navy.mil; not fetched"
    )


@respx.mock
@pytest.mark.asyncio
async def test_jtwc_text_without_forecast_positions_is_unparsed_and_refetched(
    fetcher: Fetcher, no_sleep: None
) -> None:
    """Synthetic body: the first two lines of 29W's warning text and its
    REMARKS date line, no forecast blocks (a real JTWC final warning has
    not been observed). Reported unparsed, degraded, and not kept for the
    next call. The date line fixes the month of "171500"; without it the
    issue time would follow the fetch time (see
    test_jtwc_text_without_a_date_line_resolves_against_the_fetch_time)."""
    routes = _mock_jtwc()
    routes["29W"].mock(
        return_value=httpx.Response(
            200,
            text=_crlf(
                "WTPN32 PGTW 171500\nSUBJ/TYPHOON 29W (CHAMPI) WARNING NR 013//\n"
                "REMARKS:\n171500Z POSITION NEAR 12.7N 145.5E.\n"
                "17OCT26. TYPHOON 29W (CHAMPI), LOCATED APPROXIMATELY 96 NM\n"
            ),
        )
    )

    result = await fetch_jtwc_cyclones(fetcher, include_forecast=True)

    champi = next(s for s in result["storms"] if s["id"] == "29W")["forecast"]
    assert champi["track_status"] == "unparsed"
    assert champi["warning_number"] == 13
    assert champi["issued"] == "2026-10-17T15:00:00Z"
    assert champi["points"] == []
    assert result["degraded"] is True
    assert any("Champi (29W)" in g and "unparsed" in g for g in result["data_gaps"])

    await fetch_jtwc_cyclones(fetcher, include_forecast=True)
    assert routes["29W"].call_count == 2


def test_jtwc_warning_without_remarks_date_uses_the_given_anchor() -> None:
    text = _fixture("jtwc_wp2926web_wrn013.txt").replace("17OCT26. ", "")
    anchor = datetime(2026, 10, 17, 16, 45, tzinfo=timezone.utc)
    parsed = cf.parse_jtwc_warning(text, anchor)
    assert parsed["points"][0]["valid_time"] == "2026-10-18T00:00:00Z"
    assert parsed["issued"] == "2026-10-17T15:00:00Z"


def test_jtwc_text_with_no_forecast_blocks_is_unparsed() -> None:
    parsed = cf.parse_jtwc_warning(
        "WTPN32 PGTW 171500\nSUBJ/TYPHOON 29W (CHAMPI) WARNING NR 013//\n",
        datetime(2026, 10, 17, tzinfo=timezone.utc),
    )
    assert parsed["points"] == []


# ---------------------------------------------------------------------------
# Tool layer: the opt-in reaches the source through the real handler
# ---------------------------------------------------------------------------


def _tools_registry():
    """Import world_intel_mcp.tools under the suite's temp cache path
    (same reason and mechanism as test_daily_digest._tools_registry)."""
    import importlib
    import os

    from .test_server_registry import _TMP_CACHE

    prior = os.environ.get("WORLD_INTEL_CACHE_DB")
    os.environ["WORLD_INTEL_CACHE_DB"] = str(_TMP_CACHE)
    try:
        return importlib.import_module("world_intel_mcp.tools")
    finally:
        if prior is None:
            os.environ.pop("WORLD_INTEL_CACHE_DB", None)
        else:
            os.environ["WORLD_INTEL_CACHE_DB"] = prior


@respx.mock
@pytest.mark.asyncio
async def test_cyclone_tools_expose_and_honor_include_forecast(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch, no_sleep: None
) -> None:
    tools_pkg = _tools_registry()
    from world_intel_mcp import runtime

    monkeypatch.setattr(runtime, "fetcher", fetcher)
    schemas = {t.name: t.inputSchema for t in tools_pkg.ALL_TOOLS}
    for name in ("intel_cyclones", "intel_jtwc_cyclones"):
        prop = schemas[name]["properties"]["include_forecast"]
        assert prop["type"] == "boolean"
        assert prop["default"] is False

    _mock_nhc()
    _mock_jtwc()

    nhc = await tools_pkg.ALL_HANDLERS["intel_cyclones"]({"include_forecast": True})
    assert nhc["storms"][0]["forecast"]["track_status"] == "ok"
    plain = await tools_pkg.ALL_HANDLERS["intel_cyclones"]({})
    assert "forecast" not in plain["storms"][0]
    # A non-boolean never switches the fan-out on.
    stringly = await tools_pkg.ALL_HANDLERS["intel_cyclones"]({"include_forecast": "false"})
    assert "forecast" not in stringly["storms"][0]

    jtwc = await tools_pkg.ALL_HANDLERS["intel_jtwc_cyclones"]({"include_forecast": True})
    by_id = {s["id"]: s for s in jtwc["storms"]}
    assert by_id["29W"]["forecast"]["track_status"] == "ok"


@respx.mock
@pytest.mark.asyncio
async def test_jtwc_tool_ignores_a_non_boolean_include_forecast(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    tools_pkg = _tools_registry()
    from world_intel_mcp import runtime

    monkeypatch.setattr(runtime, "fetcher", fetcher)
    routes = _mock_jtwc()

    stringly = await tools_pkg.ALL_HANDLERS["intel_jtwc_cyclones"]({"include_forecast": "false"})

    assert stringly["count"] == 3
    assert all("forecast" not in s for s in stringly["storms"])
    assert not any(routes[k].called for k in ("29W", "01P", "21E"))


# ---------------------------------------------------------------------------
# The clock: product dates decide, not the day the suite runs
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_jtwc_text_without_a_date_line_resolves_against_the_fetch_time(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch, no_sleep: None
) -> None:
    """A warning text with no REMARKS date line places its day-only times
    in the month nearest the fetch time ("now", set here to the capture
    time; at the module's 2027-03-01 they would land in February)."""
    monkeypatch.setattr(
        _FrozenClock, "at", datetime(2026, 10, 17, 16, 45, tzinfo=timezone.utc)
    )
    routes = _mock_jtwc()
    routes["29W"].mock(
        return_value=httpx.Response(
            200, text=_crlf(_fixture("jtwc_wp2926web_wrn013.txt").replace("17OCT26. ", ""))
        )
    )

    result = await fetch_jtwc_cyclones(fetcher, include_forecast=True)

    champi = next(s for s in result["storms"] if s["id"] == "29W")["forecast"]
    assert champi["track_status"] == "ok"
    assert champi["issued"] == "2026-10-17T15:00:00Z"
    assert _as_tuples(champi["points"]) == _CHAMPI_POINTS


@respx.mock
@pytest.mark.asyncio
async def test_nhc_page_with_crlf_line_ends_parses_the_same(fetcher: Fetcher) -> None:
    """Line ends are normalised before parsing: with CRLF the issuance
    line would not match, and the times would follow "now" instead."""
    _mock_nhc(tcm=httpx.Response(200, text=_crlf(_tcm_page())))

    result = await fetch_cyclones(fetcher, include_forecast=True)

    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == "ok"
    assert fc["issued"] == "2026-10-17T15:00:00Z"
    assert _as_tuples(fc["points"]) == _TRUDY_POINTS


# ---------------------------------------------------------------------------
# Feed and text out of step: which number came from where
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_nhc_page_newer_than_the_feed_is_feed_lags_text_and_kept(
    fetcher: Fetcher,
) -> None:
    """CurrentStorms.json is cached for 600 s, so after an issuance it can
    still list advisory 016 while the bin page already serves 17. The page
    is then the newer product: labelled feed_lags_text with both numbers,
    not stale, not degraded, and kept for the next call."""
    current = _current_storms()
    current["activeStorms"][0]["forecastAdvisory"]["advNum"] = "016"
    routes = _mock_nhc(current=current)

    result = await fetch_cyclones(fetcher, include_forecast=True)

    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == "feed_lags_text"
    assert fc["advisory_number"] == "17"
    assert fc["feed_advisory_number"] == "016"
    assert "expected_advisory_number" not in fc
    assert _as_tuples(fc["points"]) == _TRUDY_POINTS
    assert "degraded" not in result
    [gap] = result["data_gaps"]
    assert "feed_lags_text" in gap and "advisory 17" in gap and "016" in gap

    again = await fetch_cyclones(fetcher, include_forecast=True)
    assert again["storms"][0]["forecast"]["track_status"] == "feed_lags_text"
    assert routes["tcm"].call_count == 1


@respx.mock
@pytest.mark.asyncio
async def test_jtwc_text_newer_than_the_rss_is_feed_lags_text_and_kept(
    fetcher: Fetcher, no_sleep: None
) -> None:
    routes = _mock_jtwc()
    rss = _fixture("jtwc_rss_20261017.xml").replace(
        "Typhoon  29W (Champi) Warning #13", "Typhoon  29W (Champi) Warning #12"
    )
    routes["rss"].mock(return_value=httpx.Response(200, text=rss))

    result = await fetch_jtwc_cyclones(fetcher, include_forecast=True)

    storm = next(s for s in result["storms"] if s["id"] == "29W")
    assert storm["warning_number"] == 12  # the RSS's number, at storm level
    champi = storm["forecast"]
    assert champi["track_status"] == "feed_lags_text"
    assert champi["warning_number"] == 13
    assert champi["feed_warning_number"] == 12
    assert _as_tuples(champi["points"]) == _CHAMPI_POINTS
    gap = next(g for g in result["data_gaps"] if "Champi" in g)
    assert "feed_lags_text" in gap and "warning 13" in gap and "12" in gap

    await fetch_jtwc_cyclones(fetcher, include_forecast=True)
    assert routes["29W"].call_count == 1


@respx.mock
@pytest.mark.asyncio
async def test_jtwc_stale_warning_text_is_not_kept(fetcher: Fetcher) -> None:
    """A lagging JTWC text is labelled stale and fetched again on the next
    call, never served from the cache."""
    routes = _mock_jtwc(
        trudy=httpx.Response(200, text=_crlf(_fixture("jtwc_wp2926web_wrn013.txt")))
    )

    await fetch_jtwc_cyclones(fetcher, include_forecast=True)
    second = await fetch_jtwc_cyclones(fetcher, include_forecast=True)

    trudy = next(s for s in second["storms"] if s["id"] == "21E")["forecast"]
    assert trudy["track_status"] == "stale_warning"
    assert trudy["expected_warning_number"] == 17
    assert routes["21E"].call_count == 2


@respx.mock
@pytest.mark.asyncio
async def test_nhc_unparsed_page_is_not_kept(fetcher: Fetcher) -> None:
    routes = _mock_nhc(
        tcm=httpx.Response(200, text="<html><body><h1>Not Found</h1></body></html>")
    )

    await fetch_cyclones(fetcher, include_forecast=True)
    second = await fetch_cyclones(fetcher, include_forecast=True)

    assert second["storms"][0]["forecast"]["track_status"] == "unparsed"
    assert routes["tcm"].call_count == 2


@respx.mock
@pytest.mark.asyncio
async def test_nhc_stale_feed_copy_is_marked_and_a_newer_page_is_not_called_stale(
    fetcher: Fetcher, no_sleep: None
) -> None:
    """CurrentStorms.json down, an expired copy listing advisory 016 in the
    cache: the fetcher serves the copy marked _stale, and the response
    keeps the marker, with or without forecasts. The text page (17) is
    newer than that copy, so it reads feed_lags_text, not stale."""
    old = _current_storms()
    old["activeStorms"][0]["forecastAdvisory"]["advNum"] = "016"
    fetcher.cache.set("cyclones:nhc:current", old, -1)
    routes = _mock_nhc()
    routes["current"].mock(return_value=httpx.Response(503))

    plain = await fetch_cyclones(fetcher)

    assert plain["count"] == 1
    assert plain["_stale"] is True
    assert plain["_stale_age_seconds"] >= 0
    assert "forecast" not in plain["storms"][0]

    with_forecast = await fetch_cyclones(fetcher, include_forecast=True)

    assert with_forecast["_stale"] is True
    fc = with_forecast["storms"][0]["forecast"]
    assert fc["track_status"] == "feed_lags_text"
    assert fc["feed_advisory_number"] == "016"


# ---------------------------------------------------------------------------
# An unreadable advisory or warning number: one rule for both agencies
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_an_unreadable_issuance_number_is_stale_for_both_agencies(
    fetcher: Fetcher, no_sleep: None
) -> None:
    """The number is the only check that a per-bin text belongs to the
    issuance the feed lists. A text whose number cannot be read cannot be
    shown to be current, so NHC and JTWC both label it stale (degraded,
    not kept) and say the number was unreadable."""
    nhc_routes = _mock_nhc(
        tcm=httpx.Response(
            200, text=_tcm_page().replace("FORECAST/ADVISORY NUMBER  17", "FORECAST/ADVISORY")
        )
    )
    jtwc_routes = _mock_jtwc()
    jtwc_routes["29W"].mock(
        return_value=httpx.Response(
            200,
            text=_crlf(
                _fixture("jtwc_wp2926web_wrn013.txt").replace("WARNING NR 013", "WARNING 013")
            ),
        )
    )

    nhc = await fetch_cyclones(fetcher, include_forecast=True)
    jtwc = await fetch_jtwc_cyclones(fetcher, include_forecast=True)

    fc = nhc["storms"][0]["forecast"]
    assert fc["track_status"] == "stale_advisory"
    assert fc["advisory_number"] is None
    assert "no readable advisory number" in fc["track_error"]
    assert len(fc["points"]) == 8
    assert nhc["degraded"] is True
    champi = next(s for s in jtwc["storms"] if s["id"] == "29W")["forecast"]
    assert champi["track_status"] == "stale_warning"
    assert champi["warning_number"] is None
    assert "no readable warning number" in champi["track_error"]
    assert len(champi["points"]) == 8

    await fetch_cyclones(fetcher, include_forecast=True)
    await fetch_jtwc_cyclones(fetcher, include_forecast=True)
    assert nhc_routes["tcm"].call_count == 2
    assert jtwc_routes["29W"].call_count == 2


# ---------------------------------------------------------------------------
# Redirects: a product comes from its own URL or not at all
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_nhc_product_redirects_are_refused_never_followed(fetcher: Fetcher) -> None:
    """The review's redirect probe: allowlisted product URLs answering 302
    to a private address and to the link-local metadata address. Neither
    target is requested, each redirect is named as a refused link, and the
    reported URLs are the only ones fetched."""
    routes = _mock_nhc(
        tcm=httpx.Response(302, headers={"location": "http://10.0.0.5:8080/tcm"}),
        cone=httpx.Response(
            302, headers={"location": "http://169.254.169.254/latest/cone.kmz"}
        ),
    )
    inner_tcm = respx.get("http://10.0.0.5:8080/tcm").mock(
        return_value=httpx.Response(200, text=_tcm_page())
    )
    inner_cone = respx.get("http://169.254.169.254/latest/cone.kmz").mock(
        return_value=httpx.Response(200, content=_cone_kmz())
    )

    result = await fetch_cyclones(fetcher, include_forecast=True)

    assert inner_tcm.called is False
    assert inner_cone.called is False
    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == "refused_link"
    assert fc["track_url"] == _TCM_URL
    assert "'http://10.0.0.5:8080/tcm'" in fc["track_error"]
    assert fc["points"] == []
    assert fc["cone_status"] == "refused_link"
    assert fc["cone_url"] == _CONE_URL
    assert "'http://169.254.169.254/latest/cone.kmz'" in fc["cone_error"]
    assert fc["cone"] is None
    assert result["degraded"] is True
    assert sum("redirect" in g for g in result["data_gaps"]) == 2
    # A redirect is an answer, not an outage: asked once, not retried.
    assert routes["tcm"].call_count == 1
    assert routes["cone"].call_count == 1


@respx.mock
@pytest.mark.asyncio
async def test_same_host_https_product_redirect_is_not_followed_either(
    fetcher: Fetcher,
) -> None:
    """No product redirect is followed, even one that stays on
    www.nhc.noaa.gov over https, so track_url is always the URL the track
    came from."""
    _mock_nhc(
        tcm=httpx.Response(
            301, headers={"location": "https://www.nhc.noaa.gov/text/refresh/MIATCMEP1.shtml"}
        )
    )
    moved = respx.get("https://www.nhc.noaa.gov/text/refresh/MIATCMEP1.shtml").mock(
        return_value=httpx.Response(200, text=_tcm_page())
    )

    result = await fetch_cyclones(fetcher, include_forecast=True)

    fc = result["storms"][0]["forecast"]
    assert moved.called is False
    assert fc["track_status"] == "refused_link"
    assert "not followed" in fc["track_error"]
    assert fc["cone_status"] == "ok"


@respx.mock
@pytest.mark.asyncio
async def test_jtwc_warning_text_redirect_is_refused_never_followed(
    fetcher: Fetcher, no_sleep: None
) -> None:
    routes = _mock_jtwc()
    routes["29W"].mock(
        return_value=httpx.Response(
            301, headers={"location": "http://127.0.0.1:9/wp2926web.txt"}
        )
    )
    inner = respx.get("http://127.0.0.1:9/wp2926web.txt").mock(
        return_value=httpx.Response(200, text=_crlf(_fixture("jtwc_wp2926web_wrn013.txt")))
    )

    result = await fetch_jtwc_cyclones(fetcher, include_forecast=True)

    champi = next(s for s in result["storms"] if s["id"] == "29W")["forecast"]
    assert inner.called is False
    assert champi["track_status"] == "refused_link"
    assert champi["track_url"] == _JTWC_29W_URL
    assert "'http://127.0.0.1:9/wp2926web.txt'" in champi["track_error"]
    assert champi["points"] == []
    assert routes["29W"].call_count == 1


# ---------------------------------------------------------------------------
# Feed values of the wrong type; links with whitespace or control characters
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "value", "track_status", "cone_status"),
    [
        (("forecastAdvisory",), "n/a", "refused_link", "ok"),
        (("forecastAdvisory", "url"), 17, "refused_link", "ok"),
        (("trackCone",), [1], "ok", "refused_link"),
        (("trackCone", "kmzFile"), {"x": 1}, "ok", "refused_link"),
        (("trackCone",), [], "ok", "not_linked"),
    ],
)
@respx.mock
@pytest.mark.asyncio
async def test_malformed_feed_values_are_named_never_a_failed_call(
    fetcher: Fetcher, path: tuple, value: object, track_status: str, cone_status: str
) -> None:
    """The review's malformed CurrentStorms.json values: four raised out of
    fetch_cyclones and lost the storm list."""
    current = _current_storms()
    target = current["activeStorms"][0]
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    _mock_nhc(current=current)

    result = await fetch_cyclones(fetcher, include_forecast=True)

    assert result["count"] == 1
    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == track_status
    assert fc["cone_status"] == cone_status


@pytest.mark.parametrize(
    "url",
    [
        "https://www.nhc.noaa.gov/text/MIATCM\tEP1.shtml",
        " https://www.nhc.noaa.gov/text/MIATCMEP1.shtml",
        "https://www.nhc.noaa.gov/text/MIATCMEP1.shtml\r\nHost: evil.example",
        "https://www.nhc.noaa.gov/text/MIATCMEP1.shtml\x00",
        "https://www.nhc.noaa.gov/text/MIATCM EP1.shtml",
        "https://www.nhc.noaa.gov/text/MIATCMEP1.shtml​",
    ],
)
def test_links_with_whitespace_or_control_characters_are_refused(url: str) -> None:
    """urllib strips tab, CR and LF before reading the host, so the
    allowlist passed links that httpx then refused with InvalidURL."""
    reason = cf._refused_link(url, cf._NHC_PRODUCT_HOSTS)
    assert reason is not None
    assert "whitespace or control" in reason


@pytest.mark.parametrize(
    "value", [17, {"x": 1}, ["https://www.nhc.noaa.gov/x"], b"https://www.nhc.noaa.gov/x"]
)
def test_links_that_are_not_strings_are_refused(value: object) -> None:
    reason = cf._refused_link(value, cf._NHC_PRODUCT_HOSTS)
    assert reason is not None
    assert "not a string" in reason


@respx.mock
@pytest.mark.asyncio
async def test_cone_link_with_a_tab_is_refused_not_requested(fetcher: Fetcher) -> None:
    """In the review this link passed the allowlist and httpx's
    InvalidURL escaped fetch_cyclones."""
    current = _current_storms()
    current["activeStorms"][0]["trackCone"]["kmzFile"] = (
        "https://www.nhc.noaa.gov/storm_graphics/api/EP212026_017adv\t_CONE.kmz"
    )
    _mock_nhc(current=current)

    result = await fetch_cyclones(fetcher, include_forecast=True)

    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == "ok"
    assert fc["cone_status"] == "refused_link"
    assert "whitespace or control" in fc["cone_error"]


# ---------------------------------------------------------------------------
# One bad product never fails the response
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("method", [zipfile.ZIP_LZMA, zipfile.ZIP_BZIP2])
@respx.mock
@pytest.mark.asyncio
async def test_corrupt_bzip2_or_lzma_cone_is_a_named_gap_not_a_failed_call(
    fetcher: Fetcher, method: int
) -> None:
    """The review's corrupt LZMA and bzip2 members raised LZMAError and
    OSError out of fetch_cyclones, losing the storm list."""
    raw = bytearray(_kmz(_fixture("nhc_ep212026_017adv_CONE.kml"), method=method))
    start = 30 + len("ep212026_017adv_CONE.kml")
    for i in range(start + 12, start + 40):
        raw[i] ^= 0x5A
    _mock_nhc(cone=httpx.Response(200, content=bytes(raw)))

    result = await fetch_cyclones(fetcher, include_forecast=True)

    assert result["count"] == 1
    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == "ok"
    assert fc["cone_status"] == "unparsed"
    assert "compression method" in fc["cone_error"]
    assert result["degraded"] is True


@respx.mock
@pytest.mark.asyncio
async def test_an_unexpected_error_in_one_product_keeps_the_rest(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    _mock_nhc()

    async def broken_cone(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(cf, "_nhc_cone", broken_cone)

    result = await fetch_cyclones(fetcher, include_forecast=True)

    assert result["count"] == 1
    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == "ok"
    assert _as_tuples(fc["points"]) == _TRUDY_POINTS
    assert fc["cone_status"] == "error"
    assert fc["cone_url"] == _CONE_URL
    assert fc["cone"] is None
    assert "RuntimeError: boom" in fc["cone_error"]
    assert result["degraded"] is True
    assert any("Trudy" in g and "boom" in g for g in result["data_gaps"])


@respx.mock
@pytest.mark.asyncio
async def test_an_unexpected_error_in_the_nhc_track_is_a_failure_named_per_storm(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The track side of the same net: the cone still arrives, and the
    track's error alone marks the response degraded."""
    _mock_nhc()

    def broken(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(cf, "parse_nhc_forecast_advisory", broken)

    result = await fetch_cyclones(fetcher, include_forecast=True)

    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == "error"
    assert fc["track_url"] == _TCM_URL
    assert fc["feed_advisory_number"] == "017"
    assert fc["points"] == []
    assert "RuntimeError: boom" in fc["track_error"]
    assert fc["cone_status"] == "ok"
    assert result["degraded"] is True


@respx.mock
@pytest.mark.asyncio
async def test_any_exception_reading_the_cone_kmz_is_unparsed(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The review's corrupt bzip2 member raised OSError, which the cone's
    old exception list did not name. Any exception from the KMZ parser is
    now an unparsed cone, named with its type."""
    _mock_nhc()

    def corrupt(content: bytes) -> dict:
        raise OSError("Invalid data stream")

    monkeypatch.setattr(cf, "parse_nhc_cone_kmz", corrupt)

    result = await fetch_cyclones(fetcher, include_forecast=True)

    fc = result["storms"][0]["forecast"]
    assert fc["cone_status"] == "unparsed"
    assert "OSError: Invalid data stream" in fc["cone_error"]
    assert fc["track_status"] == "ok"


def test_unexpected_errors_are_named_but_cancellation_is_not_swallowed() -> None:
    assert cf._unexpected(RuntimeError("boom")) == "RuntimeError: boom"
    with pytest.raises(asyncio_mod.CancelledError):
        cf._unexpected(asyncio_mod.CancelledError())


@respx.mock
@pytest.mark.asyncio
async def test_an_unexpected_error_in_one_jtwc_storm_keeps_the_others(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch, no_sleep: None
) -> None:
    _mock_jtwc()
    real = cf.parse_jtwc_warning

    def flaky(text: str, anchor: datetime) -> dict:
        if "CHAMPI" in text:
            raise RuntimeError("boom")
        return real(text, anchor)

    monkeypatch.setattr(cf, "parse_jtwc_warning", flaky)

    result = await fetch_jtwc_cyclones(fetcher, include_forecast=True)

    assert result["count"] == 3
    by_id = {s["id"]: s["forecast"] for s in result["storms"]}
    assert by_id["29W"]["track_status"] == "error"
    assert by_id["29W"]["track_url"] == _JTWC_29W_URL
    assert "RuntimeError: boom" in by_id["29W"]["track_error"]
    assert by_id["29W"]["points"] == []
    assert by_id["29W"]["cone_status"] == "not_published"
    assert _as_tuples(by_id["01P"]["points"]) == _WATI_POINTS
    assert any("Champi" in g and "boom" in g for g in result["data_gaps"])


# ---------------------------------------------------------------------------
# KMZ members: compression method, expansion ratio, declared size
# ---------------------------------------------------------------------------


def _record_member_opens(monkeypatch: pytest.MonkeyPatch) -> list:
    """Record every ZipFile.open call (a member read) from here on."""
    opened: list = []
    real_open = zipfile.ZipFile.open

    def recording_open(self, *args, **kwargs):
        opened.append(args[0] if args else kwargs.get("name"))
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(zipfile.ZipFile, "open", recording_open)
    return opened


@pytest.mark.parametrize("method", [zipfile.ZIP_BZIP2, zipfile.ZIP_LZMA])
def test_cone_kmz_member_not_stored_or_deflated_is_refused_unread(
    method: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """zipfile runs bzip2 and LZMA members through decompressors with no
    output bound (the review measured a 621-byte bzip2 KMZ taking the
    process to 1 GiB RSS), so such a member is refused before it is
    opened."""
    kmz = _kmz(_fixture("nhc_ep212026_017adv_CONE.kml"), method=method)
    opened = _record_member_opens(monkeypatch)
    with pytest.raises(ValueError, match="compression method"):
        cf.parse_nhc_cone_kmz(kmz)
    assert opened == []


def test_cone_kmz_with_a_stored_member_parses() -> None:
    cone = cf.parse_nhc_cone_kmz(
        _kmz(_fixture("nhc_ep212026_017adv_CONE.kml"), method=zipfile.ZIP_STORED)
    )
    assert cone["source_vertex_count"] == 1167
    assert cone["vertices"][0] == [16.33344, -103.90001]


def test_cone_kmz_member_expanding_past_the_ratio_bound_is_refused_unread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """4 MiB of padding inside the KML: under the 8 MiB cap, but the member
    then expands about 300:1 (the live cone's member expands 2.8:1)."""
    kml = _fixture("nhc_ep212026_017adv_CONE.kml").replace(
        "<Document>\n", "<Document>\n" + " " * (4 * 1024 * 1024), 1
    )
    kmz = _kmz(kml)
    info = zipfile.ZipFile(io.BytesIO(kmz)).infolist()[0]
    assert info.file_size <= cf._KML_MAX_BYTES
    assert info.file_size > cf._KML_MAX_RATIO * info.compress_size
    opened = _record_member_opens(monkeypatch)
    with pytest.raises(ValueError, match="expands"):
        cf.parse_nhc_cone_kmz(kmz)
    assert opened == []


def test_cone_kmz_member_declaring_more_than_the_kml_cap_is_refused_unread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A member over 8 MiB is refused from its declared size, before any of
    it is decompressed. Random bytes keep the compression ratio near 1:1,
    so only the size check can refuse it unopened."""
    kmz = _kmz(random.Random(0).randbytes(cf._KML_MAX_BYTES + 1))
    opened = _record_member_opens(monkeypatch)
    with pytest.raises(ValueError, match="exceeds cap"):
        cf.parse_nhc_cone_kmz(kmz)
    assert opened == []


def test_cone_kmz_member_larger_than_it_declares_is_refused() -> None:
    """A header that understates the member (1 MiB of KML declared as
    1,000 bytes): zipfile stops reading at the declared size, and the
    checksum then refuses the member."""
    kml = _fixture("nhc_ep212026_017adv_CONE.kml").replace(
        "<Document>\n", "<Document>\n" + " " * (1024 * 1024), 1
    )
    raw = bytearray(_kmz(kml))
    struct.pack_into("<I", raw, 22, 1000)  # local file header: uncompressed size
    central = raw.rfind(b"PK\x01\x02")
    struct.pack_into("<I", raw, central + 24, 1000)  # central directory entry
    with pytest.raises(zipfile.BadZipFile, match="CRC"):
        cf.parse_nhc_cone_kmz(bytes(raw))


@pytest.mark.parametrize(
    "bad", ["-100.0,91.0,0", "-100.0,-90.5,0", "-100.0,nan,0", "nan,15.0,0", "inf,15.0,0"]
)
def test_cone_vertex_outside_the_coordinate_range_is_refused(bad: str) -> None:
    tokens = ["-100.0,15.0,0", "-99.0,15.0,0", bad, "-100.0,16.0,0", "-100.0,15.0,0"]
    with pytest.raises(ValueError, match="out of range"):
        cf.parse_cone_kml_ring(_coords_kml(tokens).encode())


def test_cone_outline_under_three_vertices_is_refused() -> None:
    """The review's ring 0.004 degrees across simplified to one vertex at
    the 0.01 degree tolerance and still read ok."""
    pts = [(15.0, -100.0), (15.004, -100.0), (15.004, -100.004), (15.0, -100.004)]
    tokens = [f"{lon},{lat},0" for lat, lon in pts + pts[:1]]
    with pytest.raises(ValueError, match="at least 3"):
        cf.parse_nhc_cone_kmz(_kmz(_coords_kml(tokens)))


# ---------------------------------------------------------------------------
# Simplifier cost and outline size
# ---------------------------------------------------------------------------


def _zigzag_kml(vertices: int, amplitude: float) -> str:
    """The review's worst case: a ring zig-zagging ``amplitude`` degrees,
    above the 0.01 degree tolerance, so nearly every vertex is kept and
    each split peels off one vertex."""
    points = [
        f"{-100 + i * 1e-3:.4f},{15 + (i % 2) * amplitude:.4f},0" for i in range(vertices)
    ]
    return _coords_kml(points + points[:1])


def test_cone_simplifier_cost_is_bounded_at_the_vertex_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """At the old 10,000-vertex cap this ring kept 9,990 vertices and took
    15.1 s of CPU (the review's measurement, 2026-10-17) and 49,994,955
    distance evaluations (calls to _segment_distance, counted the same way
    on the unfixed module, the same day). The bound is counted, not timed,
    so the test is deterministic; it is absolute, so raising either cap
    past it fails here; and the counter stops a regressed run early
    instead of letting it take seconds."""
    budget = 2_000_000
    calls = 0
    real = cf._segment_distance

    def counted(*args):
        nonlocal calls
        calls += 1
        if calls > budget:
            raise AssertionError(f"simplifier passed {budget:,} distance evaluations")
        return real(*args)

    kmz = _kmz(_zigzag_kml(cf._CONE_MAX_VERTICES, 0.05))
    monkeypatch.setattr(cf, "_segment_distance", counted)
    with pytest.raises(ValueError, match="over the outline cap"):
        cf.parse_nhc_cone_kmz(kmz)
    assert calls <= budget


def test_simplified_outline_over_the_vertex_cap_is_refused() -> None:
    square = [(0.0, 0.0), (0.5, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]
    assert len(cf.simplify_closed_ring(square, 0.01, max_vertices=4)) == 4
    with pytest.raises(ValueError, match="more than 3"):
        cf.simplify_closed_ring(square, 0.01, max_vertices=3)


@respx.mock
@pytest.mark.asyncio
async def test_cone_is_parsed_off_the_event_loop_thread(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[int] = []
    real = cf.parse_nhc_cone_kmz

    def recording(content: bytes) -> dict:
        seen.append(threading.get_ident())
        return real(content)

    monkeypatch.setattr(cf, "parse_nhc_cone_kmz", recording)
    _mock_nhc()

    result = await fetch_cyclones(fetcher, include_forecast=True)

    assert result["storms"][0]["forecast"]["cone_status"] == "ok"
    assert len(seen) == 1
    assert seen[0] != threading.get_ident()


# ---------------------------------------------------------------------------
# Parsers: values stay inside their own block and fix
# ---------------------------------------------------------------------------


def test_nhc_point_without_a_wind_line_does_not_borrow_the_next_one() -> None:
    text = _fixture("nhc_tcm_ep212026_adv017.txt")
    old = "FORECAST VALID 19/0000Z 20.1N 106.4W\nMAX WIND  85 KT...GUSTS 105 KT.\n"
    assert old in text
    parsed = cf.parse_nhc_forecast_advisory(
        text.replace(old, "FORECAST VALID 19/0000Z 20.1N 106.4W\n")
    )
    by_tau = {p["tau_hours"]: p for p in parsed["points"]}
    assert by_tau[36]["max_wind_kt"] is None
    assert by_tau[36]["gust_kt"] is None
    assert by_tau[48]["max_wind_kt"] == 65


def test_jtwc_point_without_a_wind_line_does_not_borrow_the_next_one() -> None:
    text = _fixture("jtwc_wp2926web_wrn013.txt")
    old = "181200Z --- 13.5N 141.6E\n   MAX SUSTAINED WINDS - 135 KT, GUSTS 165 KT\n"
    assert old in text
    parsed = cf.parse_jtwc_warning(text.replace(old, "181200Z --- 13.5N 141.6E\n"), _FAR_NOW)
    by_tau = {p["tau_hours"]: p for p in parsed["points"]}
    assert by_tau[24]["max_wind_kt"] is None
    assert by_tau[36]["max_wind_kt"] == 140


def test_jtwc_pressure_for_another_time_is_not_the_warning_fix_pressure() -> None:
    text = _fixture("jtwc_wp2926web_wrn013.txt")
    old = "MINIMUM CENTRAL PRESSURE AT 171200Z IS 945 MB"
    assert old in text
    parsed = cf.parse_jtwc_warning(
        text.replace(old, "MINIMUM CENTRAL PRESSURE AT 171800Z IS 945 MB"), _FAR_NOW
    )
    assert parsed["initial"]["valid_time"] == "2026-10-17T12:00:00Z"
    assert parsed["initial"]["pressure_mb"] is None


# ---------------------------------------------------------------------------
# Per-issuance cache keys
# ---------------------------------------------------------------------------


def _next_advisory_feed() -> dict:
    """Trudy's feed entry moved on to advisory 018 (new numbers, file times
    and cone URL; the text URL is the same bin page)."""
    current = _current_storms()
    storm = current["activeStorms"][0]
    storm["forecastAdvisory"].update(advNum="018", fileUpdateTime="2026-10-17T20:37:11.524Z")
    storm["trackCone"].update(
        advNum="018",
        fileUpdateTime="2026-10-17T20:39:40.332Z",
        kmzFile=_CONE_URL.replace("_017adv_", "_018adv_"),
    )
    return current


def _advisory_18_cone_kml() -> str:
    kml = _fixture("nhc_ep212026_017adv_CONE.kml")
    old = '<Data name="advisoryNum">\n                <value>17</value>'
    assert kml.count(old) == 1
    return kml.replace(old, old.replace(">17<", ">18<"))


@respx.mock
@pytest.mark.asyncio
async def test_nhc_track_and_cone_are_fetched_again_for_a_new_advisory(
    fetcher: Fetcher,
) -> None:
    """The cache keys carry the advisory number and file time: a new
    advisory never reuses the previous advisory's text or cone."""
    routes = _mock_nhc()
    first = await fetch_cyclones(fetcher, include_forecast=True)
    assert first["storms"][0]["forecast"]["advisory_number"] == "17"
    assert first["storms"][0]["forecast"]["cone"]["advisory_number"] == "17"

    fetcher.cache.delete("cyclones:nhc:current")  # the feed itself is cached 600 s
    routes["current"].mock(return_value=httpx.Response(200, json=_next_advisory_feed()))
    routes["tcm"].mock(
        return_value=httpx.Response(
            200,
            text=_tcm_page().replace(
                "FORECAST/ADVISORY NUMBER  17", "FORECAST/ADVISORY NUMBER  18"
            ),
        )
    )
    cone_18 = respx.get(_CONE_URL.replace("_017adv_", "_018adv_")).mock(
        return_value=httpx.Response(200, content=_kmz(_advisory_18_cone_kml()))
    )

    second = await fetch_cyclones(fetcher, include_forecast=True)

    fc = second["storms"][0]["forecast"]
    assert fc["track_status"] == "ok"
    assert fc["advisory_number"] == "18"
    assert fc["cone_status"] == "ok"
    assert fc["cone"]["advisory_number"] == "18"
    assert routes["tcm"].call_count == 2
    assert cone_18.call_count == 1


@respx.mock
@pytest.mark.asyncio
async def test_nhc_products_are_fetched_again_when_only_the_advisory_number_changes(
    fetcher: Fetcher,
) -> None:
    """The keys carry the number itself, not only the file time: a feed
    entry for advisory 018 that kept 017's file times still gets fresh
    products. A key on the file time alone would serve 017's text (read
    as stale) and 017's cone."""
    routes = _mock_nhc()
    await fetch_cyclones(fetcher, include_forecast=True)

    renumbered = _current_storms()
    storm = renumbered["activeStorms"][0]
    storm["forecastAdvisory"]["advNum"] = "018"
    storm["trackCone"]["advNum"] = "018"
    fetcher.cache.delete("cyclones:nhc:current")
    routes["current"].mock(return_value=httpx.Response(200, json=renumbered))
    routes["tcm"].mock(
        return_value=httpx.Response(
            200,
            text=_tcm_page().replace(
                "FORECAST/ADVISORY NUMBER  17", "FORECAST/ADVISORY NUMBER  18"
            ),
        )
    )
    routes["cone"].mock(return_value=httpx.Response(200, content=_kmz(_advisory_18_cone_kml())))

    second = await fetch_cyclones(fetcher, include_forecast=True)

    fc = second["storms"][0]["forecast"]
    assert fc["track_status"] == "ok"
    assert fc["advisory_number"] == "18"
    assert fc["cone"]["advisory_number"] == "18"
    assert routes["tcm"].call_count == 2
    assert routes["cone"].call_count == 2


@respx.mock
@pytest.mark.asyncio
async def test_nhc_corrected_product_with_the_same_number_is_fetched_again(
    fetcher: Fetcher,
) -> None:
    """Same advisory number, new file time (a corrected product): both
    products are fetched again, not served from the cache."""
    routes = _mock_nhc()
    await fetch_cyclones(fetcher, include_forecast=True)

    corrected = _current_storms()
    storm = corrected["activeStorms"][0]
    storm["forecastAdvisory"]["fileUpdateTime"] = "2026-10-17T15:10:00.000Z"
    storm["trackCone"]["fileUpdateTime"] = "2026-10-17T15:12:00.000Z"
    fetcher.cache.delete("cyclones:nhc:current")
    routes["current"].mock(return_value=httpx.Response(200, json=corrected))

    await fetch_cyclones(fetcher, include_forecast=True)

    assert routes["tcm"].call_count == 2
    assert routes["cone"].call_count == 2


@respx.mock
@pytest.mark.asyncio
async def test_jtwc_text_is_fetched_again_when_only_the_warning_number_changes(
    fetcher: Fetcher, no_sleep: None
) -> None:
    """Warning 14 listed with warning 13's issue time still gets a fresh
    text: the key carries the number, not only the time."""
    routes = _mock_jtwc()
    await fetch_jtwc_cyclones(fetcher, include_forecast=True)

    rss_14 = _fixture("jtwc_rss_20261017.xml").replace(
        "Typhoon  29W (Champi) Warning #13", "Typhoon  29W (Champi) Warning #14"
    )
    text_14 = _fixture("jtwc_wp2926web_wrn013.txt").replace("WARNING NR 013", "WARNING NR 014")
    fetcher.cache.delete("jtwc:rss:current")
    routes["rss"].mock(return_value=httpx.Response(200, text=rss_14))
    routes["29W"].mock(return_value=httpx.Response(200, text=_crlf(text_14)))

    second = await fetch_jtwc_cyclones(fetcher, include_forecast=True)

    champi = next(s for s in second["storms"] if s["id"] == "29W")["forecast"]
    assert champi["track_status"] == "ok"
    assert champi["warning_number"] == 14
    assert routes["29W"].call_count == 2


@respx.mock
@pytest.mark.asyncio
async def test_jtwc_text_is_fetched_again_for_a_new_or_reissued_warning(
    fetcher: Fetcher, no_sleep: None
) -> None:
    routes = _mock_jtwc()
    await fetch_jtwc_cyclones(fetcher, include_forecast=True)

    rss_14 = (
        _fixture("jtwc_rss_20261017.xml")
        .replace("Typhoon  29W (Champi) Warning #13", "Typhoon  29W (Champi) Warning #14")
        .replace("Issued at 17/1500Z", "Issued at 17/2100Z")
    )
    text_14 = _fixture("jtwc_wp2926web_wrn013.txt").replace("WARNING NR 013", "WARNING NR 014")
    fetcher.cache.delete("jtwc:rss:current")  # the RSS itself is cached 600 s
    routes["rss"].mock(return_value=httpx.Response(200, text=rss_14))
    routes["29W"].mock(return_value=httpx.Response(200, text=_crlf(text_14)))

    second = await fetch_jtwc_cyclones(fetcher, include_forecast=True)

    champi = next(s for s in second["storms"] if s["id"] == "29W")["forecast"]
    assert champi["track_status"] == "ok"
    assert champi["warning_number"] == 14
    assert routes["29W"].call_count == 2

    # Reissued: the same warning number at a new issue time.
    fetcher.cache.delete("jtwc:rss:current")
    routes["rss"].mock(
        return_value=httpx.Response(
            200, text=rss_14.replace("Issued at 17/2100Z", "Reissued at 17/2200Z")
        )
    )
    await fetch_jtwc_cyclones(fetcher, include_forecast=True)
    assert routes["29W"].call_count == 3


# ---------------------------------------------------------------------------
# Small paths: number comparison, text size cap, tiny rings
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("found", "listed", "standing"),
    [
        ("17", "017", "same"),
        ("18", "017", "newer"),
        ("16", "017", "older"),
        (13, 12, "newer"),
        (None, "017", "unreadable"),
        ("17A", "017a", "same"),  # not plain integers: equal or not, never ordered
        ("17A", "017", "older"),
    ],
)
def test_issuance_numbers_are_compared_as_integers_when_they_are_integers(
    found: object, listed: object, standing: str
) -> None:
    assert cf._compare_issuance(found, listed) == standing


@respx.mock
@pytest.mark.asyncio
async def test_nhc_text_over_the_size_cap_is_a_named_fetch_failure(fetcher: Fetcher) -> None:
    _mock_nhc(tcm=httpx.Response(200, content=b"x" * (cf._TEXT_MAX_BYTES + 1)))

    result = await fetch_cyclones(fetcher, include_forecast=True)

    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == "fetch_failed"
    assert f"exceeds cap {cf._TEXT_MAX_BYTES}" in fc["track_error"]
    assert result["degraded"] is True


@respx.mock
@pytest.mark.asyncio
async def test_cone_kmz_over_the_size_cap_is_a_named_fetch_failure(fetcher: Fetcher) -> None:
    _mock_nhc(cone=httpx.Response(200, content=b"x" * (cf._CONE_MAX_BYTES + 1)))

    result = await fetch_cyclones(fetcher, include_forecast=True)

    fc = result["storms"][0]["forecast"]
    assert fc["track_status"] == "ok"
    assert fc["cone_status"] == "fetch_failed"
    assert f"exceeds cap {cf._CONE_MAX_BYTES}" in fc["cone_error"]


def test_the_documented_limits_are_the_enforced_ones() -> None:
    """The CHANGELOG and the module docstring state these numbers, and the
    behaviour tests above are written against the constants; this pins
    the constants to the stated values, so changing a limit means
    changing its documentation too."""
    assert cf.MAX_FORECAST_STORMS == 10
    assert cf._TEXT_MAX_BYTES == 1024 * 1024
    assert cf._CONE_MAX_BYTES == 2 * 1024 * 1024
    assert cf._KML_MAX_BYTES == 8 * 1024 * 1024
    assert cf._KML_MAX_RATIO == 100
    assert cf._CONE_MAX_VERTICES == 3_000
    assert cf._CONE_MAX_OUTLINE_VERTICES == 500


def test_simplify_ring_too_short_to_simplify_still_honours_the_cap() -> None:
    two_point_ring = [(0.0, 0.0), (1.0, 0.0), (0.0, 0.0)]
    assert cf.simplify_closed_ring(two_point_ring, 0.01, max_vertices=2) == [(0.0, 0.0), (1.0, 0.0)]
    with pytest.raises(ValueError, match="more than 1"):
        cf.simplify_closed_ring(two_point_ring, 0.01, max_vertices=1)
