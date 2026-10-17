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


def _kmz(kml: str, name: str = "ep212026_017adv_CONE.kml") -> bytes:
    """Zip a KML the way the live CONE.kmz is packed (one deflated member)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
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
async def test_jtwc_text_without_forecast_positions_is_unparsed_and_refetched(
    fetcher: Fetcher, no_sleep: None
) -> None:
    """Synthetic body: the first two lines of 29W's warning text, no
    forecast blocks (a real JTWC final warning has not been observed).
    Reported unparsed, degraded, and not kept for the next call."""
    routes = _mock_jtwc()
    routes["29W"].mock(
        return_value=httpx.Response(
            200,
            text=_crlf("WTPN32 PGTW 171500\nSUBJ/TYPHOON 29W (CHAMPI) WARNING NR 013//\n"),
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
