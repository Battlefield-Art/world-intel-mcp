"""Border crossings: redirects are never followed and every download has its own cap.

All offline: respx serves the fixtures of ``test_sources_border_crossings``. Each of the
four requests (CBP dataset page, CBP CSV, GOV.UK Content API, Home Office ODS) is tested on
its own: a 302 whose target would return good data, a body one byte past the cap, and a
body exactly at the cap.

Gaps / not covered:

* The fetcher's streaming cap itself (decoded size, one network read past the cap) is the
  fetcher's to test; here respx delivers each body in one piece.
* No live publisher was asked whether it redirects today; a redirect reads as an outage.
"""

from __future__ import annotations

import io
import json
import re
import zipfile
from dataclasses import dataclass

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import border_crossings as bc

from .test_sources_border_crossings import (
    _CBP_CSV_URL,
    _CBP_HEADER,
    _CBP_NORTHERN_USBP_AUG,
    _CBP_PAGE,
    _UK_ITEM,
    _UK_ODS_URL,
    _main_ods,
    _no_sleep,
)

_MIB = 1024 * 1024


@dataclass(frozen=True)
class _Site:
    name: str  # the request under test
    what: str  # how the module names it
    url: str
    cap_mib: int
    us: bool


_SITES = [
    _Site("cbp_page", "CBP dataset page", bc.CBP_DATASET_PAGE, 2, True),
    _Site("cbp_csv", "CBP encounters file", _CBP_CSV_URL, 32, True),
    _Site("uk_api", "GOV.UK Content API", bc.UK_CONTENT_API, 2, False),
    _Site("uk_ods", "Home Office time series", _UK_ODS_URL, 8, False),
]
_IDS = [site.name for site in _SITES]


def _good() -> dict[str, httpx.Response]:
    return {
        bc.CBP_DATASET_PAGE: httpx.Response(200, text=_CBP_PAGE, headers={"content-type": "text/html; charset=utf-8"}),
        _CBP_CSV_URL: httpx.Response(200, text=_CBP_HEADER + "\n" + _CBP_NORTHERN_USBP_AUG,
                                     headers={"content-type": "text/csv"}),
        bc.UK_CONTENT_API: httpx.Response(200, json=_UK_ITEM),
        _UK_ODS_URL: httpx.Response(200, content=_main_ods(), headers={"content-type": bc._ODS_TYPE}),
    }


def _good_body(site: _Site) -> bytes:
    return _good()[site.url].content


async def _call(fetcher: Fetcher, site: _Site) -> dict:
    if site.us:
        return await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
    return await bc.fetch_uk_small_boat_arrivals(fetcher)


async def _with(fetcher: Fetcher, site: _Site, response: httpx.Response, extra: dict | None = None) -> tuple:
    """The tool's result with `site` answering `response` and the other requests answering well."""
    routes = {**_good(), site.url: response}
    with respx.mock(assert_all_called=False) as mock:
        handles = {url: mock.get(url).mock(return_value=answer) for url, answer in {**routes, **(extra or {})}.items()}
        result = await _call(fetcher, site)
    return result, handles


def _figures(site: _Site) -> str:
    return "latest" if site.us else "latest_day"


# ---------------------------------------------------------------------------
# Redirects
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("site", _SITES, ids=_IDS)
async def test_a_redirect_is_never_followed_and_is_named(fetcher: Fetcher, site: _Site) -> None:
    # Invented target, robots-disallowed for CBP; it would answer with good data if requested.
    target = ("https://www.cbp.gov/sites/default/files/assets/documents/2026-10/elsewhere.csv" if site.us
              else "https://assets.publishing.service.gov.uk/media/elsewhere/time_series.ods")
    result, handles = await _with(fetcher, site, httpx.Response(302, headers={"Location": target}),
                                  extra={target: httpx.Response(200, content=_good_body(site))})
    assert not handles[target].called  # never requested
    assert result["degraded"] is True and result["reason"] == ("cbp_fetch_failed" if site.us else "uk_fetch_failed")
    assert result["error"] == (f"{site.what} redirects to {target} (HTTP 302); redirects are not followed, since "
                               "their target is not checked against the host and robots.txt rules")
    assert result["data_gaps"] == [f"No figures: {result['error']}."]
    assert _figures(site) not in result


@pytest.mark.parametrize("status", [301, 303, 307, 308])
async def test_every_redirect_status_reads_as_a_failed_refresh(fetcher: Fetcher, status: int) -> None:
    site = _SITES[0]
    target = "https://www.cbp.gov/document/stats/moved"  # invented
    result, handles = await _with(fetcher, site, httpx.Response(status, headers={"Location": target}),
                                  extra={target: httpx.Response(200, text=_CBP_PAGE)})
    assert not handles[target].called
    assert result["degraded"] is True and f"redirects to {target} (HTTP {status})" in result["error"]


async def test_a_redirect_location_has_its_credentials_masked(fetcher: Fetcher) -> None:
    site = _SITES[2]
    target = "https://someone@gov.uk.example/api/content/x"  # invented
    result, handles = await _with(fetcher, site, httpx.Response(302, headers={"Location": target}),
                                  extra={target: httpx.Response(200, json=_UK_ITEM)})
    assert not handles[target].called
    assert "redirects to https://***@gov.uk.example/api/content/x (HTTP 302)" in result["error"]
    assert "someone" not in json.dumps(result)


@pytest.mark.parametrize("site", _SITES, ids=_IDS)
async def test_a_redirect_on_refresh_serves_the_last_good_copy_marked_stale(
        fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch, site: _Site) -> None:
    _no_sleep(monkeypatch)
    first, _handles = await _with(fetcher, site, _good()[site.url])
    key = bc.CBP_CACHE_KEY if site.us else bc.UK_CACHE_KEY
    value, _created = fetcher.cache.get_stale_meta(key)
    fetcher.cache.set(key, value, ttl_seconds=-1)
    target = "https://evil.example/x"  # invented
    stale, handles = await _with(fetcher, site, httpx.Response(302, headers={"Location": target}),
                                 extra={target: httpx.Response(200, content=b"")})
    assert not handles[target].called
    assert stale["stale"] is True and stale[_figures(site)] == first[_figures(site)]
    assert any(gap.startswith(f"Refresh failed ({site.what} redirects to {target} (HTTP 302)")
               for gap in stale["data_gaps"])


# ---------------------------------------------------------------------------
# Download caps
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("site", _SITES, ids=_IDS)
async def test_a_body_one_byte_past_its_cap_is_refused_and_the_cap_named(fetcher: Fetcher, site: _Site) -> None:
    body = _good_body(site)
    body += b" " * (site.cap_mib * _MIB + 1 - len(body))  # good data, one byte too long
    result, _handles = await _with(fetcher, site, httpx.Response(200, content=body))
    assert result["degraded"] is True and _figures(site) not in result
    assert re.fullmatch(rf"{re.escape(site.what)} is larger than this tool's {site.cap_mib} MiB cap "
                        r"\(stopped after [0-9,]+ bytes\); refused", result["error"]), result["error"]
    assert result["data_gaps"] == [f"No figures: {result['error']}."]


def _padded_ods(size: int) -> bytes:
    """The fixture ODS with a stored padding member, exactly `size` bytes long."""
    def build(pad: int) -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(io.BytesIO(_main_ods())) as source, zipfile.ZipFile(buf, "w") as out:
            for info in source.infolist():
                out.writestr(info, source.read(info.filename))
            out.writestr("Thumbnails/padding.bin", b"\0" * pad, compress_type=zipfile.ZIP_STORED)
        return buf.getvalue()

    data = build(size - len(build(0)))
    assert len(data) == size
    return data


def _at_cap(site: _Site) -> bytes:
    cap = site.cap_mib * _MIB
    if site.name == "uk_ods":
        return _padded_ods(cap)
    body = _good_body(site)
    if site.name == "cbp_csv":  # a trailing line of padding: one unreadable row, named
        return body + b"\n" + b"x" * (cap - len(body) - 1)
    return body + b" " * (cap - len(body))


@pytest.mark.parametrize("site", _SITES, ids=_IDS)
async def test_a_body_exactly_at_its_cap_is_read(fetcher: Fetcher, site: _Site) -> None:
    body = _at_cap(site)
    assert len(body) == site.cap_mib * _MIB
    result, _handles = await _with(fetcher, site, httpx.Response(200, content=body))
    assert "degraded" not in result, result.get("error")
    if site.us:
        assert result["latest"]["encounters"] == 563
    else:
        assert result["latest_day"]["date"] == "2026-10-15"


def test_caps_fit_each_payload() -> None:
    # At least 4x the 17 October 2026 payload, so a cap is not hit by normal growth.
    sizes = {"cbp_page": 123_000, "cbp_csv": 6_677_601, "uk_api": 133_000, "uk_ods": 76_787}
    caps = {"cbp_page": bc._CBP_PAGE_MAX_BYTES, "cbp_csv": bc._CBP_CSV_MAX_BYTES,
            "uk_api": bc._UK_API_MAX_BYTES, "uk_ods": bc._UK_ODS_MAX_BYTES}
    assert {name: cap // _MIB for name, cap in caps.items()} == {site.name: site.cap_mib for site in _SITES}
    assert all(caps[name] >= 4 * size for name, size in sizes.items())


async def test_another_failure_still_reads_unavailable(fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    _no_sleep(monkeypatch)
    site = _SITES[3]
    result, _handles = await _with(fetcher, site, httpx.Response(503))
    assert result["degraded"] is True and result["error"].startswith("Home Office time series unavailable: ")
