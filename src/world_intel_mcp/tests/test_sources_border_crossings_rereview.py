"""Border crossings: fixes from the second review round (RB1 to RB5).

All offline: respx serves the fixtures of ``test_sources_border_crossings``; every
link, date and attachment URL marked "invented" is not from a publisher.

Gaps / not covered:

* How CBP's server resolves "//" or a backslash in a path was not tested (no network);
  such links are refused whatever the server would do.
* GOV.UK attachments are read only from assets.publishing.service.gov.uk in the
  publication checked on 2026-10-17; the www.gov.uk upload rule rests on GOV.UK's
  robots.txt as captured that day.
"""

from __future__ import annotations

import copy
from datetime import date

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import border_crossings as bc

from .test_sources_border_crossings import (
    _CBP_CSV_URL,
    _CBP_NORTHERN_USBP_AUG,
    _UK_DAILY,
    _UK_ITEM,
    _UK_ODS_URL,
    _UK_WEEKLY,
    _cbp_mock,
    _main_ods,
    _no_sleep,
    _ods,
    _uk_mock,
)
from .test_sources_border_crossings_edges import _with_links

# ---------------------------------------------------------------------------
# RB1, RB2: cached copies of an older schema
# ---------------------------------------------------------------------------

async def test_cbp_cached_copy_in_the_schema_1_shape_is_refetched_and_never_the_fallback(
        fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    # The shape 457a49f stored: schema 1, no rows_with_bad_bytes_elsewhere, no meta.skipped_links.
    # Read as current, a copy like this would raise KeyError on every call.
    _no_sleep(monkeypatch)
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG):
        await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
    value, _created = fetcher.cache.get_stale_meta(bc.CBP_CACHE_KEY)
    old = copy.deepcopy(value)
    old["schema"] = 1
    del old["rows_with_bad_bytes_elsewhere"]
    del old["meta"]["skipped_links"]
    fetcher.cache.set(bc.CBP_CACHE_KEY, old, ttl_seconds=3600)
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG) as mock:
        fresh = await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
        urls = [str(call.request.url) for call in mock.calls]
    assert urls == [bc.CBP_DATASET_PAGE, _CBP_CSV_URL]  # refetched, not served
    assert fresh["latest"]["encounters"] == 563 and fresh["stale"] is False

    fetcher.cache.set(bc.CBP_CACHE_KEY, old, ttl_seconds=-1)
    with respx.mock:
        respx.get(bc.CBP_DATASET_PAGE).mock(return_value=httpx.Response(503))
        result = await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
    assert result["degraded"] is True and result["reason"] == "cbp_fetch_failed" and "latest" not in result


async def test_uk_cached_copy_of_schema_1_is_refetched_and_never_the_fallback(
        fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    # A schema-1 copy may hold a row dated year 1, which the response's date arithmetic cannot take.
    _no_sleep(monkeypatch)
    with _uk_mock(_main_ods()):
        await bc.fetch_uk_small_boat_arrivals(fetcher)
    value, _created = fetcher.cache.get_stale_meta(bc.UK_CACHE_KEY)
    old = copy.deepcopy(value)
    old["schema"] = 1
    old["weekly"].insert(0, ["0001-01-03", 5, 1, 0, None, None, False])  # invented
    fetcher.cache.set(bc.UK_CACHE_KEY, old, ttl_seconds=3600)
    with _uk_mock(_main_ods()) as mock:
        fresh = await bc.fetch_uk_small_boat_arrivals(fetcher, weeks=520)
        urls = [str(call.request.url) for call in mock.calls]
    assert urls == [bc.UK_CONTENT_API, _UK_ODS_URL]
    assert fresh["dataset"]["coverage"]["weekly_from"] == "2024-04-28" and len(fresh["weeks"]) == 10

    fetcher.cache.set(bc.UK_CACHE_KEY, old, ttl_seconds=-1)
    with respx.mock:
        respx.get(bc.UK_CONTENT_API).mock(return_value=httpx.Response(503))
        result = await bc.fetch_uk_small_boat_arrivals(fetcher, weeks=520)
    assert result["degraded"] is True and result["reason"] == "uk_fetch_failed" and "latest_day" not in result


# ---------------------------------------------------------------------------
# RB2: UK dates outside the series
# ---------------------------------------------------------------------------

_WINDOW_GAP = "1 row(s) of {sheet} are dated outside 2018-01-01 to {last} and are not counted."


async def test_uk_rows_dated_outside_the_series_are_left_out_and_named(
        fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    # Invented rows: a week ending in year 1 (a blank date exported as a minimum) and a day in 9999.
    monkeypatch.setattr(bc, "_today", lambda: date(2026, 10, 17))
    ods = _ods(_UK_DAILY.splitlines() + ["9999-12-31,5,1,0"], ["0001-01-03,5,1,0,-,-"] + _UK_WEEKLY.splitlines())
    with _uk_mock(ods):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    gaps = [_WINDOW_GAP.format(sheet="SB_01", last="2026-10-24"), _WINDOW_GAP.format(sheet="SB_02", last="2026-10-24")]
    assert result["data_gaps"] == gaps
    assert result["latest_day"]["date"] == "2026-10-15" and result["dataset"]["daily_data_through"] == "2026-10-15"
    assert result["dataset"]["coverage"]["weekly_from"] == "2024-04-28"
    widest = await bc.fetch_uk_small_boat_arrivals(fetcher, weeks=520)  # from the cache
    assert len(widest["weeks"]) == 10 and widest["data_gaps"] == gaps


async def test_uk_weekly_table_with_only_an_out_of_range_week_has_no_weeks(
        fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bc, "_today", lambda: date(2026, 10, 17))
    with _uk_mock(_ods(_UK_DAILY.splitlines(), ["0001-01-03,5,1,0,-,-"])):  # invented
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["latest_week"] is None and result["weeks"] == []
    assert result["data_gaps"] == [_WINDOW_GAP.format(sheet="SB_02", last="2026-10-24")]


@pytest.mark.parametrize("today, last_day, gaps", [
    (date(2026, 10, 8), "2026-10-15", []),
    (date(2026, 10, 7), "2026-10-14", [_WINDOW_GAP.format(sheet="SB_01", last="2026-10-14")]),
])
async def test_uk_series_window_ends_seven_days_after_today(
        fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch, today: date, last_day: str, gaps: list[str]) -> None:
    monkeypatch.setattr(bc, "_today", lambda: today)
    with _uk_mock(_main_ods()):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["latest_day"]["date"] == last_day and result["data_gaps"] == gaps


def test_today_is_the_utc_date() -> None:
    from datetime import datetime, timezone
    assert bc._today() == datetime.now(timezone.utc).date()


# ---------------------------------------------------------------------------
# RB3, RB4: CBP links
# ---------------------------------------------------------------------------

_ODD_PATHS = (
    "/sites/default/files/assets//documents/2026-10/nationwide-encounters-fy23-fy26-sep-aor.csv",
    "/sites/default/files/2026-10\\..\\assets\\documents\\2026-10/nationwide-encounters-fy23-fy26-sep-aor.csv",
    "/sites/default/files/2026-10%5C..%5Cassets%5Cdocuments%5C2026-10/nationwide-encounters-fy23-fy26-sep-aor.csv",
    "/sites/default/files/2026-10%5c..%5cassets%5cdocuments%5c2026-10/nationwide-encounters-fy23-fy26-sep-aor.csv",
)  # invented


@pytest.mark.parametrize("href", _ODD_PATHS)
def test_cbp_paths_with_a_backslash_or_an_empty_segment_are_refused_and_named(href: str) -> None:
    # Each may resolve into the disallowed directory on a server that merges "//" or reads "\"
    # as "/", which the literal robots.txt prefix match does not see.
    assert bc._cbp_link(href) is None
    assert bc._cbp_link("https://www.cbp.gov" + href) is None
    page = _with_links(href)
    latest = bc.latest_cbp_file(page)
    assert latest["url"] == _CBP_CSV_URL and latest["newer_disallowed_url"] is None
    assert bc._skipped_links(bc._parse_page(page)) == [href]


async def test_cbp_odd_paths_are_never_requested_and_are_named(fetcher: Fetcher) -> None:
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG, page=_with_links(*_ODD_PATHS)) as mock:
        result = await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
        urls = [str(call.request.url) for call in mock.calls]
    assert urls == [bc.CBP_DATASET_PAGE, _CBP_CSV_URL]
    assert result["latest"]["encounters"] == 563
    gap = next(g for g in result["data_gaps"] if "cannot read" in g)
    assert gap.startswith("CBP's dataset page links 4 encounters file(s) this tool cannot read")


async def test_cbp_encounters_links_with_another_separator_are_named(fetcher: Fetcher) -> None:
    # Invented names: "_" or an encoded space between "nationwide" and "encounters".
    odd = ("/sites/default/files/2026-10/nationwide_encounters-fy23-fy26-sep-aor.csv",
           "/sites/default/files/2026-10/nationwide%20encounters-fy23-fy26-sep-aor.csv")
    assert bc._skipped_links(bc._parse_page(_with_links(*odd))) == ["https://www.cbp.gov" + path for path in odd]
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG, page=_with_links(*odd)):
        result = await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
    assert result["dataset"]["url"] == _CBP_CSV_URL
    gap = next(g for g in result["data_gaps"] if "cannot read" in g)
    assert "2 encounters file(s)" in gap and all("https://www.cbp.gov" + path in gap for path in odd)


# ---------------------------------------------------------------------------
# RB5: the UK attachment on www.gov.uk
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("href", [
    "https://www.gov.uk/search/all.ods",  # robots.txt: Disallow: /search/all*
    "https://www.gov.uk/government/print",  # robots.txt: Disallow: /*/print$
    "/government/publications/x/time_series.ods",  # on www.gov.uk, not an upload
    "https://www.gov.uk/government/uploads/../../search/all.ods",
    "https://www.gov.uk/government/uploads/%2E%2E/%2E%2E/search/all.ods",
    "https://www.gov.uk/government/uploads%2F..%2F..%2Fsearch/all.ods",
    "https://www.gov.uk/government/uploads/x%2F..%2F..%2F..%2Fsearch/all.ods",  # leaves only once decoded
    "https://assets.publishing.service.gov.uk/media/x//time_series.ods",
    "https://assets.publishing.service.gov.uk/media/x\\time_series.ods",
    "https://assets.publishing.service.gov.uk/media/x%5Ctime_series.ods",
])  # invented
def test_uk_attachment_on_www_gov_uk_must_be_an_upload(href: str) -> None:
    item = copy.deepcopy(_UK_ITEM)
    item["details"]["attachments"][2]["url"] = href  # altered
    with pytest.raises(bc._SourceError, match="^the time-series attachment is not a file on GOV.UK over https"):
        bc._uk_attachment(item)


async def test_uk_attachment_outside_the_uploads_is_degraded_and_never_requested(fetcher: Fetcher) -> None:
    item = copy.deepcopy(_UK_ITEM)
    item["details"]["attachments"][2]["url"] = "https://www.gov.uk/search/all.ods"  # altered
    with respx.mock(assert_all_called=False) as mock:
        mock.get(bc.UK_CONTENT_API).mock(return_value=httpx.Response(200, json=item))
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
        urls = [str(call.request.url) for call in mock.calls]
    assert urls == [bc.UK_CONTENT_API]
    assert result["degraded"] is True and result["reason"] == "uk_unexpected_format"


def test_uk_attachment_under_government_uploads_is_accepted() -> None:
    item = copy.deepcopy(_UK_ITEM)
    url = "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/1/time_series.ods"  # invented
    item["details"]["attachments"][2]["url"] = url  # altered
    assert bc._uk_attachment(item)["url"] == url
