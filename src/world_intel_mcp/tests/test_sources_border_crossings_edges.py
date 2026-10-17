"""Edge cases for sources/border_crossings.py: damaged or hostile files, the URL rules, and the
like-for-like rules, added with the fixes for the 2026-10-17 review of the border tools.

The fixtures come from test_sources_border_crossings (captured live on 2026-10-17). Every row,
link or cell changed here is marked as invented or altered where it is built; figures asserted
on altered data are sums of the fixture, not publisher figures.

Gaps / not covered: redirects (the shared fetcher follows them; see the module's Gaps), the
download caps (checked by the fetcher), memory use and run time themselves (measured outside the
suite, since an assertion on peak RSS or seconds would be flaky on a shared host; these tests
check that each limit refuses), and the live publishers.
"""

from __future__ import annotations

import asyncio
import copy
import io
import time
import types
import zipfile
from datetime import datetime

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import border_crossings as bc

from .test_sources_border_crossings import (
    _BLANK_TAIL,
    _CBP_CSV_URL,
    _CBP_FEATURE_ROWS,
    _CBP_HEADER,
    _CBP_NORTHERN_USBP_AUG,
    _CBP_PAGE,
    _DAILY_HEADER,
    _ODS_NS,
    _UK_DAILY,
    _UK_ITEM,
    _UK_NOTES_ROWS,
    _UK_ODS_URL,
    _UK_WEEKLY,
    _WEEKLY_HEADER,
    _cbp_mock,
    _main_ods,
    _no_numbers,
    _no_sleep,
    _ods,
    _ods_bytes,
    _sheet,
    _uk_mock,
)

# The first of _CBP_FEATURE_ROWS (verbatim from the live file): 86 Border Patrol apprehensions.
_BIG_BEND = ("2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Southwest Land Border,Big Bend Sector,BBT,Single Adults,"
             "MEXICO,Title 8,Apprehensions,86\n")
# Verbatim, line 41,668 of the live file: October FY2025 (October 2024) for the same cell.
_BIG_BEND_FY2025_OCT = ("2025,FYTD,OCT,U.S. Border Patrol,Southwest Land Border,Big Bend Sector,BBT,"
                        "Single Adults,MEXICO,Title 8,Apprehensions,205\n")
_SEPTEMBER = "/sites/default/files/2026-10/nationwide-encounters-fy23-fy26-sep-aor.csv"  # invented, newest-looking


def _with_links(*hrefs: str) -> str:
    """The captured dataset page with invented links added before the real ones."""
    links = "".join(f'<li><a href="{href}">FY23 - FY26 (FYTD) Encounters - September</a></li>' for href in hrefs)
    return _CBP_PAGE.replace('<ul class="undecorated list">', '<ul class="undecorated list">' + links)


# ---------------------------------------------------------------------------
# CBP: which link is read
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("href", [
    "https://www.cbp.gov/sites/default/files/2026-10/../assets/documents/2026-10/"
    "nationwide-encounters-fy23-fy26-sep-aor.csv",
    "/sites/default/files/2026-10/%2E%2E/assets/documents/2026-10/nationwide-encounters-fy23-fy26-sep-aor.csv",
    "/sites/default/files/%61ssets/documents/2026-10/nationwide-encounters-fy23-fy26-sep-aor.csv",
])
def test_cbp_robots_rule_is_checked_on_the_path_httpx_requests(href: str) -> None:
    # Invented links: each reaches the disallowed directory once the path is normalized.
    url = "https://www.cbp.gov/sites/default/files/assets/documents/2026-10/nationwide-encounters-fy23-fy26-sep-aor.csv"
    assert bc._cbp_link(href) == (url, url[len("https://www.cbp.gov"):], False)
    latest = bc.latest_cbp_file(_with_links(href))
    assert latest["url"] == _CBP_CSV_URL
    assert latest["newer_disallowed_url"] == url


def test_cbp_an_encoded_slash_into_the_disallowed_directory_counts_as_disallowed() -> None:
    href = "/sites/default/files/assets%2Fdocuments/2026-10/nationwide-encounters-fy23-fy26-sep-aor.csv"  # invented
    url, path, allowed = bc._cbp_link(href)
    assert (path, allowed) == (href, False)
    assert bc.latest_cbp_file(_with_links(href))["url"] == _CBP_CSV_URL


_OFF_SITE = (
    "https://cbp.gov.example" + _SEPTEMBER,  # another host
    "http://www.cbp.gov" + _SEPTEMBER,  # not https
    "https://www.cbp.gov:8443" + _SEPTEMBER,  # another port
    "https://someone@www.cbp.gov" + _SEPTEMBER,  # credentials
)


def test_cbp_links_off_https_www_cbp_gov_are_never_chosen() -> None:
    for href in _OFF_SITE:
        assert bc._cbp_link(href) is None, href
    assert bc._cbp_link("https://www.cbp.gov:443" + _SEPTEMBER)[0] == "https://www.cbp.gov" + _SEPTEMBER
    latest = bc.latest_cbp_file(_with_links(*_OFF_SITE))
    assert latest["url"] == _CBP_CSV_URL and latest["newer_disallowed_url"] is None


def test_cbp_links_that_are_not_urls_are_never_chosen_and_are_named() -> None:
    bad = ("https://www.cbp.gov:notaport" + _SEPTEMBER, "https://[::1" + _SEPTEMBER)  # invented
    assert [bc._cbp_link(href) for href in bad] == [None, None]
    page = _with_links(*bad)
    assert bc.latest_cbp_file(page)["url"] == _CBP_CSV_URL
    assert bc._skipped_links(bc._parse_page(page)) == list(bad)


async def test_cbp_off_site_links_are_named_and_never_requested(fetcher: Fetcher) -> None:
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG, page=_with_links(*_OFF_SITE)) as mock:
        result = await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
        urls = [str(call.request.url) for call in mock.calls]
    assert urls == [bc.CBP_DATASET_PAGE, _CBP_CSV_URL]
    assert result["latest"]["encounters"] == 563
    gap = next(g for g in result["data_gaps"] if "cannot read" in g)
    assert gap.startswith("CBP's dataset page links 4 encounters file(s) this tool cannot read")
    for shown in ("https://cbp.gov.example/", "http://www.cbp.gov/", "https://www.cbp.gov:8443/",
                  "https://***@www.cbp.gov/"):
        assert shown in gap
    assert "someone" not in gap


async def test_cbp_encounters_links_the_name_rule_cannot_read_are_named(fetcher: Fetcher) -> None:
    # Invented names one step off the pattern; any of them could be a newer file.
    odd = ("/sites/default/files/2026-10/nationwide-encounters-fy23-fy26-sept-aor.csv",
           "/sites/default/files/2026-10/nationwide-encounters-fy23-fy26-sep-AOR.csv",
           "/sites/default/files/2026-10/nationwide-encounters-fy23-fy26-sep-aor.CSV")
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG, page=_with_links(*odd)):
        result = await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
    assert result["dataset"]["url"] == _CBP_CSV_URL
    gap = next(g for g in result["data_gaps"] if "cannot read" in g)
    assert "3 encounters file(s)" in gap and all("https://www.cbp.gov" + path in gap for path in odd)

    # With no readable file at all, the refusal names them too.
    only = "<html><body>" + "".join(f'<a href="{path}">x</a>' for path in odd) + "</body></html>"
    fetcher.cache.delete(bc.CBP_CACHE_KEY)
    with _cbp_mock("", page=only):
        refused = await bc.fetch_us_border_encounters(fetcher)
    assert refused["degraded"] is True and refused["reason"] == "cbp_unexpected_format"
    assert "sept-aor.csv" in refused["error"] and _no_numbers(refused)


def test_cbp_files_rank_by_fiscal_month_not_calendar_month() -> None:
    # Invented: FY23-FY26 files through October, December and August. FY2026 runs from
    # October 2025 to September 2026, so the August file is the newest of the three.
    links = ("/sites/default/files/2025-11/nationwide-encounters-fy23-fy26-oct-aor.csv",
             "/sites/default/files/2026-01/nationwide-encounters-fy23-fy26-dec-aor.csv",
             "/sites/default/files/2026-09/nationwide-encounters-fy23-fy26-aug-aor_0.csv")
    page = "<html><body>" + "".join(f'<a href="{href}">x</a>' for href in links) + "</body></html>"
    assert bc.latest_cbp_file(page)["url"] == "https://www.cbp.gov" + links[2]


# ---------------------------------------------------------------------------
# CBP: reading the file
# ---------------------------------------------------------------------------

def test_cbp_rows_that_cannot_be_read_are_counted_and_left_out() -> None:
    # Altered copies of _BIG_BEND.
    rows = [
        _BIG_BEND,
        _BIG_BEND.replace(",86\n", ",-3\n"),  # a negative count
        _BIG_BEND.replace(",86\n", ",eighty\n"),  # not a number
        _BIG_BEND.replace("MEXICO", "M\ufffdXICO"),  # a byte that was not UTF-8, in a column read
        _BIG_BEND.replace("Big Bend Sector", "Big B\ufffdnd Sector"),  # the same, in a column not read
        _BIG_BEND.replace("Big Bend Sector", '"' + "x" * 200_000 + '"'),  # past csv's 131,072-character field limit
        _BIG_BEND.replace("Big Bend Sector", "Big Bend\rSector"),  # a bare carriage return splits the row
    ]
    parsed = bc.parse_cbp_csv(_CBP_HEADER + "\n" + "".join(rows))
    assert (parsed["rows"], parsed["unreadable_rows"], parsed["rows_with_bad_bytes_elsewhere"]) == (8, 6, 1)
    assert sum(cell[-1] for cell in parsed["cells"]) == 172  # the good row and the one with a bad AOR byte

    # A quote left open swallows the rows after it: each line it spans is counted.
    parsed = bc.parse_cbp_csv(_CBP_HEADER + "\n" + _BIG_BEND + _BIG_BEND.replace("Big Bend", '"Big Bend')
                              + _BIG_BEND + _BIG_BEND)
    assert (parsed["rows"], parsed["unreadable_rows"]) == (4, 3)
    assert sum(cell[-1] for cell in parsed["cells"]) == 86


def test_cbp_record_over_two_lines_is_counted_once_per_line() -> None:
    # Altered: a quoted newline in the area of responsibility. None of CBP's fields holds one,
    # so a record over two lines is read as damage, not data.
    parsed = bc.parse_cbp_csv(_CBP_HEADER + "\n" + _BIG_BEND
                              + _BIG_BEND.replace("Big Bend Sector", '"Big Bend\nSector"'))
    assert (parsed["rows"], parsed["unreadable_rows"]) == (3, 2)
    assert sum(cell[-1] for cell in parsed["cells"]) == 86
    with pytest.raises(ValueError, match="CBP file header not readable: field larger than field limit"):
        bc.parse_cbp_csv('"' + "x" * 200_000 + '",' + _CBP_HEADER + "\n" + _BIG_BEND)


async def test_cbp_file_with_bytes_that_are_not_utf8_is_read_row_by_row(fetcher: Fetcher) -> None:
    # Altered: Latin-1 bytes in two copies of _BIG_BEND, one in the area of responsibility
    # (not read here) and one in the citizenship (read here).
    body = (_CBP_HEADER + "\n" + _BIG_BEND).encode()
    body += _BIG_BEND.encode().replace(b"Big Bend Sector", b"Big Bend Sect\xe9r")
    body += _BIG_BEND.encode().replace(b"MEXICO", b"M\xc9XICO")
    with _cbp_mock("", csv_response=httpx.Response(200, content=body, headers={"content-type": "text/csv"})):
        result = await bc.fetch_us_border_encounters(fetcher, region="southwest")
    assert result["latest"]["encounters"] == 172
    assert result["data_gaps"] == [
        "1 of the file's 3 rows could not be read and are not counted.",
        "1 row(s) of the file have bytes that are not UTF-8 in columns this tool does not read (such as the area "
        "of responsibility); they are counted.",
    ]


async def test_cbp_fiscal_year_in_progress_ends_at_the_data_end(fetcher: Fetcher) -> None:
    with _cbp_mock(_CBP_FEATURE_ROWS):
        nation = await bc.fetch_us_border_encounters(fetcher)
    years = {entry["fiscal_year"]: entry for entry in nation["fiscal_years"]}
    assert nation["dataset"]["coverage"]["last_period"] == "2026-08"
    assert years[2026]["period_end"] == "2026-08-31"  # the file's last month, not 2026-09-30
    assert years[2025]["period_end"] == "2025-09-30"
    assert "the last day of the file's last month" in nation["date_semantics"]


async def test_cbp_fiscal_year_to_date_compares_the_same_months_of_the_prior_year(fetcher: Fetcher) -> None:
    with _cbp_mock(_CBP_FEATURE_ROWS + _BIG_BEND_FY2025_OCT):
        mexico = await bc.fetch_us_border_encounters(fetcher, citizenship="MEXICO", months=120)
    years = {entry["fiscal_year"]: entry for entry in mexico["fiscal_years"]}
    # FY2026 so far: October 2025 (105) and August 2026 (86); the same months a year earlier:
    # October 2024 (205) and August 2025 (123).
    assert years[2026]["encounters"] == 191
    assert years[2026]["prior_fiscal_year_same_months"] == 328


# ---------------------------------------------------------------------------
# CBP: the cache
# ---------------------------------------------------------------------------

async def test_cbp_concurrent_cold_calls_download_once(fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    real = fetcher.get_bytes

    async def waits(*args, **kwargs):
        await asyncio.sleep(0)  # a network wait: lets the other call run
        return await real(*args, **kwargs)

    monkeypatch.setattr(fetcher, "get_bytes", waits)
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG) as mock:
        first, second = await asyncio.gather(
            bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp"),
            bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp"))
        urls = [str(call.request.url) for call in mock.calls]
    assert urls == [bc.CBP_DATASET_PAGE, _CBP_CSV_URL]
    assert first["latest"]["encounters"] == second["latest"]["encounters"] == 563


async def test_cbp_stale_copy_reports_its_real_age(fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    _no_sleep(monkeypatch)
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG):
        await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
    value, _created = fetcher.cache.get_stale_meta(bc.CBP_CACHE_KEY)
    fetcher.cache.set(bc.CBP_CACHE_KEY, value, ttl_seconds=-1)
    conn = fetcher.cache._get_conn()
    conn.execute("UPDATE cache SET created_at = ? WHERE key = ?", (time.time() - 3600, bc.CBP_CACHE_KEY))
    conn.commit()
    with respx.mock:
        respx.get(bc.CBP_DATASET_PAGE).mock(return_value=httpx.Response(503))
        stale = await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
    assert stale["stale"] is True
    assert 3599 <= stale["stale_age_seconds"] < 3700


async def test_cbp_cached_copy_of_another_schema_is_never_served(
        fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    _no_sleep(monkeypatch)
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG):
        await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
    value, _created = fetcher.cache.get_stale_meta(bc.CBP_CACHE_KEY)
    old = copy.deepcopy(value)
    old["schema"] = bc._CBP_SCHEMA - 1
    for cell in old["cells"]:
        cell[-1] = 0  # altered: nothing in the old copy matches the file
    fetcher.cache.set(bc.CBP_CACHE_KEY, old, ttl_seconds=3600)
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG) as mock:
        fresh = await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
        urls = [str(call.request.url) for call in mock.calls]
    assert urls == [bc.CBP_DATASET_PAGE, _CBP_CSV_URL]  # refetched, not served
    assert fresh["latest"]["encounters"] == 563

    # Nor is it the fallback when a refresh fails.
    fetcher.cache.set(bc.CBP_CACHE_KEY, old, ttl_seconds=-1)
    with respx.mock:
        respx.get(bc.CBP_DATASET_PAGE).mock(return_value=httpx.Response(503))
        result = await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
    assert result["degraded"] is True and "latest" not in result


# ---------------------------------------------------------------------------
# Failures read as degraded or stale, with a reason
# ---------------------------------------------------------------------------

async def test_degraded_responses_name_their_gap(fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    _no_sleep(monkeypatch)
    with respx.mock:
        respx.get(bc.CBP_DATASET_PAGE).mock(return_value=httpx.Response(503))
        respx.get(bc.UK_CONTENT_API).mock(return_value=httpx.Response(503))
        us = await bc.fetch_us_border_encounters(fetcher)
        uk = await bc.fetch_uk_small_boat_arrivals(fetcher)
    for result in (us, uk):
        assert result["degraded"] is True
        assert result["data_gaps"] == [f"No figures: {result['error']}."]


async def test_cbp_parser_fault_serves_the_stale_copy_with_its_reason(
        fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    _no_sleep(monkeypatch)
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG):
        await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
    value, _created = fetcher.cache.get_stale_meta(bc.CBP_CACHE_KEY)
    fetcher.cache.set(bc.CBP_CACHE_KEY, value, ttl_seconds=-1)

    def fault(text: str) -> dict:
        raise RecursionError("simulated parser fault")

    monkeypatch.setattr(bc, "parse_cbp_csv", fault)
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG):
        stale = await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
    assert stale["stale"] is True and stale["latest"]["encounters"] == 563
    assert any("CBP encounters file not readable: RecursionError: simulated parser fault" in gap
               for gap in stale["data_gaps"])

    fetcher.cache.delete(bc.CBP_CACHE_KEY)
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG):
        cold = await bc.fetch_us_border_encounters(fetcher)
    assert cold["degraded"] is True and cold["reason"] == "cbp_unexpected_format"

    def out_of_memory(text: str) -> dict:
        raise MemoryError

    monkeypatch.setattr(bc, "parse_cbp_csv", out_of_memory)
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG), pytest.raises(MemoryError):  # never reported as a bad file
        await bc.fetch_us_border_encounters(fetcher)


async def test_uk_parser_fault_is_degraded_with_its_reason(fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    def fault(tables: dict) -> dict:
        raise KeyError("simulated")

    monkeypatch.setattr(bc, "parse_uk_small_boats", fault)
    with _uk_mock(_main_ods()):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["degraded"] is True and result["reason"] == "uk_unexpected_format"
    assert result["error"] == "Home Office time series not readable: KeyError: 'simulated'"


async def test_cbp_file_unavailable_or_page_fault_is_degraded(
        fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    _no_sleep(monkeypatch)
    with _cbp_mock("", csv_response=httpx.Response(503)):
        result = await bc.fetch_us_border_encounters(fetcher)
    assert result["degraded"] is True and result["reason"] == "cbp_fetch_failed"
    assert result["error"].startswith("CBP encounters file unavailable: ")

    def fault(html: str) -> None:
        raise RuntimeError("simulated page fault")

    monkeypatch.setattr(bc, "_parse_page", fault)
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG):
        result = await bc.fetch_us_border_encounters(fetcher)
    assert result["degraded"] is True and result["reason"] == "cbp_unexpected_format"
    assert result["error"] == "CBP dataset page not readable: RuntimeError: simulated page fault"

    def out_of_memory(html: str) -> None:
        raise MemoryError

    monkeypatch.setattr(bc, "_parse_page", out_of_memory)
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG), pytest.raises(MemoryError):
        await bc.fetch_us_border_encounters(fetcher)


async def test_uk_out_of_memory_is_never_reported_as_a_bad_file(
        fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    def out_of_memory(*_args: object) -> None:
        raise MemoryError

    monkeypatch.setattr(bc, "read_ods_tables", out_of_memory)
    with _uk_mock(_main_ods()), pytest.raises(MemoryError):
        await bc.fetch_uk_small_boat_arrivals(fetcher)
    monkeypatch.setattr(bc, "json", types.SimpleNamespace(loads=out_of_memory))  # only the module's json
    with _uk_mock(_main_ods()), pytest.raises(MemoryError):
        await bc.fetch_uk_small_boat_arrivals(fetcher)


# ---------------------------------------------------------------------------
# UK: the Content API item and its attachment
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("href", [
    "https://gov.uk.example/media/x/time_series.ods",  # another host
    "http://assets.publishing.service.gov.uk/media/x/time_series.ods",  # not https
    "https://assets.publishing.service.gov.uk:8443/media/x/time_series.ods",  # another port
    "https://someone@assets.publishing.service.gov.uk/media/x/time_series.ods",  # credentials
    "https://assets.publishing.service.gov.uk:notaport/media/x/time_series.ods",  # not a URL
    "https://assets.publishing.service.gov.uk",  # no file
    "/",
    "",
    "   ",
    None,
])
async def test_uk_attachment_that_is_not_a_gov_uk_file_is_degraded_and_never_requested(
        fetcher: Fetcher, href: str | None) -> None:
    item = copy.deepcopy(_UK_ITEM)
    item["details"]["attachments"][2]["url"] = href  # altered
    with respx.mock(assert_all_called=False) as mock:
        mock.get(bc.UK_CONTENT_API).mock(return_value=httpx.Response(200, json=item))
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
        urls = [str(call.request.url) for call in mock.calls]
    assert urls == [bc.UK_CONTENT_API]
    assert result["degraded"] is True and result["reason"] == "uk_unexpected_format"
    assert "attachment" in result["error"] and "someone" not in result["error"]


@pytest.mark.parametrize("href", ["", "   ", None, 5])
def test_uk_attachment_without_a_url_says_so(href: object) -> None:
    item = copy.deepcopy(_UK_ITEM)
    item["details"]["attachments"][2]["url"] = href  # altered
    with pytest.raises(bc._SourceError, match="^the time-series attachment has no URL$"):
        bc._uk_attachment(item)


def test_uk_time_series_attachment_is_preferred_over_another_spreadsheet() -> None:
    item = copy.deepcopy(_UK_ITEM)
    item["details"]["attachments"].insert(0, {  # invented: another ODS listed first
        "attachment_type": "file", "content_type": bc._ODS_TYPE, "title": "Arrivals by nationality",
        "url": "https://assets.publishing.service.gov.uk/media/x/nationality.ods"})
    assert bc._uk_attachment(item)["url"] == _UK_ODS_URL
    assert bc._uk_attachment(_UK_ITEM)["url"] == _UK_ODS_URL  # the URL httpx requests, unchanged


@pytest.mark.parametrize("content, words", [
    (b"[" * 100_000 + b"]" * 100_000, "RecursionError"),  # JSON nested past the parser's depth
    (b'{"details": {"attachments": 5}}', "lists no ODS time-series attachment"),
    (None, "change_history is not a list (int)"),
])
async def test_uk_content_api_item_of_an_unexpected_shape_is_degraded(
        fetcher: Fetcher, content: bytes | None, words: str) -> None:
    if content is None:
        item = copy.deepcopy(_UK_ITEM)
        item["details"]["change_history"] = 5  # altered
        response = httpx.Response(200, json=item)
    else:
        response = httpx.Response(200, content=content, headers={"content-type": "application/json"})
    with respx.mock(assert_all_called=False) as mock:
        mock.get(bc.UK_CONTENT_API).mock(return_value=response)
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
        urls = [str(call.request.url) for call in mock.calls]
    assert urls == [bc.UK_CONTENT_API]
    assert result["degraded"] is True and result["reason"] == "uk_unexpected_format"
    assert words in result["error"]


# ---------------------------------------------------------------------------
# UK: year to date compares equal spans only
# ---------------------------------------------------------------------------

async def test_uk_year_to_date_says_how_many_days_it_covers(fetcher: Fetcher) -> None:
    with _uk_mock(_main_ods()):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    ytd = result["year_to_date"]
    assert (ytd["complete"], ytd["days_reported"], ytd["days_in_period"]) == (True, 288, 288)
    assert "every day of both spans has an SB_01 row" in result["date_semantics"]


async def test_uk_year_to_date_with_days_missing_this_year_has_no_comparison(fetcher: Fetcher) -> None:
    # The live rows with June 2026 taken out (30 days, 2,742 migrants): the review's case.
    daily = [line for line in _UK_DAILY.splitlines() if not line.startswith("2026-06-")]
    with _uk_mock(_ods(daily, _UK_WEEKLY.splitlines())):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    ytd = result["year_to_date"]
    assert (ytd["complete"], ytd["days_reported"], ytd["days_in_period"]) == (False, 258, 288)
    assert ytd["migrants_arrived"] == 18193  # the 258 days reported, and labelled so
    assert ytd["prior_year"] is None
    assert [ytd[key] for key in ("change_migrants", "pct_change_migrants", "change_boats", "pct_change_boats")] == [
        None] * 4
    assert ("SB_01 has no complete daily series for 2026-01-01 to 2026-10-15 (258 of 288 days), so year to date "
            "has no prior-year comparison.") in result["data_gaps"]


async def test_uk_year_to_date_with_days_missing_last_year_has_no_comparison(fetcher: Fetcher) -> None:
    # The live rows with June 2025 taken out: this year's span is whole, last year's is not.
    daily = [line for line in _UK_DAILY.splitlines() if not line.startswith("2025-06-")]
    with _uk_mock(_ods(daily, _UK_WEEKLY.splitlines())):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    ytd = result["year_to_date"]
    assert (ytd["complete"], ytd["days_reported"], ytd["days_in_period"]) == (True, 288, 288)
    assert ytd["migrants_arrived"] == 20935
    assert ytd["prior_year"] is None and ytd["change_migrants"] is None and ytd["pct_change_boats"] is None
    assert ("SB_01 has no complete daily series for 2025-01-01 to 2025-10-15 (258 of 288 days), so year to date "
            "has no prior-year comparison.") in result["data_gaps"]


# ---------------------------------------------------------------------------
# UK: the ODS reader
# ---------------------------------------------------------------------------

def _ods_raw(content: bytes, method: int = zipfile.ZIP_DEFLATED) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("mimetype", bc._ODS_TYPE)
        zf.writestr("content.xml", content, compress_type=method)
    return buf.getvalue()


def _document(tables_xml: str, prolog: str = '<?xml version="1.0" encoding="UTF-8"?>') -> str:
    return (f'{prolog}<office:document-content {_ODS_NS}><office:body><office:spreadsheet>{tables_xml}'
            '</office:spreadsheet></office:body></office:document-content>')


def _one_cell(paragraph: str, name: str = "T") -> str:
    return (f'<table:table table:name="{name}"><table:table-row><table:table-cell office:value-type="string">'
            f'<text:p>{paragraph}</text:p></table:table-cell></table:table-row></table:table>')


def _repeated(name: str, rows: int, columns: int) -> str:
    """One float cell repeated across columns and rows: the review's shape for a tiny file."""
    return (f'<table:table table:name="{name}"><table:table-row table:number-rows-repeated="{rows}">'
            f'<table:table-cell office:value-type="float" office:value="1" table:number-columns-repeated="{columns}"/>'
            '</table:table-row></table:table>')


def _main_tables(notes: str = _UK_NOTES_ROWS) -> str:
    """The three sheets of _main_ods(), as XML to combine with others."""
    return (f'<table:table table:name="Notes" table:style-name="ta1">{notes}{_BLANK_TAIL}</table:table>'
            + _sheet("SB_01", _DAILY_HEADER, _UK_DAILY.splitlines())
            + _sheet("SB_02", _WEEKLY_HEADER, _UK_WEEKLY.splitlines()))


def _replace_after(xml: str, marker: str, old: str, new: str) -> str:
    hit = xml.index(old, xml.index(marker))
    return xml[:hit] + new + xml[hit + len(old):]


def _patch_zip(data: bytes, member: str, patch) -> bytes:
    """Call patch(buffer, header offset, is_central) on each local and central header of member."""
    out = bytearray(data)
    for signature, name_length_at, name_at, central in ((b"PK\x03\x04", 26, 30, False), (b"PK\x01\x02", 28, 46, True)):
        pos = out.find(signature)
        while pos != -1:
            length = int.from_bytes(out[pos + name_length_at:pos + name_length_at + 2], "little")
            if out[pos + name_at:pos + name_at + length] == member.encode():
                patch(out, pos, central)
            pos = out.find(signature, pos + 4)
    return bytes(out)


_BAD_NUMBER = '<table:table-cell office:value-type="float" office:value="n/a"/>'  # invented


def test_ods_dtd_is_refused_in_any_encoding() -> None:
    # Invented: an internal entity whose text would land in a cell.
    for encoding in ("utf-8", "utf-16"):
        xml = _document(_one_cell("&inj;"), prolog=f'<?xml version="1.0" encoding="{encoding.upper()}"?>'
                        '<!DOCTYPE office:document-content [<!ENTITY inj "VALUE FROM AN INTERNAL ENTITY">]>')
        with pytest.raises(ValueError, match="declares a DTD"):
            bc.read_ods_tables(_ods_raw(xml.encode(encoding)))
    with pytest.raises(ValueError, match="declares a DTD"):
        bc.read_ods_tables(_ods_raw(_document(_one_cell("x"), prolog="<!DOCTYPE x>").encode()))
    # With no DTD an entity reference is simply not well-formed.
    with pytest.raises(ValueError, match="not well-formed"):
        bc.read_ods_tables(_ods_raw(_document(_one_cell("&inj;")).encode()))


def test_ods_inline_text_nested_deeply_is_read_without_recursion() -> None:
    deep = "<text:span>" * 5_000 + 'a<text:s text:c="2"/>b' + "</text:span>" * 5_000
    assert bc.read_ods_tables(_ods_bytes(_one_cell(deep))) == {"T": [["a  b"]]}
    too_deep = "<text:span>" * 10_000 + "x" + "</text:span>" * 10_000
    with pytest.raises(ValueError, match="nests elements more than 10,000 deep"):
        bc.read_ods_tables(_ods_bytes(_one_cell(too_deep)))


def test_ods_row_budget_is_one_for_the_whole_file() -> None:
    data = _ods_bytes(_repeated("SB_01", 150_000, 1) + _repeated("SB_02", 60_000, 1))  # each under 200,000
    with pytest.raises(ValueError, match="more than 200,000 rows"):
        bc.read_ods_tables(data)
    assert len(bc.read_ods_tables(data, ["SB_01"])["SB_01"]) == 150_000


def test_ods_cell_budget_is_one_for_the_whole_file() -> None:
    data = _ods_bytes(_repeated("SB_01", 3_000, 256) + _repeated("SB_02", 1_000, 256))  # 768,000 + 256,000
    with pytest.raises(ValueError, match="more than 1,000,000 cells"):
        bc.read_ods_tables(data)
    assert bc.read_ods_tables(data, ["SB_02"])["SB_02"][0] == [1.0] * 256


def test_ods_text_budget_counts_expanded_spaces_across_sheets() -> None:
    spaces = '<text:s text:c="1024"/>' * 2_560  # 2,621,440 spaces in each sheet, from 59 kB of markup
    data = _ods_bytes(_one_cell(spaces, "SB_01") + _one_cell(spaces, "SB_02"))
    with pytest.raises(ValueError, match="more than 4,194,304 characters of text"):
        bc.read_ods_tables(data)
    assert len(bc.read_ods_tables(data, ["SB_01"])["SB_01"][0][0]) == 2_621_440


def test_ods_names_are_capped_even_in_sheets_not_read() -> None:
    elements = "".join(f"<n{i}/>" for i in range(2_100))
    attributes = "<x " + " ".join(f'a{i}=""' for i in range(2_100)) + "/>"
    prefixes = "".join(f'<p{i}:x xmlns:p{i}="urn:example"/>' for i in range(2_100))
    for junk in (elements, attributes, prefixes):  # invented: 2,100 distinct names on a sheet not read
        data = _ods_bytes(f'<table:table table:name="Cover_sheet"><table:table-row><table:table-cell>{junk}'
                          '</table:table-cell></table:table-row></table:table>')
        with pytest.raises(ValueError, match="more than 2,000 distinct"):
            bc.read_ods_tables(data, ["SB_01"])


def test_ods_tag_or_text_run_longer_than_1_mib_is_refused_before_expat_holds_it() -> None:
    wide = "<x " + " ".join(f'a{i}=""' for i in range(120_000)) + "/>"  # one tag, 1.2 MB
    for junk in (wide, "x" * (1024 * 1024 + 1)):  # invented, on a sheet not read
        data = _ods_bytes(f'<table:table table:name="Cover_sheet"><table:table-row><table:table-cell>{junk}'
                          '</table:table-cell></table:table-row></table:table>')
        with pytest.raises(ValueError, match="holds a tag or text run longer than 1 MiB; refused"):
            bc.read_ods_tables(data, ["SB_01"])


def test_ods_member_compressed_with_bzip2_is_refused_unopened() -> None:
    data = _ods_raw(_document(_one_cell("x")).encode(), method=zipfile.ZIP_BZIP2)
    with pytest.raises(ValueError, match="content.xml is compressed with method 12, not stored or deflated"):
        bc.read_ods_tables(data)


def test_ods_content_xml_larger_than_16_mib_by_its_directory_entry_is_refused() -> None:
    for claimed in (16 * 1024 * 1024 + 1, 64 * 1024 * 1024 + 1):
        def claim(out: bytearray, pos: int, central: bool) -> None:
            if central:  # zipfile takes the size from the central directory
                out[pos + 24:pos + 28] = claimed.to_bytes(4, "little")

        data = _patch_zip(_ods_bytes(_one_cell("x")), "content.xml", claim)  # altered: the real member is tiny
        with pytest.raises(ValueError, match="larger than 16 MiB"):
            bc.read_ods_tables(data)


def test_ods_encrypted_member_is_refused_as_unreadable() -> None:
    def encrypt(out: bytearray, pos: int, central: bool) -> None:
        out[pos + (8 if central else 6)] |= 1  # the "encrypted" flag bit

    with pytest.raises(ValueError, match="ODS archive not readable: .*encrypted"):
        bc.read_ods_tables(_patch_zip(_main_ods(), "content.xml", encrypt))


def test_ods_two_sheets_of_one_name_are_refused_when_read() -> None:
    data = _ods_bytes(_one_cell("a", "SB_01") + _one_cell("b", "SB_01") + _one_cell("c", "Notes"))
    with pytest.raises(ValueError, match="two sheets named 'SB_01'"):
        bc.read_ods_tables(data)
    assert bc.read_ods_tables(data, ["Notes"]) == {"Notes": [["c"]]}


def test_ods_unreadable_values_and_repeats_keep_their_place() -> None:
    row = ('<table:table-row><table:table-cell office:value-type="float" office:value="7"/>' + _BAD_NUMBER
           + '<table:table-cell office:value-type="date" office:date-value="15/10/2026"/>'
           + '<table:table-cell office:value-type="float" office:value="8" table:number-columns-repeated="two"/>'
           + '<table:table-cell office:value-type="float" office:value="9"/></table:table-row>'
           + '<table:table-row table:number-rows-repeated="many">'
           + '<table:table-cell office:value-type="float" office:value="1"/></table:table-row>')
    tables = bc.read_ods_tables(_ods_bytes(f'<table:table table:name="T">{row}</table:table>'))
    assert tables == {"T": [
        [7.0, bc.UnreadableCell("unreadable number 'n/a'"), bc.UnreadableCell("unreadable date '15/10/2026'"),
         bc.UnreadableCell("unreadable table:number-columns-repeated 'two'")],  # where 9 sits is unknown
        None,  # how many rows it stands for is unknown
    ]}
    assert len({bc.UnreadableCell("x"), bc.UnreadableCell("x"), bc.UnreadableCell("y")}) == 2


def test_ods_cells_of_every_kind_read_as_their_values() -> None:
    cells = (
        '<table:table-cell office:value-type="boolean" office:boolean-value="true"/>'
        '<table:table-cell office:value-type="time" office:time-value="PT12H30M00S"/>'
        '<table:table-cell office:value-type="string" office:string-value="from an attribute">'
        '<text:p>shown</text:p></table:table-cell>'
        '<table:table-cell office:value-type="date" office:date-value="2026-10-15T08:30:00"/>'
        '<table:table-cell office:value-type="percentage" office:value="0.5"><text:p>50%</text:p></table:table-cell>'
        '<table:table-cell><text:p>a<text:tab/>b<text:line-break/>c<text:s text:c="x"/>d</text:p>'
        '<text:p>second</text:p></table:table-cell>'
        '<table:table-cell><office:annotation><text:p>a comment</text:p></office:annotation>'
        '<text:p>kept<text:note><text:note-body><text:p>a note</text:p></text:note-body></text:note></text:p>'
        '</table:table-cell>'
        '<table:covered-table-cell/>'
        '<table:table-cell><table:table table:name="inner"><table:table-row><table:table-cell><text:p>inner</text:p>'
        '</table:table-cell></table:table-row></table:table><text:p>outer</text:p></table:table-cell>'
    )
    sheet = (f'<table:table table:name="T"><table:table table:name="nested"/><table:table-row>{cells}'
             '</table:table-row></table:table>')
    assert bc.read_ods_tables(_ods_bytes(sheet)) == {"T": [[
        True, "PT12H30M00S", "from an attribute", datetime(2026, 10, 15, 8, 30), 0.5, "a\tb\nc d\nsecond", "kept",
        None, "outer"]]}


def test_ods_columns_past_the_256th_are_not_read() -> None:
    def floats(n: int) -> str:
        return "".join(f'<table:table-cell office:value-type="float" office:value="{i}"/>' for i in range(n))

    sheet = (f'<table:table table:name="T"><table:table-row>{floats(300)}</table:table-row>'
             f'<table:table-row>{floats(255)}<table:table-cell table:number-columns-repeated="10"/>'
             '<table:table-cell office:value-type="float" office:value="999"/></table:table-row>'
             f'<table:table-row>{floats(250)}<table:table-cell office:value-type="float" office:value="7" '
             'table:number-columns-repeated="100000"/></table:table-row></table:table>')
    assert bc.read_ods_tables(_ods_bytes(sheet))["T"] == [[float(i) for i in range(256)],
                                                         [float(i) for i in range(255)],
                                                         [float(i) for i in range(250)] + [7.0] * 6]


def test_ods_archive_that_is_not_a_spreadsheet_is_refused() -> None:
    with pytest.raises(ValueError, match="not a zip archive"):
        bc.read_ods_tables(b"PK not really a zip")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("mimetype", "application/vnd.oasis.opendocument.text")
        zf.writestr("content.xml", _document(_one_cell("x")))
    with pytest.raises(ValueError, match="not an ODS spreadsheet"):
        bc.read_ods_tables(buf.getvalue())
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("mimetype", bc._ODS_TYPE)
    with pytest.raises(ValueError, match="no content.xml"):
        bc.read_ods_tables(buf.getvalue())
    with pytest.raises(ValueError, match="ODS file has no sheets"):
        bc.read_ods_tables(_ods_raw(_document("").encode()))
    with pytest.raises(ValueError, match="not well-formed"):
        bc.read_ods_tables(_ods_raw(b"<office:document-content"))


# ---------------------------------------------------------------------------
# UK: what the tool reads, and what it names
# ---------------------------------------------------------------------------

async def test_uk_tiny_file_that_would_expand_past_the_budget_is_degraded(fetcher: Fetcher) -> None:
    # The review's file: four sheets of 199,999 x 256 repeated cells, 508 bytes as it built it.
    ods = _ods_bytes("".join(_repeated(f"S{i}", 199_999, 256) for i in range(4)))
    assert len(ods) < 1_000
    with _uk_mock(ods):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["degraded"] is True and result["reason"] == "uk_unexpected_format"
    assert "more than 1,000,000 cells" in result["error"]
    assert result["data_gaps"] == [f"No figures: {result['error']}."]


async def test_uk_sheets_the_tool_does_not_use_are_never_read(fetcher: Fetcher) -> None:
    # Invented cover sheet: an unreadable number, and a row repeated past 200,000.
    cover = ('<table:table table:name="Cover_sheet"><table:table-row>' + _BAD_NUMBER + '</table:table-row>'
             '<table:table-row table:number-rows-repeated="200001"><table:table-cell office:value-type="float" '
             'office:value="1" table:number-columns-repeated="256"/></table:table-row></table:table>')
    with _uk_mock(_ods_bytes(cover + _main_tables())):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert "degraded" not in result and result["data_gaps"] == []
    assert result["latest_week"]["migrants_arrived"] == 1066


async def test_uk_daily_table_under_another_sheet_name_is_still_found(fetcher: Fetcher) -> None:
    renamed = _main_tables().replace('table:name="SB_01"', 'table:name="Daily arrivals"')  # invented name
    with _uk_mock(_ods_bytes(renamed)):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["latest_day"]["date"] == "2026-10-15" and result["data_gaps"] == []


async def test_uk_unreadable_row_above_the_header_is_named(fetcher: Fetcher) -> None:
    # Altered: a title row above SB_01's header, with a repeat count that is not a number.
    xml = _replace_after(_main_tables(), 'table:name="SB_01"', '<table:table-row table:style-name="ro10">',
                         '<table:table-row table:number-rows-repeated="x"><table:table-cell office:value-type="string">'
                         '<text:p>Title</text:p></table:table-cell></table:table-row>'
                         '<table:table-row table:style-name="ro10">')
    with _uk_mock(_ods_bytes(xml)):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["latest_day"]["date"] == "2026-10-15" and result["year_to_date"]["complete"] is True
    assert result["data_gaps"] == ["1 row(s) of SB_01 could not be read (a repeat count that is not a number); any "
                                   "figures in them are not counted."]


async def test_uk_unreadable_cell_in_a_figure_column_is_null_and_named(fetcher: Fetcher) -> None:
    # Altered: the migrants cell of 15 October 2026 (0 in the file) has an empty value.
    xml = _replace_after(_main_tables(), 'office:date-value="2026-10-15T00:00:00"', 'office:value="0"',
                         'office:value=""')
    with _uk_mock(_ods_bytes(xml)):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["data_gaps"] == ["1 cell(s) of SB_01 are neither a count nor '-' and are read as null."]
    assert result["latest_day"]["migrants_arrived"] is None and result["latest_day"]["boats_arrived"] == 0
    assert result["last_7_days"]["migrants_arrived"] is None
    assert result["year_to_date"]["migrants_arrived"] is None and result["year_to_date"]["change_boats"] == -288


async def test_uk_negative_count_is_null_and_named(fetcher: Fetcher) -> None:
    # Altered: -5 migrants on 15 October 2026.
    xml = _replace_after(_main_tables(), 'office:date-value="2026-10-15T00:00:00"', 'office:value="0"',
                         'office:value="-5"')
    with _uk_mock(_ods_bytes(xml)):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["latest_day"]["migrants_arrived"] is None
    assert result["data_gaps"] == ["1 cell(s) of SB_01 are neither a count nor '-' and are read as null."]


async def test_uk_unreadable_cells_outside_the_figures_are_named_not_fatal(fetcher: Fetcher) -> None:
    padding = '<table:table-cell table:number-columns-repeated="16380"'
    # Altered: unreadable numbers in SB_01 past the header's last column, in the Notes column on
    # 13 October 2026 and two columns past the figures on 14 October 2026; one in the second
    # column of note 7; and a Notes row whose only cell is unreadable.
    header_padding = '<table:table-cell table:number-columns-repeated="16379"'
    xml = _replace_after(_main_tables(), 'table:name="SB_01"', header_padding,
                         _BAD_NUMBER + '<table:table-cell table:number-columns-repeated="16378"')
    xml = _replace_after(xml, 'office:date-value="2026-10-13T00:00:00"', padding, _BAD_NUMBER + padding)
    xml = _replace_after(xml, 'office:date-value="2026-10-14T00:00:00"', padding,
                         "<table:table-cell/>" + _BAD_NUMBER + padding)
    xml = _replace_after(xml, "7. Data on preventions", '<table:table-cell table:number-columns-repeated="9"',
                         _BAD_NUMBER + '<table:table-cell table:number-columns-repeated="9"')
    xml = xml.replace(_BLANK_TAIL, "<table:table-row>" + _BAD_NUMBER + "</table:table-row>" + _BLANK_TAIL, 1)
    with _uk_mock(_ods_bytes(xml)):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert "degraded" not in result and result["latest_week"]["migrants_arrived"] == 1066
    assert len(result["publisher_caveats"]) == 3
    assert result["data_gaps"] == [
        "3 cell(s) of SB_01 outside the figures read could not be read; they are not used.",
        "1 row(s) of the Notes sheet could not be read; any Home Office caveats in them are missing.",
        "1 cell(s) of the Notes sheet outside its first column could not be read; they are not used.",
    ]


async def test_uk_daily_row_with_an_unreadable_repeat_count_is_named(fetcher: Fetcher) -> None:
    xml = _main_tables()
    marker = xml.index('office:date-value="2026-10-14T00:00:00"')
    start = xml.rindex('<table:table-row table:style-name="ro16">', 0, marker)
    xml = (xml[:start] + '<table:table-row table:style-name="ro16" table:number-rows-repeated="x">'  # altered
           + xml[start + len('<table:table-row table:style-name="ro16">'):])
    with _uk_mock(_ods_bytes(xml)):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert ("1 row(s) of SB_01 could not be read (a repeat count that is not a number); any figures in them are "
            "not counted.") in result["data_gaps"]
    assert result["year_to_date"]["complete"] is False and result["year_to_date"]["prior_year"] is None
    assert result["last_7_days"]["days_reported"] == 6


async def test_uk_deeply_nested_note_is_read_and_absurd_nesting_is_degraded(fetcher: Fetcher) -> None:
    note = "7. Data on preventions are operational estimates collected and provided to the UK by French authorities."

    def nested(depth: int) -> str:
        return _UK_NOTES_ROWS.replace(f"<text:p>{note}<text:s/></text:p>",
                                      f"<text:p>{'<text:span>' * depth}{note}{'</text:span>' * depth}</text:p>")

    with _uk_mock(_ods_bytes(_main_tables(notes=nested(1_100)))):  # past where a recursive walk failed
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["publisher_caveats"][2] == note
    fetcher.cache.delete(bc.UK_CACHE_KEY)
    with _uk_mock(_ods_bytes(_main_tables(notes=nested(10_000)))):
        refused = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert refused["degraded"] is True and refused["reason"] == "uk_unexpected_format"
    assert "10,000 deep" in refused["error"]


async def test_uk_encrypted_time_series_is_degraded_with_a_reason(fetcher: Fetcher) -> None:
    def encrypt(out: bytearray, pos: int, central: bool) -> None:
        out[pos + (8 if central else 6)] |= 1

    with _uk_mock(_patch_zip(_main_ods(), "content.xml", encrypt)):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["degraded"] is True and result["reason"] == "uk_unexpected_format"
    assert "encrypted" in result["error"]
