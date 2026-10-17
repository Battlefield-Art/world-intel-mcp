"""Border crossings: monthly U.S. CBP encounters and daily UK small-boat arrivals.

Crossings are FLOWS (events per day, week or month). The UNHCR and World Bank
tools in ``sources/migration.py`` report year-end STOCKS of people. They are
different measures, and nothing here adds, subtracts or compares the two.

Publishers, as measured on 2026-10-17 (HTTP status, robots.txt, terms):

* U.S. Customs and Border Protection: the "Nationwide Encounters" dataset page
  https://www.cbp.gov/document/stats/nationwide-encounters (200) lists one CSV
  per monthly update under /sites/default/files/YYYY-MM/. The newest (FY2023 to
  FY2026, through August 2026) is 6.7 MB and 52,986 rows, one per fiscal month,
  component, land border region, area of responsibility, demographic,
  citizenship and encounter type. https://www.cbp.gov/robots.txt (200)
  disallows /sites/default/files/assets/documents/, where the older FY2020 to
  FY2023 files and the data dictionary live, so nothing there is requested.
  CBP's copyright notice: site information is in the public domain unless a
  copyright is indicated, and CBP asks to be cited as the source.
* UK Home Office: "Small boat activity in the English Channel" on GOV.UK, read
  through the GOV.UK Content API (200, keyless, at most 10 requests per second
  per client). The time series is published only as an ODS spreadsheet, on
  assets.publishing.service.gov.uk (robots.txt 404). ODS is a zip of XML, read
  here with zipfile and ElementTree, so no new dependency. GOV.UK content is
  under the Open Government Licence v3.0.
* Frontex is not built. Its monthly detections file is XLSX only and sits under
  /assets/, which https://www.frontex.europa.eu/robots.txt (200) disallows for
  every user agent. Nothing under /assets/ is requested.

A refresh makes two requests (the listing, then the file) with an identifying
User-Agent, and the parsed dataset is cached (CBP 12 h, UK 3 h). A failed
refresh serves the last good copy marked ``stale`` with its age and the
failure in ``data_gaps``; with no copy the tool returns a degraded error that
carries no figures, so an outage never reads as zero.

Gaps / not covered:

* Frontex (robots.txt disallows its only data file, which is XLSX).
* CBP: only the newest file is read, so the series covers its four fiscal
  years (from October 2022 in the file read on 2026-10-17); older years sit
  under the disallowed path. Area-of-responsibility (sector and field
  office) and by-state detail are not exposed. Figures are provisional
  until CBP's fiscal-year close and change between monthly files. CBP's
  caveats are quoted as captured on 2026-10-17: the two on the dataset page
  are re-checked on every refresh, the ones on its encounters page are not
  re-read.
* UK: the Home Office figures are provisional operational data, superseded by
  the quarterly "Illegal entry routes to the UK" statistics, which are not read
  here. Month, year and year-to-date figures are this module's sums of the
  daily rows, not Home Office figures. The Notes column is read only for
  "Revised". The last-7-days and weekly-summary HTML pages are not read (on
  2026-10-17 their figures matched the ODS).
* robots.txt is not re-read at run time; the CBP path rule is pinned as read on
  2026-10-17.
* Neither file has a person identifier, so neither series counts unique
  people; the responses say so as a tool note, not in the publishers' words.
"""

from __future__ import annotations

import asyncio
import calendar
import csv
import io
import json
import logging
import re
import time
import weakref
import xml.etree.ElementTree as ET
import zipfile
import zlib
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
from typing import Any, Awaitable, Callable
from urllib.parse import urljoin, urlsplit

from ..fetcher import BinaryResult, Fetcher

logger = logging.getLogger("world-intel-mcp.sources.border_crossings")

USER_AGENT = "world-intel-mcp (+https://github.com/marc-shade/world-intel-mcp)"
_HEADERS = {"User-Agent": USER_AGENT}
_MONTHS = ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")

_FLOW_NOT_STOCK = (
    "Flow, not stock: these are events per period. The UNHCR and World Bank migration tools report "
    "year-end stocks of people; the two measures do not add, subtract or compare."
)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _usage(source: str, error: str) -> dict[str, Any]:
    return {"error": error, "source": source, "timestamp": _now()}


def _degraded(source: str, error: str, reason: str) -> dict[str, Any]:
    return {"error": error, "degraded": True, "reason": reason, "source": source, "timestamp": _now()}


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _why(result: BinaryResult) -> str:
    if result.error:
        return result.error
    return f"HTTP {result.status}" if result.status is not None else "no response"


def _pct(current: int | None, prior: int | None) -> float | None:
    if current is None or not prior:
        return None
    return round(100 * (current - prior) / prior, 1)


def _diff(current: int | None, prior: int | None) -> int | None:
    return None if current is None or prior is None else current - prior


class _SourceError(Exception):
    """A refresh failed; ``reason`` is the degraded-response reason code."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


_refresh_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[str, asyncio.Lock]] = (
    weakref.WeakKeyDictionary()
)


def _refresh_lock(key: str) -> asyncio.Lock:
    """One refresh per dataset at a time (per event loop), so concurrent cold
    calls download a file once instead of once each."""
    locks = _refresh_locks.setdefault(asyncio.get_running_loop(), {})
    if key not in locks:
        locks[key] = asyncio.Lock()
    return locks[key]


def _valid(value: Any, schema: int) -> dict[str, Any] | None:
    return value if isinstance(value, dict) and value.get("schema") == schema else None


async def _load(
    fetcher: Fetcher,
    key: str,
    ttl: int,
    schema: int,
    fetch: Callable[[Fetcher], Awaitable[dict[str, Any]]],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """The parsed dataset and its freshness: cached, refreshed, or the last good copy marked stale.

    Returns ``(None, {"error", "reason"})`` when the refresh fails and no copy exists.
    """
    cached = _valid(fetcher.cache.get(key), schema)
    if cached is not None:
        return cached, {"stale": False}
    async with _refresh_lock(key):
        cached = _valid(fetcher.cache.get(key), schema)
        if cached is not None:
            return cached, {"stale": False}
        try:
            dataset = await fetch(fetcher)
        except _SourceError as exc:
            logger.warning("Refresh of %s failed: %s", key, exc.message)
            meta = fetcher.cache.get_stale_meta(key)
            value = _valid(meta[0], schema) if meta else None
            if value is None:
                return None, {"error": exc.message, "reason": exc.reason}
            age = round(max(0.0, time.time() - meta[1]), 1)
            return value, {"stale": True, "stale_age_seconds": age, "refresh_error": exc.message}
        fetcher.cache.set(key, dataset, ttl_seconds=ttl)
        return dataset, {"stale": False}


def _freshness(result: dict[str, Any], state: dict[str, Any], gaps: list[str], fetched_at: str) -> None:
    result["stale"] = state["stale"]
    if state["stale"]:
        result["stale_age_seconds"] = state["stale_age_seconds"]
        gaps.append(f"Refresh failed ({state['refresh_error']}); this is the copy fetched at {fetched_at}.")


# ---------------------------------------------------------------------------
# U.S. Customs and Border Protection: nationwide encounters
# ---------------------------------------------------------------------------

CBP_SOURCE = "cbp"
CBP_DATASET_PAGE = "https://www.cbp.gov/document/stats/nationwide-encounters"
CBP_STATS_PAGE = "https://www.cbp.gov/newsroom/stats/nationwide-encounters"
CBP_CACHE_KEY = "border_crossings:cbp_encounters"
_CBP_SCHEMA = 1
_CBP_TTL = 43200  # 12 h; CBP posts one file a month
_CBP_PAGE_MAX_BYTES = 8 * 1024 * 1024
_CBP_CSV_MAX_BYTES = 64 * 1024 * 1024  # the August 2026 file is 6.7 MB
_CBP_HOST = "www.cbp.gov"
# https://www.cbp.gov/robots.txt, read 2026-10-17: "#Disallow the files/assets/documents directory".
_CBP_DISALLOWED_PREFIXES = ("/sites/default/files/assets/documents/",)
_CBP_FILE_NAME = re.compile(
    r"/nationwide-encounters-fy(?P<fy_from>\d{2})-fy(?P<fy_to>\d{2})(?:-(?P<month>[a-z]{3}))?-aor(?:_(?P<dup>\d+))?\.csv$"
)
_CBP_FILE_FOLDER = re.compile(r"^/sites/default/files/(\d{4}-\d{2})/[^/]+$")
_LAST_MODIFIED = re.compile(r"Last Modified:\s*([A-Za-z]{3})[A-Za-z]*\.?\s+(\d{1,2}),\s*(\d{4})")

CBP_REGIONS = {"southwest": "Southwest Land Border", "northern": "Northern Land Border", "other": "Other"}
CBP_COMPONENTS = {"usbp": "U.S. Border Patrol", "ofo": "Office of Field Operations"}
_CBP_OTHER = "OTHER"
_CBP_COLUMNS = {
    "fiscal_year": "Fiscal Year",
    "month": "Month (abbv)",
    "component": "Component",
    "region": "Land Border Region",
    "demographic": "Demographic",
    "citizenship": "Citizenship",
    "encounter_type": "Encounter Type",
    "count": "Encounter Count",
}
# Order of the dimensions in a compact cell: [period, region, component, type, demographic, citizenship, count].
_CBP_DIMS = ("region", "component", "encounter_type", "demographic", "citizenship")
_REGION, _COMPONENT, _TYPE, _DEMOGRAPHIC, _CITIZENSHIP, _COUNT = 1, 2, 3, 4, 5, 6

# Quoted from the dataset page (CBP_DATASET_PAGE), captured 2026-10-17 and
# re-checked against the live page on every refresh.
_CBP_PAGE_CAVEATS = (
    "Encounter data includes U.S. Border Patrol Title 8 apprehensions, Office of Field Operations Title 8 "
    "inadmissibles, and all Title 42 expulsions for fiscal years 2020 to date. Data is available for the "
    "Northern Land Border, Southwest Land Border, and Nationwide (i.e., air, land, and sea modes of "
    "transportation) encounters.",
    "Data is extracted from live CBP systems and data sources. Statistical information is subject to change "
    "due to corrections, systems changes, change in data definition, additional information, or encounters "
    "pending final review. Final statistics are available at the conclusion of each fiscal year.",
)
# Quoted from CBP's encounters page (CBP_STATS_PAGE), captured 2026-10-17; not re-read at run time.
_CBP_STATS_CAVEATS = (
    "Note: Nationwide encounters are the sum of CBP encounters across all areas of responsibility including "
    "Northern Land Border, Southwest Land Border, OFO non-land border ports of entry (e.g., airports, "
    "seaports), and USBP sectors that do not share a land border with Canada or Mexico (e.g., Miami Sector).",
    "To ensure that law enforcement sensitive statistics are not publicly released, modifications to data "
    "filters have been made that may alter the format of data previously obtained from this dashboard.",
)
_CBP_DEFINITIONS = {
    "encounter_types": {
        "Apprehensions": (
            "Apprehensions refers to the physical control or temporary detainment of a person by USBP between "
            "POEs who is not lawfully in the U.S. which may or may not result in an arrest."
        ),
        "Inadmissibles": (
            "Inadmissibles refers to individuals encountered at ports of entry (POEs) by OFO who are seeking "
            "lawful admission into the United States (U.S.) but are determined to be inadmissible, individuals "
            "presenting themselves to seek humanitarian protection under our laws, and individuals who withdraw "
            "an application for admission and return to their countries of origin within a short timeframe."
        ),
        "Expulsions": (
            "Title 42 Expulsions refers to individuals encountered by USBP and OFO and expelled to the country "
            "of last transit or home country in the interest of public health under Title 42 U.S.C. 265 from "
            "March 21, 2020 to May 11, 2023."
        ),
    },
    "demographics": {
        "Accompanied Minors": "Accompanied Minors (AM)",
        "FMUA": "Individuals in a Family Unit (FMUA)",
        "Single Adults": "Single Adults",
        "UAC": "Unaccompanied Alien Children (UAC) / Single Minors",
    },
    "source": f"Quoted from {CBP_STATS_PAGE} as captured 2026-10-17.",
}
_CBP_MEASURE = (
    "CBP encounters per calendar month: U.S. Border Patrol (USBP) Title 8 apprehensions between ports of "
    "entry, Office of Field Operations (OFO) Title 8 inadmissibles at ports of entry, and Title 42 "
    "expulsions (21 March 2020 to 11 May 2023)."
)
_CBP_UNIT = "encounters (enforcement events) per calendar month"
_CBP_DATE_SEMANTICS = (
    "Each figure counts the encounters in one calendar month. CBP labels months by fiscal year (1 October to "
    "30 September), so October to December belong to the next fiscal year: FY2026 October is October 2025. "
    "'(FYTD)' is CBP's label for the fiscal year in progress. Figures are as of the file's posting "
    "(dataset.file_date) and can change in later files until CBP's fiscal-year close."
)
_CBP_NOTES = (
    _FLOW_NOT_STOCK,
    "Encounters are events, not unique people: CBP's file has no person identifier, so one person "
    "encountered twice is counted twice. (Tool note from the file's structure; the CBP pages read on "
    "2026-10-17 do not say it in these words.)",
    "CBP's file lists only non-zero cells (no count of 0 appears and counts of 1 are published), so a month "
    "in the file with no row for this selection is reported as 0. Months the file does not contain are never "
    "listed or filled in (see dataset.coverage).",
    "OTHER is CBP's bucket for every citizenship it does not name; it is reported as "
    "other_citizenships_latest_month and never ranked as a country.",
    "Region 'other' is CBP's 'Other' land border region: OFO ports of entry away from the land borders (CBP's "
    "nationwide note gives airports and seaports as examples; the file also has a Preclearance Field Office) "
    "and the Miami, New Orleans and Ramey Border Patrol sectors.",
)
_CBP_TERMS = (
    "Public domain unless a copyright is indicated; CBP asks to be cited as the source "
    "(https://www.cbp.gov/site-policy-notices/copyright-notice)."
)


def cbp_period(fiscal_year: int, month_abbv: str) -> str:
    """Calendar month (YYYY-MM) of a CBP fiscal-year month: October to December fall in fiscal_year - 1."""
    month = str(month_abbv).strip().upper()
    if month not in _MONTHS:
        raise ValueError(f"unknown CBP month {month_abbv!r}")
    number = _MONTHS.index(month) + 1
    return f"{fiscal_year - 1 if number >= 10 else fiscal_year:04d}-{number:02d}"


def _fiscal_year(period: str) -> int:
    year, month = int(period[:4]), int(period[5:7])
    return year + 1 if month >= 10 else year


def _month_bounds(period: str) -> tuple[str, str]:
    year, month = int(period[:4]), int(period[5:7])
    return f"{period}-01", f"{period}-{calendar.monthrange(year, month)[1]:02d}"


def _shift_months(period: str, delta: int) -> str:
    index = int(period[:4]) * 12 + int(period[5:7]) - 1 + delta
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def parse_cbp_csv(text: str) -> dict[str, Any]:
    """Aggregate CBP's encounters CSV into compact cells, summing out the area of responsibility.

    Raises ValueError when the header lacks a needed column or no data row is readable.
    """
    reader = csv.reader(io.StringIO(text.lstrip("\ufeff")))
    names = [name.strip() for name in next(reader, None) or []]
    missing = [column for column in _CBP_COLUMNS.values() if column not in names]
    if missing:
        raise ValueError(f"CBP file lacks the columns {', '.join(missing)}")
    index = {key: names.index(column) for key, column in _CBP_COLUMNS.items()}
    width = max(index.values()) + 1
    vocab: dict[str, dict[str, int]] = {dim: {} for dim in _CBP_DIMS}
    totals: dict[tuple, int] = defaultdict(int)
    fiscal_year_labels: dict[str, str] = {}
    rows = unreadable = 0
    for row in reader:
        if not any(cell.strip() for cell in row):
            continue
        rows += 1
        try:
            if len(row) < width:
                raise ValueError("short row")
            label = row[index["fiscal_year"]].strip()
            match = re.match(r"(\d{4})\b", label)
            if match is None:
                raise ValueError(f"fiscal year {label!r}")
            fiscal_year = int(match.group(1))
            period = cbp_period(fiscal_year, row[index["month"]])
            count = int(row[index["count"]].strip().replace(",", ""))
            if count < 0:
                raise ValueError(f"negative count {count}")
            values = [" ".join(row[index[dim]].split()) for dim in _CBP_DIMS]
            if not all(values):
                raise ValueError("empty dimension")
        except ValueError:
            unreadable += 1
            continue
        keys = tuple(vocab[dim].setdefault(value, len(vocab[dim])) for dim, value in zip(_CBP_DIMS, values))
        fiscal_year_labels.setdefault(str(fiscal_year), label)
        totals[(period, *keys)] += count
    if rows == 0:
        raise ValueError("CBP file has no data rows")
    if not totals:
        raise ValueError(f"none of the CBP file's {rows} data rows could be read")
    periods = sorted({key[0] for key in totals})
    position = {period: i for i, period in enumerate(periods)}
    return {
        "schema": _CBP_SCHEMA,
        "periods": periods,
        "fiscal_year_labels": fiscal_year_labels,
        "dims": {dim: list(vocab[dim]) for dim in _CBP_DIMS},
        "cells": [[position[key[0]], *key[1:], count] for key, count in totals.items()],
        "rows": rows,
        "unreadable_rows": unreadable,
    }


class _PageParser(HTMLParser):
    """Links, table rows (their links and data-sort-value cells) and visible text of an HTML page."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.anchors: list[tuple[str, str]] = []
        self.rows: list[tuple[list[str], list[str]]] = []
        self.chunks: list[str] = []
        self._anchor: list[str] | None = None
        self._href = ""
        self._row: tuple[list[str], list[str]] | None = None
        self._hidden = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = {name: value or "" for name, value in attrs}
        if tag in ("script", "style", "noscript"):
            self._hidden += 1
        elif tag == "a" and attr.get("href"):
            self._href, self._anchor = attr["href"].strip(), []
            if self._row is not None:
                self._row[0].append(self._href)
        elif tag == "tr":
            if self._row is not None:
                self.rows.append(self._row)
            self._row = ([], [])
        elif tag == "td" and self._row is not None and "data-sort-value" in attr:
            self._row[1].append(attr["data-sort-value"].strip())

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style", "noscript"):
            self._hidden = max(0, self._hidden - 1)
        elif tag == "a" and self._anchor is not None:
            self.anchors.append((self._href, " ".join("".join(self._anchor).split())))
            self._anchor = None
        elif tag == "tr" and self._row is not None:
            self.rows.append(self._row)
            self._row = None

    def handle_data(self, data: str) -> None:
        if self._hidden:
            return
        if self._anchor is not None:
            self._anchor.append(data)
        self.chunks.append(data)


def _parse_page(html: str) -> _PageParser:
    parser = _PageParser()
    parser.feed(html)
    parser.close()
    return parser


def _page_text(page: _PageParser) -> str:
    return " ".join("".join(page.chunks).split())


def _cbp_link(href: str) -> tuple[str, str, bool] | None:
    """(absolute URL, path, robots.txt allows it) of a link on www.cbp.gov, else None."""
    url = urljoin(CBP_DATASET_PAGE, href).split("#", 1)[0]
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.hostname != _CBP_HOST:
        return None
    allowed = not any(parts.path.startswith(prefix) for prefix in _CBP_DISALLOWED_PREFIXES)
    return url, parts.path, allowed


def _us_date(value: str) -> str | None:
    try:
        return datetime.strptime(value, "%m/%d/%Y").date().isoformat()
    except ValueError:
        return None


def _latest_file(page: _PageParser) -> dict[str, Any] | None:
    best: tuple[tuple, str, str, str | None] | None = None
    newest_disallowed: tuple[tuple, str] | None = None
    for href, text in page.anchors:
        link = _cbp_link(href)
        if link is None:
            continue
        url, path, allowed = link
        name = _CBP_FILE_NAME.search(path)
        month = (name["month"] or "").upper() if name else ""
        if name is None or (month and month not in _MONTHS):
            continue
        folder = _CBP_FILE_FOLDER.match(path)
        rank = (
            int(name["fy_to"]),
            (_MONTHS.index(month) - 9) % 12 + 1 if month else 12,  # through which fiscal month: OCT=1 .. SEP=12
            folder.group(1) if folder else "",
            int(name["dup"] or -1),
        )
        if not allowed:
            if newest_disallowed is None or rank > newest_disallowed[0]:
                newest_disallowed = (rank, url)
        elif best is None or rank > best[0]:
            best = (rank, url, path, text or None)
    if best is None:
        return None
    rank, url, path, title = best
    file_date = size = None
    for hrefs, sort_values in page.rows:
        if path not in {link[1] for link in map(_cbp_link, hrefs) if link}:
            continue
        for value in sort_values:
            if re.fullmatch(r"\d{2}/\d{2}/\d{4}", value):
                file_date = _us_date(value)
            elif value.isdigit():
                size = int(value)
        break
    newer = newest_disallowed[1] if newest_disallowed and newest_disallowed[0] > rank else None
    return {"url": url, "title": title, "file_date": file_date, "size_bytes": size, "newer_disallowed_url": newer}


def latest_cbp_file(html: str) -> dict[str, Any] | None:
    """The newest nationwide encounters CSV (by area of responsibility) on CBP's dataset page.

    Ranked by its last fiscal year, then the fiscal month it runs through (from the
    file name), then its upload folder. A link under a path robots.txt disallows, or off
    www.cbp.gov, is never chosen; when such a link looks newer than the one chosen, its
    URL is returned as ``newer_disallowed_url`` so the response can say so. ``file_date``
    and ``size_bytes`` come from the page's file table (its Date and Size columns).
    """
    return _latest_file(_parse_page(html))


def _last_modified(text: str) -> str | None:
    match = _LAST_MODIFIED.search(text)
    if match is None or match.group(1).upper() not in _MONTHS:
        return None
    try:
        return date(int(match.group(3)), _MONTHS.index(match.group(1).upper()) + 1, int(match.group(2))).isoformat()
    except ValueError:
        return None


def cbp_page_last_modified(html: str) -> str | None:
    """ISO date of the page's "Last Modified: Sep 16, 2026" line, or None."""
    return _last_modified(_page_text(_parse_page(html)))


async def _fetch_cbp_dataset(fetcher: Fetcher) -> dict[str, Any]:
    page_result = await fetcher.get_bytes(
        CBP_DATASET_PAGE, CBP_SOURCE, headers=_HEADERS, max_bytes=_CBP_PAGE_MAX_BYTES
    )
    if page_result.content is None:
        raise _SourceError("cbp_fetch_failed", f"CBP dataset page unavailable: {_why(page_result)}")
    page = _parse_page(page_result.content.decode("utf-8", errors="replace"))
    latest = _latest_file(page)
    if latest is None:
        raise _SourceError(
            "cbp_unexpected_format",
            "CBP's dataset page lists no nationwide encounters CSV by area of responsibility at a path "
            "robots.txt allows",
        )
    csv_result = await fetcher.get_bytes(latest["url"], CBP_SOURCE, headers=_HEADERS, max_bytes=_CBP_CSV_MAX_BYTES)
    if csv_result.content is None:
        raise _SourceError("cbp_fetch_failed", f"CBP encounters file unavailable: {_why(csv_result)}")
    try:
        dataset = parse_cbp_csv(csv_result.content.decode("utf-8-sig"))
    except ValueError as exc:  # includes UnicodeDecodeError
        raise _SourceError("cbp_unexpected_format", f"CBP encounters file not readable: {exc}") from exc
    text = _page_text(page)
    dataset["meta"] = {
        "title": latest["title"],
        "url": latest["url"],
        "page": CBP_DATASET_PAGE,
        "file_date": latest["file_date"],
        "file_size_bytes": latest["size_bytes"],
        "page_last_modified": _last_modified(text),
        "newer_disallowed_url": latest["newer_disallowed_url"],
        "fetched_at": _now(),
    }
    dataset["page_caveats_missing"] = [c for c in _CBP_PAGE_CAVEATS if " ".join(c.split()) not in text]
    return dataset


def _named(counter: Counter, names: list[str]) -> dict[str, int]:
    """Counter keyed by vocabulary index -> {name: count}, non-zero only, largest first."""
    items = sorted(((names[k], v) for k, v in counter.items() if v), key=lambda item: (-item[1], item[0]))
    return dict(items)


async def fetch_us_border_encounters(
    fetcher: Fetcher,
    region: str | None = "nationwide",
    component: str | None = "all",
    citizenship: str | None = None,
    months: int | None = 13,
) -> dict[str, Any]:
    """Monthly CBP encounters for a region, component and citizenship, with year-over-year context."""
    region_key = "nationwide" if region is None else str(region).strip().lower()
    component_key = "all" if component is None else str(component).strip().lower()
    months = 13 if months is None else months
    if region_key != "nationwide" and region_key not in CBP_REGIONS:
        return _usage(CBP_SOURCE, f"region must be one of: nationwide, {', '.join(CBP_REGIONS)}")
    if component_key != "all" and component_key not in CBP_COMPONENTS:
        return _usage(CBP_SOURCE, f"component must be one of: all, {', '.join(CBP_COMPONENTS)}")
    if citizenship is not None and (not isinstance(citizenship, str) or not citizenship.strip()):
        return _usage(CBP_SOURCE, "citizenship must be a non-empty string")
    if not _is_int(months) or not 1 <= months <= 120:
        return _usage(CBP_SOURCE, "months must be an integer from 1 to 120")

    dataset, state = await _load(fetcher, CBP_CACHE_KEY, _CBP_TTL, _CBP_SCHEMA, _fetch_cbp_dataset)
    if dataset is None:
        return _degraded(CBP_SOURCE, state["error"], state["reason"])

    dims = dataset["dims"]
    filters: dict[int, int] = {}
    for position, dim, labels, key in (
        (_REGION, "region", CBP_REGIONS, region_key),
        (_COMPONENT, "component", CBP_COMPONENTS, component_key),
    ):
        if key in labels:
            if labels[key] not in dims[dim]:
                return _degraded(
                    CBP_SOURCE,
                    f"CBP's file has no rows for the {dim} {labels[key]!r} (it has: {', '.join(dims[dim])})",
                    "cbp_unexpected_format",
                )
            filters[position] = dims[dim].index(labels[key])
    citizenship_label = None
    if citizenship is not None:
        wanted = " ".join(citizenship.split()).upper()
        found = [i for i, name in enumerate(dims["citizenship"]) if name.upper() == wanted]
        if not found:
            named = sorted(name for name in dims["citizenship"] if name != _CBP_OTHER)
            return _usage(
                CBP_SOURCE,
                f"CBP publishes no citizenship {citizenship.strip()!r}. It names {len(named)} citizenships and "
                f"counts every other one in OTHER: {', '.join(named)}",
            )
        filters[_CITIZENSHIP] = found[0]
        citizenship_label = dims["citizenship"][found[0]]

    periods: list[str] = dataset["periods"]
    position_of = {period: i for i, period in enumerate(periods)}
    last = len(periods) - 1
    totals = [0] * len(periods)
    breakdown = {dim: [Counter() for _ in periods] for dim in ("component", "encounter_type", "demographic")}
    latest_by_citizenship: Counter = Counter()
    for cell in dataset["cells"]:
        if any(cell[pos] != want for pos, want in filters.items()):
            continue
        period, count = cell[0], cell[_COUNT]
        totals[period] += count
        breakdown["component"][period][cell[_COMPONENT]] += count
        breakdown["encounter_type"][period][cell[_TYPE]] += count
        breakdown["demographic"][period][cell[_DEMOGRAPHIC]] += count
        if period == last:
            latest_by_citizenship[cell[_CITIZENSHIP]] += count

    def month(i: int) -> dict[str, Any]:
        start, end = _month_bounds(periods[i])
        return {
            "period": periods[i],
            "period_start": start,
            "period_end": end,
            "fiscal_year": _fiscal_year(periods[i]),
            "encounters": totals[i],
            "by_component": _named(breakdown["component"][i], dims["component"]),
            "by_encounter_type": _named(breakdown["encounter_type"][i], dims["encounter_type"]),
            "by_demographic": _named(breakdown["demographic"][i], dims["demographic"]),
        }

    gaps: list[str] = []
    meta = dataset["meta"]
    window = list(range(max(0, len(periods) - months), len(periods)))
    first_shown, span = periods[window[0]], []
    while first_shown <= periods[last]:
        span.append(first_shown)
        first_shown = _shift_months(first_shown, 1)
    absent = [p for p in span if p not in position_of]
    if absent:
        gaps.append(
            f"{len(absent)} month(s) between {periods[window[0]]} and {periods[last]} have no rows in CBP's file "
            f"and are not listed: {', '.join(absent[:12])}{' ...' if len(absent) > 12 else ''}."
        )
    if dataset["unreadable_rows"]:
        gaps.append(
            f"{dataset['unreadable_rows']} of the file's {dataset['rows']} rows could not be read and are not counted."
        )
    if meta.get("newer_disallowed_url"):
        gaps.append(f"CBP lists a newer-looking encounters file at a path its robots.txt disallows "
                    f"({meta['newer_disallowed_url']}); it is not read, so these may not be CBP's newest figures.")
    for caveat in dataset.get("page_caveats_missing", []):
        gaps.append(f"CBP's dataset page no longer shows this caveat word for word (quoted as captured 2026-10-17): "
                    f"{caveat[:90]}...")

    latest_start, latest_end = _month_bounds(periods[last])
    prior_index = position_of.get(_shift_months(periods[last], -12))
    prior = totals[prior_index] if prior_index is not None else None
    latest = {
        "period": periods[last],
        "period_start": latest_start,
        "period_end": latest_end,
        "fiscal_year": _fiscal_year(periods[last]),
        "encounters": totals[last],
        "same_month_prior_year": prior,
        "change_vs_prior_year": _diff(totals[last], prior),
        "pct_change_vs_prior_year": _pct(totals[last], prior),
    }

    by_fiscal_year: dict[int, list[int]] = defaultdict(list)
    for i, period in enumerate(periods):
        by_fiscal_year[_fiscal_year(period)].append(i)
    fiscal_years = []
    for fiscal_year in sorted(by_fiscal_year):
        members = by_fiscal_year[fiscal_year]
        label = dataset["fiscal_year_labels"].get(str(fiscal_year), str(fiscal_year))
        entry: dict[str, Any] = {
            "fiscal_year": fiscal_year,
            "period_start": f"{fiscal_year - 1}-10-01",
            "period_end": f"{fiscal_year}-09-30",
            "months_reported": len(members),
            "complete": len(members) == 12,
            "fiscal_year_to_date": "FYTD" in label.upper(),
            "encounters": sum(totals[i] for i in members),
        }
        if entry["fiscal_year_to_date"]:
            earlier = [position_of.get(_shift_months(periods[i], -12)) for i in members]
            # Only a like-for-like comparison: every same month of the prior year must be in the file.
            entry["prior_fiscal_year_same_months"] = (
                None if any(j is None for j in earlier) else sum(totals[j] for j in earlier)
            )
        fiscal_years.append(entry)

    result: dict[str, Any] = {
        "publisher": "U.S. Customs and Border Protection (CBP)",
        "dataset": {
            "title": meta["title"],
            "url": meta["url"],
            "page": meta["page"],
            "file_date": meta["file_date"],
            "file_size_bytes": meta["file_size_bytes"],
            "page_last_modified": meta["page_last_modified"],
            "coverage": {"first_period": periods[0], "last_period": periods[last]},
            "fetched_at": meta["fetched_at"],
        },
        "measure_kind": "flow",
        "measure": _CBP_MEASURE,
        "unit": _CBP_UNIT,
        "date_semantics": _CBP_DATE_SEMANTICS,
        "definitions": _CBP_DEFINITIONS,
        "selection": {
            "region": CBP_REGIONS.get(region_key, "Nationwide"),
            "component": CBP_COMPONENTS.get(component_key, "All components"),
            "citizenship": citizenship_label,
        },
        "latest": latest,
        "months": [month(i) for i in window],
        "fiscal_years": fiscal_years,
    }
    if citizenship_label is None:
        other = dims["citizenship"].index(_CBP_OTHER) if _CBP_OTHER in dims["citizenship"] else None
        ranked = sorted(
            ((dims["citizenship"][k], v) for k, v in latest_by_citizenship.items() if k != other and v),
            key=lambda item: (-item[1], item[0]),
        )[:10]
        result["top_citizenships_latest_month"] = [
            {"citizenship": name, "encounters": count,
             "share_pct": round(100 * count / totals[last], 1) if totals[last] else None}
            for name, count in ranked
        ]
        result["other_citizenships_latest_month"] = latest_by_citizenship.get(other, 0) if other is not None else None
    result["publisher_caveats"] = [*_CBP_PAGE_CAVEATS, *_CBP_STATS_CAVEATS]
    result["publisher_caveats_source"] = (
        f"Quoted as captured 2026-10-17: the first two from {CBP_DATASET_PAGE} (re-checked on every refresh), "
        f"the others from {CBP_STATS_PAGE} (not re-read)."
    )
    result["terms"] = _CBP_TERMS
    result["notes"] = list(_CBP_NOTES)
    _freshness(result, state, gaps, meta["fetched_at"])
    result["data_gaps"] = gaps
    result["source"] = CBP_SOURCE
    result["timestamp"] = _now()
    return result


# ---------------------------------------------------------------------------
# UK Home Office: small boats crossing the English Channel
# ---------------------------------------------------------------------------

UK_SOURCE = "uk-home-office"
UK_PUBLICATION_PATH = "/government/publications/migrants-detected-crossing-the-english-channel-in-small-boats"
UK_PUBLICATION_PAGE = "https://www.gov.uk" + UK_PUBLICATION_PATH
UK_CONTENT_API = "https://www.gov.uk/api/content" + UK_PUBLICATION_PATH
UK_CACHE_KEY = "border_crossings:uk_small_boats"
_UK_SCHEMA = 1
_UK_TTL = 10800  # 3 h; its change history shows 72 updates from 1 August to 16 October 2026
_UK_API_MAX_BYTES = 8 * 1024 * 1024
_UK_ODS_MAX_BYTES = 32 * 1024 * 1024  # the 16 October 2026 file is 76,787 bytes
_UK_FILE_HOSTS = {"assets.publishing.service.gov.uk", "www.gov.uk"}
_ODS_TYPE = "application/vnd.oasis.opendocument.spreadsheet"
_ODS_MAX_XML_BYTES = 64 * 1024 * 1024  # the live content.xml is 2.5 MB
_ODS_MAX_COLUMNS = 256
_ODS_MAX_ROWS = 200_000
_T = "{urn:oasis:names:tc:opendocument:xmlns:table:1.0}"
_O = "{urn:oasis:names:tc:opendocument:xmlns:office:1.0}"
_X = "{urn:oasis:names:tc:opendocument:xmlns:text:1.0}"

_UK_MEASURE = (
    "Migrants and boats detected crossing the English Channel in small boats and arriving in the UK "
    "(Home Office provisional operational data): per day (table SB_01) and per week (table SB_02, which "
    "adds migrants and events prevented by French authorities)."
)
_UK_UNIT = "people (migrants) and boats per day or week; preventions per week"
_UK_DATE_SEMANTICS = (
    "SB_01 'Date' is the day of arrival. SB_02 'Week ending' is the Sunday that closes a Monday-to-Sunday week "
    "(period_start is six days earlier); the Home Office notes the activity may have occurred on any day "
    "within that week. Months, years and year to date are this tool's sums of SB_01 days. "
    "dataset.public_updated_at is when GOV.UK last changed the publication."
)
_UK_NOTES = (
    _FLOW_NOT_STOCK,
    "Arrivals are counted when detected and the file has no person identifier, so a person who crosses more "
    "than once is counted each time. (Tool note, not a Home Office statement.)",
    "'-' in the file means no figure is published; it is returned as null, never 0 (dataset.coverage gives "
    "the first date with a figure). A month, year or week sum is null when any day in it is null.",
    "Months, years and year to date are sums of the SB_01 daily rows computed here, not Home Office "
    "publications; the quarterly 'Illegal entry routes to the UK' statistics hold the final figures.",
)
_UK_TERMS = "Open Government Licence v3.0 (GOV.UK terms); the time series is Crown copyright."
_UK_NOTE_ROW = re.compile(r"^\d+\.\s")


def _repeat(element: ET.Element, attribute: str) -> int:
    raw = element.get(_T + attribute)
    if raw is None:
        return 1
    try:
        return max(int(raw), 1)
    except ValueError:
        raise ValueError(f"unreadable table:{attribute} {raw!r}") from None


def _ods_inline(element: ET.Element) -> str:
    parts = [element.text or ""]
    for child in element:
        if child.tag == _X + "s":
            try:
                parts.append(" " * min(max(int(child.get(_X + "c", "1")), 1), 1024))
            except ValueError:
                parts.append(" ")
        elif child.tag == _X + "tab":
            parts.append("\t")
        elif child.tag == _X + "line-break":
            parts.append("\n")
        elif child.tag not in (_O + "annotation", _X + "note"):
            parts.append(_ods_inline(child))
        parts.append(child.tail or "")
    return "".join(parts)


def _ods_text(cell: ET.Element) -> str:
    paragraphs: list[str] = []

    def walk(node: ET.Element) -> None:
        for child in node:
            if child.tag in (_X + "p", _X + "h"):
                paragraphs.append(_ods_inline(child))
            elif child.tag != _O + "annotation":  # cell comments are not cell content
                walk(child)

    walk(cell)
    return "\n".join(paragraphs)


def _ods_value(cell: ET.Element) -> Any:
    kind = cell.get(_O + "value-type")
    if kind in ("float", "percentage", "currency"):
        raw = cell.get(_O + "value")
        try:
            return float(raw)
        except (TypeError, ValueError):
            raise ValueError(f"unreadable number {raw!r}") from None
    if kind == "date":
        raw = cell.get(_O + "date-value") or ""
        try:
            moment = datetime.fromisoformat(raw)
        except ValueError:
            raise ValueError(f"unreadable date {raw!r}") from None
        if moment.tzinfo is None and moment.time() == datetime.min.time():
            return moment.date()
        return moment
    if kind == "boolean":
        return (cell.get(_O + "boolean-value") or "").lower() == "true"
    if kind == "time":
        return cell.get(_O + "time-value")
    if kind == "string" and cell.get(_O + "string-value") is not None:
        return cell.get(_O + "string-value")
    text = _ods_text(cell)
    return text if text else None


def _ods_row(row: ET.Element) -> list[Any]:
    values: list[Any] = []
    pending_blank = 0
    for cell in row:
        if cell.tag not in (_T + "table-cell", _T + "covered-table-cell"):
            continue
        repeat = _repeat(cell, "number-columns-repeated")
        value = _ods_value(cell)
        if value is None:
            pending_blank += repeat  # counted, never expanded: trailing blanks are dropped
            continue
        if len(values) + pending_blank >= _ODS_MAX_COLUMNS:
            break
        values.extend([None] * pending_blank)
        pending_blank = 0
        values.extend([value] * min(repeat, _ODS_MAX_COLUMNS - len(values)))
        if len(values) >= _ODS_MAX_COLUMNS:
            break
    return values


def read_ods_tables(data: bytes) -> dict[str, list[list[Any]]]:
    """Every sheet of an ODS spreadsheet as rows of cell values.

    Numbers come back as float, dates as ``datetime.date`` (``datetime`` when the cell has a
    time of day), booleans as bool, anything else as its text with paragraphs joined by newlines;
    an empty cell is None. Blank rows are dropped and trailing blank cells trimmed, so the repeat
    counts that pad a sheet to 16,384 columns and 1,048,576 rows are never expanded. Columns past
    the 256th are not read. Raises ValueError for anything that is not a readable ODS spreadsheet.
    """
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as exc:
        raise ValueError("not an ODS file (not a zip archive)") from exc
    try:
        with archive:
            names = set(archive.namelist())
            if "mimetype" in names:
                with archive.open("mimetype") as handle:
                    mimetype = handle.read(200).decode("ascii", errors="replace").strip()
                if mimetype != _ODS_TYPE:
                    raise ValueError(f"not an ODS spreadsheet (mimetype {mimetype!r})")
            if "content.xml" not in names:
                raise ValueError("ODS file has no content.xml")
            if archive.getinfo("content.xml").file_size > _ODS_MAX_XML_BYTES:
                raise ValueError("ODS content.xml is larger than 64 MiB")
            with archive.open("content.xml") as handle:
                xml_bytes = handle.read(_ODS_MAX_XML_BYTES + 1)
    except (zipfile.BadZipFile, zlib.error, EOFError, OSError, NotImplementedError) as exc:
        raise ValueError(f"ODS archive not readable: {exc}") from exc
    if len(xml_bytes) > _ODS_MAX_XML_BYTES:
        raise ValueError("ODS content.xml is larger than 64 MiB")
    # Entities can only be declared in a DTD, so refusing one rules out entity
    # expansion ("billion laughs") and external entities (XXE) before the
    # standard-library parser runs;
    # ODF content.xml never carries a DTD. A hardened third-party XML parser
    # would be a new dependency.
    if b"<!DOCTYPE" in xml_bytes or b"<!ENTITY" in xml_bytes:
        raise ValueError("ODS content.xml declares a DTD; refused")
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise ValueError(f"ODS content.xml is not well-formed XML: {exc}") from exc
    tables: dict[str, list[list[Any]]] = {}
    for table in root.iter(_T + "table"):
        name = table.get(_T + "name") or f"Sheet{len(tables) + 1}"
        rows: list[list[Any]] = []
        for row in table.iter(_T + "table-row"):
            values = _ods_row(row)
            if not values:
                continue
            repeat = _repeat(row, "number-rows-repeated")
            if len(rows) + repeat > _ODS_MAX_ROWS:
                raise ValueError(f"sheet {name!r} has more than {_ODS_MAX_ROWS} rows")
            rows.extend(list(values) for _ in range(repeat))
        tables[name] = rows
    if not tables:
        raise ValueError("ODS file has no sheets")
    return tables


def _header(value: Any) -> str:
    return " ".join(value.split()).lower() if isinstance(value, str) else ""


def _locate(tables: dict[str, list[list[Any]]], preferred: str, first: str) -> tuple[str, int, dict] | None:
    """(sheet, header row index, column positions) of the table whose header names `first` and the arrivals."""
    order = [preferred, *[name for name in tables if name != preferred]] if preferred in tables else list(tables)
    for name in order:
        for i, row in enumerate(tables[name][:30]):
            heads = [_header(value) for value in row]
            if first in heads and "migrants arrived" in heads and "boats arrived" in heads:
                def find(label: str) -> int | None:
                    return heads.index(label) if label in heads else None

                return name, i, {
                    "when": heads.index(first),
                    "migrants": heads.index("migrants arrived"),
                    "boats": heads.index("boats arrived"),
                    "uncontrolled": next((j for j, head in enumerate(heads) if "uncontrolled" in head), None),
                    "prevented": find("migrants prevented"),
                    "events": find("events prevented"),
                    "notes": find("notes"),
                }
    return None


def _uk_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        for parse in (date.fromisoformat, lambda s: datetime.strptime(s, "%d/%m/%Y").date()):
            try:
                return parse(text)
            except ValueError:
                continue
    return None


def _uk_count(value: Any) -> tuple[int | None, bool]:
    """(count, readable): '-' is a published "no figure" (None, readable)."""
    if isinstance(value, float) and value.is_integer() and value >= 0:
        return int(value), True
    if _is_int(value) and value >= 0:
        return value, True
    if isinstance(value, str) and value.strip() == "-":
        return None, True
    return None, False


_UK_COLUMN_LABELS = {
    "migrants": "Migrants arrived",
    "boats": "Boats arrived",
    "uncontrolled": "Boats arrived - involved in uncontrolled landings",
    "prevented": "Migrants prevented",
    "events": "Events prevented",
}


def _uk_series(table: list[list[Any]], located: tuple[str, int, dict], keys: tuple[str, ...],
               gaps: list[str]) -> list[list[Any]]:
    sheet, header_row, columns = located
    for key in keys:
        if columns[key] is None:
            gaps.append(f"{sheet} has no {_UK_COLUMN_LABELS[key]!r} column; those figures are null.")
    records: dict[str, list[Any]] = {}
    undated = unreadable = duplicates = 0
    for row in table[header_row + 1:]:
        def cell(key: str) -> Any:
            col = columns[key]
            return row[col] if col is not None and col < len(row) else None

        day = _uk_date(cell("when"))
        if day is None:
            undated += 1
            continue
        if day.isoformat() in records:
            duplicates += 1
            continue
        record: list[Any] = [day.isoformat()]
        for key in keys:
            if columns[key] is None:
                record.append(None)
                continue
            value, readable = _uk_count(cell(key))
            unreadable += not readable
            record.append(value)
        note = cell("notes")
        record.append(isinstance(note, str) and "revised" in note.lower())
        records[day.isoformat()] = record
    if undated:
        gaps.append(f"{undated} row(s) of {sheet} have no readable date and are not counted.")
    if unreadable:
        gaps.append(f"{unreadable} cell(s) of {sheet} are neither a count nor '-' and are read as null.")
    if duplicates:
        gaps.append(f"{duplicates} row(s) of {sheet} repeat an earlier date; the first is kept.")
    return [records[key] for key in sorted(records)]


def parse_uk_small_boats(tables: dict[str, list[list[Any]]]) -> dict[str, Any]:
    """Daily (SB_01) and weekly (SB_02) rows plus the numbered notes of the Home Office time series.

    Daily rows are [date, migrants, boats, uncontrolled, revised]; weekly rows are
    [week ending, migrants, boats, uncontrolled, prevented, events prevented, revised].
    Raises ValueError when there is no daily table or it has no dated rows.
    """
    gaps: list[str] = []
    located = _locate(tables, "SB_01", "date")
    if located is None:
        raise ValueError("no SB_01 daily table (Date, Migrants arrived, Boats arrived) in the ODS")
    daily = _uk_series(tables[located[0]], located, ("migrants", "boats", "uncontrolled"), gaps)
    if not daily:
        raise ValueError(f"the daily table {located[0]} has no dated rows")
    weekly: list[list[Any]] = []
    located_weekly = _locate(tables, "SB_02", "week ending")
    if located_weekly is None:
        gaps.append("no SB_02 weekly table (Week ending, Migrants arrived, ...) in the ODS; weekly figures and "
                    "preventions are missing.")
    else:
        weekly = _uk_series(tables[located_weekly[0]], located_weekly,
                            ("migrants", "boats", "uncontrolled", "prevented", "events"), gaps)
    notes: list[str] = []
    for row in tables.get("Notes", []):
        if row and isinstance(row[0], str):
            text = "\n".join(line for line in (" ".join(part.split()) for part in row[0].split("\n")) if line)
            if _UK_NOTE_ROW.match(text):
                notes.append(text)
    if not notes:
        gaps.append("the ODS has no numbered notes on a Notes sheet; the Home Office caveats are missing.")
    return {"schema": _UK_SCHEMA, "daily": daily, "weekly": weekly, "notes": notes, "gaps": gaps}


def _uk_attachment(item: Any) -> dict[str, Any]:
    details = item.get("details") if isinstance(item, dict) else None
    attachments = details.get("attachments") if isinstance(details, dict) else None
    candidates = [
        a for a in attachments or []
        if isinstance(a, dict)
        and (a.get("content_type") == _ODS_TYPE or str(a.get("filename") or "").lower().endswith(".ods"))
    ]
    if not candidates:
        raise _SourceError("uk_unexpected_format", "the GOV.UK publication lists no ODS time-series attachment")
    candidates.sort(key=lambda a: "time series" not in str(a.get("title") or "").lower())
    chosen = candidates[0]
    url = urljoin("https://www.gov.uk/", str(chosen.get("url") or ""))
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.hostname not in _UK_FILE_HOSTS:
        raise _SourceError("uk_unexpected_format",
                           f"the time-series attachment is not on GOV.UK ({parts.hostname or 'no host'})")
    size = chosen.get("file_size")
    return {"url": url, "title": chosen.get("title"), "file_size_bytes": size if _is_int(size) else None}


async def _fetch_uk_dataset(fetcher: Fetcher) -> dict[str, Any]:
    api = await fetcher.get_bytes(
        UK_CONTENT_API, UK_SOURCE, headers={**_HEADERS, "Accept": "application/json"}, max_bytes=_UK_API_MAX_BYTES
    )
    if api.content is None:
        raise _SourceError("uk_fetch_failed", f"GOV.UK Content API unavailable: {_why(api)}")
    try:
        item = json.loads(api.content)
    except ValueError as exc:
        raise _SourceError("uk_unexpected_format", f"GOV.UK Content API did not return JSON: {exc}") from exc
    attachment = _uk_attachment(item)
    ods = await fetcher.get_bytes(attachment["url"], UK_SOURCE, headers=_HEADERS, max_bytes=_UK_ODS_MAX_BYTES)
    if ods.content is None:
        raise _SourceError("uk_fetch_failed", f"Home Office time series unavailable: {_why(ods)}")
    try:
        dataset = parse_uk_small_boats(read_ods_tables(ods.content))
    except ValueError as exc:
        raise _SourceError("uk_unexpected_format", f"Home Office time series not readable: {exc}") from exc
    history = [h for h in (item.get("details") or {}).get("change_history") or [] if isinstance(h, dict)]
    newest = max(history, key=lambda h: str(h.get("public_timestamp") or ""), default={})
    dataset["meta"] = {
        "title": attachment["title"],
        "url": attachment["url"],
        "publication": item.get("title"),
        "page": "https://www.gov.uk" + str(item.get("base_path") or UK_PUBLICATION_PATH),
        "public_updated_at": item.get("public_updated_at"),
        "latest_change_note": str(newest.get("note") or "").strip() or None,
        "file_size_bytes": attachment["file_size_bytes"],
        "fetched_at": _now(),
    }
    return dataset


def _total(rows: list[list[Any]], column: int) -> int | None:
    """Sum of one column, or None when any row has no figure for it."""
    values = [row[column] for row in rows]
    return None if any(v is None for v in values) else sum(values)


def _iso_days(start: date, end: date) -> list[str]:
    return [(start + timedelta(days=k)).isoformat() for k in range((end - start).days + 1)]


async def fetch_uk_small_boat_arrivals(
    fetcher: Fetcher,
    days: int | None = 14,
    weeks: int | None = 8,
    months: int | None = 13,
) -> dict[str, Any]:
    """UK small-boat arrivals: latest days and weeks, month and year sums, year to date vs last year."""
    days = 14 if days is None else days
    weeks = 8 if weeks is None else weeks
    months = 13 if months is None else months
    for name, value, high in (("days", days, 400), ("weeks", weeks, 520), ("months", months, 240)):
        if not _is_int(value) or not 1 <= value <= high:
            return _usage(UK_SOURCE, f"{name} must be an integer from 1 to {high}")

    dataset, state = await _load(fetcher, UK_CACHE_KEY, _UK_TTL, _UK_SCHEMA, _fetch_uk_dataset)
    if dataset is None:
        return _degraded(UK_SOURCE, state["error"], state["reason"])

    daily: list[list[Any]] = dataset["daily"]
    weekly: list[list[Any]] = dataset["weekly"]
    gaps = list(dataset["gaps"])
    meta = dataset["meta"]
    by_date = {row[0]: row for row in daily}
    first_day, last_day = date.fromisoformat(daily[0][0]), date.fromisoformat(daily[-1][0])
    missing_days = (last_day - first_day).days + 1 - len(daily)
    if missing_days:
        gaps.append(f"{missing_days} day(s) between {first_day} and {last_day} have no SB_01 row; sums over them "
                    "cover only the days reported (see days_reported).")

    def day(row: list[Any]) -> dict[str, Any]:
        return {"date": row[0], "migrants_arrived": row[1], "boats_arrived": row[2],
                "boats_uncontrolled_landings": row[3], "revised": row[4]}

    def week(row: list[Any]) -> dict[str, Any]:
        return {"week_ending": row[0],
                "period_start": (date.fromisoformat(row[0]) - timedelta(days=6)).isoformat(),
                "period_end": row[0], "migrants_arrived": row[1], "boats_arrived": row[2],
                "boats_uncontrolled_landings": row[3], "migrants_prevented": row[4], "events_prevented": row[5],
                "revised": row[6]}

    seven = [row for row in daily if row[0] >= (last_day - timedelta(days=6)).isoformat()]
    last_7_days = {
        "period_start": (last_day - timedelta(days=6)).isoformat(),
        "period_end": last_day.isoformat(),
        "days_reported": len(seven),
        "migrants_arrived": _total(seven, 1),
        "boats_arrived": _total(seven, 2),
        "boats_uncontrolled_landings": _total(seven, 3),
    }

    shown_weeks = weekly[-weeks:]
    for row in shown_weeks:
        ending = date.fromisoformat(row[0])
        days_in_week = [by_date.get(d) for d in _iso_days(ending - timedelta(days=6), ending)]
        if not all(days_in_week):
            continue
        for column, label in ((1, "migrants"), (2, "boats")):
            summed = _total(days_in_week, column)
            if row[column] is not None and summed is not None and summed != row[column]:
                gaps.append(f"SB_02 week ending {row[0]} has {row[column]} {label}; the seven SB_01 days sum to "
                            f"{summed}.")

    by_month: dict[str, list[list[Any]]] = defaultdict(list)
    by_year: dict[int, list[list[Any]]] = defaultdict(list)
    for row in daily:
        by_month[row[0][:7]].append(row)
        by_year[int(row[0][:4])].append(row)
    window_start = _shift_months(last_day.isoformat()[:7], -(months - 1))
    month_list = []
    for period in sorted(p for p in by_month if p >= window_start):
        rows = by_month[period]
        year, number = int(period[:4]), int(period[5:])
        length = calendar.monthrange(year, number)[1]
        month_list.append({
            "period": period,
            "period_start": max(date(year, number, 1), first_day).isoformat(),
            "period_end": min(date(year, number, length), last_day).isoformat(),
            "complete": len(rows) == length,
            "days_reported": len(rows),
            "days_in_month": length,
            "migrants_arrived": _total(rows, 1),
            "boats_arrived": _total(rows, 2),
            "boats_uncontrolled_landings": _total(rows, 3),
        })
    year_list = []
    for year in sorted(by_year):
        rows = by_year[year]
        year_list.append({
            "year": year,
            "period_start": max(date(year, 1, 1), first_day).isoformat(),
            "period_end": min(date(year, 12, 31), last_day).isoformat(),
            "complete": len(rows) == (366 if calendar.isleap(year) else 365),
            "days_reported": len(rows),
            "migrants_arrived": _total(rows, 1),
            "boats_arrived": _total(rows, 2),
            "boats_uncontrolled_landings": _total(rows, 3),
        })

    ytd_rows = [row for row in daily if row[0] >= f"{last_day.year:04d}-01-01"]
    prior_start = date(last_day.year - 1, 1, 1)
    prior_end = (last_day.replace(day=28) if (last_day.month, last_day.day) == (2, 29) else last_day).replace(
        year=last_day.year - 1)
    prior_rows = [by_date.get(d) for d in _iso_days(prior_start, prior_end)]
    prior = None
    if all(prior_rows):
        prior = {"period_start": prior_start.isoformat(), "period_end": prior_end.isoformat(),
                 "migrants_arrived": _total(prior_rows, 1), "boats_arrived": _total(prior_rows, 2)}
    else:
        gaps.append(f"SB_01 has no complete daily series for {prior_start} to {prior_end}, so year to date has no "
                    "prior-year comparison.")
    current_migrants, current_boats = _total(ytd_rows, 1), _total(ytd_rows, 2)
    prior_migrants = prior["migrants_arrived"] if prior else None
    prior_boats = prior["boats_arrived"] if prior else None
    year_to_date = {
        "period_start": f"{last_day.year:04d}-01-01",
        "period_end": last_day.isoformat(),
        "migrants_arrived": current_migrants,
        "boats_arrived": current_boats,
        "prior_year": prior,
        "change_migrants": _diff(current_migrants, prior_migrants),
        "pct_change_migrants": _pct(current_migrants, prior_migrants),
        "change_boats": _diff(current_boats, prior_boats),
        "pct_change_boats": _pct(current_boats, prior_boats),
    }

    def first_with(rows: list[list[Any]], column: int) -> str | None:
        return next((row[0] for row in rows if row[column] is not None), None)

    result: dict[str, Any] = {
        "publisher": "UK Home Office",
        "dataset": {
            "title": meta["title"],
            "url": meta["url"],
            "publication": meta["publication"],
            "page": meta["page"],
            "public_updated_at": meta["public_updated_at"],
            "latest_change_note": meta["latest_change_note"],
            "file_size_bytes": meta["file_size_bytes"],
            "daily_data_through": last_day.isoformat(),
            "weekly_data_through": weekly[-1][0] if weekly else None,
            "coverage": {
                "daily_from": first_day.isoformat(),
                "uncontrolled_landings_from": first_with(daily, 3),
                "weekly_from": weekly[0][0] if weekly else None,
                "preventions_from": first_with(weekly, 4),
            },
            "fetched_at": meta["fetched_at"],
        },
        "measure_kind": "flow",
        "measure": _UK_MEASURE,
        "unit": _UK_UNIT,
        "date_semantics": _UK_DATE_SEMANTICS,
        "latest_day": day(daily[-1]),
        "last_7_days": last_7_days,
        "daily": [day(row) for row in daily[-days:]],
        "latest_week": week(weekly[-1]) if weekly else None,
        "weeks": [week(row) for row in shown_weeks],
        "months": month_list,
        "years": year_list,
        "year_to_date": year_to_date,
        "revised_dates": [row[0] for row in daily if row[4] and row[0][:7] >= window_start],
        "publisher_caveats": list(dataset["notes"]),
        "publisher_caveats_source": "Notes sheet of the time-series file (dataset.url), read on every refresh.",
        "terms": _UK_TERMS,
        "notes": list(_UK_NOTES),
    }
    _freshness(result, state, gaps, meta["fetched_at"])
    result["data_gaps"] = gaps
    result["source"] = UK_SOURCE
    result["timestamp"] = _now()
    return result
