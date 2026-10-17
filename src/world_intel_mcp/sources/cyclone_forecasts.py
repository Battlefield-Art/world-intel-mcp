"""Tropical cyclone forecast tracks (NHC and JTWC) for world-intel-mcp.

Opt-in companion to sources/cyclones.py (NHC) and sources/jtwc.py
(JTWC): their fetchers call in here only when a caller passes
``include_forecast=True``, so the default responses, and the collector's
call, are unchanged. Each storm gains a ``forecast`` dict; the response
gains ``data_gaps`` naming every track or cone it could not deliver.

Upstream facts, measured live 2026-10-17 against Hurricane Trudy (NHC
EP212026 advisory 17, also JTWC 21E), Typhoon 29W (Champi) warning 13
and Tropical Cyclone 01P (Wati) warning 3. Every product named below
answered HTTP 200 without a key to the fetcher's User-Agent.

NHC: CurrentStorms.json links each storm's forecast/advisory text
(``forecastAdvisory.url``, the TCM, an HTML page whose only <pre> holds
the product) and its 5-day track and cone (``forecastTrack`` and
``trackCone``, each as a KMZ and a shapefile zip). Track points come
from the TCM, the one product of the three that states UTC valid times
and post-tropical/dissipated status outright: the KMZ track gives local
times ("6:00 PM CST October 17, 2026"), and the shapefile's point table
stamps its tau-0 row 17/1200 while giving the 1500Z advisory position,
and labels the 96 h post-tropical remnant low "Subtropical Depression"
where the TCM says POST-TROP/REMNT LOW. The cone comes from
``trackCone.kmzFile``: one Polygon ring (1,168 vertices for Trudy #17)
in a KML whose disclaimer reads "The official forecast track in KML
format is an experimental product." The ring is simplified here
(Douglas-Peucker, 0.01 degrees, [lat, lon] vertices) and labelled as
such; NHC's own geometry stays at ``cone_url``. nhc.noaa.gov robots.txt
disallows nothing; the NWS disclaimer puts the products in the public
domain and asks for request cycles matched to each product's update
frequency, which the per-advisory cache keys below give.

JTWC: the RSS links each storm's warning text (``warning_text_url``),
served with CRLF line ends. Forecast blocks read "12 HRS, VALID AT:",
"180000Z --- 13.0N 143.7E", "MAX SUSTAINED WINDS - 120 KT, GUSTS 145
KT", then status lines such as BECOMING EXTRATROPICAL. Times carry day
and hour only; the month comes from the REMARKS date line ("17OCT26.").
JTWC links no cone of uncertainty: its Google Earth overlay (a KMZ of
about 450 KB) holds a "34 knot Danger Swath" polygon, a different
product, which is not fetched. The JTWC HTML home page and robots.txt
answered 403 (CloudFront "Request blocked") to the same client that got
200 for the RSS and the warning texts, so JTWC's site terms could not
be read.

The text URLs are per bin, not per issuance (MIATCMEP1.shtml,
wp2926web.txt), so a page can lag the feed entry pointing at it. The
advisory or warning number in the text is checked against the feed; a
mismatch is returned labelled stale and its cached copy is dropped.

Gaps / not covered: wind radii and seas are not parsed. Not observed
live: a Central Pacific (CPHC) forecast/advisory, potential tropical
cyclone products, a cone crossing the antimeridian (a cone with more
than one ring is refused, never trimmed), and a JTWC final warning (a
warning text without forecast blocks is reported unparsed). JTWC's
danger swath is not fetched.
"""

import asyncio
import html
import io
import logging
import math
import re
import xml.etree.ElementTree as ET
import zipfile
import zlib
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

from ..fetcher import Fetcher

logger = logging.getLogger("world-intel-mcp.sources.cyclone_forecasts")

# Per-call bound on per-storm product fetches. Simultaneous active storms
# per feed have stayed well below this; storms past it are named in
# data_gaps, never dropped silently.
MAX_FORECAST_STORMS = 10

# Planar tolerance (degrees) for simplifying NHC's cone ring. About 1.1 km
# of latitude; Trudy #17's 1,167-vertex ring keeps 56 vertices.
CONE_SIMPLIFY_TOLERANCE_DEG = 0.01

# Cache keys carry the advisory/warning number, so a new issuance is a
# new key; the TTL only bounds how long one issuance is reused.
_FORECAST_CACHE_TTL = 3600
_CONE_MAX_BYTES = 2 * 1024 * 1024
_KML_MAX_BYTES = 8 * 1024 * 1024
# Douglas-Peucker is quadratic in the worst case, so a ring past this is
# refused before simplifying (Trudy #17's ring has 1,167 vertices).
_CONE_MAX_VERTICES = 10_000

# Product links come from the agencies' feeds; one is fetched only when
# it is https on the agency's own host, so a malformed or tampered feed
# cannot point the server at another host.
_NHC_PRODUCT_HOSTS = frozenset({"www.nhc.noaa.gov"})
_JTWC_PRODUCT_HOSTS = frozenset({"www.metoc.navy.mil"})

_MONTHS = {
    name: number
    for number, name in enumerate(
        ("JAN", "FEB", "MAR", "APR", "MAY", "JUN",
         "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"),
        start=1,
    )
}

# Track statuses meaning the caller asked for data and did not get it
# (or got an older issuance). not_linked / not_published are upstream
# coverage facts: named in data_gaps, not degradation.
_FAILED_STATUSES = frozenset(
    {
        "fetch_failed",
        "unparsed",
        "not_fetched_cap",
        "stale_advisory",
        "stale_warning",
        "refused_link",
    }
)

_CONE_NOTE = (
    "Outline simplified from NHC's ring (Douglas-Peucker, 0.01 degree "
    "tolerance); not the official geometry, which is at cone_url. NHC's "
    "KML disclaimer calls the KML format experimental."
)
_JTWC_CONE_GAP = (
    "JTWC links no cone of uncertainty: cone is null for every JTWC storm "
    "(the 34 knot danger swath in its KMZ overlay is a different product "
    "and is not fetched)"
)

_LATLON = r"(\d{1,2}(?:\.\d+)?)([NS])[ \t]+(\d{1,3}(?:\.\d+)?)([EW])"

# --- NHC forecast/advisory (TCM) ---
_PRE_RE = re.compile(r"<pre[^>]*>(.*?)</pre>", re.S | re.I)
_TCM_ISSUED_RE = re.compile(
    r"^[ \t]*(\d{1,2})(\d{2}) UTC [A-Z]{3} ([A-Z]{3}) (\d{1,2}) (\d{4})[ \t]*$", re.M
)
_TCM_ADVISORY_RE = re.compile(r"FORECAST/ADVISORY NUMBER[ \t]+(\w+)")
_TCM_CENTER_RE = re.compile(
    r"CENTER LOCATED NEAR[ \t]+" + _LATLON + r"[ \t]+AT[ \t]+(\d{2})/(\d{2})(\d{2})Z"
)
_TCM_SYNOPTIC_RE = re.compile(r"AT[ \t]+(\d{2})/(\d{2})(\d{2})Z[ \t]+CENTER WAS LOCATED NEAR")
_TCM_PRESSURE_RE = re.compile(r"MINIMUM CENTRAL PRESSURE[ \t]+(\d+)[ \t]*MB")
_TCM_SUSTAINED_RE = re.compile(
    r"MAX SUSTAINED WINDS[ \t]+(\d+)[ \t]*KT WITH GUSTS TO[ \t]+(\d+)[ \t]*KT"
)
_TCM_POINT_RE = re.compile(
    r"^[ \t]*(FORECAST|OUTLOOK) VALID[ \t]+(\d{2})/(\d{2})(\d{2})Z"
    r"(?:[ \t]+" + _LATLON + r")?"
    r"(?:[ \t]*\.\.\.[ \t]*(?P<status>[^\n]*\S))?",
    re.M,
)
_TCM_WIND_RE = re.compile(r"MAX WIND[ \t]+(\d+)[ \t]*KT\.\.\.GUSTS[ \t]+(\d+)[ \t]*KT")

# --- JTWC warning text ---
_JTWC_HEADER_RE = re.compile(r"^[ \t]*WT[A-Z]{2}\d{2}[ \t]+[A-Z]{4}[ \t]+(\d{2})(\d{2})(\d{2})\b", re.M)
_JTWC_NUMBER_RE = re.compile(r"WARNING NR[ \t]+(\d+)")
_JTWC_DATE_RE = re.compile(
    r"^[ \t]*(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})\.", re.M
)
_JTWC_POSITION_RE = re.compile(
    r"WARNING POSITION:\s*(\d{2})(\d{2})(\d{2})Z\s*-+\s*NEAR\s+" + _LATLON
)
_JTWC_PRESENT_RE = re.compile(
    r"PRESENT WIND DISTRIBUTION:\s*MAX SUSTAINED WINDS\s*-\s*(\d+)\s*KT,\s*GUSTS\s+(\d+)\s*KT"
)
_JTWC_PRESSURE_RE = re.compile(
    r"MINIMUM CENTRAL PRESSURE AT\s+(\d{2})(\d{2})(\d{2})Z\s+IS\s+(\d+)\s+MB"
)
_JTWC_POINT_RE = re.compile(
    r"(\d+)\s+HRS,\s+VALID AT:\s*(\d{2})(\d{2})(\d{2})Z\s*-+\s*" + _LATLON
)
_JTWC_WIND_RE = re.compile(r"MAX SUSTAINED WINDS\s*-\s*(\d+)\s*KT,\s*GUSTS\s+(\d+)\s*KT")
_JTWC_SECTION_RE = re.compile(
    r"^[ \t]*(FORECASTS|EXTENDED OUTLOOK|LONG RANGE OUTLOOK|REMARKS):", re.M
)
_JTWC_SECTIONS = {
    "FORECASTS": "forecast",
    "EXTENDED OUTLOOK": "extended_outlook",
    "LONG RANGE OUTLOOK": "long_range_outlook",
}
# Lines of a forecast block that are structure, not status. Anything
# else in the block ("BECOMING EXTRATROPICAL") is reported as status.
_JTWC_STRUCTURAL_RE = re.compile(
    r"^(?:MAX SUSTAINED WINDS|WIND RADII VALID|RADIUS OF|\d{3} NM [A-Z]+ QUADRANT"
    r"|VECTOR TO|-+$|(?:FORECASTS|EXTENDED OUTLOOK|LONG RANGE OUTLOOK|REMARKS):)"
)


def _iso(value: datetime | None) -> str | None:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ") if value else None


def _normalize_newlines(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _coord(value: str, hemisphere: str) -> float:
    number = float(value)
    return -number if hemisphere in ("S", "W") else number


def _hours_between(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    return round((end - start).total_seconds() / 3600)


def _same_number(found: Any, expected: Any) -> bool:
    try:
        return int(found) == int(expected)
    except (TypeError, ValueError):
        return str(found).lstrip("0").upper() == str(expected).lstrip("0").upper()


def _refused_link(url: str, hosts: frozenset[str]) -> str | None:
    """Why a feed-supplied product link is not fetched, or None if it may be."""
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        return f"link {url!r} is not a valid URL; not fetched"
    if parts.scheme != "https" or (parts.hostname or "").lower() not in hosts or port not in (None, 443):
        return f"link {url!r} is not https on {', '.join(sorted(hosts))}; not fetched"
    return None


def resolve_day_time(
    day: int, hour: int, minute: int, anchor: datetime | None
) -> datetime | None:
    """Place a day-of-month time ("18/0000Z", "180000Z") in the month
    nearest ``anchor``. Both agencies give the day only; a forecast never
    reaches more than a few days from its issuance."""
    if anchor is None:
        return None
    best: datetime | None = None
    for offset in (-1, 0, 1):
        year, month0 = divmod(anchor.year * 12 + anchor.month - 1 + offset, 12)
        try:
            candidate = datetime(year, month0 + 1, day, hour, minute, tzinfo=timezone.utc)
        except ValueError:
            continue
        if best is None or abs(candidate - anchor) < abs(best - anchor):
            best = candidate
    return best


# ---------------------------------------------------------------------------
# NHC forecast/advisory text
# ---------------------------------------------------------------------------


def parse_nhc_forecast_advisory(
    page: str, fallback_anchor: datetime | None = None
) -> dict:
    """Parse an NHC forecast/advisory (TCM) into an initial fix and points.

    ``page`` is the HTML page CurrentStorms.json links (product inside
    its one <pre>) or the bare product. Day-only times resolve against
    the product's own issuance line, else ``fallback_anchor``.
    ``tau_hours`` counts from the synoptic fix ("AT 17/1200Z CENTER WAS
    LOCATED NEAR"), as NHC labels its forecast hours; it is None when
    that line is missing.
    """
    pre = _PRE_RE.search(page)
    text = _normalize_newlines(html.unescape(pre.group(1)) if pre else page)

    issued: datetime | None = None
    stamp = _TCM_ISSUED_RE.search(text)
    if stamp and stamp.group(3) in _MONTHS:
        try:
            issued = datetime(
                int(stamp.group(5)),
                _MONTHS[stamp.group(3)],
                int(stamp.group(4)),
                int(stamp.group(1)),
                int(stamp.group(2)),
                tzinfo=timezone.utc,
            )
        except ValueError:
            issued = None
    anchor = issued or fallback_anchor

    def when(match: re.Match, first: int) -> datetime | None:
        return resolve_day_time(
            int(match.group(first)),
            int(match.group(first + 1)),
            int(match.group(first + 2)),
            anchor,
        )

    initial = None
    center = _TCM_CENTER_RE.search(text)
    if center:
        sustained = _TCM_SUSTAINED_RE.search(text)
        pressure = _TCM_PRESSURE_RE.search(text)
        initial = {
            "valid_time": _iso(when(center, 5)),
            "latitude": _coord(center.group(1), center.group(2)),
            "longitude": _coord(center.group(3), center.group(4)),
            "max_wind_kt": int(sustained.group(1)) if sustained else None,
            "gust_kt": int(sustained.group(2)) if sustained else None,
            "pressure_mb": int(pressure.group(1)) if pressure else None,
        }

    synoptic_match = _TCM_SYNOPTIC_RE.search(text)
    synoptic = when(synoptic_match, 1) if synoptic_match else None

    headers = list(_TCM_POINT_RE.finditer(text))
    points = []
    for i, header in enumerate(headers):
        block_end = headers[i + 1].start() if i + 1 < len(headers) else len(text)
        wind = _TCM_WIND_RE.search(text, header.end(), block_end)
        valid = when(header, 2)
        positioned = header.group(5) is not None
        points.append(
            {
                "tau_hours": _hours_between(synoptic, valid),
                "valid_time": _iso(valid),
                "latitude": _coord(header.group(5), header.group(6)) if positioned else None,
                "longitude": _coord(header.group(7), header.group(8)) if positioned else None,
                "max_wind_kt": int(wind.group(1)) if wind else None,
                "gust_kt": int(wind.group(2)) if wind else None,
                "status": header.group("status"),
                "section": header.group(1).lower(),
            }
        )

    advisory = _TCM_ADVISORY_RE.search(text)
    return {
        "advisory_number": advisory.group(1) if advisory else None,
        "issued": _iso(issued),
        "initial": initial,
        "points": points,
    }


# ---------------------------------------------------------------------------
# NHC cone KMZ
# ---------------------------------------------------------------------------


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _cone_root(kml: bytes) -> ET.Element:
    # Remote XML, stdlib parser (no new dependency): decode strictly as
    # UTF-8 so the check below sees every byte, then refuse any DOCTYPE.
    # Entity expansion and external entities both need a DTD; NHC's cone
    # KML has none. A non-UTF-8 body raises UnicodeDecodeError (a
    # ValueError).
    text = kml.decode("utf-8-sig")
    if re.search(r"<!DOCTYPE", text, re.I):
        raise ValueError("KML with a DOCTYPE refused")
    return ET.fromstring(text)


def _ring_from_root(root: ET.Element) -> list[tuple[float, float]]:
    rings = [el for el in root.iter() if _local(el.tag) == "LinearRing"]
    if len(rings) != 1:
        raise ValueError(f"expected one cone ring, found {len(rings)} rings")
    coords = next((el for el in rings[0].iter() if _local(el.tag) == "coordinates"), None)
    if coords is None or not (coords.text or "").strip():
        raise ValueError("cone ring has no coordinates")
    tokens = coords.text.split()
    if len(tokens) > _CONE_MAX_VERTICES + 1:  # + 1: the ring repeats its first vertex
        raise ValueError(
            f"cone ring has {len(tokens)} vertices, over the cap of {_CONE_MAX_VERTICES}"
        )
    ring: list[tuple[float, float]] = []
    for token in tokens:
        parts = token.split(",")
        if len(parts) < 2:
            raise ValueError(f"malformed KML coordinate {token!r}")
        lon, lat = float(parts[0]), float(parts[1])
        if not (-90.0 <= lat <= 90.0 and math.isfinite(lon)):
            raise ValueError(f"KML coordinate out of range {token!r}")
        ring.append((lat, lon))
    if len(ring) > 1 and ring[0] == ring[-1]:
        ring.pop()
    if len(ring) < 3:
        raise ValueError(f"cone ring has {len(ring)} vertices")
    return ring


def parse_cone_kml_ring(kml: bytes) -> list[tuple[float, float]]:
    """The cone KML's one ring as open (lat, lon) vertices. A KML with
    more than one ring (a split cone, a hole) is refused, never trimmed."""
    return _ring_from_root(_cone_root(kml))


def _segment_distance(
    point: tuple[float, float], start: tuple[float, float], end: tuple[float, float]
) -> float:
    (px, py), (ax, ay), (bx, by) = point, start, end
    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        return math.dist(point, start)
    t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
    return math.dist(point, (ax + t * dx, ay + t * dy))


def simplify_closed_ring(
    ring: list[tuple[float, float]], tolerance: float
) -> list[tuple[float, float]]:
    """Douglas-Peucker over a closed ring (first vertex repeated last),
    returned open. Every dropped vertex lies within ``tolerance`` (planar,
    in the coordinates' own units) of the kept outline."""
    count = len(ring)
    closed = count > 1 and ring[0] == ring[-1]
    if count < 4:
        return list(ring[:-1]) if closed else list(ring)
    keep = [False] * count
    keep[0] = keep[-1] = True
    stack = [(0, count - 1)]
    while stack:
        first, last = stack.pop()
        worst_index, worst = -1, -1.0
        for i in range(first + 1, last):
            distance = _segment_distance(ring[i], ring[first], ring[last])
            if distance > worst:
                worst_index, worst = i, distance
        if worst_index != -1 and worst > tolerance:
            keep[worst_index] = True
            stack.append((first, worst_index))
            stack.append((worst_index, last))
    kept = [vertex for vertex, flag in zip(ring, keep) if flag]
    return kept[:-1] if closed else kept


def parse_nhc_cone_kmz(content: bytes) -> dict:
    """Parse NHC's CONE.kmz into a simplified [lat, lon] outline.

    Raises zipfile.BadZipFile / ValueError / ET.ParseError (and the zip
    layer's zlib.error, RuntimeError, EOFError) on anything unreadable;
    the caller turns those into a named cone failure.
    """
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        members = [m for m in archive.infolist() if m.filename.lower().endswith(".kml")]
        if not members:
            raise ValueError("KMZ holds no .kml document")
        if members[0].file_size > _KML_MAX_BYTES:
            raise ValueError(
                f"KML of {members[0].file_size} bytes exceeds cap {_KML_MAX_BYTES}"
            )
        with archive.open(members[0]) as handle:
            kml = handle.read(_KML_MAX_BYTES + 1)
    if len(kml) > _KML_MAX_BYTES:
        raise ValueError(f"KML exceeds cap {_KML_MAX_BYTES}")

    root = _cone_root(kml)
    ring = _ring_from_root(root)
    extended: dict[str, str] = {}
    for el in root.iter():
        if _local(el.tag) == "Data" and el.get("name"):
            value = next((c for c in el if _local(c.tag) == "value"), None)
            if value is not None and value.text is not None:
                extended[el.get("name")] = value.text.strip()
    period = extended.get("fcstpd")

    outline = simplify_closed_ring(ring + [ring[0]], CONE_SIMPLIFY_TOLERANCE_DEG)
    return {
        "vertices": [[lat, lon] for lat, lon in outline],
        "vertex_count": len(outline),
        "source_vertex_count": len(ring),
        "simplify_tolerance_deg": CONE_SIMPLIFY_TOLERANCE_DEG,
        "advisory_number": extended.get("advisoryNum"),
        "forecast_period_hours": int(period) if period and period.isdigit() else None,
        "note": _CONE_NOTE,
    }


# ---------------------------------------------------------------------------
# JTWC warning text
# ---------------------------------------------------------------------------


def parse_jtwc_warning(text: str, fallback_anchor: datetime) -> dict:
    """Parse a JTWC warning text into an initial fix and forecast points.

    Day-only times resolve against the REMARKS date line ("17OCT26."),
    else ``fallback_anchor`` (the fetch time). ``section`` is JTWC's own
    block label: forecast, extended_outlook, long_range_outlook.
    """
    text = _normalize_newlines(text)

    anchor = fallback_anchor
    dated = _JTWC_DATE_RE.search(text)
    if dated:
        try:
            anchor = datetime(
                2000 + int(dated.group(3)),
                _MONTHS[dated.group(2)],
                int(dated.group(1)),
                tzinfo=timezone.utc,
            )
        except ValueError:
            anchor = fallback_anchor

    def when(match: re.Match, first: int) -> datetime | None:
        return resolve_day_time(
            int(match.group(first)),
            int(match.group(first + 1)),
            int(match.group(first + 2)),
            anchor,
        )

    header = _JTWC_HEADER_RE.search(text)
    number = _JTWC_NUMBER_RE.search(text)

    initial = None
    position = _JTWC_POSITION_RE.search(text)
    if position:
        present = _JTWC_PRESENT_RE.search(text)
        pressure = _JTWC_PRESSURE_RE.search(text)
        same_fix = pressure is not None and pressure.group(1, 2, 3) == position.group(1, 2, 3)
        initial = {
            "valid_time": _iso(when(position, 1)),
            "latitude": _coord(position.group(4), position.group(5)),
            "longitude": _coord(position.group(6), position.group(7)),
            "max_wind_kt": int(present.group(1)) if present else None,
            "gust_kt": int(present.group(2)) if present else None,
            "pressure_mb": int(pressure.group(4)) if same_fix else None,
        }

    sections = [(m.start(), m.group(1)) for m in _JTWC_SECTION_RE.finditer(text)]
    remarks_at = next((at for at, name in sections if name == "REMARKS"), len(text))
    blocks = [m for m in _JTWC_POINT_RE.finditer(text) if m.start() < remarks_at]
    points = []
    for i, block in enumerate(blocks):
        block_end = blocks[i + 1].start() if i + 1 < len(blocks) else remarks_at
        body = text[block.end() : block_end]
        wind = _JTWC_WIND_RE.search(body)
        status_lines = [
            line.strip()
            for line in body.split("\n")
            if line.strip() and not _JTWC_STRUCTURAL_RE.match(line.strip())
        ]
        section = None
        for at, name in sections:
            if at < block.start() and name in _JTWC_SECTIONS:
                section = _JTWC_SECTIONS[name]
        points.append(
            {
                "tau_hours": int(block.group(1)),
                "valid_time": _iso(when(block, 2)),
                "latitude": _coord(block.group(5), block.group(6)),
                "longitude": _coord(block.group(7), block.group(8)),
                "max_wind_kt": int(wind.group(1)) if wind else None,
                "gust_kt": int(wind.group(2)) if wind else None,
                "status": "; ".join(status_lines) or None,
                "section": section,
            }
        )

    return {
        "warning_number": int(number.group(1)) if number else None,
        "issued": _iso(when(header, 1)) if header else None,
        "initial": initial,
        "points": points,
    }


# ---------------------------------------------------------------------------
# Fetch orchestration
# ---------------------------------------------------------------------------


async def _nhc_track(fetcher: Fetcher, storm_id: str, advisory: dict, now: datetime) -> dict:
    url = advisory.get("url")
    out: dict[str, Any] = {
        "track_url": url,
        "track_status": "ok",
        "advisory_number": None,
        "issued": None,
        "initial": None,
        "points": [],
    }
    if not url:
        out["track_status"] = "not_linked"
        out["track_error"] = "CurrentStorms.json links no forecast/advisory for this storm"
        return out
    refused = _refused_link(url, _NHC_PRODUCT_HOSTS)
    if refused:
        out["track_status"] = "refused_link"
        out["track_error"] = "forecast/advisory " + refused
        return out

    expected = advisory.get("advNum")
    key = f"cyclones:nhc:tcm:{storm_id}:{expected}:{advisory.get('fileUpdateTime')}"
    page = await fetcher.get_text(
        url, source="nhc-forecast", cache_key=key, cache_ttl=_FORECAST_CACHE_TTL
    )
    if page is None:
        out["track_status"] = "fetch_failed"
        out["track_error"] = "NHC forecast/advisory text unavailable (no live or cached data)"
        return out

    parsed = parse_nhc_forecast_advisory(page, fallback_anchor=now)
    out.update(parsed)
    if not parsed["points"]:
        fetcher.cache.delete(key)
        out["track_status"] = "unparsed"
        out["track_error"] = "forecast/advisory page carried no parseable forecast points"
    elif expected and not _same_number(parsed["advisory_number"], expected):
        fetcher.cache.delete(key)
        out["track_status"] = "stale_advisory"
        out["expected_advisory_number"] = expected
        out["track_error"] = (
            f"forecast/advisory page shows advisory {parsed['advisory_number']}, "
            f"CurrentStorms.json lists {expected}"
        )
    return out


async def _nhc_cone(fetcher: Fetcher, storm_id: str, cone_meta: dict) -> dict:
    url = cone_meta.get("kmzFile")
    out: dict[str, Any] = {"cone_url": url, "cone_status": "ok", "cone": None}
    if not url:
        out["cone_status"] = "not_linked"
        return out
    refused = _refused_link(url, _NHC_PRODUCT_HOSTS)
    if refused:
        out["cone_status"] = "refused_link"
        out["cone_error"] = "cone KMZ " + refused
        return out

    key = f"cyclones:nhc:cone:{storm_id}:{cone_meta.get('advNum')}:{cone_meta.get('fileUpdateTime')}"
    cached = fetcher.cache.get(key)
    if cached is not None:
        out["cone"] = cached
        return out

    result = await fetcher.get_bytes(url, source="nhc-gis", max_bytes=_CONE_MAX_BYTES)
    if not result.ok:
        out["cone_status"] = "fetch_failed"
        out["cone_error"] = "NHC cone KMZ fetch failed: " + (
            result.error or f"HTTP {result.status}"
        )
        return out
    try:
        # In a worker thread: simplifying is CPU work and must not stall the event loop.
        cone = await asyncio.to_thread(parse_nhc_cone_kmz, result.content)
    except (zipfile.BadZipFile, zlib.error, ET.ParseError, ValueError, RuntimeError, EOFError) as exc:
        out["cone_status"] = "unparsed"
        out["cone_error"] = f"NHC cone KMZ unreadable: {type(exc).__name__}: {exc}"
        return out
    fetcher.cache.set(key, cone, _FORECAST_CACHE_TTL)
    out["cone"] = cone
    return out


async def _nhc_storm_forecast(fetcher: Fetcher, raw: dict, now: datetime) -> dict:
    storm_id = raw.get("id") or "unknown"
    track, cone = await asyncio.gather(
        _nhc_track(fetcher, storm_id, raw.get("forecastAdvisory") or {}, now),
        _nhc_cone(fetcher, storm_id, raw.get("trackCone") or {}),
    )
    return {"agency": "NHC", **track, **cone}


async def _jtwc_storm_forecast(fetcher: Fetcher, storm: dict, now: datetime) -> dict:
    url = storm.get("warning_text_url")
    out: dict[str, Any] = {
        "agency": "JTWC",
        "track_url": url,
        "track_status": "ok",
        "warning_number": None,
        "issued": None,
        "initial": None,
        "points": [],
        "cone_url": None,
        "cone_status": "not_published",
        "cone": None,
    }
    if not url:
        out["track_status"] = "not_linked"
        out["track_error"] = "the JTWC RSS item links no warning text for this storm"
        return out
    refused = _refused_link(url, _JTWC_PRODUCT_HOSTS)
    if refused:
        out["track_status"] = "refused_link"
        out["track_error"] = "warning text " + refused
        return out

    expected = storm.get("warning_number")
    key = f"jtwc:warning:{storm.get('id')}:{expected}:{storm.get('issued_at')}"
    text = await fetcher.get_text(
        url, source="jtwc-warning", cache_key=key, cache_ttl=_FORECAST_CACHE_TTL
    )
    if text is None:
        out["track_status"] = "fetch_failed"
        out["track_error"] = "JTWC warning text unavailable (no live or cached data)"
        return out

    parsed = parse_jtwc_warning(text, now)
    out.update(parsed)
    if not parsed["points"]:
        fetcher.cache.delete(key)
        out["track_status"] = "unparsed"
        out["track_error"] = "warning text carried no parseable forecast positions"
    elif (
        expected is not None
        and parsed["warning_number"] is not None
        and parsed["warning_number"] != expected
    ):
        fetcher.cache.delete(key)
        out["track_status"] = "stale_warning"
        out["expected_warning_number"] = expected
        out["track_error"] = (
            f"warning text shows warning {parsed['warning_number']}, "
            f"the RSS lists {expected}"
        )
    return out


def _capped(agency: str) -> dict:
    cone_status = "not_published" if agency == "JTWC" else "not_fetched_cap"
    return {
        "agency": agency,
        "track_status": "not_fetched_cap",
        "track_error": f"not fetched: per-call cap of {MAX_FORECAST_STORMS} storms",
        "initial": None,
        "points": [],
        "cone_status": cone_status,
        "cone": None,
    }


def _summarize(storms: list[dict]) -> tuple[list[str], bool]:
    """Name every missing track or cone; report whether any was a failure."""
    gaps: list[str] = []
    degraded = False
    for storm in storms:
        forecast = storm.get("forecast") or {}
        label = f"{storm.get('name') or 'unnamed'} ({storm.get('id') or 'no id'})"
        status = forecast.get("track_status")
        if status == "not_fetched_cap":
            gaps.append(
                f"{label}: forecast not fetched, per-call cap of {MAX_FORECAST_STORMS} storms"
            )
            degraded = True
            continue
        if status != "ok":
            gaps.append(f"{label}: forecast track {status}: {forecast.get('track_error')}")
            degraded = degraded or status in _FAILED_STATUSES
        cone_status = forecast.get("cone_status")
        if cone_status in ("fetch_failed", "unparsed", "refused_link"):
            gaps.append(f"{label}: no cone, {forecast.get('cone_error')}")
            degraded = True
        elif cone_status == "not_linked":
            gaps.append(f"{label}: no cone, CurrentStorms.json links no cone KMZ for it")
    return gaps, degraded


async def attach_nhc_forecasts(
    fetcher: Fetcher, entries: list[tuple[dict, dict]]
) -> tuple[list[str], bool]:
    """Give each NHC storm a ``forecast`` (track from its forecast/advisory,
    cone from its CONE.kmz). ``entries`` pairs each output storm with its
    raw CurrentStorms.json record. Returns (data_gaps, degraded)."""
    now = datetime.now(timezone.utc)
    targets = entries[:MAX_FORECAST_STORMS]
    forecasts = await asyncio.gather(
        *(_nhc_storm_forecast(fetcher, raw, now) for _, raw in targets)
    )
    for (storm, _), forecast in zip(targets, forecasts):
        storm["forecast"] = forecast
    for storm, _ in entries[MAX_FORECAST_STORMS:]:
        storm["forecast"] = _capped("NHC")
    return _summarize([storm for storm, _ in entries])


async def attach_jtwc_forecasts(
    fetcher: Fetcher, storms: list[dict]
) -> tuple[list[str], bool]:
    """Give each JTWC storm a ``forecast`` from its linked warning text.
    Returns (data_gaps, degraded)."""
    now = datetime.now(timezone.utc)
    targets = storms[:MAX_FORECAST_STORMS]
    forecasts = await asyncio.gather(
        *(_jtwc_storm_forecast(fetcher, storm, now) for storm in targets)
    )
    for storm, forecast in zip(targets, forecasts):
        storm["forecast"] = forecast
    for storm in storms[MAX_FORECAST_STORMS:]:
        storm["forecast"] = _capped("JTWC")
    gaps, degraded = _summarize(storms)
    if storms:
        gaps.append(_JTWC_CONE_GAP)
    return gaps, degraded
