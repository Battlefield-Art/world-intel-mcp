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
wp2926web.txt), so a page and the feed entry pointing at it can be out
of step either way: a bin page can lag the feed, and the feed itself is
cached for 600 s. The advisory or warning number in the text is compared
with the feed's. A text older than the feed, or one whose number cannot
be read, is labelled stale and not kept; a text newer than the feed is
labelled feed_lags_text and kept. Both numbers are in the response:
``advisory_number`` / ``warning_number`` from the text,
``feed_advisory_number`` / ``feed_warning_number`` from the feed, which
the storm's other fields also come from.

Products are fetched from the linked URL only: a redirect is never
followed (it is reported as refused_link, naming its target), so
``track_url`` and ``cone_url`` are where the data came from. The fetcher
stops reading a text or KMZ once its decoded size passes the cap below
(text 1 MiB, KMZ 2 MiB), though one network read can decode to more
before that check (about 64 MiB of gzip). A KML member is read only when
it is stored or deflated and declares at most 100:1 expansion, and its
decompressed size is capped at 8 MiB.

Gaps / not covered: wind radii and seas are not parsed. Not observed
live: a Central Pacific (CPHC) forecast/advisory, potential tropical
cyclone products, a cone crossing the antimeridian (a cone with more
than one ring is refused, never trimmed), and a JTWC final warning (a
warning text without forecast blocks is reported unparsed). JTWC's
danger swath is not fetched. The cone is not checked against the
advisory number (its cache key carries the number and file time). A
failed text fetch is never answered from an expired copy, even one of
the same issuance.
"""

import asyncio
import html
import io
import logging
import math
import re
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

from ..fetcher import BinaryResult, Fetcher

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
# Body caps: the fetcher stops reading once a decoded body passes its cap.
_TEXT_MAX_BYTES = 1024 * 1024
_CONE_MAX_BYTES = 2 * 1024 * 1024
_KML_MAX_BYTES = 8 * 1024 * 1024
# Declared expansion bound for the KML member (the fixture's KML,
# deflated by zipfile, expands 2.8:1).
_KML_MAX_RATIO = 100
# Douglas-Peucker costs up to (kept vertices) x (ring vertices) distance
# evaluations, so both are capped: a ring past _CONE_MAX_VERTICES is
# refused before simplifying, an outline past _CONE_MAX_OUTLINE_VERTICES
# while simplifying. Trudy #17: 1,167 vertices in, 56 kept.
_CONE_MAX_VERTICES = 3_000
_CONE_MAX_OUTLINE_VERTICES = 500

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
# coverage facts, and feed_lags_text a text newer than its feed entry:
# named in data_gaps, not degradation. error is an unexpected exception
# while building one product; the rest of the response stands.
_FAILED_STATUSES = frozenset(
    {
        "fetch_failed",
        "unparsed",
        "not_fetched_cap",
        "stale_advisory",
        "stale_warning",
        "refused_link",
        "error",
    }
)
# Texts kept in the cache for their issuance.
_KEPT_STATUSES = frozenset({"ok", "feed_lags_text"})

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


def _issuance_number(value: Any) -> int | None:
    """An advisory or warning number as an int ("017" -> 17), or None."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().isdecimal():
        return int(value.strip())
    return None


def _compare_issuance(found: Any, listed: Any) -> str:
    """How the number a text shows stands against the one its feed lists:
    "same", "newer" (the text is ahead of the feed), "older", or
    "unreadable" (the text shows none). Numbers that are not plain
    integers can only match or not; a mismatch then counts as older."""
    if found is None:
        return "unreadable"
    text_number, feed_number = _issuance_number(found), _issuance_number(listed)
    if text_number is not None and feed_number is not None:
        if text_number == feed_number:
            return "same"
        return "newer" if text_number > feed_number else "older"
    same = str(found).strip().lstrip("0").upper() == str(listed).strip().lstrip("0").upper()
    return "same" if same else "older"


def _refused_link(url: Any, hosts: frozenset[str]) -> str | None:
    """Why a feed-supplied product link is not fetched, or None if it may be."""
    if not isinstance(url, str):
        return f"link of type {type(url).__name__} is not a string; not fetched"
    # urlsplit drops tab, CR and LF before it reads the host, and httpx
    # refuses them: refuse here, so the check and the request see one URL.
    if any(ch.isspace() or not ch.isprintable() for ch in url):
        return f"link {url!r} contains whitespace or control characters; not fetched"
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
    ring: list[tuple[float, float]], tolerance: float, max_vertices: int | None = None
) -> list[tuple[float, float]]:
    """Douglas-Peucker over a closed ring (first vertex repeated last),
    returned open. Every dropped vertex lies within ``tolerance`` (planar,
    in the coordinates' own units) of the kept outline.

    With ``max_vertices``, an outline that would keep more vertices
    raises ValueError as soon as it passes the cap, so the cost stays
    under about ``max_vertices`` times the ring's length in distance
    evaluations (each kept vertex costs at most one pass over the ring).
    """
    count = len(ring)
    closed = count > 1 and ring[0] == ring[-1]
    over_cap = f"simplified outline has more than {max_vertices} vertices (over the outline cap)"
    if count < 4:
        outline = list(ring[:-1]) if closed else list(ring)
        if max_vertices is not None and len(outline) > max_vertices:
            raise ValueError(over_cap)
        return outline
    keep = [False] * count
    keep[0] = keep[-1] = True
    kept = 1 if closed else 2  # vertices of the open outline returned
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
            kept += 1
            if max_vertices is not None and kept > max_vertices:
                raise ValueError(over_cap)
            stack.append((first, worst_index))
            stack.append((worst_index, last))
    outline = [vertex for vertex, flag in zip(ring, keep) if flag]
    return outline[:-1] if closed else outline


def parse_nhc_cone_kmz(content: bytes) -> dict:
    """Parse NHC's CONE.kmz into a simplified [lat, lon] outline.

    Raises on anything unreadable (zipfile.BadZipFile, ValueError,
    ET.ParseError, and whatever the zip layer raises); the caller turns
    any exception into a named cone failure.
    """
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        members = [m for m in archive.infolist() if m.filename.lower().endswith(".kml")]
        if not members:
            raise ValueError("KMZ holds no .kml document")
        member = members[0]
        # zipfile feeds bzip2 and LZMA members to decompressors with no
        # output bound, so only stored and deflated members are opened.
        if member.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
            raise ValueError(
                f"KML member uses zip compression method {member.compress_type}; "
                "only stored (0) or deflated (8) members are read"
            )
        if member.file_size > _KML_MAX_BYTES:
            raise ValueError(f"KML of {member.file_size} bytes exceeds cap {_KML_MAX_BYTES}")
        if member.file_size > _KML_MAX_RATIO * max(member.compress_size, 1):
            raise ValueError(
                f"KML member expands {member.compress_size} bytes to {member.file_size}, "
                f"past the {_KML_MAX_RATIO}:1 bound"
            )
        # A header that understates the member cannot get past this read:
        # zipfile stops at the declared size, and the CRC check refuses it.
        with archive.open(member) as handle:
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

    outline = simplify_closed_ring(
        ring + [ring[0]], CONE_SIMPLIFY_TOLERANCE_DEG, max_vertices=_CONE_MAX_OUTLINE_VERTICES
    )
    if len(outline) < 3:
        raise ValueError(
            f"simplified cone outline has {len(outline)} vertices; an outline needs at least 3"
        )
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


def _fetch_reason(result: BinaryResult) -> str:
    """One line on why a product fetch gave no body."""
    if result.status is not None and result.status >= 400:
        return f"HTTP {result.status}"
    return (result.error or f"HTTP {result.status}").split("\n", 1)[0]


def _redirect_refusal(result: BinaryResult) -> str:
    return (
        f"link answered HTTP {result.status}, a redirect to {result.location!r}; "
        "redirects are not followed, so nothing was fetched from it"
    )


def _unexpected(exc: BaseException) -> str:
    """Name an unexpected exception from one product's task. Anything
    that is not an Exception (cancellation, exit) is raised again."""
    if not isinstance(exc, Exception):
        raise exc
    return f"{type(exc).__name__}: {exc}"


async def _fetch_text(
    fetcher: Fetcher, url: str, source: str, key: str
) -> tuple[str | None, BinaryResult | None]:
    """A product text: this issuance's kept copy, else the body of ``url``
    itself (a redirect is not followed). Returns (text, None) for the kept
    copy, (text, result) for a fresh body, (None, result) for no body."""
    kept = fetcher.cache.get(key)
    if isinstance(kept, str):
        return kept, None
    result = await fetcher.get_bytes(
        url, source=source, max_bytes=_TEXT_MAX_BYTES, follow_redirects=False
    )
    if not result.ok:
        return None, result
    return result.content.decode("utf-8", errors="replace"), result


def _nhc_track_base(advisory: Any) -> dict:
    record = advisory if isinstance(advisory, dict) else {}
    url = record.get("url")
    return {
        "track_url": url if isinstance(url, str) else None,
        "track_status": "ok",
        "advisory_number": None,
        "feed_advisory_number": record.get("advNum"),
        "issued": None,
        "initial": None,
        "points": [],
    }


async def _nhc_track(fetcher: Fetcher, storm_id: str, advisory: Any, now: datetime) -> dict:
    out = _nhc_track_base(advisory)
    if not isinstance(advisory, dict):
        out["track_status"] = "refused_link"
        out["track_error"] = (
            f"CurrentStorms.json forecastAdvisory is a {type(advisory).__name__}, "
            "not an object; nothing fetched"
        )
        return out
    url = advisory.get("url")
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
    page, fetched = await _fetch_text(fetcher, url, "nhc-forecast", key)
    if page is None:
        if fetched.location is not None:
            out["track_status"] = "refused_link"
            out["track_error"] = "forecast/advisory " + _redirect_refusal(fetched)
        else:
            out["track_status"] = "fetch_failed"
            out["track_error"] = "NHC forecast/advisory text unavailable: " + _fetch_reason(fetched)
        return out

    parsed = parse_nhc_forecast_advisory(page, fallback_anchor=now)
    out.update(parsed)
    number = parsed["advisory_number"]
    standing = "same" if expected in (None, "") else _compare_issuance(number, expected)
    if not parsed["points"]:
        out["track_status"] = "unparsed"
        out["track_error"] = "forecast/advisory page carried no parseable forecast points"
    elif standing == "newer":
        out["track_status"] = "feed_lags_text"
        out["track_error"] = (
            f"forecast/advisory page shows advisory {number}, newer than advisory "
            f"{expected} that CurrentStorms.json lists; the track is advisory {number}, "
            "the storm's other fields are the feed's"
        )
    elif standing == "unreadable":
        out["track_status"] = "stale_advisory"
        out["expected_advisory_number"] = expected
        out["track_error"] = (
            "forecast/advisory page carries no readable advisory number, so it cannot "
            f"be matched to advisory {expected} that CurrentStorms.json lists"
        )
    elif standing == "older":
        out["track_status"] = "stale_advisory"
        out["expected_advisory_number"] = expected
        out["track_error"] = (
            f"forecast/advisory page shows advisory {number}, "
            f"CurrentStorms.json lists {expected}"
        )
    if fetched is not None and out["track_status"] in _KEPT_STATUSES:
        fetcher.cache.set(key, page, _FORECAST_CACHE_TTL)
    return out


def _nhc_cone_base(cone_meta: Any) -> dict:
    url = cone_meta.get("kmzFile") if isinstance(cone_meta, dict) else None
    return {"cone_url": url if isinstance(url, str) else None, "cone_status": "ok", "cone": None}


async def _nhc_cone(fetcher: Fetcher, storm_id: str, cone_meta: Any) -> dict:
    out = _nhc_cone_base(cone_meta)
    if not isinstance(cone_meta, dict):
        out["cone_status"] = "refused_link"
        out["cone_error"] = (
            f"CurrentStorms.json trackCone is a {type(cone_meta).__name__}, "
            "not an object; nothing fetched"
        )
        return out
    url = cone_meta.get("kmzFile")
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

    result = await fetcher.get_bytes(
        url, source="nhc-gis", max_bytes=_CONE_MAX_BYTES, follow_redirects=False
    )
    if result.location is not None:
        out["cone_status"] = "refused_link"
        out["cone_error"] = "cone KMZ " + _redirect_refusal(result)
        return out
    if not result.ok:
        out["cone_status"] = "fetch_failed"
        out["cone_error"] = "NHC cone KMZ fetch failed: " + _fetch_reason(result)
        return out
    try:
        # In a worker thread: simplifying is CPU work and must not stall the event loop.
        cone = await asyncio.to_thread(parse_nhc_cone_kmz, result.content)
    except Exception as exc:  # any unreadable KMZ is a named gap, never a failed call
        out["cone_status"] = "unparsed"
        out["cone_error"] = f"NHC cone KMZ unreadable: {type(exc).__name__}: {exc}"
        return out
    fetcher.cache.set(key, cone, _FORECAST_CACHE_TTL)
    out["cone"] = cone
    return out


async def _nhc_storm_forecast(fetcher: Fetcher, raw: dict, now: datetime) -> dict:
    storm_id = raw.get("id") or "unknown"
    advisory = raw.get("forecastAdvisory") or {}
    cone_meta = raw.get("trackCone") or {}
    # One product's unexpected exception is named as that product's
    # "error"; it never fails the other product, the storm or the call.
    track, cone = await asyncio.gather(
        _nhc_track(fetcher, storm_id, advisory, now),
        _nhc_cone(fetcher, storm_id, cone_meta),
        return_exceptions=True,
    )
    if isinstance(track, BaseException):
        reason = _unexpected(track)
        track = _nhc_track_base(advisory)
        track.update(track_status="error", track_error="forecast track not built: " + reason)
    if isinstance(cone, BaseException):
        reason = _unexpected(cone)
        cone = _nhc_cone_base(cone_meta)
        cone.update(cone_status="error", cone_error="cone not built: " + reason)
    return {"agency": "NHC", **track, **cone}


def _jtwc_base(storm: dict) -> dict:
    url = storm.get("warning_text_url")
    return {
        "agency": "JTWC",
        "track_url": url if isinstance(url, str) else None,
        "track_status": "ok",
        "warning_number": None,
        "feed_warning_number": storm.get("warning_number"),
        "issued": None,
        "initial": None,
        "points": [],
        "cone_url": None,
        "cone_status": "not_published",
        "cone": None,
    }


async def _jtwc_storm_forecast(fetcher: Fetcher, storm: dict, now: datetime) -> dict:
    out = _jtwc_base(storm)
    url = storm.get("warning_text_url")
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
    text, fetched = await _fetch_text(fetcher, url, "jtwc-warning", key)
    if text is None:
        if fetched.location is not None:
            out["track_status"] = "refused_link"
            out["track_error"] = "warning text " + _redirect_refusal(fetched)
        else:
            out["track_status"] = "fetch_failed"
            out["track_error"] = "JTWC warning text unavailable: " + _fetch_reason(fetched)
        return out

    parsed = parse_jtwc_warning(text, now)
    out.update(parsed)
    number = parsed["warning_number"]
    standing = "same" if expected is None else _compare_issuance(number, expected)
    if not parsed["points"]:
        out["track_status"] = "unparsed"
        out["track_error"] = "warning text carried no parseable forecast positions"
    elif standing == "newer":
        out["track_status"] = "feed_lags_text"
        out["track_error"] = (
            f"warning text shows warning {number}, newer than warning {expected} that "
            f"the RSS lists; the track is warning {number}, the storm's other fields "
            "are the RSS's"
        )
    elif standing == "unreadable":
        out["track_status"] = "stale_warning"
        out["expected_warning_number"] = expected
        out["track_error"] = (
            "warning text carries no readable warning number, so it cannot be matched "
            f"to warning {expected} that the RSS lists"
        )
    elif standing == "older":
        out["track_status"] = "stale_warning"
        out["expected_warning_number"] = expected
        out["track_error"] = f"warning text shows warning {number}, the RSS lists {expected}"
    if fetched is not None and out["track_status"] in _KEPT_STATUSES:
        fetcher.cache.set(key, text, _FORECAST_CACHE_TTL)
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
        if cone_status in ("fetch_failed", "unparsed", "refused_link", "error"):
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
    # One storm's unexpected exception is named as its track "error";
    # it never fails the other storms or the call.
    forecasts = await asyncio.gather(
        *(_jtwc_storm_forecast(fetcher, storm, now) for storm in targets),
        return_exceptions=True,
    )
    for storm, forecast in zip(targets, forecasts):
        if isinstance(forecast, BaseException):
            reason = _unexpected(forecast)
            forecast = _jtwc_base(storm)
            forecast.update(track_status="error", track_error="forecast track not built: " + reason)
        storm["forecast"] = forecast
    for storm in storms[MAX_FORECAST_STORMS:]:
        storm["forecast"] = _capped("JTWC")
    gaps, degraded = _summarize(storms)
    if storms:
        gaps.append(_JTWC_CONE_GAP)
    return gaps, degraded
