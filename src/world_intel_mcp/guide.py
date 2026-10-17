"""Research guide served over MCP as resources and prompts.

A model connecting to this server sees 139 tools and, before this module,
nothing else: no map of which tool answers which question, and no
statement of what the data cannot support. The guide fills that gap with
three read-only resources and a set of worked research prompts
(``guide_prompts.py``):

- ``intel://guide``: how to ask a strong question, what an answer
  contains, what this server will not do, the prompts, and every
  registered tool grouped by its domain module. The tool list is built
  from the live registry (``tools._MODULES``) on every read, so it cannot
  drift from what ``tools/list`` serves.
- ``intel://guide/source-health``: circuit breakers, cache freshness and
  which keyed sources have their environment variables set. It reads
  in-process state and the cache database only; reading it never starts
  a fetch.
- ``intel://guide/measures``: a glossary of the measures the tools
  return and the mistakes they invite.

Gaps / not covered:
- Source health cannot see the collector's run log or schedule, nor the
  breaker state of other processes (collector, dashboard, CLI); it says
  so in its ``scope`` field.
- ``keyed_sources`` reports whether a variable is set, not whether the
  key is valid or within quota.
"""

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from mcp.server.lowlevel.helper_types import ReadResourceContents
from mcp.shared.exceptions import McpError
from mcp.types import (
    INVALID_PARAMS,
    ErrorData,
    GetPromptResult,
    Prompt,
    PromptArgument,
    PromptMessage,
    Resource,
    TextContent,
)

from . import guide_prompts
from .guide_prompts import PROMPTS, PromptArgumentError

GUIDE_URI = "intel://guide"
HEALTH_URI = "intel://guide/source-health"
MEASURES_URI = "intel://guide/measures"

# Display names for the domain modules under tools/. A module missing
# here still appears in the guide, titled from its module name.
_DOMAIN_TITLES = {
    "markets": "Markets and economy",
    "hazards": "Natural hazards and space",
    "conflict": "Conflict and military",
    "infrastructure": "Infrastructure and transport",
    "society": "News, society and migration",
    "intelligence": "Intelligence analysis",
    "geospatial": "Static geospatial datasets",
    "synthesis": "NLP and strategic synthesis",
    "finance": "Business finance",
    "vector": "Stored history (vector store)",
    "aoi": "Areas of interest, briefs and reports",
    "system": "System",
}


@dataclass(frozen=True)
class KeyedSource:
    source: str
    env: tuple[str, ...]
    without_key: str


# Every credential-like variable the source modules read. A test scans
# sources/*.py and fails when a variable is read there but missing here,
# or listed here but no longer read.
KEYED_SOURCES: tuple[KeyedSource, ...] = (
    KeyedSource(
        "ACLED",
        ("ACLED_EMAIL", "ACLED_PASSWORD"),
        "ACLED event, unrest and risk tools return an error naming the missing credentials.",
    ),
    KeyedSource(
        "UCDP",
        ("UCDP_ACCESS_TOKEN",),
        "Requests go out without a token. The UCDP source module notes the API "
        "has required one since 2026, so expect errors.",
    ),
    KeyedSource(
        "NASA FIRMS",
        ("NASA_FIRMS_API_KEY",),
        "intel_wildfires returns an error.",
    ),
    KeyedSource(
        "EIA",
        ("EIA_API_KEY",),
        "Energy prices, residential natural gas and electricity rates return an error.",
    ),
    KeyedSource(
        "FRED",
        ("FRED_API_KEY",),
        "intel_fred_series returns an error; the yield curve falls back to four Yahoo "
        "maturities (no 2s10s spread); central bank rates use a curated table.",
    ),
    KeyedSource(
        "Cloudflare Radar",
        ("CLOUDFLARE_API_TOKEN",),
        "Internet outages fall back to the IODA public API.",
    ),
    KeyedSource(
        "OpenSky",
        ("OPENSKY_CLIENT_ID", "OPENSKY_CLIENT_SECRET", "OPENSKY_USERNAME", "OPENSKY_PASSWORD"),
        "Requests are anonymous, with OpenSky's lower anonymous limits.",
    ),
    KeyedSource(
        "TomTom",
        ("TOMTOM_API_KEY",),
        "Traffic flow and incident tools return an error.",
    ),
    KeyedSource(
        "Windy Webcams",
        ("WINDY_API_KEY",),
        "intel_webcams returns an error.",
    ),
)


@dataclass(frozen=True)
class Domain:
    key: str
    title: str
    summary: str
    tools: tuple[Any, ...]


def _module_summary(module: Any) -> str:
    doc = (module.__doc__ or "").strip()
    first_paragraph = doc.split("\n\n", 1)[0]
    return " ".join(first_paragraph.split())


def domains(modules: list | None = None) -> list[Domain]:
    """The live registry grouped by domain module, in list_tools order."""
    if modules is None:
        from . import tools as tools_pkg

        modules = tools_pkg._MODULES
    out = []
    for module in modules:
        key = module.__name__.rsplit(".", 1)[-1]
        title = _DOMAIN_TITLES.get(key, key.replace("_", " ").capitalize())
        out.append(Domain(key, title, _module_summary(module), tuple(module.TOOLS)))
    return out


_SENTENCE_END = re.compile(r"(?<=[.!?])\s+(?=[A-Z(])")


def _one_line(description: str) -> str:
    first = _SENTENCE_END.split(" ".join(description.split()), 1)[0]
    # Em dashes in tool descriptions become hyphens: the guide is
    # published text.
    return first.replace(" — ", " - ").replace("—", "-")


def _arg_list(tool: Any) -> str:
    schema = tool.inputSchema or {}
    required = set(schema.get("required", []))
    names = [
        f"{name} (required)" if name in required else name
        for name in schema.get("properties", {})
    ]
    return ", ".join(names)


def tool_line(tool: Any) -> str:
    args = _arg_list(tool)
    args_part = f" [{args}]" if args else ""
    return f"- `{tool.name}`{args_part}: {_one_line(tool.description or '')}"


_GUIDE_INTRO = """\
# World Intelligence MCP: research guide

This server answers questions about the world with live public data:
markets, conflict, hazards, migration, aviation, cyber, infrastructure,
news and more. This guide is generated from the running server: {tools}
tools in {domains} domains, and {prompts} worked research prompts.

Other resources: `intel://guide/source-health` (which sources are
failing, stale or missing keys right now) and `intel://guide/measures`
(what the numbers mean and how they mislead).

## How to ask a strong question

- Name the period: "the last 30 days against the 30 days before", not
  "recently".
- Name the comparison: an earlier period, another place, or a stated
  baseline.
- Name the measure: event count or fatalities, price level or percentage
  change, people displaced at year end or arrivals during the year.
- Name the place: a country (many tools take an ISO-3 code), a region, a
  bounding box, or a saved area of interest.

A question with all four can be answered from tool output. A question
with none of them invites a story.

## What an answer from this server contains

- Values as the sources published them. Most responses name their source
  and carry a fetch timestamp; the date a value describes is often earlier
  than the fetch, so give both.
- Failures as failures. A source that is down or not configured returns
  an `error` field; composite tools list each domain they could not
  measure in `data_gaps`. Neither is ever an empty success.
- Citations. Briefs and digests number their sources.
- Old copies marked as old where the shape allows. When a live fetch fails
  and an older cached copy exists, dictionary responses carry `_stale` and
  `_stale_age_seconds`. Tools that reshape a response do not always pass
  that marker through, so check stale serves in the source-health
  resource.

## What this server will not do

- Trade, place orders, or say what to buy or sell.
- Forecast prices, elections or wars. Escalation, instability, risk and
  composite scores are weighted summaries of current readings against
  stated baselines, not probabilities.
- Prove causation. Series that move together, or one shortly after the
  other, may share a cause or be coincidence.
- Show history it did not record. Stored history (semantic search,
  timelines, baselines, area change detection) exists only for the time
  the collector or server was running and storing.
- Use keyed sources without their keys. Those tools return an error
  naming the variable; `intel://guide/source-health` shows which are set.
- See what its sources do not publish. Aircraft without a broadcast
  position, unlisted outlets, and regions outside a feed's coverage are
  absent, which is not the same as quiet.
- Confirm identities. A sanctions or entity name match is a text match.
"""


def render_guide(modules: list | None = None, prompts: tuple = PROMPTS) -> str:
    """The guide markdown, built from the live registry."""
    groups = domains(modules)
    total = sum(len(d.tools) for d in groups)
    parts = [
        _GUIDE_INTRO.format(tools=total, domains=len(groups), prompts=len(prompts)),
        "## Worked research prompts\n",
        "Each prompt names the tools to call and the limits to respect. "
        "Fetch one with prompts/get.\n",
    ]
    for p in prompts:
        args = ", ".join(
            f"{a.name} (required)" if a.required else a.name for a in p.arguments
        )
        args_part = f" [{args}]" if args else ""
        parts.append(f"- `{p.name}`{args_part}: {p.title}.")
    parts.append("\n## Tools by domain\n")
    for d in groups:
        parts.append(f"### {d.title} ({len(d.tools)})\n")
        if d.summary:
            parts.append(f"{d.summary}\n")
        parts.extend(tool_line(t) for t in d.tools)
        parts.append("")
    return "\n".join(parts).rstrip() + "\n"


MEASURES = """\
# Measures glossary

What the numbers returned by this server's tools mean, and the mistakes
they invite.

## Amount outstanding versus flow

An amount outstanding (a stock) is a level at one moment: people
displaced at the end of a year, a central bank's balance sheet, money
supply. A flow is what happened during a period: arrivals during a year,
fund inflows in a week, fatalities in a month. Never add a stock to a
flow. A change in a stock is a net figure: a displaced population that
grew by 100,000 may have had 300,000 arrivals and 200,000 returns.

## Growth versus acceleration

Growth is how fast a level changes (prices 3 percent higher than a year
earlier). Acceleration is how fast the growth rate itself changes
(growth up from 2 to 3 percent). Slower growth is still growth: inflation
falling from 4 to 3 percent means prices are still rising, only more
slowly.

## Percent versus percentage points

A rate that moves from 4 percent to 5 percent rose by 1 percentage
point, which is a 25 percent increase in the rate itself. Yields,
spreads, policy rates and shares are compared in points; one point is
100 basis points.

## Seasonally adjusted

Many series repeat a calendar pattern: holiday spending, winter heating,
summer travel. A seasonally adjusted series has the expected pattern
removed, so one month can be compared with the month before. An
unadjusted series should be compared with the same period a year
earlier. Never compare an adjusted figure with an unadjusted one; check
which a series is before comparing.

## Observation, publication and collection dates

- Observation date: the period a value describes (March unemployment,
  the day an event took place).
- Publication date: when the publisher released it, often weeks after
  the observation, with revisions later still.
- Collection date: when this server fetched or saved it.

A value fetched today can describe last month or last year; annual
datasets such as World Bank indicators often trail by a year or more.
Report the observation date. The fetch time only tells you how fresh the
copy is.

## Revisions

First releases are estimates. Economic series, event datasets and
displacement figures are revised after publication, sometimes by more
than the change being discussed. Say which release you used.

## Correlation and timing offsets

Two series that move together, or where one tends to move a few days
before the other, may share a cause, may be linked in either direction,
or may be coincidence. Coincidence becomes likely when many pairs or
many offsets are tried and only the best is reported. A lead or lag found
in one sample describes that sample. None of this shows that one thing
caused the other.

## Composite scores

Escalation, instability, risk, macro composite and stress scores are
weighted combinations of current inputs against stated baselines. They
summarize their inputs, they are not probabilities, and they hold no
information their inputs do not. Report the components with the score.

## Missing data is not zero

A source that failed, is not configured, or does not cover a place
returns an error, a note or a `data_gaps` entry. That means unknown, not
none. Zero events in a region a source covers is a measurement; zero
events in a region it does not cover is nothing at all.
"""


_HEALTH_SCOPE = (
    "circuit_breakers covers requests made by this server process since it "
    "started; the collector, dashboard and CLI keep their own. An empty map "
    "means no requests yet, not that every source is healthy.",
    "cache_freshness reads the shared cache database: the newest entry per "
    "key prefix, written by whichever process fetched it. A fresh entry shows "
    "a recent successful fetch, not that the collector is on schedule.",
    "keyed_sources says whether each variable is set in this server's "
    "environment. It never shows values and does not check that a key works.",
    "Not read here: vector store statistics (intel_vector_stats) and the "
    "collector's run history. Reading this resource starts no fetches.",
)


def source_health(
    breaker: Any,
    cache: Any,
    environ: Mapping[str, str] | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """Read-only health snapshot from in-process state and the cache DB."""
    env = os.environ if environ is None else environ
    breakers = breaker.status()
    freshness = cache.freshness()
    keyed = []
    for ks in KEYED_SOURCES:
        set_vars = [name for name in ks.env if env.get(name)]
        keyed.append(
            {
                "source": ks.source,
                "env": list(ks.env),
                "configured": [name for name in ks.env if name in set_vars],
                "missing": [name for name in ks.env if name not in set_vars],
                "without_key": ks.without_key,
            }
        )
    generated = datetime.fromtimestamp(time.time() if now is None else now, timezone.utc)
    return {
        "generated_at": generated.isoformat(),
        "scope": list(_HEALTH_SCOPE),
        "open_breakers": sorted(
            name for name, info in breakers.items() if info.get("status") != "closed"
        ),
        "stale_serves": {
            name: info["total_stale_serves"]
            for name, info in sorted(breakers.items())
            if info.get("total_stale_serves")
        },
        "stale_cache_prefixes": sorted(
            prefix for prefix, info in freshness.items() if info.get("is_stale")
        ),
        "circuit_breakers": breakers,
        "cache": cache.stats(),
        "cache_freshness": freshness,
        "keyed_sources": keyed,
    }


# ---------------------------------------------------------------------------
# MCP glue: server.py's handlers delegate here.
# ---------------------------------------------------------------------------

RESOURCES: tuple[Resource, ...] = (
    Resource(
        uri=GUIDE_URI,
        name="research-guide",
        title="Research guide",
        description=(
            "How to ask this server a strong question, what an answer contains, "
            "what it will not do, the worked prompts, and every tool by domain "
            "(generated from the live registry)."
        ),
        mimeType="text/markdown",
    ),
    Resource(
        uri=HEALTH_URI,
        name="source-health",
        title="Source health",
        description=(
            "Circuit breakers, cache freshness, stale serves and which keyed "
            "sources are configured. Read-only; starts no fetches."
        ),
        mimeType="application/json",
    ),
    Resource(
        uri=MEASURES_URI,
        name="measures-glossary",
        title="Measures glossary",
        description=(
            "Stock versus flow, growth versus acceleration, percentage points, "
            "seasonal adjustment, the three dates, correlation versus causation, "
            "missing data versus zero."
        ),
        mimeType="text/markdown",
    ),
)


def list_resources() -> list[Resource]:
    return list(RESOURCES)


def read_resource(uri: str) -> list[ReadResourceContents]:
    uri = str(uri)
    if uri == GUIDE_URI:
        return [ReadResourceContents(content=render_guide(), mime_type="text/markdown")]
    if uri == MEASURES_URI:
        return [ReadResourceContents(content=MEASURES, mime_type="text/markdown")]
    if uri == HEALTH_URI:
        from . import runtime

        health = source_health(runtime.breaker, runtime.cache)
        return [
            ReadResourceContents(
                content=json.dumps(health, indent=2, default=str),
                mime_type="application/json",
            )
        ]
    known = ", ".join(str(r.uri) for r in RESOURCES)
    raise McpError(
        ErrorData(code=INVALID_PARAMS, message=f"Unknown resource {uri}; available: {known}")
    )


def list_prompts() -> list[Prompt]:
    return [
        Prompt(
            name=p.name,
            title=p.title,
            description=p.description,
            arguments=[
                PromptArgument(name=a.name, description=a.description, required=a.required)
                for a in p.arguments
            ],
        )
        for p in PROMPTS
    ]


def get_prompt(name: str, arguments: dict[str, str] | None) -> GetPromptResult:
    prompt = guide_prompts.get(name)
    if prompt is None:
        known = ", ".join(p.name for p in PROMPTS)
        raise McpError(
            ErrorData(code=INVALID_PARAMS, message=f"Unknown prompt '{name}'; available: {known}")
        )
    try:
        text = guide_prompts.render(prompt, arguments)
    except PromptArgumentError as exc:
        raise McpError(ErrorData(code=INVALID_PARAMS, message=str(exc))) from exc
    return GetPromptResult(
        description=prompt.description,
        messages=[PromptMessage(role="user", content=TextContent(type="text", text=text))],
    )
