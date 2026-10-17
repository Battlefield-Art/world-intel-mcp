"""Worked research questions served as MCP prompts.

Each prompt is a research question a model can run against this server:
the tools to call (named exactly as registered), what each call is for,
and the limits an honest answer has to respect. ``guide.py`` turns these
into MCP ``Prompt`` / ``GetPromptResult`` objects; this module holds the
data and the text rendering only.

Every tool named here must exist in the live registry, and every
``intel_*`` name that appears anywhere in a rendered prompt must be one
of that prompt's listed tools. ``tests/test_guide.py`` pins both, so a
renamed or removed tool fails the suite instead of leaving a prompt that
sends a model to a tool that is not there.
"""

import re
from dataclasses import dataclass

# Rendered into every prompt after its own limits.
COMMON_LIMITS: tuple[str, ...] = (
    "A tool that returns an error, a note or a data_gaps entry has told you "
    "what is missing: report it as missing. Missing data is never zero, calm "
    "or peace.",
    "Give each figure the date it describes (the observation date) as well "
    "as when it was fetched; they are often different.",
    "Describe what the data show. This server does not forecast prices, "
    "give trading advice or prove causation.",
)

ANSWER_SHAPE = (
    "Answer with: the place, period and measure you used; each number with "
    "its unit and date; the comparison you were asked for; every data gap; "
    "and one line on what this data cannot tell."
)

_MAX_ARG_CHARS = 200
_PLACEHOLDER = re.compile(r"\{([a-z_]+)\}")
TOOL_NAME = re.compile(r"\bintel_[a-z0-9_]+\b")


@dataclass(frozen=True)
class PromptArg:
    name: str
    description: str
    required: bool = False
    default: str | None = None
    choices: tuple[str, ...] = ()


@dataclass(frozen=True)
class ResearchPrompt:
    name: str
    title: str
    domain: str
    question: str
    steps: tuple[tuple[str, str], ...]
    limits: tuple[str, ...]
    arguments: tuple[PromptArg, ...] = ()

    @property
    def tools(self) -> tuple[str, ...]:
        return tuple(tool for tool, _ in self.steps)

    @property
    def description(self) -> str:
        return f"{self.title}. Uses {', '.join(self.tools)}."


class PromptArgumentError(ValueError):
    """A prompt was requested with missing, unknown or invalid arguments."""


def _resolve_args(prompt: ResearchPrompt, given: dict[str, str] | None) -> dict[str, str]:
    given = dict(given or {})
    known = {a.name for a in prompt.arguments}
    unknown = sorted(set(given) - known)
    if unknown:
        accepted = ", ".join(sorted(known)) or "none"
        raise PromptArgumentError(
            f"Prompt '{prompt.name}' does not take {', '.join(unknown)} "
            f"(accepted: {accepted})"
        )
    values: dict[str, str] = {}
    for arg in prompt.arguments:
        raw = given.get(arg.name)
        value = str(raw).strip()[:_MAX_ARG_CHARS] if raw is not None else ""
        if not value:
            if arg.required:
                raise PromptArgumentError(
                    f"Prompt '{prompt.name}' needs argument '{arg.name}': "
                    f"{arg.description}"
                )
            value = arg.default or ""
        if arg.choices and value not in arg.choices:
            raise PromptArgumentError(
                f"Prompt '{prompt.name}' argument '{arg.name}' must be one of "
                f"{', '.join(arg.choices)} (got '{value}')"
            )
        values[arg.name] = value
    return values


def render(prompt: ResearchPrompt, arguments: dict[str, str] | None = None) -> str:
    """The prompt's user-message text with its arguments filled in."""
    values = _resolve_args(prompt, arguments)

    def fill(text: str) -> str:
        return _PLACEHOLDER.sub(lambda m: values[m.group(1)], text)

    lines = [f"Research question: {fill(prompt.question)}", "", "Tools to call:"]
    for i, (tool, purpose) in enumerate(prompt.steps, 1):
        lines.append(f"{i}. {tool}: {fill(purpose)}")
    lines += ["", "Limits to respect:"]
    lines += [f"- {fill(limit)}" for limit in prompt.limits]
    lines += [f"- {limit}" for limit in COMMON_LIMITS]
    lines += ["", ANSWER_SHAPE]
    return "\n".join(lines)


_COUNTRY = PromptArg("country", "Country name, e.g. Sudan", required=True)
_ISO3 = PromptArg("country", "ISO alpha-3 country code, e.g. UKR", required=True)


PROMPTS: tuple[ResearchPrompt, ...] = (
    # --- conflict -------------------------------------------------------
    ResearchPrompt(
        name="conflict-trend",
        title="Conflict trend in one country against the previous period",
        domain="conflict",
        question=(
            "How has organized violence in {country} changed over the last "
            "{days} days, and is it more or less intense than the {days} days "
            "before that?"
        ),
        arguments=(
            _COUNTRY,
            PromptArg("days", "Length of each comparison window in days", default="30"),
        ),
        steps=(
            (
                "intel_ucdp_events",
                "state-based violence events. It has no country filter, so ask "
                "for twice the window with a high limit (it caps results), keep "
                "the events in {country} yourself, split them at the midpoint "
                "and say that you filtered",
            ),
            (
                "intel_acled_events",
                "events for {country} over both windows (needs ACLED_EMAIL and "
                "ACLED_PASSWORD; without them it returns an error)",
            ),
            ("intel_unrest_events", "protests and riots in {country} (also ACLED), kept apart from armed violence"),
        ),
        limits=(
            "Event datasets are revised and arrive late, so the most recent "
            "days are the least complete. A quiet final week is not proof of calm.",
            "Count events and fatalities separately: fewer events with more "
            "deaths is not de-escalation.",
            "ACLED and UCDP define an event differently. Compare each source "
            "with itself across the two windows, never one source's count "
            "against the other's.",
        ),
    ),
    ResearchPrompt(
        name="hotspot-escalation",
        title="Which hotspots are escalating and why",
        domain="conflict",
        question=(
            "Which of the built-in hotspots score highest for escalation right "
            "now, and which measured component drives each score?"
        ),
        steps=(
            ("intel_hotspot_escalation", "scores and their components for the 22 hotspots"),
            ("intel_military_surge", "foreign military aircraft concentrations against baseline"),
            ("intel_theater_posture", "military aircraft presence by theater"),
        ),
        limits=(
            "Escalation scores are weighted heuristics against fixed "
            "baselines, renormalized over the components actually measured. "
            "They are not probabilities of war. List the unavailable_components.",
            "Aircraft tracking sees only aircraft that broadcast a position, "
            "and many military flights do not, so counts are a floor.",
        ),
    ),
    # --- markets and economy ---------------------------------------------
    ResearchPrompt(
        name="market-risk-snapshot",
        title="Cross-asset risk picture today",
        domain="markets",
        question=(
            "What does the current risk picture look like across equities, "
            "volatility, the dollar, rates and crypto?"
        ),
        steps=(
            ("intel_macro_signals", "Fear and Greed, VIX, DXY, gold, 10-year yield, BTC dominance"),
            ("intel_sector_heatmap", "which US equity sectors are leading and lagging"),
            ("intel_yield_curve", "curve shape and spreads"),
            ("intel_macro_composite", "the composite score and the inputs it weighs"),
        ),
        limits=(
            "Report current readings and their recent change only. Quote the "
            "composite score next to its inputs, since it is a weighted summary "
            "of them.",
        ),
    ),
    ResearchPrompt(
        name="yield-curve-check",
        title="Is the Treasury curve inverted, and by how much",
        domain="markets",
        question=(
            "Is the US Treasury yield curve inverted today, by how much, and "
            "how has the 10-year minus 2-year spread moved recently?"
        ),
        steps=(
            ("intel_yield_curve", "maturities, the 2s10s and 3m10y spreads and the inversion flag"),
            ("intel_fred_series", "series T10Y2Y for the spread's recent history (needs FRED_API_KEY)"),
            ("intel_central_bank_rates", "the policy rate the short end is anchored to"),
        ),
        limits=(
            "Spreads are in percentage points. A move from 0.10 to 0.30 is "
            "0.20 points, not a 200 percent rise.",
            "Without FRED_API_KEY the curve falls back to four Yahoo "
            "maturities (3M, 5Y, 10Y, 30Y), so spread_2s10s comes back null. "
            "Say so; do not swap in another spread without saying which.",
            "An inverted curve describes today's yields. It is not a "
            "recession forecast.",
        ),
    ),
    ResearchPrompt(
        name="central-bank-compare",
        title="Where major central bank policy rates stand",
        domain="markets",
        question=(
            "Where do the major central banks' policy rates stand, and which "
            "of them changed most recently?"
        ),
        steps=(
            ("intel_central_bank_rates", "policy rates for 15 central banks with their source"),
            ("intel_fred_series", "series DFF for the US effective federal funds rate history (needs FRED_API_KEY)"),
            ("intel_major_crosses", "how the major currencies sit against each other today"),
        ),
        limits=(
            "Without FRED_API_KEY some rates come from a curated table. Give "
            "each rate's as-of date and source, and never present a curated "
            "value as live.",
            "Rate changes are in percentage points or basis points (one point "
            "is 100 basis points), not percent of the rate.",
        ),
    ),
    ResearchPrompt(
        name="fx-move",
        title="A currency pair's move and the coverage around it",
        domain="markets",
        question=(
            "How has {symbol} moved against {base} over the last {days} days, "
            "and what news coverage ran alongside the largest daily moves?"
        ),
        arguments=(
            PromptArg("symbol", "Quote currency, ISO code such as EUR", required=True),
            PromptArg("base", "Base currency, ISO code", default="USD"),
            PromptArg("days", "Days of history", default="30"),
        ),
        steps=(
            ("intel_forex_timeseries", "base {base}, symbol {symbol}, days {days}"),
            ("intel_major_crosses", "where the pair sits among the majors today"),
            ("intel_gdelt_search", "articles about the currency around the dates of the largest moves"),
        ),
        limits=(
            "ECB reference rates are published once per business day. A "
            "weekend or holiday has no observation, which is not a zero change.",
            "News published alongside a move is context. Timing alone does "
            "not show that the news caused the move.",
        ),
    ),
    ResearchPrompt(
        name="company-events",
        title="A company's recent disclosures and results",
        domain="markets",
        question=(
            "What material events has {ticker} disclosed recently, and how did "
            "its reported results compare with estimates?"
        ),
        arguments=(PromptArg("ticker", "Stock ticker, e.g. AAPL", required=True),),
        steps=(
            ("intel_company_profile", "quote, sector and recent news for {ticker}"),
            ("intel_company_filings", "recent 10-K, 10-Q and 8-K filings for {ticker}"),
            ("intel_earnings_surprise", "actual against estimated earnings for {ticker}"),
        ),
        limits=(
            "A filing date is when the company filed, not when the event "
            "happened; give both when the filing states the event date.",
            "No buy, sell or hold view: describe what was disclosed.",
        ),
    ),
    ResearchPrompt(
        name="energy-pressure",
        title="Energy prices from futures to the pump",
        domain="markets",
        question=(
            "Are energy prices rising for producers and for consumers, and is "
            "any chokepoint exposure relevant to the move?"
        ),
        steps=(
            ("intel_commodity_quotes", "crude oil and natural gas futures"),
            ("intel_energy_prices", "EIA crude and gas prices (needs EIA_API_KEY)"),
            ("intel_gas_prices", "AAA US retail gasoline and diesel averages"),
            ("intel_trade_routes", "oil flows through the major chokepoints"),
        ),
        limits=(
            "Futures, wholesale and retail pump prices are different measures "
            "with different dates. Never join them into one series.",
            "Chokepoint oil flows are curated reference values, not live "
            "tanker counts.",
        ),
    ),
    ResearchPrompt(
        name="crypto-structure",
        title="Crypto market structure today",
        domain="markets",
        question=(
            "What is the state of the crypto market today: prices, stablecoin "
            "pegs, spot ETF trading and Bitcoin's trend indicators?"
        ),
        steps=(
            ("intel_crypto_quotes", "prices and market caps of the largest coins"),
            ("intel_stablecoin_status", "whether the major stablecoins hold their peg"),
            ("intel_etf_flows", "Bitcoin spot ETF prices and trading volume"),
            ("intel_btc_technicals", "moving averages, Mayer Multiple and distance from the high"),
        ),
        limits=(
            "intel_etf_flows reports prices and trading volume, not creations "
            "or redemptions. Volume is not net flow.",
            "Technical indicators summarize past prices; they do not predict "
            "the next move.",
        ),
    ),
    # --- hazards ----------------------------------------------------------
    ResearchPrompt(
        name="earthquake-exposure",
        title="Recent strong earthquakes near people",
        domain="hazards",
        question=(
            "Which earthquakes of magnitude {min_magnitude} or more in the "
            "last {hours} hours struck near large populations, and did any "
            "raise a disaster alert?"
        ),
        arguments=(
            PromptArg("min_magnitude", "Smallest magnitude to include", default="5.5"),
            PromptArg("hours", "Lookback in hours", default="48"),
        ),
        steps=(
            ("intel_earthquakes", "min_magnitude {min_magnitude}, hours {hours}"),
            ("intel_population_exposure", "event_types earthquake, for people near each event"),
            ("intel_disaster_alerts", "event_type EQ, for the GDACS alert level of the same events"),
        ),
        limits=(
            "Exposure adds up the population of major cities inside the "
            "radius. It is not a casualty estimate and misses rural people.",
            "Magnitudes and locations are revised in the first hours after an "
            "event; say when you read them.",
        ),
    ),
    ResearchPrompt(
        name="severe-weather",
        title="Severe weather warnings in the covered regions",
        domain="hazards",
        question=(
            "What severe weather warnings are active in {us_area} and in "
            "{europe_country}, and are any tropical cyclones active?"
        ),
        arguments=(
            PromptArg("us_area", "Two-letter US state code, e.g. TX", default="the United States"),
            PromptArg("europe_country", "Meteoalarm country, e.g. france", default="Europe"),
        ),
        steps=(
            ("intel_weather_alerts", "US National Weather Service alerts (pass area when a state was given)"),
            ("intel_meteoalarm_alerts", "European warnings (one country per call; without country it lists the countries)"),
            ("intel_cyclones", "Atlantic and eastern and central Pacific storms from the National Hurricane Center"),
            ("intel_jtwc_cyclones", "western Pacific, Indian Ocean and Southern Hemisphere storms from JTWC"),
        ),
        limits=(
            "NWS covers the US and Meteoalarm covers its participating "
            "European countries only. No alert outside those areas says "
            "nothing about the weather there.",
            "NHC and JTWC cover different basins and can label the same storm "
            "differently. Match storms by position and time before counting.",
        ),
    ),
    ResearchPrompt(
        name="wildfire-situation",
        title="Wildfire activity and the conditions around it",
        domain="hazards",
        question="How active are wildfires in {region} now, and are temperatures or rainfall abnormal there?",
        arguments=(
            PromptArg(
                "region",
                "FIRMS region: north_america, south_america, europe, africa, "
                "middle_east, south_asia, east_asia, southeast_asia or oceania",
                default="all FIRMS regions",
            ),
        ),
        steps=(
            ("intel_wildfires", "satellite fire detections (needs NASA_FIRMS_API_KEY)"),
            ("intel_environmental_events", "category wildfires, for named fires NASA EONET tracks"),
            ("intel_climate_anomalies", "temperature and precipitation against the prior year"),
        ),
        limits=(
            "FIRMS detections are satellite heat signatures, not mapped fire "
            "perimeters; one large fire produces many detections.",
            "Without NASA_FIRMS_API_KEY the fire tool returns an error. Report "
            "that, not zero fires.",
            "Climate anomalies are measured against the prior year, not a "
            "long-term normal.",
        ),
    ),
    ResearchPrompt(
        name="space-weather-impact",
        title="Space weather and coincident outages",
        domain="hazards",
        question=(
            "How strong is current geomagnetic and solar activity, and are "
            "internet or cloud outages being reported at the same time?"
        ),
        steps=(
            ("intel_space_weather", "Kp index, flare class, solar wind and SWPC alerts"),
            ("intel_internet_outages", "internet outages reported over the last 7 days"),
            ("intel_service_status", "active incidents at the major cloud providers"),
        ),
        limits=(
            "An outage at the same time as a storm does not show the storm "
            "caused it; most outages have local causes.",
            "Kp runs 0 to 9 on a quasi-logarithmic scale: Kp 8 is far more "
            "than twice Kp 4.",
        ),
    ),
    # --- migration --------------------------------------------------------
    ResearchPrompt(
        name="displacement-history",
        title="A country's displacement record since 1951",
        domain="migration",
        question=(
            "How has displacement linked to {country} as {role} changed since "
            "records began, and which years saw the largest waves?"
        ),
        arguments=(
            _COUNTRY,
            PromptArg(
                "role",
                "origin (its people abroad, plus IDPs) or host (people it hosts)",
                default="origin",
                choices=("origin", "host"),
            ),
        ),
        steps=(
            ("intel_migration_history", "country {country}, role {role}"),
            ("intel_migration_flows", "the latest year's corridors with {country} as origin or destination"),
            ("intel_displacement_summary", "the global totals for context"),
        ),
        limits=(
            "UNHCR figures are year-end stocks (people displaced at the end of "
            "each year), not arrivals during the year. A change between two "
            "years is a net change in the stock.",
            "UNHCR revises past years. Say which reporting year you used.",
        ),
    ),
    ResearchPrompt(
        name="migration-surges",
        title="The largest year-over-year displacement changes",
        domain="migration",
        question=(
            "Which origins and corridors had the largest year-over-year change "
            "in displacement in {year}, and how does that compare with net "
            "migration from all causes?"
        ),
        arguments=(PromptArg("year", "Reporting year", default="the latest year with data"),),
        steps=(
            ("intel_migration_surges", "largest increases and decreases (pass year if one was given)"),
            ("intel_net_migration", "World Bank net migration for the leading countries"),
            ("intel_humanitarian_summary", "HDX crisis datasets for the leading origins"),
        ),
        limits=(
            "World Bank net migration counts every reason, work included; "
            "UNHCR counts forcibly displaced people. Show them side by side "
            "and never add them together.",
            "The mass, major and notable levels are the tool's stated "
            "thresholds, not official designations.",
        ),
    ),
    # --- aviation ---------------------------------------------------------
    ResearchPrompt(
        name="airspace-anomaly",
        title="Unusual business jet or military air activity",
        domain="aviation",
        question=(
            "Is {cohort} air activity over {region} unusual for this time of "
            "the week, and if so, where did the extra aircraft come from?"
        ),
        arguments=(
            PromptArg("cohort", "bizjet or military", default="bizjet", choices=("bizjet", "military")),
            PromptArg(
                "region",
                "global, conus, north_america, europe, middle_east or east_asia",
                default="global",
            ),
        ),
        steps=(
            ("intel_jet_cohort_snapshot", "cohort {cohort}, region {region}; also builds the tracked cohort"),
            ("intel_jet_activity_anomaly", "the latest half-hour against the same slot on earlier weeks"),
            ("intel_jet_excess_origins", "only if an excess is flagged: where the extra departures came from"),
            ("intel_military_flights", "for the military cohort, a second source of positions"),
        ),
        limits=(
            "ADS-B shows only aircraft whose transponders volunteer receivers "
            "pick up; many military and some private aircraft do not broadcast "
            "a usable position, so counts are a floor.",
            "An anomaly is a deviation from the same slot on earlier weeks. "
            "It is not evidence of anyone's intent.",
            "The anomaly check needs a learned cohort; a fresh install reports "
            "that gap first.",
        ),
    ),
    ResearchPrompt(
        name="airport-disruption",
        title="US airport delays and their likely weather context",
        domain="aviation",
        question="Which major US airports have delays now, and is severe weather active near them?",
        steps=(
            ("intel_airport_delays", "FAA delays at 20 major US airports"),
            ("intel_aviation_domestic", "the airborne traffic snapshot for scale"),
            ("intel_weather_alerts", "NWS alerts for the states of the delayed airports"),
        ),
        limits=(
            "FAA data covers major US airports only.",
            "The airborne total is a snapshot of aircraft OpenSky receives, "
            "not a count of all flights.",
        ),
    ),
    # --- cyber and infrastructure ----------------------------------------
    ResearchPrompt(
        name="cyber-landscape",
        title="Current cyber threat landscape",
        domain="cyber",
        question=(
            "What does the current cyber threat picture look like: newly "
            "known-exploited vulnerabilities, active malware infrastructure, "
            "and which threat actors are in the headlines?"
        ),
        steps=(
            ("intel_cyber_threats", "Feodo, CISA KEV, SANS and URLhaus indicators"),
            ("intel_keyword_spikes", "CVE and APT names rising in headlines against baseline"),
            ("intel_extract_entities", "CVEs, APT groups and organizations in recent headlines"),
        ),
        limits=(
            "Feeds list reported indicators and known-exploited "
            "vulnerabilities. A listing is not attribution and does not show "
            "that any particular organization was hit.",
            "Keyword spikes measure mentions in headlines, not attack volume.",
        ),
    ),
    ResearchPrompt(
        name="internet-disruption",
        title="Internet disruption in one country",
        domain="infrastructure",
        question=(
            "Is internet connectivity in {country} disrupted now, and could a "
            "cable corridor failure explain it?"
        ),
        arguments=(
            _COUNTRY,
            PromptArg(
                "resource",
                "An ASN or prefix to check, e.g. AS3333",
                default="none given (skip the BGP check)",
            ),
        ),
        steps=(
            ("intel_internet_outages", "outages over the last 7 days; keep those in {country}"),
            ("intel_bgp_status", "routing visibility for resource {resource}"),
            ("intel_cable_health", "the condition of the cable corridors"),
            ("intel_cascade_analysis", "which countries depend on an at-risk corridor"),
        ),
        limits=(
            "An empty outage list can mean the sources returned nothing. "
            "Check for a note field and the source health resource before "
            "calling {country} connected.",
            "Cascade analysis is a simulation over curated cable "
            "dependencies, not a measurement of a real outage.",
            "BGP status checks one ASN or prefix at a time; it is not a "
            "global hijack detector.",
        ),
    ),
    ResearchPrompt(
        name="cloud-incidents",
        title="Cloud provider incidents and the regions involved",
        domain="infrastructure",
        question="Which cloud providers report active incidents, and which of their regions are involved?",
        steps=(
            ("intel_service_status", "active incidents at AWS, Azure, GCP, Cloudflare and GitHub"),
            ("intel_cloud_regions", "where the named regions are"),
        ),
        limits=(
            "Status pages are published by the providers themselves and can "
            "lag or understate an incident.",
        ),
    ),
    ResearchPrompt(
        name="maritime-chokepoints",
        title="Pressure on maritime chokepoints",
        domain="maritime",
        question=(
            "Are navigational warnings building at the strategic waterways, "
            "and is shipping stress rising in markets?"
        ),
        steps=(
            ("intel_vessel_snapshot", "warning-based scores for 9 strategic waterways"),
            ("intel_nav_warnings", "the warning texts behind the scores"),
            ("intel_trade_routes", "what share of oil and trade each chokepoint carries"),
            ("intel_shipping_index", "a stress score from shipping-linked ETF and equity prices"),
        ),
        limits=(
            "The vessel snapshot scores navigational warnings; it does not "
            "count ships. Warnings are not yet filtered by distance to each "
            "waterway, so read the warnings themselves before naming one.",
            "The shipping stress score is a market proxy, not a freight rate.",
        ),
    ),
    # --- areas of interest --------------------------------------------------
    ResearchPrompt(
        name="aoi-watch-setup",
        title="Start watching an area of interest",
        domain="aoi",
        question=(
            "Set up a watch on {name} (centre {lat}, {lon}, radius {radius_km} "
            "km): what is inside it now, and what should later checks compare against?"
        ),
        arguments=(
            PromptArg("name", "A name for the area", required=True),
            PromptArg("lat", "Centre latitude in degrees", required=True),
            PromptArg("lon", "Centre longitude in degrees", required=True),
            PromptArg("radius_km", "Radius in kilometres", required=True),
        ),
        steps=(
            ("intel_aoi_define", "name {name}, lat {lat}, lon {lon}, radius_km {radius_km}"),
            ("intel_aoi_brief", "the cited picture inside {name} today"),
            ("intel_aoi_changes", "the first run records a baseline for {name}"),
        ),
        limits=(
            "The first change check records a baseline and claims nothing "
            "entered or left.",
            "A domain that fails is listed in data_gaps and left out of the "
            "diff. Never read it as everything leaving.",
            "Change history between your own calls exists only while the "
            "collector daemon runs its AOI sweeps.",
        ),
    ),
    ResearchPrompt(
        name="aoi-review",
        title="What changed in a watched area",
        domain="aoi",
        question="What entered or left {name} since the last sweep, and is the area escalating?",
        arguments=(PromptArg("name", "The AOI name", required=True),),
        steps=(
            ("intel_aoi_list", "confirm {name} exists and see its shape"),
            ("intel_aoi_changes", "new, departed and unchanged items since the last sweep"),
            ("intel_aoi_escalation", "the escalation score and its components for {name}"),
            ("intel_aoi_brief", "cited detail behind any change worth reporting"),
        ),
        limits=(
            "Changes are relative to the previous sweep, whenever that ran; "
            "give its time.",
            "News for an AOI is found by place name near its centre, so a "
            "nearby place with the same name can leak in.",
        ),
    ),
    # --- news, politics, health ---------------------------------------------
    ResearchPrompt(
        name="story-breadth",
        title="How widely a story is covered and how that changed",
        domain="news",
        question=(
            "How much coverage has {topic} received, how has that changed over "
            "time, and which story clusters carry it?"
        ),
        arguments=(PromptArg("topic", "Search terms for the story", required=True),),
        steps=(
            ("intel_gdelt_search", "query {topic}, mode timelinevol for volume over time, then artlist for articles"),
            ("intel_news_clusters", "the clusters of related stories in the RSS set"),
            ("intel_news_feed", "the latest items from the RSS feeds"),
        ),
        limits=(
            "Coverage volume measures attention, not truth or importance.",
            "The RSS set is a fixed list of feeds. A story missing there may "
            "still be covered elsewhere.",
        ),
    ),
    ResearchPrompt(
        name="election-watch",
        title="Upcoming elections and the signals around them",
        domain="politics",
        question="Which elections are coming up in {country}, and what do current signals show around them?",
        arguments=(PromptArg("country", "ISO-3 code or country name", default="all countries"),),
        steps=(
            ("intel_election_calendar", "upcoming elections and their proximity risk"),
            ("intel_country_dossier", "economy, sanctions, news and conflict context for a named country"),
            ("intel_social_signals", "discussion velocity on the tracked subreddits"),
            ("intel_prediction_markets", "active Polymarket questions; keep the ones about this election"),
        ),
        limits=(
            "Proximity risk rises as an election nears; it is a stated "
            "heuristic, not a forecast of unrest or of the result.",
            "Reddit and prediction markets reflect their own participants, "
            "not the electorate.",
        ),
    ),
    ResearchPrompt(
        name="sanctions-screen",
        title="Search the OFAC sanctions list for a name",
        domain="finance",
        question="Does {query} appear on the US Treasury OFAC SDN list, and under which programs?",
        arguments=(PromptArg("query", "Name to search for", required=True),),
        steps=(
            ("intel_sanctions_search", "query {query}"),
            ("intel_company_profile", "only if the name is a listed company: who it is"),
        ),
        limits=(
            "Matching is by substring on names. A name match is not an "
            "identity match, and this is not a compliance screening service.",
            "The SDN list changes. Say when you searched.",
        ),
    ),
    ResearchPrompt(
        name="disease-watch",
        title="Disease outbreak reports",
        domain="health",
        question="Which disease outbreaks are being reported now, and which involve high-concern pathogens?",
        steps=(
            ("intel_disease_outbreaks", "WHO, ProMED and CIDRAP outbreak reports"),
            ("intel_humanitarian_summary", "crisis datasets for the affected countries"),
            ("intel_news_feed", "recent coverage, for context only"),
        ),
        limits=(
            "Outbreak reports arrive after cases occur and case counts are "
            "revised. Give each report's date.",
            "Reporting is uneven across countries. Fewer reports does not "
            "mean fewer cases.",
        ),
    ),
    # --- history and country views ------------------------------------------
    ResearchPrompt(
        name="history-recall",
        title="What the stored history says about a topic",
        domain="history",
        question=(
            "What has this server stored about {topic} over the last {hours} "
            "hours, and has anything similar been recorded before?"
        ),
        arguments=(
            PromptArg("topic", "Natural language topic", required=True),
            PromptArg("hours", "How far back to search, in hours", default="168"),
        ),
        steps=(
            ("intel_domain_summary", "hours {hours}: what is stored, per category, with the earliest timestamp"),
            ("intel_semantic_search", "query {topic}, hours {hours}"),
            ("intel_timeline", "the stored entries in time order for the matching domain"),
            ("intel_similar_events", "earlier stored entries that resemble the strongest match"),
        ),
        limits=(
            "History exists only for what the collector stored while it was "
            "running. Before calling anything the first or the largest, check "
            "the earliest stored timestamp.",
            "Semantic search ranks by similarity of wording. A high score is "
            "not a factual match.",
            "If the vector store is not available these tools return an "
            "error; say that history could not be read.",
        ),
    ),
    ResearchPrompt(
        name="country-overview",
        title="One country across economy, stability and markets",
        domain="country",
        question=(
            "What is the current picture in {country} across economy, "
            "stability, active signals and its stock market?"
        ),
        arguments=(_ISO3,),
        steps=(
            ("intel_country_dossier", "country {country}"),
            ("intel_instability_index", "country_code {country}, with its four components"),
            ("intel_signal_summary", "signals for the country (this tool takes the country name)"),
            ("intel_country_stocks", "country {country}, its main index"),
        ),
        limits=(
            "The instability index is a weighted composite with country "
            "multipliers. Quote its components, not only the total.",
            "World Bank figures in the dossier are annual and often a year or "
            "more old. Give the year of each.",
        ),
    ),
)


def get(name: str) -> ResearchPrompt | None:
    for prompt in PROMPTS:
        if prompt.name == name:
            return prompt
    return None
