# Changelog

## Unreleased

### Fixed: development runs no longer touch user databases
- Temporal and keyword spike databases now follow `XDG_CACHE_HOME` like
  the cache: `$XDG_CACHE_HOME/world-intel-mcp/temporal.db` and
  `$XDG_CACHE_HOME/world-intel-mcp/keyword_spikes.db`. With it unset, they
  remain in `~/.cache/world-intel-mcp/`. Users who set `XDG_CACHE_HOME`
  can keep existing baselines by moving these two files from
  `~/.cache/world-intel-mcp/` to `$XDG_CACHE_HOME/world-intel-mcp/`.
  Importing the package no longer opens databases; they open on first use.
- Apply per-source request spacing to every HTTP retry, including JSON,
  text, XML and binary fetches.

### Added: liquidity and money (Phase 29)
Keyless sources only: the FRED graph CSV (`fred.stlouisfed.org/graph/fredgraph.csv`,
allowed by robots.txt with `Crawl-delay: 1`, honored by a 1 s `fred-graph` rate
floor) and the ECB Data Portal (SDMX-JSON). Every point carries `observed_at`,
`value` and `units`; every series is typed `amount_outstanding`, `flow`,
`growth_rate`, `index` or `unknown` and carries a freshness block that flags data older than
its publication cadence allows (a served stale cache copy or an outage shows up
there). `published_at` is null: neither payload carries release dates, and the
responses say so. `limit=0` returns the full history.
- `intel_net_liquidity`: weekly Fed total assets (WALCL) minus Treasury General
  Account (WDTGAL) minus overnight reverse repo (RRPONTSYD), in USD billions.
  WALCL and WDTGAL are USD millions and are divided by 1,000; RRPONTSYD is already
  USD billions. One point per WALCL Wednesday; the TGA must be reported for that
  Wednesday; RRP comes from that day or, when blank on or after 2013-09-23,
  Tuesday only. Before that date a blank Wednesday has no fallback. Each point
  names the RRP date used. Weeks without aligned components are skipped and counted, never filled
  with zero, and a failed component makes the whole response an error. The TGA
  term is the Wednesday level (WDTGAL), not the week average (WTREGEN). A
  `method` block, returned with errors too, states each term's observation basis
  (WALCL and WDTGAL Wednesday levels in USD millions, RRPONTSYD daily in USD
  billions), its conversion, the RRP alignment and why WTREGEN is not used.
- `intel_liquidity_board`: latest reading and change vs the previous observation
  for the Fed balance sheet, TGA, RRP, net liquidity, US M2, euro area M1/M2/M3
  stocks and Eurosystem total assets, sortable by `biggest_move` or `newest`.
  `biggest_move` ranks absolute changes in billions of each row currency, with
  no FX conversion; observation horizons differ. Percentage changes do not
  affect ranking. A
  failed source becomes an `unavailable` row; net liquidity is dropped, not
  computed with a zero term, when any of its components fails. A `blocked` list
  names each measure not covered with its publisher and the status measured on
  2026-10-17.
- `intel_money_supply`: US M2 (FRED M2SL) and euro area M1, M2 and M3 from the ECB
  BSI dataset (end-of-month stocks, monthly flows, annual growth rates), kept
  separate and never summed. US M3 is reported as discontinued (FRED M3SL ends at
  2006-02-01).
- `intel_fed_liquidity_series`: WALCL, WDTGAL, WTREGEN, RRPONTSYD or M2SL with full
  history; blank source dates (2,833 for RRPONTSYD on 2026-10-17, 2,679 of them
  before 2013-09-23, when daily values begin) are counted, not read as zero.
- `intel_ecb_series`: any ECB BSI or ILM series by SDMX key, with units, scale,
  titles and provisional flags taken from the response. A growth rate's change is
  reported in points, not as a percent of a percent.
- Stale cache responses carry `served_from_stale_cache` and `cache_age_seconds`,
  and name the upstream failure in `data_gaps`. Only parsed bodies enter the cache.
  ECB 404 and empty responses are final no-data answers, without outage retries.
  Missing ECB roles and board rows are named; index bases, provisional status
  and percentage-point change units are retained. ECB keys must specify every
  dimension and request at most 50 series combinations.
- CLI: `intel liquidity`, `intel net-liquidity`, `intel money-supply`,
  `intel fed-liquidity`, `intel ecb-series`. Collector: `liquidity_board` and
  `net_liquidity` (group `liquidity`).
- Live reading 2026-10-17: net liquidity for Wednesday 2026-10-14 was
  USD 5,869.615 billion (WALCL 6,755,639 million, WDTGAL 885,823 million,
  RRPONTSYD 0.201 billion, all observed 2026-10-14).

Not added, measured 2026-10-17: ICI money market fund assets and ETF net issuance
(403 on every path, robots.txt included); FINRA margin statistics (the finra.org
Terms of Use, Restrictions d and e, forbid building a database from the site and
data mining, scraping or harvesting tools, robots included); BLS public API (keyless
v1 answers for CPI-U and payrolls, but api.bls.gov robots.txt disallows all paths
and the terms page answers 403). Fund flows and ETF net issuance therefore have no
source here, and no proxy is substituted for any of these measures.
### Added: news vs market reaction
- `intel_news_market_reaction` (CLI: `intel news-reaction`): the companies and
  countries named in recent story clusters, mapped to a listing (the entity
  table's ticker, confirmed against SEC's company_tickers.json for US
  listings) or to the country's primary index (the table `intel_country_stocks`
  uses), with the move on the story's market session from Yahoo daily bars.
  Company rows give the stock's close-to-close return (adjusted closes), the
  benchmark's return over the same two dates (SPY for US listings, the
  exchange country's index otherwise) and the excess return; country rows
  give the index return and a z-score against its previous up-to-20 sessions.
- Session rule, stated in every response: the story time (earliest
  `published` in the cluster) is converted to the exchange's timezone; before
  the regular close on a trading day it is that session, at or after the close
  the next one, and a weekend or holiday (read from the listing's own bars)
  maps to the next trading day. A session that has not closed is `pending`.
- Honest gaps: a failed quote, a missing benchmark bar, a stock series missing
  a day its benchmark traded, a story timestamp later than the collection time
  and a story older than the bar range are each a `data_gap` row with the
  reason and no number, never a 0% move. Unmapped names (private companies,
  countries without an index) are listed, not dropped. Company names that are
  also common words (apple, amazon, meta, shell, bp, alphabet, chevron,
  samsung) are excluded with a stated reason unless the headlines carry a
  corroborating term or an article comes from a feed category where the
  company sense dominates (a hyphen-joined use such as "meta-analysis" does
  not count). Responses say they describe same-session co-movement, not
  causation, and are not a trading signal. Tools 139 -> 140.
### Added: research guide over MCP (prompts and resources)
- The server now answers `resources/list`, `resources/read`, `prompts/list`
  and `prompts/get`, and advertises the resources and prompts capabilities
  at initialize. Before this it exposed tools only. No tools were added or
  changed.
- `intel://guide` (markdown): how to ask a strong question (period,
  comparison, measure, place), what an answer contains, what this server
  will not do, the worked prompts, and every registered tool grouped by
  domain module. Built from the live registry on every read; a test fails
  if a registered tool is missing or listed twice.
- `intel://guide/source-health` (JSON): open circuit breakers, stale
  serves, stale cache prefixes, cache freshness, and which keyed sources
  have their variables set (names only, never values). Reads in-process
  state and the cache database only; reading it starts no fetch. It says
  that breaker state covers this server process only. The keyed-source
  list is checked against the credentials `sources/*.py` actually read.
- `intel://guide/measures` (markdown): stock versus flow, growth versus
  acceleration, percentage points, seasonal adjustment, the three dates
  (observation, publication, collection), revisions, correlation and
  timing offsets, composite scores, missing data versus zero.
- 29 prompts across conflict, markets, hazards, migration, aviation, cyber,
  infrastructure, maritime, areas of interest, news, politics, sanctions,
  health, stored history and country views. Each names the tools to call
  and the limits to respect; arguments (country, topic, period and so on)
  are validated, and a missing, unknown or invalid one is an
  `invalid params` error. A test fails if any prompt names a tool that is
  not registered.
- `intel guide` prints the guide markdown; `intel guide --measures` prints
  the glossary.
- A test spawns the installed `world-intel-mcp` console script and drives
  initialize, resources/list, resources/read, prompts/list, prompts/get
  and tools/list over stdio.

### Added: tropical cyclone forecast tracks
- `intel_cyclones` and `intel_jtwc_cyclones` take `include_forecast`
  (boolean, default false). With it, each storm gains a `forecast`: the
  `initial` fix and forecast `points` (forecast hour, UTC valid time,
  latitude and longitude, max wind and gust in knots, status such as
  POST-TROP/REMNT LOW or DISSIPATED, and the agency's section label),
  `track_status`, and the advisory or warning number and issue time.
  Without it, both responses and the collector's NHC call are unchanged.
- NHC points come from the forecast/advisory text that CurrentStorms.json
  links: the one NHC product that states UTC valid times and post-tropical
  status outright (the KMZ track gives local times, and the shapefile
  labels a post-tropical remnant low "Subtropical Depression"). The cone
  comes from the linked CONE.kmz: its one ring, simplified
  (Douglas-Peucker, 0.01 degrees; Hurricane Trudy advisory 17 went from
  1,167 vertices to 56) to `[lat, lon]` vertices, with the source vertex
  count, the tolerance, and a note pointing at the official geometry
  (`cone_url`).
- JTWC points come from each storm's linked warning text (its forecast,
  extended outlook and long range outlook blocks). JTWC links no cone:
  `cone_status` is `not_published`, and `data_gaps` says so.

### Data honesty: forecast tracks
- `data_gaps` names every track or cone not delivered, per storm, with a
  status: `fetch_failed`, `unparsed`, `refused_link`, `not_linked`,
  `not_published` or `not_fetched_cap` (at most 10 storms per call; the rest are named,
  never dropped). A failure marks the response `degraded`; a product the
  agency does not link is a gap, not degradation. A quiet basin still
  answers zero storms, not degraded, and makes no product fetches; a feed
  outage keeps its existing degraded shape and makes none either.
- The text pages are per storm bin, not per issuance, so one can lag the
  feed: an advisory or warning number that disagrees with the feed is
  returned as `stale_advisory` or `stale_warning` with the expected
  number, and its cached copy is dropped.
- Each product is fetched once per issuance (cache keys carry the
  advisory or warning number; one-hour TTL) under its own circuit breaker
  (`nhc-forecast`, `nhc-gis`, `jtwc-warning`), so a product outage does
  not trip the warning feeds' breakers.
- Remote KML is parsed with the standard library: strict UTF-8, any
  DOCTYPE refused, size caps (KMZ 2 MB, KML 8 MB), and a cone with more
  than one ring refused, never trimmed. A ring of more than 10,000
  vertices is refused before simplifying, and simplifying runs off the
  event loop.
- Product links come from the agencies' feeds and are fetched only when
  they are https on the agency's own host (www.nhc.noaa.gov,
  www.metoc.navy.mil). Any other link is reported as `refused_link` and
  never requested.
- Measured 2026-10-17: every product used answered HTTP 200 without a
  key; nhc.noaa.gov robots.txt disallows nothing and the NWS disclaimer
  puts the products in the public domain. The JTWC home page and
  robots.txt answered 403 (CloudFront "Request blocked"), so JTWC's site
  terms could not be read; its RSS and warning texts served 200 to the
  same client. Wind radii and seas are not parsed.

### Added: observation history and collection health
- A history store in the cache SQLite file. Every collector source run is a
  capture: source, collected_at, status (ok, degraded or error), duration,
  record count, reason and payload hash. A failed fetch is an error capture
  with its reason and no record count, never an ok capture with zero
  records. An empty payload is degraded, because a quiet period and a
  silent upstream failure look the same.
- Payload bodies are kept once per distinct payload as compressed JSON
  (top-level fetch-time stamps are ignored when hashing). Retention, each
  0 = keep forever: `WORLD_INTEL_HISTORY_PAYLOAD_DAYS` (default 7) for
  bodies, `WORLD_INTEL_HISTORY_CAPTURE_DAYS` (default 30) for the run log,
  `WORLD_INTEL_HISTORY_DAYS` (default 365) for observations. Measured over
  three full 55-source cycles: 90 KB and 193 KB of new compressed bodies in
  the two follow-up cycles (about 70 KB from sources that change every
  cycle, plus the military flight snapshot when it succeeds) and about
  14 KB of capture rows per cycle. At `--interval 300` that is about
  130-340 MB of bodies at the 7-day default and about 100 MB of run log at
  30 days. Quote observations on a trading day were not measured.
- Observations with three dates: `observed_at` (from the payload, or null
  when it has none; never the collection time), `published_at` (when the
  payload names a release time) and `collected_at`. Built-in extractors:
  NOAA Kp, USGS per-event magnitude, FRED-sourced central bank rates
  (curated rows are skipped), Yahoo index and commodity quotes (null
  `observed_at`: the payload carries no quote time).
  `history.register_extractor()` is the public API for new sources.
- MCP tools `intel_collection_health`, `intel_capture_history`,
  `intel_series_history` and `intel_revisions`, and the CLI group
  `intel history health|captures|series|revisions`. History accrues only
  while the collector runs.
- The collector records every source outcome of a cycle in one
  transaction. Its summary gains a `history` key (and the CLI summary a
  History line); a history write failure is reported there and in the
  log, and never fails the cycle.
- Expected cadence for health is 300 s (the shipped plist interval);
  `WORLD_INTEL_HISTORY_CADENCE_S` and
  `WORLD_INTEL_HISTORY_CADENCES=source=seconds,...` override it.

### Notes
- The cycle summary's `successes` still counts a source that returned
  `{"error": ...}` (unchanged, pinned by an existing test); that run's
  history capture is an error.
- Tools 139 -> 143.

### Added: news signals (ROADMAP Phase 29)
- `intel_hostility_timeline`: per UTC day, GDELT DOC 2.0 coverage of a
  country or a pair of countries (`timelinevolraw`): matching articles,
  articles that also use threat / sanction / force / criticism terms (one
  GDELT query per category, every term listed in `queries`), GDELT's total
  monitored articles, and average tone (`timelinetone`, weighted by the
  per-bucket article counts). A failed call leaves its column null for
  every day; a day GDELT did not return is null in every column; both are
  listed in `data_gaps`. Partial days (GDELT answered 2026-10-17 in
  15-minute buckets for 3 days and hourly buckets for 7) are flagged
  `partial`, judged from the first category series that answered when the
  base call failed, and today's unfinished UTC day is flagged
  `in_progress`. A
  failed call is retried once.
- `intel_situations`: country pairs ("between": a headline naming two or
  more countries) and single countries ("inside": a headline naming one)
  ranked over a recent RSS window by most covered, most hostile (headline
  keyword heuristic, `classifier.classify_hostility`) or biggest change.
  Change is defined in the response: current minus prior window, counted
  only over feeds whose retained history reaches back past the prior
  window's start; the feeds that do not are named.
- `intel_top_stories`: story clusters in the window carried by at least
  `min_outlets` distinct outlets, plus attention: the countries and
  organizations whose headline mentions grew most against the prior window.
- `intel_news_clusters` clusters gain `outlets` and `outlet_count`
  (distinct publishers among all members; sister feeds such as BBC World
  and BBC Mundo count once). Existing keys are unchanged.
- CLI: `intel hostility COUNTRY [OTHER]`, `intel situations`,
  `intel top-stories`. Nulls render as `n/a`, never 0; partial days are
  marked `*` and today `~`.
- Collector: `situations` and `top_stories` (57 sources).

### Changed
- Concurrent `fetch_news_feed` reads of the same feed set share one
  request per feed. The collector starts its four news sources in one
  gather, and each used to request every feed itself; measured live
  2026-10-17, a news-group cycle made 119 feed requests for 119 feeds
  (4 of 4 sources ok, 9.8 s). Each caller still gets its own limit and
  its own item copies, and a caller's timeout does not cancel the read
  the others are waiting on.
- Live GDELT calls are spaced at least 5 s apart by the fetcher
  (`_SOURCE_RATE_LIMITS["gdelt"]`), for every GDELT caller: GDELT's 429
  body asks for "one every 5 seconds" (measured 2026-10-17). Cache hits are
  not delayed. The limiter is per process, so the MCP server and the
  collector do not share it, and the fetcher's own retries inside one call
  are not spaced by it.

### Fixed
- RSS outage read as a slow news day (ROADMAP Phase 24.5):
  `fetch_news_feed` returned `count: 0` with no marker when every feed
  failed. It now reports `feeds_attempted`, `feeds_ok`, `feeds_failed` and
  `feeds_stale` on every call, and `error` / `degraded` /
  `reason: all_feeds_failed` when no feed answered live;
  `intel_news_clusters` and `intel_trending_keywords` pass the marker
  through, so `intel trending` prints an error instead of an empty
  keyword table.
- RSS outage behind a warm cache read as healthy news: a feed whose fetch
  failed came back as its expired cached copy (the fetcher's stale
  fallback, which marks only dict payloads), so in a running deployment
  an outage counted every feed as fine until the expired copies were
  evicted (the collector evicts about hourly; a server running without
  the collector keeps them until `intel sync`). Such feeds are now listed
  in `feeds_stale`, never counted in `feeds_ok`, and kept out of
  situation and attention change figures.
  Verified live 2026-10-17: with an expired copy seeded for DW Europe
  (its feed answers 404), the europe category reported
  `feeds_stale: ["DW Europe"]`.
- Cluster `sources` was always empty: the clusterer read `source_name` /
  `source`, but RSS items name their outlet `feed_name` (measured live
  2026-10-17: 23 of 23 clusters had `sources: []`, which also emptied
  `intel_world_brief` top-story sources).
- RSS `published` times were shifted by the host's UTC offset:
  `_parse_published` read feedparser's UTC struct with `time.mktime`
  (local time) instead of `calendar.timegm`. The struct carries
  `tm_isdst=0`, so the shift is the zone's standard offset: on mac-studio
  (US Eastern) an item published 14:43:52 UTC came out `19:43:52Z`, five
  hours ahead (measured 2026-10-17). The amount depends on the host zone
  (none on a UTC host). It skewed every RSS-based time window and any
  ordering against non-RSS times. The regression test sets its own zone,
  so it fails on the old code on a UTC CI runner too.
- Country dossier news was always empty, with no marker (ROADMAP Phase
  24.5), for three reasons: it asked `fetch_news_feed` for
  `category="all"`, which is not a category (the feed answered with an
  error dict); it read `articles` and `source`, while the feed emits
  `items` and `feed_name`; and it checked only its own `_error`, never the
  feed's `error`. It now reads every feed, passes a feed error or outage
  through as `error` / `degraded` / `reason` with `mention_count: null`
  and a `data_gaps` entry, and names failed or stale feeds. A fourth
  quiet zero: 28 of the 50 accepted country codes (GB, DE, JP and others)
  have no headline keywords, so the search could never match; the section
  now says "mentions not searched" instead of reporting zero.
  `intel dossier` prints `News: not measured` and the gaps in that case.

## 0.11.0 - 2026-10-12

Cross-border migration (MCP tools, CLI, collector and a dashboard
corridor map), aircraft-cohort early warning, and fixes to the
displacement summary and the dashboard AOI detail panel. Tools 132 -> 139.

### Added: migration
- `intel_migration_flows`: every UNHCR origin -> destination corridor for a
  year (about 6,000 with people in them for 2025: refugees, asylum seekers
  and others in need of international protection), filterable by country
  (ISO-3, ISO-2 or name) or region (UNHCR major areas and UN sub-regions),
  with each corridor's change against the prior year and top origins and
  destinations of the selection.
- `intel_migration_surges`: largest year-over-year increases and decreases
  in people abroad, by origin and by corridor, and in internal displacement,
  with each origin's change as a share of its population (World Bank
  SP.POP.TOTL). Levels mass / major / notable are stated heuristics.
  UNHCR's non-country buckets ("Unknown" origin) are reported separately
  and never ranked.
- `intel_migration_history`: one country's yearly UNHCR series since 1951,
  as an origin or as a host, with its peak year and largest annual waves.
- `intel_net_migration`: World Bank net migration (all reasons, economic
  included) and international migrant stock: latest-year ranking in
  absolute and per-1,000 terms with world regions, a World Bank region
  filter, or one country's series since 1960.

### Added: migration corridor map layer
- The dashboard gains a "Migration Flows" layer and `GET /api/migration`
  (`?limit=` 1-150, default 40; `?year=`): the largest UNHCR origin ->
  destination corridors plus the fastest-rising ones, drawn capital to
  capital with weight by people and colour by year-over-year change.
  Coordinates are the capital-city latitude/longitude of the World Bank
  country list (`wb_countries` now returns `capital`, `latitude`,
  `longitude`); the legend says so. Corridors with no coordinates (UNHCR
  "Unknown" origin, Stateless, West Bank and Gaza) are counted in
  `unplotted` and listed, never dropped. An upstream failure is a 503 with
  `error`, shown as `!` and a red legend line, never an empty map.

### Added: migration in the CLI and collector
- `intel migration-flows`, `intel migration-surges`, `intel migration-history`
  and `intel net-migration` render the four tools as tables, with the same
  error surfacing as every other command. A change with no prior-year
  figure renders "n/a", never "+0".
- The collector gains `migration_surges` and `net_migration` (55 sources)
  and a `migration` domain group (`--sources migration`, which also takes
  `displacement`).

### Fixed: migration surges
- A country whose UNHCR rows were all internal (IDPs, nobody abroad) never
  entered the surge comparison, so its IDP jump was dropped.
- Internal-displacement rows now carry `idps_first_reported` when the prior
  year had no IDP figure. Live 2024 -> 2025 ranked Guatemala (+573,000) and
  Cambodia (+517,555) as IDP surges from a prior figure of 0, which can be
  a first report rather than new displacement; the CLI labels those rows.

### Fixed: displacement summary
- `intel_displacement_summary` never sent UNHCR `coo_all=true`, so UNHCR
  answered with one global row and `by_origin` held a single entry named
  "-" (found by a live call 2026-10-11). It now returns the top 30 origins
  with ISO-3 codes. Names prefer the short TIER1 name ("Syria") so
  `intel_country_signals` can join displacement with the conflict sources;
  that join had no displacement input before.

### Fixed: dashboard detail panels
- Clicking an AOI on the map opened an empty detail panel (label only,
  since 0.10.0): the click called `showDetail`, but the AOI branch lived in
  `showDrawerDetail`, which AOIs never reach. The branch now sits in
  `showDetail`; circle, polygon and corridor AOIs show name, shape,
  centre and last-sweep counts.
- Map clicks on pipelines and trade routes opened the same empty panel.
  Pipelines now go to the pipeline renderer the drawer already used; trade
  routes gain one (type, oil flow, vessel transits, share of global trade,
  ISO-3 countries, notes, all from `config/trade_routes.py`).
- Drawer rows for UCDP events and conflict-zone hotspots showed a raw dump
  of the first 12 keys under a JSON title. UCDP rows now show both sides,
  violence type, deaths (best and low-high), dates, coordinates, headline,
  source and UCDP id; conflict-zone rows use the map's conflict renderer.
- UCDP events on the map's conflict layer opened the ACLED-shaped panel:
  "Conflict Event", 0 fatalities, no dates. They now open the UCDP panel.
- The conflict-zone hotspot fallback (used when ACLED and UCDP both have no
  events) was built only by `/api/overview`, which the page never calls,
  so with no conflict feed the map layer and the drawer's "Active Conflict
  Zones" section stayed empty. `/api/stream` now sends it from the same
  builder once both feeds report, or `null` when one has events.
- `test_dashboard_detail_routing.py` fails if any dispatched type lacks a
  branch in the renderer it reaches, or if UCDP map clicks miss the UCDP
  renderer; `test_dashboard_stream_conflict.py` drives one stream tick.

### Docs
- README's per-tool reference listed 126 of the 135 tools while every
  headline count said 135: the 5 newer AOI tools, `intel_bgp_status`,
  `intel_daily_digest`, and the severe-weather, cyclone, launch and
  volcano tools were missing. All 139 are listed now, and
  `test_readme_documents_every_registered_tool` pins the reference and
  each section's count to the live registry. Collector count 51 -> 53 in
  README; pyproject description; ROADMAP phases 27-28; CLAUDE.md
  dispatch note (no `match/case` since Phase 26) and the UNHCR code trap.

### Data honesty: migration
- UNHCR figures are year-end stocks, not border crossings; every response
  carries a `measure` string saying so. World Bank net migration is
  modelled by the UN World Population Prospects, and its latest years can
  be projections.
- UNHCR's own country codes are not ISO ("AUS" is Austria), so requests
  send `cf_type=ISO` and rows are read through `coo_iso` / `coa_iso`.
- Not included: near-real-time displacement (IOM DTM and HDX HAPI need
  keys), regional border-crossing series, and bilateral economic migration
  flows (only national net totals exist in a keyless API).

### Added: aircraft-cohort early warning
Approach inspired by
[kylemcdonald/ews](https://github.com/kylemcdonald/ews) (independent
implementation; no ews code used).

- `intel_jet_cohort_snapshot`: live airborne snapshot of the business-jet
  cohort (69 ICAO type designators in one adsb.lol `/v2/type` request) or
  the military cohort (`/v2/mil`), with per-type counts and PIA/LADD
  flags. Each call learns cohort membership into a `jet_cohort` table in
  the cache DB. The collector gains `bizjet_cohort` and `military_cohort`
  so the cohort keeps learning 24/7 (collector sources 51 -> 53).
- `intel_jet_activity_anomaly`: peak concurrent airborne cohort aircraft
  in a UTC half-hour, from adsb.lol `globe_history` heatmaps, against the
  same slot on 2-8 prior same-weekday dates; dates within a day of a US
  federal holiday are swapped for earlier weeks. Robust z with a counting
  noise floor; a second z on the cohort's share of all tracked aircraft
  guards against feeder-coverage swings, and the level (1-5) uses the
  smaller. Deficits (z <= -3) are flagged too.
- `intel_jet_excess_origins`: takeoff/landing inference (ground/air
  transitions plus low climbs/descents at coverage edges), density
  clustering of endpoints across the current and baseline windows, and a
  departure/arrival anchor, regional, diffuse or no-excess label, with
  optional distance to a reference point.
- `Fetcher.get_bytes` (binary fetch with rate floor, retries, breaker;
  404 reported distinctly) and an on-disk LRU for heatmaps
  (`WORLD_INTEL_HEATMAP_CACHE_MB`, default 1024).

### Data honesty: aircraft cohort
- Cohort selection bias is gated: only members first seen
  `min_learning_lead_hours` (default 24) before the window count. A fresh
  install answers `warming_up`. A negative lead is allowed for a first
  look but marks the result degraded and suppresses the alert level and
  pattern label (bypassing the gate measured 1,002 vs a 206 baseline
  median on 2026-10-11, i.e. pure selection artefact).
- Missing, unpublished or low-coverage heatmaps are listed in
  `data_gaps`; fewer than 3 usable baseline windows is
  `insufficient_baseline`, never a zero-excess "normal".

### Not ported from ews
- FAA / global registry cohort importers: the FAA ReleasableAircraft.zip
  answers 403 to non-browser clients, and the ADS-B Exchange database and
  `globe_history` are under JETNET terms (robots.txt disallows
  `/globe_history/`), so the cohort is learned from adsb.lol instead.
- Year-long backfill, learned holiday profiles and annual ratios,
  non-ICAO (`~hex`) activity stores, and the paid SMS/email/Telegram
  alerting stack.

## 0.10.0 - 2026-09-02

Geofences become visible and their news becomes geographic: the
dashboard draws every AOI with its last-sweep counts, and AOI news is
searched by the place an area sits in rather than by its name. The
roadmap's last planned item ships here; along the way, every live
GDELT fetch was found to have been silently failing and was fixed.

### Added
- Dashboard AOI layer: a toggleable "AOI Geofences" Leaflet layer draws
  every defined area (circle, polygon, corridor) from the shared store,
  with a tooltip and detail panel showing what the last collector sweep
  counted inside it. Backed by a new `/api/aois` endpoint that reads
  the stored change snapshot and never gathers live, so drawing a shape
  costs nothing upstream. An AOI that has never been swept says so
  (`last_sweep: null`) instead of showing zeros; a broken store is a 503
  with the reason, never an empty map that reads as "no areas defined".
  Verified in headless Chromium: 2 shapes rendered, the 50 km circle at
  50,000 m, no console errors. First endpoint tests for the dashboard
  (Starlette TestClient); a real SQLite thread-affinity error surfaced
  during them and was fixed by opening the store per request.

### Changed
- AOI news is now scoped by where the area IS, not what it is named.
  Every other AOI domain was filtered geometrically; news alone was a
  GDELT search for the AOI's name, so an area called "Home" returned a
  Chinese A-share IPO article and "PGH Square" an Indian op-ed on Gaza
  (both measured live 2026-09-02, both for a fence around Pittsburgh).
  New `sources/geocode.py` reverse-geocodes the AOI centre via OSM
  Nominatim (no key; 1 req/s floor and an identifying User-Agent per
  its usage policy; 30-day cache since places do not move) and the
  brief/changes/digest search GDELT for the settlement and county
  (`("Pittsburgh" OR "Allegheny County")`). The brief reports how news
  was scoped in `news_scoping`; when geocoding is unavailable (geocoder
  down, open ocean) it falls back to the AOI name and says so in
  `data_gaps`, because that is exactly when articles may be unrelated.

### Fixed
- Every live GDELT fetch had been failing and hiding behind stale
  cache. Root cause, measured: api.gdeltproject.org TCP-connects in
  0.03 s but its TLS handshake took 19.7 s, 17.4 s, then >45 s across
  three samples (Nominatim and USGS: 0.05 s on the same network), and
  the Fetcher's 15 s default counts TLS inside the connect phase, so
  every request was a `ConnectTimeout`. Both GDELT call sites (news
  search, company profile) now use a shared 45 s `GDELT_TIMEOUT`;
  live-verified afterwards with 10 real articles for the Pittsburgh
  AOI. Handshakes slower than that still degrade to the existing honest
  `error`/`degraded` shape.
- GDELT DOC API requires OR clauses in parentheses: a bare `"a" OR "b"`
  returns nothing at all (measured live). The news query builder wraps
  multi-term queries; a regression test pins the exact form.
- A failed Nominatim request (e.g. HTTP 429) was reported as "returned
  an unexpected response shape", pointing a reader at parsing when the
  cause was rate limiting. A failed fetch and a genuinely odd payload
  now carry distinct messages.

### Testing
- The AOI test suite had briefly become non-hermetic: the shared
  domain stub did not cover the new geocoder, so ~150 tests were each
  making a live Nominatim request (suite time 0.9 s -> 15.8 s, and a
  real 429 from OSM). The stub now covers it (back to 0.7 s), proven by
  temporarily making any live geocode call raise: only the six tests
  that mock Nominatim themselves touched it.

## 0.9.0 - 2026-09-02

AOI geofences become a continuous watch: the collector daemon sweeps
every defined area on its interval, and a webhook fires when something
enters or leaves.

### Added
- Scheduled AOI sweeps: `aoi_digest` is now a collector source
  (`analysis.aoi.fetch_aoi_sweep`, a fetcher-only wrapper that opens
  the AOI store on the fetcher's own cache database), so
  `intel-collector --daemon` advances every defined geofence's change
  snapshot on each cycle - the daemon interval is the watch cadence,
  and the existing launchd wrapper makes it survive reboots. New `aoi`
  domain group for `--sources aoi`. Digests land in the collector log
  and the vector store, and in the webhook below when configured.
- AOI change notifications: set `WORLD_INTEL_AOI_WEBHOOK` and the
  sweep POSTs every non-quiet digest (something entered or left a
  geofence) to that URL - `json` payload by default
  (title/totals/markdown/timestamp), or raw markdown with a `Title`
  header via `WORLD_INTEL_AOI_WEBHOOK_FORMAT=text` for ntfy-style
  sinks. Quiet sweeps never fire; an unreachable sink or non-2xx
  response never fails the sweep (snapshots already advanced) but is
  recorded honestly in the digest's `notification` field. New
  `Fetcher.post()` carries the delivery - deliberately outside the
  caching/retry/breaker machinery, since the target is the operator's
  own endpoint. Live-verified end-to-end against a local sink: a real
  sweep detected a change and the sink received the JSON payload.
- Per-source collector timeout overrides (`SOURCE_TIMEOUTS`): a live
  smoke caught a single 50 km AOI sweep on a cold cache blowing the
  flat 45 s budget (the digest fans out to ~8 domains per AOI, several
  rate-floored); the sweep now gets 240 s and the override path is
  regression-tested.

## 0.8.0 - 2026-09-01

AOIs become operational (one-call sweeps and a full CLI), and the
domain roster reaches Europe, the West Pacific, and the routing table.

### Added
- `intel_aoi_digest` (+1 tool): the change sweep across every defined
  AOI (or a named subset) in one call - per-AOI new/departed items,
  counts, data_gaps, and a combined markdown digest. Each AOI's
  snapshot advances, so a scheduler can call this one tool on an
  interval to learn what entered or left all watched areas. No AOIs is
  a note; unknown requested names are an error; one AOI's failed
  domain stays in that AOI's data_gaps.
- `intel aoi` CLI command group: define, define-polygon,
  define-corridor, list, update, delete, brief, escalation, changes -
  full parity with the MCP tools, sharing the server's store semantics
  (same cache-resolved SQLite file) and the 0.6.0 error and
  markup-safety conventions. Live-smoked through the real entry point.
- `intel_meteoalarm_alerts` (+1): Meteoalarm (EUMETNET) severe-weather
  warnings for 39 European countries. Live verification found there is
  NO Europe-wide feed (per-country only; calling without a country
  returns the roster) and that awareness color lives only in the
  warning title, so the module derives it and says so.
- `intel_jtwc_cyclones` (+1): JTWC warnings for the NW Pacific, North
  Indian Ocean, and Southern Hemisphere, complementing NHC
  (`intel_cyclones`). Storm positions/intensities are in the linked
  warning products, not the feed; the module links rather than
  pretends. A live run caught and fixed a wrong basin guid before
  release.
- `intel_bgp_status` (+1 = 132): per-resource BGP health from RIPEstat
  - RIS visibility, announced space, observed origins, per-origin RPKI
  validation. Deliberately scoped as a health check, not global hijack
  detection: RIPEstat has no target-free incident feed, and the tool
  description says exactly that. A polite 1s rate floor was added for
  the ripestat source.

### Not added, honestly
- FAA NOTAMs: the official API returns 401 without a key and the
  unofficial search backend is POST-only and undocumented; no genuine
  key-free machine-readable path was found (verified live 2026-09-01).
  Tracked in ROADMAP as blocked-on-upstream rather than faked.

## 0.7.0 - 2026-09-01

The monolith splits. No behavior changes.

### Changed
- server.py (2,890 lines: a 128-entry Tool list plus an 800-line
  match/case) is now a 158-line shell over 12 domain modules in
  `world_intel_mcp/tools/` (markets, hazards, conflict,
  infrastructure, society, intelligence, geospatial, synthesis,
  finance, vector, aoi, system), with shared infrastructure (cache,
  circuit breaker, AOI store, vector store, fetcher) in
  `world_intel_mcp/runtime.py`. Each module exports TOOLS and
  HANDLERS; `tools.aggregate()` refuses to import a registry whose
  tools and handlers disagree or collide, so the drift the old
  text-scan parity test could only catch in CI now prevents the
  server from starting at all.
- Pure move, verified three ways: an AST comparison of every
  name/description/inputSchema against the pre-split registry
  (127/127 byte-equal), the full suite (814 passed, 90.8% coverage),
  and a live MCP stdio session (initialize, tools/list returning all
  128, and a real tools/call round-trip).
- Registry/registration tests upgraded from text-scans of server.py
  to import-based assertions against the aggregated registry,
  including a falsifiability test proving `aggregate()` rejects
  drift and collisions.

## 0.6.0 - 2026-09-01

The CLI stops lying about outages, and the new domains join the 24/7
collector.

### Fixed
- 33 CLI commands rendered an upstream `{"error": ...}` as a healthy
  empty state ("0 earthquakes" over an empty table during a USGS
  outage, error text discarded) - the fail-reads-as-success class at
  the CLI layer. One shared bail-on-error path now prints the
  upstream error prominently in table mode and passes the raw dict
  through unchanged in --json-output mode. Partial degradation
  renders data plus a visible warning (climate names its unavailable
  zones); a displacement outage prints the error instead of "Grand
  total: 0". 43 tests failed against the old behavior; 114 pass now.
- Rich markup no longer swallows lowercase bracketed values or lets
  remote feed titles inject markup: news categories, sanctions entity
  types, ai-watch sources, and gh-trending languages render again;
  the `intel report` fallback hint prints its `[pdf]` extra verbatim;
  a feed title containing "[/]" renders literally instead of crashing
  the command with a MarkupError. Escaping is per-site; the CLI's own
  color styling is untouched.
- Remote data inside Rich TABLE cells is injection-safe too: cell
  values from upstream APIs render via rich.text.Text (literal text,
  column styling preserved), with interpolated values escaped where a
  cell deliberately keeps markup. 11 injection tests cover both the
  swallow and the crash mode; static-config columns are documented as
  deliberately unwrapped.
- The two coexisting error styles are unified: the 17 commands that
  dumped raw JSON on error in table mode now use the same red Error:
  line as everyone else, and --json-output passes the raw dict
  through on all 53 commands (intel report previously ignored the
  flag entirely).

### Added
- The four 0.5.0 domains (weather alerts, launch schedule, volcano
  activity, cyclones) joined the collector's 24/7 vector-store
  roster: 46 -> 50 sources, with the roster-count invariant test,
  domain groups, and vector-store category mappings updated together.

### Credits
- Richard Barron (@RichardBarron27, Red Specter Security Research)
  for the external security report (#21) that prompted the
  SECURITY.md threat model and the cache-permissions hardening
  shipped in 0.4.0.

## 0.5.0 - 2026-09-01

Geofences in any shape, four new hazard domains, precise entity
matching, and the last coverage zeros gone.

### Added
- **Polygon AOIs** — `intel_aoi_define_polygon` (+1 tool): a named area
  bounded by 3-64 [lat, lon] vertices, for shapes a radius cannot
  express (a border region, a strait, an EEZ). Every intel_aoi_* tool
  scopes to the exact polygon: an event inside the bounding circle but
  outside the polygon is excluded. Polygons may cross the antimeridian.
  Point infrastructure matches the exact shape; line features
  (pipelines, cables) match the bounding circle, disclosed in
  `data_gaps` rather than silently approximated.
- **Corridor AOIs** — `intel_aoi_define_corridor` (+1 tool): a route of
  waypoints plus a width in km (a shipping lane, a supply road, a cable
  run), built on the great-circle segment distance from 0.4.0.
  Distances in results are measured to the route, not to a center.
  Existing AOI databases migrate in place; old rows read as circles.
- **Four hazard/space domains** (+4 tools = 128), each verified against
  its live API before mocking: `intel_weather_alerts` (NWS CAP, US
  only, honest null coordinates for zone-based alerts),
  `intel_launch_schedule` (Launch Library 2, hour-long cache for the
  free tier), `intel_volcano_activity` (Smithsonian GVP weekly
  report), `intel_cyclones` (NHC active storms - Atlantic/E-C Pacific
  basins only; zero storms is a quiet tropics, not an outage).
- CLI and collector test waves: `cli.py` 0% -> 92% (76 tests driving
  all 52 reachable commands through CliRunner), `collector.py`
  20% -> 97% (run loop, daemon cycle, source-filter resolution). The
  last of the coverage zeros from the 0.4.0 audit.

### Fixed
- Entity extraction no longer substring-matches: "usa" inside
  "thousand" tagged the United States, "hamas" inside "Bahamas",
  "trump" inside "trumpet", "meta" inside "metadata". Countries,
  leaders, organizations, and companies now use the same precompiled
  word-boundary alternation the APT-group matcher always had (with a
  plural allowance for country demonyms). Verified faster per call on
  headline-size inputs.
- Event classification keywords are boundary-anchored stems:
  "launched" still classifies space, but "strike" inside "airstrike"
  no longer adds a social-unrest category and severity bump, "ied"
  inside "denied" no longer fires, "coup" inside "couple" no longer
  fires. A benign sentence that previously triggered four categories
  at severity 7 now classifies as nothing.

### Known issues (pinned by tests, fix planned)
- ~30 CLI commands render an upstream `{"error": ...}` as a
  healthy-looking empty state ("0 earthquakes" on an outage), and
  lowercase bracketed values in Rich output are swallowed as markup
  (the `intel report` fallback hint prints a pip command missing its
  `[pdf]` extra). Both are documented by deliberately-pinning tests
  in `test_cli.py` and tracked in ROADMAP Phase 24.5.

## 0.4.0 - 2026-09-01

Geofences that survive the dateline, notice change, a test suite that
reaches the layers the old one never imported, and outages that say so
instead of reading as good news.

### Added
- `intel_aoi_changes` (+1 tool): geofence change detection — what
  entered or left a user-defined AOI since the last sweep, per domain
  (earthquakes, military flights, ACLED conflict events, wildfire
  clusters, news mentions), built on the same scoped gather as
  `intel_aoi_brief` so the two tools can never disagree about what is
  inside the fence. The first sweep is an explicit `baseline`; a
  domain whose fetch failed goes to `data_gaps`, is excluded from the
  diff (a failed fetch must never read as "everything left the
  area"), and keeps its last real observation for the next successful
  sweep. Sampled aviation is excluded by design: diffing a 1-in-10
  global sample would manufacture fake enter/leave events every run.
- `intel_aoi_update` (+1 tool = 122): rename and/or re-center/resize
  an AOI in place with define-grade validation and collision checks.
  A rename migrates the AOI's change-detection snapshot; a geometry
  change drops it, because the old baseline described a different
  piece of the planet.
- `SECURITY.md`: explicit threat model (prompted by the external
  report in issue #21) — what the trust boundaries are, what
  hardening exists inside them (parameterized SQL throughout, no
  shell/eval/exec, server-generated report paths), and what is out of
  scope for a single-user local OSINT tool.
- Test waves for the previously unexecuted layers: the `analysis/`
  NLP modules (classifier, entities, convergence, spikes, clustering,
  signals, focal points, surge, and more) and the low-coverage
  `sources/` modules (intelligence, cyber, climate, displacement,
  fleet, prediction, service_status, maritime, military parsing) had
  0-30% statement coverage; measured overall coverage was 59% before
  this release despite a green 309-test suite. At release: 597
  non-smoke tests, 81% statement coverage.
- CI coverage ratchet: `--cov-fail-under=80` on the test job
  (measured 81% on 2026-09-01; floor set one point under for platform
  variance). The floor only moves up.

### Fixed
- Antimeridian AOIs: the bounding box derived from an AOI's radius was
  clamped at lon ±180, so a Bering Strait or Fiji geofence silently
  lost everything on the far side of the dateline in the OpenSky
  military-flight path and could report a false "no FIRMS coverage"
  wildfire gap. Circles crossing the dateline now produce two boxes;
  military fetches run per box and merge (icao24 dedup); a one-box
  failure is reported as `partial coverage` in `data_gaps` instead of
  passing half-coverage off as full.
- Pipelines and undersea cables are matched as line features:
  great-circle segment distance (cross-track with endpoint clamping)
  replaces endpoint-only / landing-point-only proximity, so a
  pipeline or cable whose midspan passes through the AOI is detected
  even when its endpoints are hundreds of km away.
- `intel_world_brief` top stories always reported `article_count: 0`:
  the brief read `article_count` from news clusters, but
  `fetch_news_clusters` emits the member count as `size` (the
  silent-zero class again; found by the new analysis-layer tests).
  Reads `size` now, with `article_count` as fallback, plus a
  regression test in the real emitted shape.
- `intel_fleet_report` per-waterway warning counts were always 0: the
  report read `warning_count` but `fetch_vessel_snapshot` emits
  `naval_warnings` (same silent-zero class, also found by the new
  tests). Reads `naval_warnings` now, with `warning_count` fallback,
  plus a regression test in the real emitted shape.
- Cache database files are created with owner-only permissions
  (0600); the `-wal`/`-shm` sidecars inherit the mode (issue #21).
- `__version__` no longer drifts from `pyproject.toml` (it had been
  stuck at 0.1.0 since the first release): it now derives from the
  installed package metadata at runtime.
- A UNHCR outage no longer reads as zero refugees worldwide:
  `intel_displacement_summary`'s failure shape carries `error`,
  `degraded`, and `reason: unhcr_fetch_failed` instead of bare zeroed
  totals (#22).
- Climate zones whose fetch failed are named in `unavailable_zones`
  (with `degraded: true`) instead of silently vanishing; a full
  Open-Meteo outage is an `error` with a reason, and an invalid zone
  filter now says so and lists the valid keys (#23).
- "Resolved: Major outage" post-mortems no longer count as active
  critical incidents (resolution status outranks incident keywords),
  and a provider whose status feed is unreachable is named in
  `unavailable_providers` instead of masquerading as healthy with no
  incidents (#24).

## 0.3.0 - 2026-08-16

Briefs that show their work, and areas you define yourself.

### Added
- User-defined areas of interest (AOIs/geofences): `intel_aoi_define`,
  `intel_aoi_list`, `intel_aoi_delete`, `intel_aoi_brief`, and
  `intel_aoi_escalation` (+5 = 119 tools). Before this, only
  `intel_signal_convergence` accepted a real point-plus-radius,
  `intel_military_flights` took a bbox, and hotspot escalation scoring was
  restricted to the 22 hardcoded `INTEL_HOTSPOTS`. `intel_aoi_define`
  persists a named point-radius area (validated: lat -90..90, lon
  -180..180, radius_km 1..2000) in a dedicated `aois` table inside the
  existing SQLite cache database, rejecting a duplicate (case-insensitive)
  name by echoing the existing definition back instead of overwriting it.
  `intel_aoi_brief` composes a cited view of earthquakes, military flights
  (bbox derived from the radius), wildfires (region-mapped, since NASA
  FIRMS has no point+radius query), ACLED conflict events, sampled
  aviation traffic, nearby static infrastructure (bases, ports, pipelines,
  nuclear facilities, undersea cables, datacenters, spaceports) with
  distances in km, and news headline mentions of the AOI name, all
  filtered to the AOI's radius, with `data_gaps` naming every domain that
  couldn't be geographically scoped. `intel_aoi_escalation` reuses the
  existing `score_hotspot` scoring engine unmodified, scoped to the AOI's
  own radius rather than the fixed 2-degree window used for the 22
  built-in hotspots (#16).
- Situation briefs now cite their sources. `fetch_situation_brief` builds a
  numbered `sources` list while walking the overview data: domain, a short
  description or headline, a URL when the upstream item carries one, and a
  timestamp when available, covering every metric that contributes to the
  brief. The Ollama prompt is instructed to cite claims inline as `[n]`
  using only that list; the structured fallback brief cites mechanically,
  since it is assembled per-metric. The response gains `sources` and
  `cited`: `cited` is true only when the returned text actually contains a
  `[n]` matching a real source number, so a brief where the model ignored
  the citation instruction (or invented an out-of-range number) is
  distinguishable from one that genuinely cited its sources. A metric with
  no traceable upstream item is reported without a citation rather than
  pointing at something that isn't really there (#15).
- `intel_daily_digest`: a cited markdown morning brief composing top
  current events by domain, recent headlines, and, when the optional
  vector store is installed, recent activity trends and a 24-hour
  timeline. Every listed item carries a source reference. When the vector
  store isn't available, the Trends and Timeline sections say so via
  `data_gaps` instead of rendering as an empty, falsely-quiet section
  (#15).
- `intel_situation_brief` (+1 = 120 tools). The cited situation brief
  (#15) was previously reachable only through the dashboard's SSE
  overview; MCP clients, the primary consumer this server exists for,
  couldn't call it at all. The new tool gathers a bounded server-side
  overview (earthquakes, military flights, ACLED conflict events,
  wildfires, cyber threats, disease outbreaks, news, space weather,
  strategic posture, and the alert digest, not the dashboard's full
  47-source fan-out), then delegates to the existing, unmodified
  `fetch_situation_brief` for the AI-generated brief or its
  mechanically-cited fallback when Ollama is unreachable (#18).

### Fixed
- `_extract_metrics` (situation brief) read the earthquake event list under
  the key `events`, but `fetch_earthquakes` returns it under `earthquakes`,
  so the lookup always missed and `max_magnitude` was silently 0 in every
  brief ever generated. Found while wiring earthquake citations off the
  same field.
- Test counts in the docs lagged the 0.2.0 regression tests (claimed 244
  total when 0.2.0 itself shipped 256). Corrected to the live count.
- `fetch_gdelt_search` returned a clean zero-article payload when the
  GDELT API call failed, byte-identical to a genuine zero-hit search:
  same class as the ACLED defect fixed in 0.2.0 (#3). Observed live as a
  429: GDELT asks for one request per 5 seconds. Failure paths now carry
  `error`, `degraded`, and `reason` keys, keeping `articles`/`count` in
  their normal empty shape; `intel_aoi_brief`'s news domain now surfaces
  the failure as a `data_gap` instead of reading as "no news mentions"
  (#17).

## 0.2.0 - 2026-08-16

The honesty release. A full review of the data paths found five ways the
server could present missing or stale data as fresh and real. All five are
fixed, each with a regression test that fails against the old code.

### Fixed
- ACLED-backed tools returned a clean zero-event payload when the API call
  failed, indistinguishable from genuine peace. Failure paths now carry
  `error`, `degraded`, and `reason` keys, and the eight downstream
  conflict/instability tools report `data_gaps` (#3).
- The documented `_stale=True` marker on cached fallback responses never
  actually existed. It does now, along with `_stale_age_seconds`; non-dict
  stale serves are counted in `CircuitBreaker.status()` (#4).
- `intel_gas_prices` restamped days-old cached prices with a fresh
  `fetched_at`. The timestamp now reflects the cached row's real age, and
  the fallback branch is reachable on real outages, which it previously
  was not (#5).
- `intel_hotspot_escalation` hardcoded its news and convergence components
  to zero for every hotspot, so an active war zone could score the lowest
  severity tier. Unmeasured components now report `null` and the score
  renormalizes over what was actually measured. The tool description no
  longer claims signals it cannot compute (#6).
- The circuit breaker stopped enforcing backoff after its first cooldown
  cycle: a failed half-open probe never re-timestamped the trip. Confirmed
  by repro, then fixed (#7).

### Added
- Continuous integration: the test suite (238 passing at this release, 18 live-network
  smoke tests deselected) now runs on every push and pull request, Python
  3.11 and 3.12 (#11).
- CONTRIBUTING.md, bug report and feature request templates (#12).
- This changelog (#14).

### Changed
- The collector launchd plist ships as a template; `collector-daemon.sh`
  resolves the repository root at runtime instead of hardcoding the
  maintainer's machine paths, and gained a `render` subcommand (#8).
- The situation brief's Ollama default is `localhost`, matching the rest
  of the codebase; `OLLAMA_API_URL` and `OLLAMA_MODEL` are documented (#9).
- README counts corrected against live code, all of them undercounts:
  226 passing tests (was "186"), 15 central banks (was "8"), 119 RSS feeds
  (was "80+"), 47 SSE sources (was "35+") (#10).
- `requirements.txt` removed; `pyproject.toml` is the single source of
  dependency truth (#13).

### Response-shape notes for MCP clients
All changes are additive (new keys only), with one exception: inside
`intel_hotspot_escalation`, each hotspot's `components` dict now reports
`social_unrest` and `convergence` as separate entries instead of the
combined `social_convergence`. Top-level shapes are unchanged.

## 0.1.0 - 2026-06-04

Everything before versioning began: 113 tools across 30+ domains built in
eighteen phases, from market data and SEC filings through conflict
tracking, military flights, climate, cyber, news, vector search over
accumulated intelligence, and a live Leaflet dashboard with SSE streaming.
The phase-by-phase history lives in ROADMAP.md.
