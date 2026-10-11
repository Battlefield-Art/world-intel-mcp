# Changelog

## Unreleased

Cross-border migration and a fix to the displacement summary.

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

### Fixed: displacement summary
- `intel_displacement_summary` never sent UNHCR `coo_all=true`, so UNHCR
  answered with one global row and `by_origin` held a single entry named
  "-" (found by a live call 2026-10-11). It now returns the top 30 origins
  with ISO-3 codes. Names prefer the short TIER1 name ("Syria") so
  `intel_country_signals` can join displacement with the conflict sources;
  that join had no displacement input before.

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


Aircraft-cohort early warning, approach inspired by
[kylemcdonald/ews](https://github.com/kylemcdonald/ews) (independent
implementation; no ews code used).

### Added
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

### Data honesty
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
