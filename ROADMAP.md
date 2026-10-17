# World Intel MCP — Feature Parity Roadmap

**Benchmark**: [koala73/worldmonitor](https://github.com/koala73/worldmonitor)
**Updated**: 2026-10-12 (0.11.0: aircraft-cohort early warning, cross-border migration)
**Current tools**: 157 (156 intel + 1 status)

---

## Legend

| Icon | Meaning |
|------|---------|
| :white_check_mark: | We have this |
| :yellow_circle: | Partial — we have the data source but lack the analysis layer |
| :red_circle: | Missing entirely |

---

## 0. Current Assessment / Gap Report (2026-09-01)

| Area | Finding | Status | Action |
|------|---------|--------|--------|
| MCP tool parity | 128 tools declared in `TOOLS`; 128 routed in `_dispatch()` | :white_check_mark: | Keep as an invariant (machine-checked by import-based tests) |
| Optional vector runtime | Missing `qdrant-client` / `fastembed` previously surfaced as runtime failures | :white_check_mark: Fixed | Vector features now degrade cleanly and report availability |
| Base-environment test run | `pytest -q` fails collection without dev extras because `respx` is not installed | :yellow_circle: | Run `pip install -e ".[dev]"` before full-suite validation |
| Test coverage truth | Was 59% overall on 2026-09-01 morning (`server.py`/`cli.py`/`collector.py` at 0%, `analysis/` NLP modules 0-14%, `sources/intelligence.py` 22%). After the same-day test waves: **81% overall, 597 tests** (analysis modules 96-100%, `sources/intelligence.py` 93%, `server.py` 47% via import-based registry tests). Remaining zeros: `cli.py` (1,076 stmts); `collector.py` at 20% (map verified, run loop untested) | :yellow_circle: Improved | CI coverage gate that ratchets (Phase 24); `cli.py` smoke tests |
| Geofence correctness | Antimeridian AOIs lost the far side of the dateline (bbox clamp); pipelines/cables matched on endpoints only | :white_check_mark: Fixed in Phase 22 | Segment distance + split bboxes shipped with tests |
| Security posture | Issue #21 (external report) assessed; no shell/eval/exec, SQL parameterized throughout, report paths server-generated | :white_check_mark: | SECURITY.md added with explicit threat model; cache db now 0600 |
| Documentation drift | Prior roadmap documented 89/110 tools while the codebase now exposes 113 | :white_check_mark: Updated below | Keep roadmap synced with phase increments |
| Maintainability | server.py split into 12 domain modules + runtime.py (158-line shell); parity enforced at import time | :white_check_mark: Shipped 2026-09-01 (Phase 26) | Largest module 444 lines |
| CLI/dashboard parity for AOIs | Was MCP-only. `intel aoi` CLI group (9 subcommands) shipped in 0.8.0; dashboard AOI layer with last-sweep counts shipped in 0.10.0 | :white_check_mark: Shipped | Phase 23 complete |

### Implemented Addendum Missing From Prior Roadmap

#### Financial Intelligence Extensions (12 tools)

| Tool | Purpose | Status |
|------|---------|--------|
| `intel_forex_rates` | Latest FX rates by base + symbol filters | :white_check_mark: |
| `intel_forex_timeseries` | Historical FX series with configurable lookback | :white_check_mark: |
| `intel_major_crosses` | Major crosses + DXY proxy snapshot | :white_check_mark: |
| `intel_yield_curve` | Treasury curve + inversion analysis | :white_check_mark: |
| `intel_bond_indices` | Bond ETF summary (AGG, TLT, HYG, LQD, TIP) | :white_check_mark: |
| `intel_earnings_calendar` | Upcoming earnings calendar | :white_check_mark: |
| `intel_earnings_surprise` | Historical earnings surprise analysis | :white_check_mark: |
| `intel_sec_filings` | Full-text SEC EDGAR filing search | :white_check_mark: |
| `intel_company_filings` | Company-specific 10-K / 10-Q / 8-K retrieval | :white_check_mark: |
| `intel_recent_8k` | Recent material-event 8-K stream | :white_check_mark: |
| `intel_company_profile` | Composite company enrichment profile | :white_check_mark: |
| `intel_macro_composite` | Weighted market regime / macro composite score | :white_check_mark: |

#### Vector & Cross-Domain Analytics (8 tools)

| Tool | Purpose | Status |
|------|---------|--------|
| `intel_semantic_search` | Natural-language search across accumulated intelligence | :white_check_mark: |
| `intel_similar_events` | Similarity search against historical events | :white_check_mark: |
| `intel_timeline` | Chronological timeline from vector store history | :white_check_mark: |
| `intel_vector_stats` | Qdrant collection statistics | :white_check_mark: |
| `intel_collect` | On-demand collection cycle for vector population | :white_check_mark: |
| `intel_cross_correlate` | Cross-category correlation for a topic/query | :white_check_mark: |
| `intel_domain_summary` | Per-category summary of stored intelligence | :white_check_mark: |
| `intel_trend_detection` | Recent-vs-baseline activity surge/drop detection | :white_check_mark: |

#### Intelligence Reports (1 tool)

| Tool | Purpose | Status |
|------|---------|--------|
| `intel_generate_report` | Generate PDF/HTML multi-domain intelligence reports with HTML fallback when `.[pdf]` is unavailable | :white_check_mark: |

---

## 1. Data Sources — Complete Inventory

### Markets & Economics (16 tools)

| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_market_quotes` | `list-market-quotes` | :white_check_mark: |
| `intel_crypto_quotes` | `list-crypto-quotes` | :white_check_mark: |
| `intel_stablecoin_status` | `list-stablecoin-markets` | :white_check_mark: |
| `intel_etf_flows` | `list-etf-flows` | :white_check_mark: |
| `intel_sector_heatmap` | `get-sector-summary` | :white_check_mark: |
| `intel_macro_signals` | `get-macro-signals` | :white_check_mark: |
| `intel_commodity_quotes` | `list-commodity-quotes` | :white_check_mark: |
| `intel_gas_prices` | AAA retail fuel prices | :white_check_mark: |
| `intel_residential_natgas` | EIA residential natural gas prices | :white_check_mark: |
| `intel_electricity_rates` | EIA electricity rates by sector/state | :white_check_mark: |
| `intel_energy_prices` | `get-energy-prices` | :white_check_mark: |
| `intel_fred_series` | `get-fred-series` | :white_check_mark: |
| `intel_world_bank_indicators` | `list-world-bank-indicators` | :white_check_mark: |
| `intel_country_stocks` | Country main index ticker | :white_check_mark: |
| `intel_btc_technicals` | BTC SMA-50/200, Mayer Multiple, cross signals | :white_check_mark: |
| `intel_central_bank_rates` | 15 central bank policy rates (FRED + curated) | :white_check_mark: |

### Natural Disasters & Climate (5 tools)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_earthquakes` | `list-earthquakes` | :white_check_mark: |
| `intel_wildfires` | `list-fire-detections` | :white_check_mark: |
| `intel_climate_anomalies` | `list-climate-anomalies` | :white_check_mark: |
| `intel_environmental_events` | NASA EONET events | :white_check_mark: |
| `intel_disaster_alerts` | GDACS global disaster alerts | :white_check_mark: |

### Conflict & Security (4 tools)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_acled_events` | `list-acled-events` | :white_check_mark: |
| `intel_ucdp_events` | `list-ucdp-events` | :white_check_mark: |
| `intel_unrest_events` | ACLED protests + GDELT dedup | :white_check_mark: |
| `intel_cyber_threats` | `list-cyber-threats` | :white_check_mark: |

### Military & Defense (8 tools)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_military_flights` | `list-military-flights` | :white_check_mark: |
| `intel_theater_posture` | `get-theater-posture` | :white_check_mark: |
| `intel_aircraft_details` | `get-aircraft-details` | :white_check_mark: |
| `intel_aircraft_batch` | Batch ICAO24 lookup | :white_check_mark: |
| `intel_vessel_snapshot` | `get-vessel-snapshot` | :white_check_mark: |
| `intel_military_surge` | `military-surge.ts` | :white_check_mark: |
| `intel_military_bases` | Static dataset (70 bases) | :white_check_mark: |
| `intel_usni_fleet` | USNI Fleet Tracker weekly disposition | :white_check_mark: |

### Infrastructure & Maritime (6 tools)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_internet_outages` | `list-internet-outages` | :white_check_mark: |
| `intel_cable_health` | `get-cable-health` | :white_check_mark: |
| `intel_nav_warnings` | `list-navigational-warnings` | :white_check_mark: |
| `intel_cascade_analysis` | `infrastructure-cascade.ts` | :white_check_mark: |
| `intel_strategic_ports` | Static dataset (40 ports) | :white_check_mark: |
| `intel_pipelines` | Static dataset (24 pipelines) | :white_check_mark: |

### Humanitarian & Social (10 tools)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_humanitarian_summary` | `get-humanitarian-summary` | :white_check_mark: |
| `intel_displacement_summary` | `get-displacement-summary` | :white_check_mark: (per-origin rows since 2026-10-11; before that one global row) |
| `intel_migration_flows` | none (beyond parity) | :white_check_mark: UNHCR origin -> destination corridors |
| `intel_migration_surges` | none (beyond parity) | :white_check_mark: year-over-year mass-movement ranking |
| `intel_migration_history` | none (beyond parity) | :white_check_mark: per-country series since 1951 |
| `intel_net_migration` | none (beyond parity) | :white_check_mark: World Bank net migration + migrant stock |
| `intel_us_border_encounters` | none (beyond parity) | :white_check_mark: CBP monthly encounters (a flow) |
| `intel_uk_small_boat_arrivals` | none (beyond parity) | :white_check_mark: Home Office small-boat arrivals (a flow) |
| `intel_social_signals` | Reddit public intelligence | :white_check_mark: |
| `intel_disease_outbreaks` | WHO/ProMED/CIDRAP RSS | :white_check_mark: |

### News & Information (4 tools)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_news_feed` | 100 RSS feeds, 4-tier sources | :white_check_mark: |
| `intel_trending_keywords` | trending-keywords service | :white_check_mark: |
| `intel_gdelt_search` | `search-gdelt-documents` | :white_check_mark: |
| `intel_ai_releases` | AI model/paper tracker | :white_check_mark: |

### Transport & Traffic (6 tools)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_prediction_markets` | `list-prediction-markets` | :white_check_mark: |
| `intel_airport_delays` | `list-airport-delays` | :white_check_mark: |
| `intel_shipping_index` | Yahoo Finance shipping ETFs | :white_check_mark: |
| `intel_traffic_flow` | Real-time congestion in 20 cities (TomTom) | :white_check_mark: |
| `intel_traffic_incidents` | Major incidents in 5 strategic regions (TomTom) | :white_check_mark: |
| `intel_aviation_domestic` | Global air traffic snapshot (OpenSky) | :white_check_mark: |

### Webcams (1 tool)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_webcams` | Public webcam locations worldwide (Windy) | :white_check_mark: |

### Analysis & Intelligence (10 tools)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_risk_scores` | `get-risk-scores` | :white_check_mark: |
| `intel_instability_index` | CII v2 (multi-signal blend) | :white_check_mark: |
| `intel_signal_convergence` | `geo-convergence.ts` | :white_check_mark: |
| `intel_focal_points` | `focal-point-detector.ts` | :white_check_mark: |
| `intel_signal_summary` | `signal-aggregator.ts` | :white_check_mark: |
| `intel_temporal_anomalies` | `temporal-baseline.ts` | :white_check_mark: |
| `intel_hotspot_escalation` | `hotspot-escalation.ts` | :white_check_mark: |
| `intel_alert_digest` | Cross-domain alert synthesis | :white_check_mark: |
| `intel_weekly_trends` | Temporal trend analysis | :white_check_mark: |

### Country & Geopolitical (4 tools)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_country_brief` | `get-country-intel-brief` | :white_check_mark: |
| `intel_country_dossier` | Comprehensive 6-source country analysis | :white_check_mark: |
| `intel_election_calendar` | Election proximity risk | :white_check_mark: |
| `intel_sanctions_search` | OFAC SDN search | :white_check_mark: |

### Strategic Synthesis (4 tools)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_strategic_posture` | Composite 9-domain risk assessment | :white_check_mark: |
| `intel_world_brief` | Structured daily intelligence summary | :white_check_mark: |
| `intel_fleet_report` | Naval fleet activity report | :white_check_mark: |
| `intel_population_exposure` | Population near active events | :white_check_mark: |

### Tech & Science (3 tools)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_hacker_news` | Top HN stories (Firebase API) | :white_check_mark: |
| `intel_trending_repos` | GitHub trending repos | :white_check_mark: |
| `intel_arxiv_papers` | Recent AI/ML papers | :white_check_mark: |

### Government (1 tool)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_usa_spending` | USAspending.gov federal data | :white_check_mark: |

### Specialist (3 tools)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_space_weather` | NOAA/SWPC feeds | :white_check_mark: |
| `intel_nuclear_monitor` | USGS seismic near test sites | :white_check_mark: |
| `intel_service_status` | Cloudflare/AWS/Azure/GCP | :white_check_mark: |

### NLP Intelligence (4 tools)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_extract_entities` | Regex NER (28 leaders, 41 orgs, 36 APTs) | :white_check_mark: |
| `intel_classify_event` | Keyword threat classification (14 categories) | :white_check_mark: |
| `intel_news_clusters` | Jaccard similarity clustering | :white_check_mark: |
| `intel_keyword_spikes` | Welford's algorithm spike detection | :white_check_mark: |

### Extended Geospatial (10 tools)
| Tool | WM Equivalent | Status |
|------|---------------|--------|
| `intel_undersea_cables` | 34 cables with landing points | :white_check_mark: |
| `intel_ai_datacenters` | 48 global AI/cloud clusters | :white_check_mark: |
| `intel_spaceports` | 27 launch facilities | :white_check_mark: |
| `intel_critical_minerals` | 27 deposit types | :white_check_mark: |
| `intel_stock_exchanges` | 82 global exchanges | :white_check_mark: |
| `intel_trade_routes` | 19 maritime chokepoints/routes | :white_check_mark: |
| `intel_cloud_regions` | 28 AWS/Azure/GCP regions | :white_check_mark: |
| `intel_financial_centers` | 20 GFCI-ranked cities | :white_check_mark: |
| `intel_nuclear_facilities` | 24 power/enrichment/research | :white_check_mark: |

### Financial Intelligence (12 tools)
| Tool | Purpose | Status |
|------|---------|--------|
| `intel_forex_rates` | Latest FX rates by base + symbol filters | :white_check_mark: |
| `intel_forex_timeseries` | Historical FX series with configurable lookback | :white_check_mark: |
| `intel_major_crosses` | Major crosses + DXY proxy snapshot | :white_check_mark: |
| `intel_yield_curve` | Treasury curve + inversion analysis | :white_check_mark: |
| `intel_bond_indices` | Bond ETF summary (AGG, TLT, HYG, LQD, TIP) | :white_check_mark: |
| `intel_earnings_calendar` | Upcoming earnings calendar | :white_check_mark: |
| `intel_earnings_surprise` | Historical earnings surprise analysis | :white_check_mark: |
| `intel_sec_filings` | Full-text SEC EDGAR filing search | :white_check_mark: |
| `intel_company_filings` | Company-specific 10-K / 10-Q / 8-K retrieval | :white_check_mark: |
| `intel_recent_8k` | Recent material-event 8-K stream | :white_check_mark: |
| `intel_company_profile` | Composite company enrichment profile | :white_check_mark: |
| `intel_macro_composite` | Weighted market regime / macro composite score | :white_check_mark: |

### Vector Intelligence (5 tools)
| Tool | Purpose | Status |
|------|---------|--------|
| `intel_semantic_search` | Natural-language search across accumulated intelligence | :white_check_mark: |
| `intel_similar_events` | Similarity search against historical events | :white_check_mark: |
| `intel_timeline` | Chronological timeline from vector store history | :white_check_mark: |
| `intel_vector_stats` | Qdrant collection statistics | :white_check_mark: |
| `intel_collect` | On-demand collection cycle for vector population | :white_check_mark: |

### Cross-Domain Analytics (3 tools)
| Tool | Purpose | Status |
|------|---------|--------|
| `intel_cross_correlate` | Cross-category correlation for a topic/query | :white_check_mark: |
| `intel_domain_summary` | Per-category summary of stored intelligence | :white_check_mark: |
| `intel_trend_detection` | Recent-vs-baseline activity surge/drop detection | :white_check_mark: |

### Reports (1 tool)
| Tool | Purpose | Status |
|------|---------|--------|
| `intel_generate_report` | PDF/HTML multi-domain intelligence reports | :white_check_mark: |

### System (1 tool)
| Tool | Purpose | Status |
|------|---------|--------|
| `intel_status` | Source health, cache stats, tool count | :white_check_mark: |

---

## 2. Static Geospatial Datasets

| Dataset | Records | Query Tool | Status |
|---------|---------|------------|--------|
| Military bases | 70 bases, 9 operators | `intel_military_bases` | :white_check_mark: |
| Strategic ports | 40 ports, 6 types | `intel_strategic_ports` | :white_check_mark: |
| Pipelines | 24 oil/gas/hydrogen | `intel_pipelines` | :white_check_mark: |
| Nuclear facilities | 24 power/enrichment/research | `intel_nuclear_facilities` | :white_check_mark: |
| Intel hotspots | 22 geopolitical hotspots | config/countries.py | :white_check_mark: |
| Conflict zones | Active conflict centers | config/countries.py | :white_check_mark: |
| Strategic waterways | 8 chokepoints | config/countries.py | :white_check_mark: |
| Nuclear test sites | 5 sites with monitoring | config/countries.py | :white_check_mark: |
| Countries config | 22 nations with risk baselines | config/countries.py | :white_check_mark: |
| Major cities | 105 cities (pop > 2M, 1B coverage) | config/population.py | :white_check_mark: |
| Undersea cables | 34 cables with landing points | `intel_undersea_cables` | :white_check_mark: |
| AI datacenters | 48 global clusters | `intel_ai_datacenters` | :white_check_mark: |
| Spaceports | 27 launch facilities | `intel_spaceports` | :white_check_mark: |
| Critical minerals | 27 deposit types | `intel_critical_minerals` | :white_check_mark: |
| Stock exchanges | 82 global exchanges | `intel_stock_exchanges` | :white_check_mark: |
| Trade routes | 19 maritime chokepoints/routes | `intel_trade_routes` | :white_check_mark: |
| Cloud regions | 28 AWS/Azure/GCP regions | `intel_cloud_regions` | :white_check_mark: |
| Financial centers | 20 GFCI-ranked cities | `intel_financial_centers` | :white_check_mark: |

---

## 3. RSS Feed Coverage

Expanded to **119 feeds** across **24 categories** with 4-tier source ranking (wire/major/specialty/aggregator) and propaganda risk labels.

| Category | Count | Status |
|----------|-------|--------|
| Geopolitics | 9 | :white_check_mark: |
| Security/Cyber | 8 | :white_check_mark: |
| Technology | 6 | :white_check_mark: |
| Finance | 6 | :white_check_mark: |
| Defense/Military | 6 | :white_check_mark: |
| Science | 6 | :white_check_mark: |
| Think Tanks | 7 | :white_check_mark: |
| Middle East | 4 | :white_check_mark: |
| Asia-Pacific | 5 | :white_check_mark: |
| Africa | 5 | :white_check_mark: |
| Latin America | 8 | :white_check_mark: |
| Multilingual (ES/FR/DE) | 7 | :white_check_mark: |
| Energy | 5 | :white_check_mark: |
| Government | 4 | :white_check_mark: |
| Crisis/Intl Orgs | 4 | :white_check_mark: |
| Europe | 4 | :white_check_mark: |
| South Asia | 3 | :white_check_mark: |
| Health | 4 | :white_check_mark: |
| Central Asia | 3 | :white_check_mark: |
| Arctic | 3 | :white_check_mark: |
| Maritime | 3 | :white_check_mark: |
| Space | 3 | :white_check_mark: |
| Nuclear | 3 | :white_check_mark: |
| Climate | 3 | :white_check_mark: |

---

## 4. Dashboard

Live Starlette app with SSE streaming at `intel-dashboard --port 8501`.

| Feature | Status |
|---------|--------|
| SSE streaming (30s refresh) | :white_check_mark: 39 data streams |
| Leaflet map (14 layers + 6 static + trade routes) | :white_check_mark: |
| 14 expandable drawer sections | :white_check_mark: |
| USNI Fleet Tracker drawer | :white_check_mark: |
| BTC Technicals drawer (SMA, Mayer, cross signals) | :white_check_mark: |
| Central Bank Rates drawer (15 banks) | :white_check_mark: |
| Data freshness monitoring drawer | :white_check_mark: |
| Per-source circuit breaker health | :white_check_mark: |
| AI situation brief (Ollama-powered) | :white_check_mark: |
| Static HTML reports | Removed (dashboard replaces) |

---

## 5. System Architecture

| Feature | Status | Notes |
|---------|--------|-------|
| SQLite WAL-mode cache | :white_check_mark: | Persistent TTL, stale fallback |
| Per-source circuit breaker | :white_check_mark: | Configurable thresholds |
| Data freshness monitoring | :white_check_mark: | Per-source staleness in dashboard |
| Per-coro timeout (45s) | :white_check_mark: | No single source blocks dashboard |

---

## Completed Phases

### Phase 1-4: Foundation (0 -> 36 tools)
Core data sources: markets, crypto, macro, earthquakes, wildfires, ACLED, UCDP, humanitarian, military flights, theater posture, aircraft, internet outages, cable health, nav warnings, climate, prediction markets, displacement, airport delays, cyber threats, news feeds, GDELT, trending keywords, country briefs, risk scores, instability index, signal convergence.

### Phase 5: Core Analysis Engine (+3 = 39 tools)
`intel_focal_points`, `intel_signal_summary`, `intel_temporal_anomalies`
Countries config with 22 nations, intel hotspots, conflict zones, strategic waterways. CII v2 upgraded with multi-signal weighted blend. Welford's online algorithm for temporal baseline anomaly detection.

### Phase 6: Military & Infrastructure Intelligence (+6 = 45 tools)
`intel_vessel_snapshot`, `intel_military_surge`, `intel_cascade_analysis`, `intel_hotspot_escalation`, `intel_commodity_quotes`, `intel_unrest_events`
AIS vessel tracking, military surge detection in 17 sensitive regions, infrastructure cascade analysis, hotspot escalation scoring for 22 locations.

### Phase 7: Domain Expansion (+10 = 55 tools)
`intel_space_weather`, `intel_ai_releases`, `intel_disease_outbreaks`, `intel_sanctions_search`, `intel_election_calendar`, `intel_shipping_index`, `intel_social_signals`, `intel_nuclear_monitor`, `intel_alert_digest`, `intel_weekly_trends`
80+ RSS feeds with 4-tier source ranking. WHO/ProMED/CIDRAP health monitoring. OFAC sanctions search. Election proximity risk scoring. Reddit social signals. Nuclear test site seismic monitoring. Cross-domain alert digest. Temporal weekly trend analysis.

### Phase 8: Service Status & Geospatial (+5 = 60 tools)
`intel_service_status`, `intel_military_bases`, `intel_strategic_ports`, `intel_pipelines`, `intel_nuclear_facilities`
Cloud service status monitoring (Cloudflare/AWS/Azure/GCP). Static geospatial datasets: 70 military bases from 9 operators, 40 strategic ports across 6 types, 24 oil/gas/hydrogen pipelines, 24 nuclear facilities. All queryable with filters. Dashboard infrastructure map layer.

### Phase 9: NLP Intelligence (+4 = 64 tools)
`intel_extract_entities`, `intel_classify_event`, `intel_news_clusters`, `intel_keyword_spikes`
Regex-based NER (28 leaders, 41 orgs, 25 companies, 36 APT groups, CVE extraction). Keyword-based threat classification into 14 categories with severity scoring. Jaccard similarity news clustering with keyword extraction. Welford's algorithm keyword spike detection against rolling baselines. Entity reference database in config/entities.py. No ML dependencies.

### Phase 10: Strategic Synthesis (+4 = 68 tools)
`intel_strategic_posture`, `intel_world_brief`, `intel_fleet_report`, `intel_population_exposure`
Composite strategic posture assessment from 9 weighted domains (military, political, conflict, infrastructure, economic, cyber, health, climate, space). Structured world intelligence brief aggregating posture, focal points, news clusters, temporal anomalies, and keyword spikes. Naval fleet activity report combining theater posture, vessel snapshots, and surge detections. Population exposure analysis near active events using 105-city dataset (1B pop coverage).

### Phase 11: Extended Data & Geospatial (+12 = 80 tools)
`intel_country_stocks`, `intel_aircraft_batch`, `intel_hacker_news`, `intel_trending_repos`, `intel_arxiv_papers`, `intel_usa_spending`, `intel_environmental_events`, `intel_disaster_alerts`, `intel_undersea_cables`, `intel_ai_datacenters`, `intel_spaceports`, `intel_critical_minerals`, `intel_stock_exchanges`, `intel_usni_fleet`

Static datasets completed: 34 undersea cables, 48 AI datacenters, 27 spaceports, 27 critical mineral deposits, 82 stock exchanges. USNI Fleet Tracker for Navy disposition. Data freshness monitoring added to dashboard. Static HTML report generation removed (live dashboard replaces).

### Phase 12: Financial Intelligence & Geospatial Expansion (+5 = 84 tools)
`intel_btc_technicals`, `intel_central_bank_rates`, `intel_trade_routes`, `intel_cloud_regions`, `intel_financial_centers`

BTC technical analysis with SMA-50/200, Mayer Multiple, golden/death cross signals, and ATH distance via CoinGecko historical data. Central bank policy rates for 15 major banks (Fed, ECB, BoE, BoJ, PBoC, RBI, RBA, BoC, SNB, BCB, BoK, CBRT, SARB, Banxico, BI) — live FRED data when API key set, curated fallback otherwise. Static geospatial datasets: 19 maritime trade routes/chokepoints with oil flow and vessel transit data, 28 cloud provider regions (AWS/Azure/GCP), 20 GFCI-ranked financial centers. Trade route markers added to dashboard infrastructure map layer. BTC technicals and central bank rates added to dashboard drawer.

### Phase 13: Country Dossier, Full Tool Exposure & Feed Expansion (+5 = 89 tools)
`intel_country_dossier`, `intel_traffic_flow`, `intel_traffic_incidents`, `intel_aviation_domestic`, `intel_webcams`

Comprehensive country intelligence dossier aggregating 6 sources in parallel (economy, markets, elections, sanctions, news, security). Exposed 4 previously hidden source functions: TomTom traffic flow (20 cities) and incidents (5 regions), OpenSky global air traffic snapshot, Windy public webcams. RSS feeds expanded from 100 to 119 across 24 categories (+6 new categories: central_asia, arctic, maritime, space, nuclear, climate). 49 CLI commands; the repo test suite has since grown well beyond this phase snapshot.

### Phase 14: Financial Intelligence Extensions (+12 = 101 tools)
`intel_forex_rates`, `intel_forex_timeseries`, `intel_major_crosses`, `intel_yield_curve`, `intel_bond_indices`, `intel_earnings_calendar`, `intel_earnings_surprise`, `intel_sec_filings`, `intel_company_filings`, `intel_recent_8k`, `intel_company_profile`, `intel_macro_composite`

Added business / market-intelligence depth: FX rates and timeseries, bond curves and ETF indices, earnings calendar and surprise analysis, SEC EDGAR search and company filings, composite company enrichment, and a weighted macro-composite market regime layer.

### Phase 15: Vector Intelligence (+5 = 106 tools)
`intel_semantic_search`, `intel_similar_events`, `intel_timeline`, `intel_vector_stats`, `intel_collect`

Qdrant-backed semantic retrieval added across all fetched intelligence, plus timeline reconstruction, store statistics, and on-demand collection. Optional dependencies remain behind `.[vector]` and now degrade cleanly when unavailable.

### Phase 16: Cross-Domain Analytics (+3 = 109 tools)
`intel_cross_correlate`, `intel_domain_summary`, `intel_trend_detection`

Added historical cross-category correlation, stored-data summarization, and recent-vs-baseline trend detection on top of the vector archive for early-warning and activity-shift analysis.

### Phase 17: Intelligence Reports (+1 = 110 tools)
`intel_generate_report`

Added PDF/HTML intelligence report generation over the existing multi-domain data collection stack. PDF output remains optional behind `.[pdf]`, with HTML fallback available when WeasyPrint is not installed.

### Phase 18: Consumer Energy Signals (+3 = 113 tools)
`intel_gas_prices`, `intel_residential_natgas`, `intel_electricity_rates`

Added retail fuel, residential natural gas, and electricity-rate tools to round out consumer energy monitoring alongside existing EIA crude, gas, FRED, and World Bank economic signals.

### Phase 19: Cited Situation Briefs + Daily Digest (+1 = 114 tools)
`intel_daily_digest`

The situation brief (used by the dashboard, not itself a separate tool) now
builds a numbered `sources` list while walking the overview data, and
carries an honest `cited` flag: true only when the returned text actually
references a real source number, so a brief where the model ignored the
citation instruction is distinguishable from one that didn't. The new
`intel_daily_digest` tool composes a cited markdown morning brief from
current events across several domains plus, when the optional vector
store is installed, recent trend detection and a timeline, degrading via
`data_gaps` rather than an empty section when the vector store isn't
available.

### Phase 20: AOI Geofences (+5 = 119 tools)
`intel_aoi_define`, `intel_aoi_list`, `intel_aoi_delete`, `intel_aoi_brief`, `intel_aoi_escalation`

User-defined areas of interest (AOIs), the intel-community term for what
consumer software calls a geofence. Before this phase, only
`intel_signal_convergence` accepted a real point-plus-radius, only
`intel_military_flights` took a bbox, and hotspot escalation scoring was
restricted to the 22 hardcoded `INTEL_HOTSPOTS`. `intel_aoi_define`
persists a named point-radius area in a dedicated table inside the
existing SQLite cache database; `intel_aoi_brief` composes a cited view
of earthquakes, military flights (bbox-derived), wildfires
(region-mapped), ACLED conflict events, sampled aviation, nearby static
infrastructure with distances in km, and news headline mentions, all
filtered to the AOI's radius, with `data_gaps` for domains that can't be
geographically scoped. `intel_aoi_escalation` reuses the existing
`score_hotspot` scoring engine unmodified for a user AOI (#16).

### Phase 21: Situation Brief over MCP (+1 = 120 tools)
`intel_situation_brief`

The cited situation brief (#15) was previously reachable only through the
dashboard's SSE overview; MCP clients, the primary consumer this server
exists for, couldn't call it. `intel_situation_brief` gathers a bounded
server-side overview (earthquakes, military flights, ACLED conflict
events, wildfires, cyber threats, disease outbreaks, news, space weather,
strategic posture, alert digest, not the dashboard's full 47-source
fan-out) and delegates to the existing, unmodified `fetch_situation_brief`
for the AI-generated brief or its mechanically-cited fallback when Ollama
is unreachable (#18).

### Phase 22: Geofence Hardening + Change Detection (+2 = 122 tools)
`intel_aoi_update`, `intel_aoi_changes`

Geofence correctness and the alerting primitive:

- **Antimeridian AOIs.** `bboxes_from_radius_km` replaces the old
  single-box clamp: an AOI whose circle crosses the dateline (Bering
  Strait, Fiji, Chukotka) now produces two bounding boxes instead of
  silently losing everything on the far side of lon ±180. Military
  flight fetches run per box and merge with icao24 dedup; a one-box
  failure surfaces as `partial coverage` in `data_gaps` instead of
  passing half-coverage off as full. Wildfire region mapping uses the
  same wrap-aware boxes, so an AOI just east of the dateline maps into
  the oceania FIRMS box instead of reporting a false coverage gap.
- **Lines are lines now.** `segment_distance_km` (great-circle
  cross-track distance with endpoint clamping) replaces endpoint-only
  proximity for pipelines and landing-point-only proximity for
  undersea cables: a pipeline or cable whose midspan passes through
  the AOI is detected even when its endpoints are far away. Still an
  approximation of surveyed routes; the docstrings say exactly how.
- **`intel_aoi_update`.** Rename and/or re-center/resize an AOI in
  place with define-grade validation and collision checks. A rename
  keeps change-detection history; a geometry change drops it (the old
  baseline described a different piece of the planet).
- **`intel_aoi_changes`.** The geofence alerting primitive: what
  entered or left since the last sweep, per domain (earthquakes,
  military flights, ACLED events, wildfire clusters, news mentions),
  built on the same scoped gather as `intel_aoi_brief` so the two
  tools can never disagree about what is inside the fence. First
  sweep is an explicit baseline; a failed domain fetch goes to
  `data_gaps` and is excluded from the diff (a failed fetch never
  reads as "everything left the area"), keeping its last real
  observation for the next successful sweep. Sampled aviation is
  excluded by design (1-in-10 sample churn is not signal).

Also in this phase: `SECURITY.md` with an explicit threat model
(prompted by issue #21), cache database created 0600, and the brief's
gather refactored into a single shared scoping path
(`_gather_scoped_domains`).

### Phase 27: Aircraft-Cohort Early Warning (+3 = 135 tools)
`intel_jet_cohort_snapshot`, `intel_jet_activity_anomaly`, `intel_jet_excess_origins`

Approach inspired by kylemcdonald/ews (independent implementation, no
ews code). Business-jet (69 ICAO types) and military cohorts from
adsb.lol, learned 24/7 by the collector (`bizjet_cohort`,
`military_cohort`, collector sources 51 -> 53). The anomaly tool compares
a UTC half-hour's peak airborne cohort count against the same slot on
prior same-weekday dates (US holidays swapped out) from `globe_history`
heatmaps; the excess-origins tool infers takeoffs/landings and clusters
them. Only aircraft first seen 24 h before the window count, so a fresh
install answers `warming_up` for a day.

### Phase 28: Cross-Border Migration (+6 = 141 tools)
`intel_migration_flows`, `intel_migration_surges`, `intel_migration_history`, `intel_net_migration`,
`intel_us_border_encounters`, `intel_uk_small_boat_arrivals`

UNHCR origin -> destination corridors (about 6,000 for 2025) with
prior-year change and country/region filters; a surge ranking by
origin, corridor and internal displacement sized against population
(World Bank SP.POP.TOTL); per-country series since 1951 as origin or
host; World Bank net migration and migrant stock for migration of
every kind. Fixed in the same change: `intel_displacement_summary` had
never sent `coo_all=true` and returned one global row. All figures are
year-end stocks (stated in each response), not crossings.

Shipped in 0.11.0 with the dashboard corridor map, CLI commands and
collector sources. Open: near-real-time displacement (IOM DTM, HDX HAPI:
both need keys).

Border crossings (Unreleased, `sources/border_crossings.py`): FLOWS, events
per day, week or month, kept apart from the stocks above and never added to
or compared with them. Publisher status as measured on 2026-10-17 with an
identifying User-Agent:

| Publisher | Measured | Status |
|-----------|----------|--------|
| U.S. CBP | Dataset page `cbp.gov/document/stats/nationwide-encounters` 200; newest file a 6.7 MB CSV under `/sites/default/files/2026-09/` (200); robots.txt 200, disallows `/sites/default/files/assets/documents/` (the FY20-FY23 files and data dictionary, never requested); public domain, cite CBP | :white_check_mark: `intel_us_border_encounters`, monthly from October 2022 |
| UK Home Office | GOV.UK Content API 200 (keyless, 10 requests/s); time series published only as ODS on `assets.publishing.service.gov.uk` (200, robots.txt 404), read with zipfile and the expat streaming parser (DTDs refused; only the three sheets used are read, under one row, cell and text budget), no new dependency; Open Government Licence v3.0 | :white_check_mark: `intel_uk_small_boat_arrivals`, daily from 2018, weekly preventions from May 2024 |
| Frontex | Migratory map page 200; its only data file is an XLSX under `/assets/`, which robots.txt (200) disallows for every user agent; not requested, no workaround | :red_circle: blocked by robots.txt (and XLSX only) |

Not exposed yet: CBP sector and field-office (area of responsibility) and
by-state detail, and CBP years before FY2023 (robots-disallowed path).
Open (shared fetcher): redirects are followed, and the download size caps
apply only after the body is read; the fix is for both tools to request
with `Fetcher.get_bytes(..., follow_redirects=False)` once the fetcher
offers it.

---

## Planned Phases

The roadmap above is history; this section is the actual road ahead.
Ordered by value; numbers are proposals, not commitments.

### Phase 29 (in progress, 2026-10-17): Research-Grade History and Evidence
Clean-room design: ideas only, no third-party code or text is used. The
pattern this phase adopts is that every answer is built from saved records
with three separate dates: the period a value describes, when its publisher
released it, and when this server collected it. The server could not do
that: the cache keeps one latest value per key, so there was no history to
correlate, revise or replay.

Wave 1 runs in parallel; wave 2 builds on the history store; wave 3 is the
dashboard.

| Wave | Feature | What it means here | Status |
|------|---------|--------------------|--------|
| 1 | Observation history store | Every collector capture saved with `observed_at` (period the data describe), `published_at` (publisher release, when known) and `collected_at` (when we saved it); unchanged payloads stored once; bounded retention; backfill for sources that serve history (FRED CSV) | :white_check_mark: Unreleased: store, three dates, dedupe, retention and `register_extractor()` done (`history.py`; extractors for Kp, USGS magnitudes, FRED central bank rates, Yahoo quotes, and in `history_extractors.py` the liquidity board, net liquidity and World Bank net migration). Backfill built 2026-10-17 (`history_backfill.py`, `intel_history_backfill`, `intel history backfill fred|ecb`) for FRED graph CSV and ECB Data Portal (BSI, ILM) series, under its own sources `backfill:fred` and `backfill:ecb`; `published_at` is NULL (neither response names a release date) and values are the vintage of the backfill day. Measured live 2026-10-17: WALCL 1,244 values (2002-12-18 to 2026-10-14), RRPONTSYD 3,348 (2,833 blank cells skipped), DGS10 16,182 (721 skipped), euro area M3 560 months (1980-01 to 2026-08); a second run and a refetch stored 0 |
| 1 | Capture history and revisions | Earlier saved copies of a source, and values a later copy revised | :white_check_mark: Unreleased (`intel_capture_history` with payload replay, `intel_revisions`) |
| 1 | Collection health | Per-source expected cadence; overdue / failing / never-run per source, measured from the run log, not inferred from cache freshness. Says what it cannot prove | :white_check_mark: Unreleased (`intel_collection_health`, `intel history health`) |
| 1 | Research guide over MCP | MCP resources (guide generated from the live registry and source health, measures glossary, "what this server will not do") and MCP prompts (worked research questions, each naming the tools it needs and the limit to respect). Server exposed no prompts or resources before | :white_check_mark: Built 2026-10-17, unreleased: 3 resources (`intel://guide`, `intel://guide/source-health`, `intel://guide/measures`), 29 prompts, `intel guide`; verified over a live stdio session |
| 1 | Hostility timeline | Per country pair or country, daily counts of threat / sanction / force / criticism coverage plus tone (`intel_hostility_timeline`); situations between and inside countries, sortable by most covered, most hostile, biggest change (`intel_situations`) | :white_check_mark: Built 2026-10-17, unreleased: `intel_hostility_timeline` (GDELT DOC 2.0 `timelinevolraw` + `timelinetone`, one call per category; failed calls, expired cached copies and missing days null; `column_status` per call; partial days judged per column) and `intel_situations` (RSS headlines, ranked by covered / hostile / change, change defined over feeds whose history spans both windows, null with `change_measured: false` when no feed does). Review fixes 2026-10-17: expired GDELT copies were shown as current counts; a day with no buckets in the judging series read as complete; "no change" was reported where nothing was compared; a GDELT 429 was retried after 1 s and then 2 s; unknown codes such as "GBR" were searched as text; a partial RSS outage was unmarked; limits below 1 misbehaved. All fixed with failing-first tests. Measured 2026-10-17: timeline modes answer (HTTP 200, `date_resolution` `hour` for `timespan=7d` and `15m` for `3d`, explicit zero buckets, `norm` = all monitored articles), but a `timespan=7d` request returned 54 hours (2026-10-15T10Z onward), and about half of calls drew 429 even 30 s apart (later probes, 16:51 to 16:53Z, 4 of 4 drew 429, so the resolution for 14 days and coverage before 2026-10-15T10Z are unmeasured; 30 days was not probed; a live MCP run at 17:00Z got 4 of 6 columns after the retry pass, one at 17:20Z got 1 of 6, consistent with the server's breaker opening after 3 failed calls, which also skips the retry pass). The fetcher now spaces every live GDELT call at least 5 s apart across the processes that share one cache DB (`shared_floor.py`), and retries a 429 no sooner than that; the timeline's retry pass stops at the first retry that fails again. The AOI sweep makes one GDELT call per AOI, so the floor bounds it: against a mocked GDELT, 45 AOIs took 220 s of the collector's 240 s `aoi_digest` budget and 50 timed out after 48 calls (every other AOI domain stubbed; `analysis/aoi.py` not changed here). Artlist was not used for daily counts: 250 records sorted by date covered 15 minutes. Situations see only the 22-country entity table, whose matcher still counts "South Sudan" as Sudan and "Indian Ocean" as India (`analysis/entities.py`, not changed here) |
| 1 | Story breadth | News clusters carry a distinct-outlet count; a top-stories view ranks by how many outlets carry a story | :white_check_mark: Built 2026-10-17, unreleased: clusters gain `outlets` / `outlet_count` (sister feeds of one publisher count once); `intel_top_stories` ranks by outlet count with an attention list (countries and organizations by mention growth). Found on the way: cluster `sources` was empty in every live cluster (key mismatch, `feed_name` vs `source_name`) and RSS times were shifted by the host UTC offset; both fixed. Concurrent RSS reads now share one request per feed (a live collector news-group cycle: 119 requests for 119 feeds). Review fixes 2026-10-17: clustering runs off the event loop; attention is empty with `attention_change_measured: false` when no feed spans both windows, never "nothing grew" |
| 1 | Liquidity and flows | Fed balance sheet, Treasury General Account, reverse repo and net liquidity (FRED keyless CSV), money supply (US M2, euro area M1/M2/M3, never summed), ECB, ICI fund flows and ETF net issuance, FINRA margin statistics, BLS series; amounts kept separate from flows | :yellow_circle: Built, unreleased: 5 tools (`intel_liquidity_board`, `intel_net_liquidity`, `intel_money_supply`, `intel_fed_liquidity_series`, `intel_ecb_series`), 5 CLI commands, 2 collector sources. FRED graph CSV and ECB Data Portal live-verified 2026-10-17. The TGA term is WDTGAL (Wednesday level, USD millions): WTREGEN is a week average; the net liquidity `method` block states each term's units, conversion and how RRPONTSYD (daily, USD billions) is aligned to the Wednesday. Blocked, measured 2026-10-17, and listed in the board's `blocked` field; no proxy is substituted, so fund flows and ETF net issuance have no source: ICI (money market fund assets at /research/stats/mmf, ETF net issuance at /research/stats/etf) answers 403 Akamai "Access Denied" on every path, robots.txt included; FINRA margin statistics XLSX answers 200, but the finra.org Terms of Use (Restrictions d and e) forbid building a database from the site and data mining, scraping or harvesting tools, robots included, and the page says data feeds are not available; BLS public API v1 answers 200 without a key (CPI-U, payrolls), but api.bls.gov robots.txt is `Disallow: /` and the terms page on www.bls.gov answers 403 to this server's User-Agent, so it is not used |
| 1 | Kalshi + cross-venue markets | Kalshi public market data next to Polymarket; same-event comparison with an explicit match confidence; settled markets scored against pre-settlement prices (Brier) | :white_check_mark: Unreleased: `intel_kalshi_markets`, `intel_prediction_compare`, `intel_prediction_settled` (`sources/kalshi.py`, `sources/prediction_cross.py`, `analysis/prediction_matching.py`), CLI and 2 collector sources. Kalshi endpoints measured 2026-10-17, all HTTP 200 without an account: `/events` (nested markets), `/series` (`include_volume`), `/markets?status=settled`, `/markets/candlesticks` (batch, capped at 10,000 candles per call), `/historical/cutoff`. The `/events` `category` parameter is ignored by the API, so categories resolve through `/series?category=`, whose filter is loose too (7 of the 869 series it returned for Economics were other categories), so series are matched exactly; an unknown category answers `{"series": null}` and is reported as a gap, not an outage. The exchange-wide settled listing is intraday sports (1,000 rows covered about 40 minutes; none open 24h), so settled samples default to the Economics category's top series. Polymarket Gamma caps a page at 100 rows. Matching is deliberately conservative: live 2026-10-17 it matched the Fed 25 bps hike/cut pairs for October and December and rejected 25 vs 50+ bps, 2026 vs 2027, and the Fed-hold pair ("no change" vs "Hike 0bps": a known false negative). Team and person aliases are not resolved, so most sports pairs stay unmatched. Kalshi requests are spaced 0.2 s apart in-process: the two collector sources scanning the same series at once drew HTTP 429, and at 0.1 s a 26-request category scan still drew one (none at 0.2 s in one live run). Settled samples are the most recently settled markets and cluster in time (one football match supplied 11 of 25 Polymarket markets in a live run), so a Brier score describes its sample, not a venue's skill. Every number a caller passes has a range stated in the input schema (at most 25 Kalshi series or pages per scan), so one call cannot fan out into unbounded upstream requests. Review round (2026-10-17): expired-cache serves are marked on every Kalshi read and on Polymarket's settled listing and histories, and a stale history is not scored; matching adds rate-action and direction gates, keeps signs and ordinals, reads a $ amount as an amount, and its candidate blocking is complete against a brute-force scan in tests; compare carries each side's price basis, spread and Kalshi last-trade time (public `/markets/trades`, HTTP 200 without an account, measured 2026-10-17) and ranks flagged pairs after comparable ones; Polymarket is scored against the earlier of endDate and closedTime (on 20 closed markets measured 2026-10-17, closedTime came 2.7 to 5.5 h after endDate for football and 164 h before it for one early resolution, and umaEndDate equalled closedTime in all 20, so no outcome-known time is published); Kalshi pacing applies only on a cache miss. The shared `polymarket` circuit breaker (three failed history calls pause all Polymarket reads for 300 s) is documented, not changed |
| 1 | News vs market reaction | Companies and countries named in story clusters mapped to tickers / country indices; same-day move against a benchmark | :yellow_circle: Built 2026-10-17, unreleased; 7 test expectations await correction at merge: `intel_news_market_reaction` and `intel news-reaction`. Company rows: stock, benchmark (SPY, or the exchange country's index) and excess return on the story's session; country rows: index return with a trailing z-score. Session rule in the exchange's timezone (after close = next session; weekends and holidays read from the listing's own bars); a last bar Yahoo has not finalized is pending. Open: `^TASI` and `^TA125` (Saudi and Israel in the country-index table) return 404 on Yahoo, so those benchmarks are data gaps; story times inherit `sources/news.py`'s RSS timestamp offset until that is fixed |
| 2 | Connections with timing offsets | Correlation between retained series over a shared sample, tested at leads and lags, with minimum sample size and false-discovery control; states that it does not prove causation | :white_check_mark: Built 2026-10-17, unreleased: `intel_lagged_connections` and `intel history connections`. Pearson r on a UTC day, week or month grid at every lag from -max_lag to +max_lag (default 5, cap 30, at most 1,000 tests a call), period-over-period changes by default, p from Student's t (regularized incomplete beta, standard library), Benjamini-Hochberg adjustment across the call (default 0.05) with significance decided on the AR(1)-adjusted `q_ar1` (independent random walks in levels: 91 of 100 seeds flagged by the unadjusted q, none by `q_ar1`), "no evidence" below an effective sample of 3, a 30-point minimum with every untested pair and lag listed with its n, latest vintage, rows without `observed_at` refused with the reason, every clamp reported in the MCP response and the CLI, and a `cannot_prove` list. p-values match scipy to 3.3e-13 relative error. Verified 2026-10-17 over a live MCP stdio session and the CLI on four real FRED series loaded into an isolated store. Not done: rank correlation, prewhitening |
| 2 | Day timeline | What every retained source recorded for a given day, by observation date or collection date | :white_check_mark: Built 2026-10-17, unreleased: `intel_day_timeline` and `intel history day` (`history_timeline.py`; read-only `mode=ro` connection, never fetches). by=collected: per-source run-log state (changed, unchanged, answered, degraded, failed, not_attempted, collector_did_not_run, never_run, unknown_past_retention) with attempts, status counts, reasons and payload changes; by=observed: date and UTC-instant values on the day, month and year values listed apart as period values, undated observations counted and excluded. Retention is stated per record kind for the day. Measured 2026-10-17: two live collector cycles of 6 sources four minutes apart (12 captures: ok 8, degraded 2, error 2; every payload in the second cycle matched the first) read back over a live MCP stdio session and the CLI; 2026-10-16 in the same store reads `collector_did_not_run` (context `before_first_capture`). On a synthetic store (475,200 captures, 1.75 million observations, 10,982 series) one call took 24 to 46 ms, and no statement full-scans a history table under SQLite 3.45.3, 3.53.4 and 3.54.0. Not measured: a live store with weeks of history |
| 3 | Dashboard views | Liquidity board, situations, top stories, collection health panels | :white_check_mark: Built 2026-10-17, unreleased: four tabs behind RESEARCH (key `p`), each loaded from its own route that calls the tool's function (`/api/liquidity-board`, `/api/situations`, `/api/top-stories`, and `/api/collection-health` on the dashboard cache's history store); a failed call renders as UNAVAILABLE or REFRESH FAILED with the function's reason, never as an empty list. Checked 2026-10-17 in headless Chrome with real upstreams (9 liquidity rows; 20 situations and 9 stories over 95 of 119 feeds; collection health after one collector cycle: 38 ok, 5 degraded, 16 failing), with FRED and ECB, every RSS feed and the history store made unreachable (each tab showed the outage and its reason), and with markup injected into a headline (shown as text). Situations and top stories take 5 to 15 s per call |

Considered and not planned:

| Feature | Reason |
|---------|--------|
| Sportsbook odds | Odds APIs are paid or key-gated; Kalshi and Polymarket sports markets are covered by the cross-venue work |
| NFL analytics (rosters, depth charts, game model) | Outside a world-intelligence server's scope |
| Bulk paper corpus (300,000+ papers) | `intel_arxiv_papers` queries live; a local corpus is a separate service |
| Game-theory actor modelling | No measurable spec yet |
| "Intelligent table" encoders and 88% "kernel" compression | Claimed without a published method, code or evaluation; nothing to reproduce |

### Phase 23 (planned): Geofence Depth
| Feature | Why | Status |
|---------|-----|--------|
| Polygon AOIs | 3-64 vertex shapes, dateline-aware, exact membership across brief/changes/escalation; DB migrates in place | :white_check_mark: Shipped in 0.5.0 (`intel_aoi_define_polygon`) |
| Corridor AOIs | Waypoint route + width; distances measured to the route | :white_check_mark: Shipped in 0.5.0 (`intel_aoi_define_corridor`) |
| AOI groups / watchlists | `intel_aoi_digest` sweeps all (or named) AOIs in one call, advancing snapshots | :white_check_mark: Shipped in 0.8.0 |
| Escalation news + convergence components | `intel_aoi_escalation` reported null for news/convergence. News is now the rise in the geocoded place's share of all GDELT coverage, the last 7 days against the 7 before (`timelinevolraw` for the brief's settlement + county query), not name-mention counts: GDELT's own intake has multi-day outages (measured 2026-10-17 for Pittsburgh: no data for 16 of 85 days), which raw counts would read as a surge on recovery, so windows with uneven intake are refused as a data gap. Convergence runs `analysis/convergence.py` over the earthquakes, military flights, ACLED events and wildfires inside the AOI's exact shape. A failed news or convergence input makes that component null plus a `data_gaps` entry, never a 0; `computed_from` names the components behind the score | :white_check_mark: Shipped (Unreleased) |
| Geo-scoped news | AOI centre reverse-geocoded via OSM Nominatim (`sources/geocode.py`); GDELT searched for the settlement + county, e.g. `("Pittsburgh" OR "Allegheny County")`. Brief reports `news_scoping`; name-mention fallback is disclosed as a data gap. GDELT GEO 2.0 API was tried first and is 404 (both path variants, measured), so per-article coordinates are not available; the curated 105-city set could not do this either (nearest to Pittsburgh: Toronto, ~360 km) | :white_check_mark: Shipped in 0.10.0 |
| CLI parity | `intel aoi` group with all 9 subcommands, shared store semantics, live-smoked | :white_check_mark: Shipped in 0.8.0 |
| Dashboard AOI layer | `/api/aois` + a toggleable "AOI Geofences" Leaflet layer: circles, polygons, and corridors drawn from the store, tooltip/detail with what the last collector sweep counted inside each. Reads the stored snapshot, never gathers live (a never-swept AOI says so instead of showing zeros; a broken store is a 503, not an empty map). Verified in headless Chromium: 2 shapes rendered, 50 km circle at 50,000 m, no console errors | :white_check_mark: Shipped in 0.10.0 |
| Scheduled AOI sweeps | `aoi_digest` is a collector source (`fetch_aoi_sweep`, 240 s budget): every daemon cycle advances all AOI snapshots, so `--daemon` + the launchd wrapper IS the schedule. Change digests land in the collector log and vector store | :white_check_mark: Shipped in 0.9.0 |
| AOI change notifications | `WORLD_INTEL_AOI_WEBHOOK` (+ `_FORMAT=json\|text`): the sweep POSTs non-quiet digests; quiet sweeps and dead sinks are honest `notification` records, never silent or fatal. Live-verified against a local sink. Email is out of scope (bring a webhook bridge) | :white_check_mark: Shipped in 0.9.0 |

### Phase 24 (planned): Test Coverage Gate
Measured 2026-09-01 (before the current test push): 59% statement
coverage overall; `server.py`, `cli.py`, `collector.py` at 0%;
`analysis/` NLP modules 0-14%; `sources/intelligence.py` 22%.

| Item | Why | Status |
|------|-----|--------|
| Analysis-layer tests (classifier, entities, convergence, spikes, clustering, signals, focal points, surge, cascade, exposure, posture, alerts, instability, world_brief) | The NLP/analysis layer had no executable verification at all | :white_check_mark: Shipped 2026-09-01: 131 tests, all 14 modules at 96-100% |
| Source-layer tests (intelligence, cyber, climate, displacement, fleet, prediction, service_status, maritime, military helpers, news) | Same class of gap on the fetch/parse layer | :white_check_mark: Shipped 2026-09-01: 89 tests, modules at 79-100% (intelligence.py 93%) |
| Import-based `server.py` registry tests | The TOOLS/`_dispatch` parity invariant was checked by reading server.py as *text*; now imported under a temp cache path and verified structurally, including a real dispatch round-trip | :white_check_mark: Shipped 2026-09-01 (server.py 0% -> 47%) |
| `collector.py` source-map test | Map verified earlier; the collect/daemon run loop, filters, and cycle accounting now tested too (97% module coverage) | :white_check_mark: Shipped 2026-09-01 |
| `cli.py` smoke tests (CliRunner) | 1,076 statements, zero executed by tests; now 76 tests driving all 52 reachable commands, 92% coverage | :white_check_mark: Shipped 2026-09-01 |
| CI coverage gate with ratchet | `--cov-fail-under=89` live in ci.yml (90% measured 2026-09-01 evening; floor one point under for platform variance). Raise as waves land; never lower | :white_check_mark: Shipped 2026-09-01, ratcheted same day |

### Phase 24.5: Data-Honesty Backlog (top three fixed 2026-09-01)
Verified bugs found by the 2026-09-01 test waves. The two cross-module
silent-zero key mismatches found at the same time (`world_brief`
`article_count` vs `size`, `fleet` `warning_count` vs
`naval_warnings`) were fixed in 0.4.0 with regression tests; the three
fail-quietly bugs below were fixed in the same-day follow-up
(issues #22-#24, each with a flipped regression test).

| Bug | Where | Class | Status |
|-----|-------|-------|--------|
| UNHCR outage returned all-zero global totals with no error/degraded key ("zero refugees worldwide") | `sources/displacement.py` | fail-reads-as-success | :white_check_mark: Fixed (#22) |
| A climate zone whose fetch failed was silently omitted; full outage yielded `{"zones": {}}` with no marker | `sources/climate.py` | silent degradation | :white_check_mark: Fixed (#23) |
| "Resolved: Major outage" classified as an active critical incident; a dead provider feed still listed as checked | `sources/service_status.py` | misclassification | :white_check_mark: Fixed (#24) |
| Naval warnings apply to all 9 waterways identically (no proximity/NAVAREA filter); `total_nearby` computed but never emitted | `sources/intelligence.py` vessel snapshot | precision | :white_check_mark: Fixed (Unreleased): a warning counts only at waterways within 300 km of a position its full text states; one that states no readable position counts nowhere (crediting positionless NAVAREA IV and XII warnings to Panama was measured wrong 5 of 5 times on the live feed and rejected) and, if naval, is listed in `unplaced_naval_warnings` with a reason; the rule is in `matching_rule`; `total_nearby` emitted; an NGA outage reads `unknown` and a frozen feed `stale`, not `clear` or a current threat level. Open: an unplaced naval warning leaves waterway statuses unchanged and is named only in `data_gaps` |
| Country/company/leader entity matching was substring-based ("usa" inside "thousand", "hamas" inside "Bahamas") | `analysis/entities.py` | false positives | :white_check_mark: Fixed in 0.5.0 (word-boundary alternations, measured faster) |
| Category keywords matched substrings ("airstrike" bumped severity via "strike"; "denied" fired "ied") | `analysis/classifier.py` | false positives | :white_check_mark: Fixed in 0.5.0 (boundary-anchored stems, per-keyword overrides) |
| Silent-empty degradation without a marker on RSS/API failure | `sources/prediction.py` (documented as intended), `sources/maritime.py`, `sources/news.py` RSS path, `sources/fleet.py` `_safe` wrapper, `sources/military.py` military flights and theater posture | silent degradation | :yellow_circle: `sources/news.py` RSS path fixed 2026-10-17 (every call reports `feeds_attempted` / `feeds_ok` / `feeds_failed` / `feeds_stale`; no feed answering live is `error` + `degraded` + `reason: all_feeds_failed`, including the warm-cache case where failed feeds came back as expired copies and counted as healthy; failing-first regression tests). Since the 2026-10-17 review: fewer than half of the feeds answering live is `degraded` + `reason: partial_feeds_failed` with no error (measured that day: 95 of 119 answered, the rest permanent 404 / 403 / 410 plus one timeout), passed through by trending keywords, situations, top stories and the dossier; links are kept only when http(s) and titles are capped at 500 characters. `maritime.py`, `fleet.py`, military flights and theater posture fixed (Unreleased): outages carry `error`/`degraded`/`reason` and `data_gaps`; failed or stale fleet components are named in `unavailable_components`, and fleet readiness reads `UNKNOWN` with none available and is marked a lower bound with some missing; military flights with both sources down carries `error`/`degraded`/`reason` (no `data_gaps`) beside `source: "none"`; posture names the theaters no flight source answered for; nav warnings flag (`feed_stale`) a feed whose newest warning is over 14 days old (NGA's API holds nothing issued after 10 May 2024, measured 2026-10-17). `sources/prediction.py` fixed, unreleased: an outage or unexpected response adds `error` + `degraded`, an expired-cache serve adds `stale`; keys unchanged; regression tests written failing-first (3 failed on the old module). The fetcher's stale fallback marks only dict payloads, so other text and XML sources can serve expired copies unmarked Open: the dashboard and collector do not yet read the new markers; strategic posture and temporal anomalies still read a posture outage as zero activity |
| Country dossier news was always empty with no marker, for every country: it asked the feed for `category="all"` (not a category, so `fetch_news_feed` answered with an error dict), read `articles` / `source` while the feed emits `items` / `feed_name`, and checked only its own `_error`, never the feed's `error`. Also, 28 of the 50 accepted codes have no headline keywords, so their search could never match | `analysis/dossier.py` news section | fail-reads-as-success | :white_check_mark: Fixed 2026-10-17, unreleased (reads every feed; a feed error or outage passes through as `error` / `degraded` / `reason` with `mention_count: null` and a data gap; failed and stale feeds named; keywordless countries report "mentions not searched"; `intel dossier` prints `News: not measured`; failing-first tests drive the real `fetch_news_feed` output). Since the 2026-10-17 review: headlines match on whole words (the substring test counted "Tirana" as Iran and "Indiana" as India), four compound names are removed first, and the section reports `matching_items`, `mention_count_capped`, `items_searched` and `searched_span` |
| 33 CLI commands rendered upstream errors as healthy empty states | `cli.py` | fail-reads-as-success | :white_check_mark: Fixed in 0.6.0 (shared bail-on-error path; 43 failing-first tests) |
| Rich markup swallowed lowercase bracketed values and let remote titles inject markup | `cli.py` | output corruption | :white_check_mark: Fixed in 0.6.0 (per-site escaping; injection test) |
| Remote data inside Rich TABLE cells was not markup-escaped | `cli.py` | output corruption | :white_check_mark: Fixed in 0.6.0 (`_cell` Text-wrapping convention, 11 injection tests; error styles unified across all 53 commands) |

### Phase 25 (planned): Missing Domains
Verified absent from `sources/` on 2026-09-01 (grep, not memory):

| Feature | Source candidate | Status |
|---------|------------------|--------|
| Severe weather alerts | NWS (US, 0.5.0) + Meteoalarm (39 EU countries, 0.8.0; per-country feeds only - no Europe-wide feed exists) | :white_check_mark: |
| Tropical cyclone tracking | NHC (0.5.0) + JTWC (0.8.0: NW Pacific/N Indian/S Hemisphere). Forecast tracks (Unreleased): opt-in `include_forecast` on both tools. NHC points come from the linked forecast/advisory text and the cone from the linked `trackCone` CONE.kmz (simplified); JTWC points come from each linked warning text, and JTWC links no cone. Not upstream-blocked, measured 2026-10-17: keyless HTTP 200 for NHC CurrentStorms.json, text/MIATCMEP1.shtml, EP212026_017adv_CONE.kmz and JTWC jtwc.rss, wp2926web.txt, sh0127web.txt; nhc.noaa.gov robots.txt disallows nothing and the NWS disclaimer is public domain; JTWC jtwc.html and robots.txt answered 403 (CloudFront), so JTWC's terms are unread. Wind radii not parsed | :white_check_mark: warnings + forecast tracks |
| Dedicated volcano monitoring | GVP weekly report shipped in 0.5.0 (`intel_volcano_activity`) | :white_check_mark: |
| NOTAMs | BLOCKED upstream: official FAA API is key-required (401 verified live 2026-09-01); unofficial backend POST-only/undocumented. Revisit if FAA opens access | :red_circle: blocked |
| Launch schedules | Launch Library 2 shipped in 0.5.0 (`intel_launch_schedule`) | :white_check_mark: |
| BGP routing status | `intel_bgp_status` (0.8.0): per-resource RIS visibility + RPKI via RIPEstat. Global incident/hijack feeds remain open (no key-free target-free feed found) | :yellow_circle: per-resource done |

Note: resolved in 0.6.0 - the four domains are in the collector's
roster (50 sources), with the invariant test and docs updated together.

### Phase 26: server.py Modularization (shipped 2026-09-01)
The 2,890-line monolith is a 158-line shell over 12 domain modules in
`tools/` plus shared infrastructure in `runtime.py`. The
TOOLS/`_dispatch` parity invariant is now enforced at import time by
`tools.aggregate()` (drift or collision refuses to start), verified by
an AST byte-parity gate against the pre-split registry, the full
suite, and a live MCP stdio session.

---

## Summary

| Category | Current | Notes |
|----------|---------|-------|
| Total MCP tools | 157 | 156 intelligence tools + `intel_status` |
| Tool parity | 157 / 157 | `TOOLS` and handlers are aligned at import time; README's per-tool reference is pinned to the registry by `test_readme_documents_every_registered_tool` |
| Static datasets | 18 | Bases, ports, pipelines, nuclear, cables, datacenters, spaceports, minerals, exchanges, trade routes, cloud regions, financial centers |
| RSS feeds | 119 | 24 categories |
| Tests in repo | 991 | 973 non-smoke tests + 18 live smoke tests (measured 2026-10-12: `pytest -m 'not smoke'` -> 973 passed, 18 deselected); full suite requires `.[dev]` |
| Statement coverage | 92% | Measured 2026-10-12 full-suite `--cov` (92.02%); 91.65% on 2026-10-11; 91.18% on 2026-09-01, 59% the morning before. CI ratchet at 89 |
| Primary remaining gaps | `cli.py` tests, coverage gate, `server.py` refactor | See Planned Phases 23-26 |

**Bottom line (2026-10-11)**: 139 tools across 30+ domains; Phases 27-28 added aircraft-cohort early warning and cross-border migration. **Earlier (2026-09-02)**: 122 tools across 30+ domains, with the roadmap aligned to the live MCP registry and, for the first time, a forward-looking plan (Phases 23-26): geofence depth, the coverage gate, missing domains, and `server.py` modularization.
