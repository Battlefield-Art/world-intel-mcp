"""Market and economic tools: equities, crypto, commodities,
energy and consumer prices, FRED and World Bank series, country stock
indices, BTC technicals, central bank rates.

Phase 26 split: Tool definitions moved byte-identical and dispatch
case bodies verbatim from server.py; handlers reach shared
infrastructure via ``runtime``. See tools/system.py for the pattern.
"""

from typing import Any

from mcp.types import Tool

from .. import runtime
from ..sources import (
    markets,
    economic,
    central_banks,
)

TOOLS: list[Tool] = [
    # --- Markets (7 tools) ---
    Tool(
        name="intel_market_quotes",
        description="Get real-time stock market index quotes (S&P 500, Dow, Nasdaq, FTSE, Nikkei, etc.). Optional: symbols (list of ticker symbols).",
        inputSchema={
            "type": "object",
            "properties": {
                "symbols": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Ticker symbols (default: major indices)",
                },
            },
        },
    ),
    Tool(
        name="intel_crypto_quotes",
        description="Get top cryptocurrency prices, market caps, and 7-day sparklines from CoinGecko. Optional: limit (int, default 20).",
        inputSchema={
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Number of coins (default 20)",
                    "default": 20,
                },
            },
        },
    ),
    Tool(
        name="intel_stablecoin_status",
        description="Check stablecoin peg health (USDT, USDC, DAI, FDUSD). Flags depegs >0.5%.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="intel_etf_flows",
        description="Get Bitcoin spot ETF prices and volumes (IBIT, FBTC, GBTC, ARKB, BITB).",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="intel_sector_heatmap",
        description="Get US equity sector performance heatmap (11 SPDR sector ETFs).",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="intel_macro_signals",
        description="Get 7 key macro signals: Fear & Greed, mempool fees, DXY, VIX, gold, 10Y Treasury, BTC dominance.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="intel_commodity_quotes",
        description="Get commodity futures quotes: gold, silver, crude oil (WTI & Brent), natural gas, corn, wheat, soybeans from Yahoo Finance.",
        inputSchema={"type": "object", "properties": {}},
    ),
    # --- Economic (6 tools) ---
    Tool(
        name="intel_gas_prices",
        description="Get today's US retail gasoline and diesel prices ($/gallon) from AAA — daily national averages for regular, mid-grade, premium, diesel, E85. Includes day-over-day, week, month, year deltas plus per-state prices. No API key required.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="intel_residential_natgas",
        description="Get US residential natural gas prices ($/thousand cubic feet) — monthly average. Requires EIA_API_KEY.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="intel_electricity_rates",
        description="Get US electricity retail rates (cents/kWh) by sector (residential, commercial, industrial). Optionally filter by state. Requires EIA_API_KEY.",
        inputSchema={
            "type": "object",
            "properties": {
                "state": {
                    "type": "string",
                    "description": "2-letter US state code (e.g., 'CA', 'TX'). Defaults to nationwide.",
                },
            },
        },
    ),
    Tool(
        name="intel_energy_prices",
        description="Get crude oil (Brent, WTI) and natural gas futures prices from EIA. Requires EIA_API_KEY.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="intel_fred_series",
        description="Get Federal Reserve economic data series (GDP, UNRATE, CPIAUCSL, DFF, T10YIE, etc.). Requires FRED_API_KEY.",
        inputSchema={
            "type": "object",
            "properties": {
                "series_id": {
                    "type": "string",
                    "description": "FRED series ID (e.g., 'UNRATE')",
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of observations",
                    "default": 30,
                },
            },
            "required": ["series_id"],
        },
    ),
    Tool(
        name="intel_world_bank_indicators",
        description="Get World Bank development indicators (GDP, inflation, unemployment) for a country.",
        inputSchema={
            "type": "object",
            "properties": {
                "country": {
                    "type": "string",
                    "description": "ISO country code (default: USA)",
                    "default": "USA",
                },
                "indicators": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "World Bank indicator codes",
                },
            },
        },
    ),
    # --- Markets Extended (1 tool) ---
    Tool(
        name="intel_country_stocks",
        description="Get real-time stock index quote for any country by ISO-3 code. Maps country to its primary exchange index ticker and fetches via Yahoo Finance.",
        inputSchema={
            "type": "object",
            "properties": {
                "country": {
                    "type": "string",
                    "description": "ISO-3 country code (USA, GBR, JPN, CHN, DEU, etc.)",
                    "default": "USA",
                },
            },
        },
    ),
    # --- BTC Technicals (1 tool) ---
    Tool(
        name="intel_btc_technicals",
        description="Bitcoin technical indicators: SMA-50, SMA-200, Mayer Multiple, golden/death cross, distance from the highest daily price of the last 200 days (ath_in_period, not the all-time high), 7d/30d changes.",
        inputSchema={"type": "object", "properties": {}},
    ),
    # --- Central Banks (1 tool) ---
    Tool(
        name="intel_central_bank_rates",
        description="Policy rates for 15 major central banks: Fed, ECB, BoE, BoJ, PBoC, RBI, RBA, BoC, SNB, BCB, BoK, CBRT, SARB, Banxico, BI. Live FRED data when API key set, curated fallback otherwise.",
        inputSchema={"type": "object", "properties": {}},
    ),
]


async def _market_quotes(arguments: dict[str, Any]) -> Any:
    return await markets.fetch_market_quotes(
        runtime.fetcher, symbols=arguments.get("symbols")
    )


async def _crypto_quotes(arguments: dict[str, Any]) -> Any:
    return await markets.fetch_crypto_quotes(
        runtime.fetcher, limit=arguments.get("limit", 20)
    )


async def _stablecoin_status(arguments: dict[str, Any]) -> Any:
    return await markets.fetch_stablecoin_status(runtime.fetcher)


async def _etf_flows(arguments: dict[str, Any]) -> Any:
    return await markets.fetch_etf_flows(runtime.fetcher)


async def _sector_heatmap(arguments: dict[str, Any]) -> Any:
    return await markets.fetch_sector_heatmap(runtime.fetcher)


async def _macro_signals(arguments: dict[str, Any]) -> Any:
    return await markets.fetch_macro_signals(runtime.fetcher)


async def _commodity_quotes(arguments: dict[str, Any]) -> Any:
    return await markets.fetch_commodity_quotes(runtime.fetcher)


async def _gas_prices(arguments: dict[str, Any]) -> Any:
    return await economic.fetch_gas_prices(runtime.fetcher)


async def _residential_natgas(arguments: dict[str, Any]) -> Any:
    return await economic.fetch_residential_natgas_prices(runtime.fetcher)


async def _electricity_rates(arguments: dict[str, Any]) -> Any:
    return await economic.fetch_electricity_rates(
        runtime.fetcher,
        state=arguments.get("state"),
    )


async def _energy_prices(arguments: dict[str, Any]) -> Any:
    return await economic.fetch_energy_prices(runtime.fetcher)


async def _fred_series(arguments: dict[str, Any]) -> Any:
    return await economic.fetch_fred_series(
        runtime.fetcher,
        series_id=arguments["series_id"],
        limit=arguments.get("limit", 30),
    )


async def _world_bank_indicators(arguments: dict[str, Any]) -> Any:
    return await economic.fetch_world_bank_indicators(
        runtime.fetcher,
        country=arguments.get("country", "USA"),
        indicators=arguments.get("indicators"),
    )


async def _country_stocks(arguments: dict[str, Any]) -> Any:
    return await markets.fetch_country_stocks(
        runtime.fetcher,
        country=arguments.get("country", "USA"),
    )


async def _btc_technicals(arguments: dict[str, Any]) -> Any:
    return await markets.fetch_btc_technicals(runtime.fetcher)


async def _central_bank_rates(arguments: dict[str, Any]) -> Any:
    return await central_banks.fetch_central_bank_rates(runtime.fetcher)


HANDLERS = {
    "intel_market_quotes": _market_quotes,
    "intel_crypto_quotes": _crypto_quotes,
    "intel_stablecoin_status": _stablecoin_status,
    "intel_etf_flows": _etf_flows,
    "intel_sector_heatmap": _sector_heatmap,
    "intel_macro_signals": _macro_signals,
    "intel_commodity_quotes": _commodity_quotes,
    "intel_gas_prices": _gas_prices,
    "intel_residential_natgas": _residential_natgas,
    "intel_electricity_rates": _electricity_rates,
    "intel_energy_prices": _energy_prices,
    "intel_fred_series": _fred_series,
    "intel_world_bank_indicators": _world_bank_indicators,
    "intel_country_stocks": _country_stocks,
    "intel_btc_technicals": _btc_technicals,
    "intel_central_bank_rates": _central_bank_rates,
}
