"""Liquidity tools and CLI commands (Phase 29): MCP handlers pass their
arguments through, and every CLI command renders values, surfaces an
upstream error as one red line, and passes raw JSON in --json-output mode.

Payloads are produced by the real source functions from the live-captured
fixtures, so the CLI renders the shapes the tools return.

Gaps / not covered: the MCP stdio transport itself (test_server_registry
covers dispatch); terminal widths below 200 columns.
"""

from __future__ import annotations

import asyncio
import json
from datetime import date
from pathlib import Path

import httpx
import pytest
import respx
from click.testing import CliRunner

import world_intel_mcp.fetcher as fetcher_mod
from world_intel_mcp import cli
from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import liquidity

from .test_sources_liquidity import _mock_all


def _liquidity_tools():
    """Import tools/liquidity.py under test_server_registry's temp cache.

    Importing any tools module imports runtime, which opens a live Cache
    at import; the shared temp path keeps it off the developer's cache and
    keeps test_server_cache_landed_in_temp_path true whichever test file
    imports runtime first (same pattern as test_aoi._tools_registry)."""
    import importlib
    import os

    from .test_server_registry import _TMP_CACHE

    prior = os.environ.get("WORLD_INTEL_CACHE_DB")
    os.environ["WORLD_INTEL_CACHE_DB"] = str(_TMP_CACHE)
    try:
        importlib.import_module("world_intel_mcp.tools")
        return importlib.import_module("world_intel_mcp.tools.liquidity")
    finally:
        if prior is None:
            os.environ.pop("WORLD_INTEL_CACHE_DB", None)
        else:
            os.environ["WORLD_INTEL_CACHE_DB"] = prior


@pytest.fixture(autouse=True)
def _isolation(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "cli-cache.db"))
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.setattr(cli, "_fetcher", None)
    monkeypatch.setattr(liquidity, "_today", lambda: date(2026, 10, 17))
    monkeypatch.setitem(fetcher_mod._SOURCE_RATE_LIMITS, liquidity.FRED_SOURCE, 0.0)


def _fetcher(tmp_path: Path) -> Fetcher:
    return Fetcher(cache=Cache(db_path=tmp_path / "f.db"), breaker=CircuitBreaker(), max_retries=0)


async def _build_payloads(tmp_path: Path) -> dict:
    f = _fetcher(tmp_path)
    try:
        with respx.mock:
            _mock_all()
            ecb_key = "M.U2.Y.V.M10+M20+M30.X.1+4+I.U2.2300.Z01.E+A"
            respx.get(f"https://data-api.ecb.europa.eu/service/data/BSI/{ecb_key}").mock(
                return_value=httpx.Response(200, json=json.loads(
                    (Path(__file__).parent / "fixtures" / "liquidity" / "ecb_bsi_money.json").read_text()
                ))
            )
            return {
                "board": await liquidity.fetch_liquidity_board(f),
                "net": await liquidity.fetch_net_liquidity(f, limit=3),
                "money": await liquidity.fetch_money_supply(f, limit=2),
                "fed": await liquidity.fetch_fed_liquidity_series(f, "WALCL", limit=2),
                "ecb": await liquidity.fetch_ecb_series(f, key=ecb_key, limit=1),
            }
    finally:
        await f.close()
        f.cache.close()


def _payloads(tmp_path: Path) -> dict:
    """Built outside any running loop: the CLI calls asyncio.run itself."""
    return asyncio.run(_build_payloads(tmp_path))


def _fake(payload, calls: list | None = None):
    async def _f(*args, **kwargs):
        if calls is not None:
            calls.append(kwargs)
        return payload

    return _f


def _invoke(*args: str):
    return CliRunner().invoke(cli.main, list(args), catch_exceptions=False)


# ---------------------------------------------------------------------------
# MCP handlers
# ---------------------------------------------------------------------------


def test_registry_has_five_liquidity_tools() -> None:
    liquidity_tools = _liquidity_tools()
    names = [t.name for t in liquidity_tools.TOOLS]
    assert names == [
        "intel_liquidity_board",
        "intel_net_liquidity",
        "intel_money_supply",
        "intel_fed_liquidity_series",
        "intel_ecb_series",
    ]
    schema = {t.name: t.inputSchema for t in liquidity_tools.TOOLS}
    assert schema["intel_fed_liquidity_series"]["properties"]["series_id"]["enum"] == sorted(liquidity.FRED_SERIES)
    assert schema["intel_ecb_series"]["required"] == ["key"]


@pytest.mark.parametrize(
    "tool, fn, args, expected",
    [
        ("intel_liquidity_board", "fetch_liquidity_board", {"sort": "newest"}, {"sort": "newest"}),
        ("intel_liquidity_board", "fetch_liquidity_board", {}, {"sort": "biggest_move"}),
        ("intel_net_liquidity", "fetch_net_liquidity", {"limit": 0, "start": "2020-01-01"},
         {"limit": 0, "start": "2020-01-01"}),
        ("intel_net_liquidity", "fetch_net_liquidity", {}, {"limit": 52, "start": None}),
        ("intel_money_supply", "fetch_money_supply", {"region": "us"}, {"region": "us", "limit": 13}),
        ("intel_fed_liquidity_series", "fetch_fed_liquidity_series", {"series_id": "M2SL", "limit": 5},
         {"series_id": "M2SL", "limit": 5, "start": None}),
        ("intel_ecb_series", "fetch_ecb_series", {"key": "W.U2.C.T000000.Z5.Z01", "dataset": "ILM"},
         {"dataset": "ILM", "key": "W.U2.C.T000000.Z5.Z01", "limit": 24, "start": None}),
    ],
)
async def test_handlers_pass_arguments(monkeypatch, tool, fn, args, expected) -> None:
    calls: list = []
    monkeypatch.setattr(liquidity, fn, _fake({"ok": tool}, calls))
    out = await _liquidity_tools().HANDLERS[tool](args)
    assert out == {"ok": tool}
    assert calls == [expected]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_cli_liquidity_board_renders_rows_and_gaps(tmp_path, monkeypatch) -> None:
    payload = _payloads(tmp_path)["board"]
    calls: list = []
    monkeypatch.setattr(cli.liquidity, "fetch_liquidity_board", _fake(payload, calls))
    out = _invoke("liquidity", "--sort", "newest").output
    assert calls == [{"sort": "newest"}]
    assert "net_liquidity" in out and "5,869.62" in out
    assert "eurosystem_total_assets" in out and "2026-W41" in out
    assert "Gap:" in out


def test_cli_liquidity_board_marks_stale_and_unavailable(tmp_path, monkeypatch) -> None:
    payload = _payloads(tmp_path)["board"]
    payload["indicators"][0]["freshness"]["stale"] = True
    payload["unavailable"] = [{"indicator": "us_m2", "error": "FRED graph CSV for M2SL unavailable"}]
    monkeypatch.setattr(cli.liquidity, "fetch_liquidity_board", _fake(payload))
    out = _invoke("liquidity").output
    assert "(stale)" in out
    assert "Unavailable: us_m2: FRED graph CSV for M2SL unavailable" in out


@pytest.mark.parametrize(
    "command, fn",
    [
        (["liquidity"], "fetch_liquidity_board"),
        (["net-liquidity"], "fetch_net_liquidity"),
        (["money-supply"], "fetch_money_supply"),
        (["fed-liquidity", "WALCL"], "fetch_fed_liquidity_series"),
        (["ecb-series", "M.U2.Y.V.M30.X.1.U2.2300.Z01.E"], "fetch_ecb_series"),
    ],
)
def test_cli_error_is_one_red_line_and_json_mode_is_raw(monkeypatch, command, fn) -> None:
    err = {"error": "net liquidity not computed: missing component(s) WDTGAL", "data_gaps": ["x"]}
    monkeypatch.setattr(cli.liquidity, fn, _fake(err))
    out = _invoke(*command).output
    assert out.strip() == "Error: net liquidity not computed: missing component(s) WDTGAL"
    raw = _invoke("--json-output", *command).output
    assert json.loads(raw) == err


def test_cli_net_liquidity_shows_formula_and_components(tmp_path, monkeypatch) -> None:
    payload = _payloads(tmp_path)["net"]
    calls: list = []
    monkeypatch.setattr(cli.liquidity, "fetch_net_liquidity", _fake(payload, calls))
    out = _invoke("net-liquidity", "-n", "3", "--start", "2026-09-01").output
    assert calls == [{"limit": 3, "start": "2026-09-01"}]
    assert "net_liquidity = WALCL - WDTGAL - RRPONTSYD" in out
    assert "6,755.64" in out and "885.82" in out and "0.20" in out
    assert "2026-10-14" in out


def test_cli_money_supply_renders_both_regions(tmp_path, monkeypatch) -> None:
    payload = _payloads(tmp_path)["money"]
    monkeypatch.setattr(cli.liquidity, "fetch_money_supply", _fake(payload))
    out = _invoke("money-supply").output
    assert "US M2" in out and "23,342.80" in out
    assert "US M3 is discontinued" in out
    assert "Euro area M3 stock" in out and "17,646,463.00" in out
    assert "Provisional value" in out
    assert "never added together" in out


def test_cli_money_supply_partial_failures(monkeypatch) -> None:
    payload = {
        "region": "all",
        "note": liquidity.NESTING_NOTE,
        "us": {"M2": {"error": "FRED graph CSV for M2SL unavailable"}, "M3": {"note": liquidity.US_M3_NOTE}},
        "euro_area": {"error": "ECB Data Portal returned no data"},
        "data_gaps": [],
    }
    monkeypatch.setattr(cli.liquidity, "fetch_money_supply", _fake(payload))
    out = _invoke("money-supply").output
    assert "US M2 unavailable: FRED graph CSV for M2SL unavailable" in out
    assert "Euro area unavailable: ECB Data Portal returned no data" in out


def test_cli_fed_liquidity_and_ecb_series_render(tmp_path, monkeypatch) -> None:
    payloads = _payloads(tmp_path)
    calls: list = []
    monkeypatch.setattr(cli.liquidity, "fetch_fed_liquidity_series", _fake(payloads["fed"], calls))
    out = _invoke("fed-liquidity", "walcl", "-n", "2").output
    assert calls == [{"series_id": "WALCL", "limit": 2, "start": None}]
    assert "6,755,639.00" in out and "USD millions" in out
    ecb_calls: list = []
    monkeypatch.setattr(cli.liquidity, "fetch_ecb_series", _fake(payloads["ecb"], ecb_calls))
    out = _invoke("ecb-series", "M.U2.Y.V.M30.X.1.U2.2300.Z01.E", "-d", "BSI", "-n", "1").output
    assert ecb_calls == [{"dataset": "BSI", "key": "M.U2.Y.V.M30.X.1.U2.2300.Z01.E", "limit": 1, "start": None}]
    assert "Monetary aggregate M3, Stocks (amount_outstanding)" in out
    assert "(growth_rate)" in out


def test_cli_liquidity_board_lists_blocked_sources(tmp_path, monkeypatch) -> None:
    payload = _payloads(tmp_path)["board"]
    monkeypatch.setattr(cli.liquidity, "fetch_liquidity_board", _fake(payload))
    out = _invoke("liquidity").output
    lines = [line for line in out.splitlines() if line.startswith("Not covered:")]
    assert len(lines) == len(liquidity.BLOCKED_SOURCES) == 4
    assert "Not covered: money market fund assets, weekly (ICI): 2026-10-17: HTTP 403" in out
    assert "(FINRA)" in out and "Restrictions d and e" in out
    assert "(BLS)" in out and "Disallow: /" in out
    # A board without the key (an older cached payload) prints no such lines.
    payload.pop("blocked")
    assert "Not covered:" not in _invoke("liquidity").output
