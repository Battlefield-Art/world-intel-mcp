"""Liquidity docs say what the code does: the biggest_move rule and each
board sort's ranking rule (payload and CLI), the `unknown` measure, the
ECB key limits, the CLI board's ranking line and row status (whole at
every terminal width from 40 to 200 columns), and the dated net liquidity
measurement in the CHANGELOG. Every fetcher and cache a test opens is
closed when it ends.

Gaps / not covered: README wording (counts and tables wait for the final
docs pass); the CHANGELOG figures themselves, which were measured live on
2026-10-17 and cannot be recomputed from the truncated fixtures (only
their internal consistency is checked here); terminals under 40 columns;
a real terminal (Rich renders to CliRunner's buffer at COLUMNS).
"""

import asyncio
import re
from pathlib import Path

import pytest
import respx

from world_intel_mcp import cli
from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import liquidity
from world_intel_mcp.tests.test_liquidity_surfaces import (
    _fake, _invoke, _isolation, _liquidity_tools, _payloads,
)
from world_intel_mcp.tests import test_liquidity_surfaces as _surfaces
from world_intel_mcp.tests.test_sources_liquidity import _mock_all

_ROOT = Path(__file__).parents[3]


@pytest.fixture
async def fetcher(tmp_path):
    """Like test_sources_liquidity's fetcher (no retries), but closes its client and cache."""
    f = Fetcher(cache=Cache(db_path=tmp_path / "liq.db"), breaker=CircuitBreaker(), default_timeout=5.0,
                max_retries=0)
    yield f
    await f.close()
    f.cache.close()


@pytest.fixture(autouse=True)
def _close_payload_and_cli_caches(_isolation, monkeypatch: pytest.MonkeyPatch):
    """No sqlite connection outlives its test: the fetchers _payloads opens
    are closed inside the loop that used them, and the cache of the CLI's
    own fetcher (cli._get_fetcher) at teardown."""
    real_fetcher, real_build = _surfaces._fetcher, _surfaces._build_payloads
    opened: list[Fetcher] = []

    def recording(tmp_path):
        opened.append(real_fetcher(tmp_path))
        return opened[-1]

    async def build_then_close(tmp_path):
        try:
            return await real_build(tmp_path)
        finally:
            while opened:
                f = opened.pop()
                await f.close()
                f.cache.close()

    monkeypatch.setattr(_surfaces, "_fetcher", recording)
    monkeypatch.setattr(_surfaces, "_build_payloads", build_then_close)
    yield
    if cli._fetcher is not None:
        cli._fetcher.cache.close()


def _flat(text: str) -> str:
    return " ".join(text.split())


def _tool(name: str):
    return next(t for t in _liquidity_tools().TOOLS if t.name == name)


def test_biggest_move_is_described_as_the_ranking_the_code_applies():
    sort = _tool("intel_liquidity_board").inputSchema["properties"]["sort"]["description"]
    help_text = _flat(_invoke("liquidity", "--help").output)
    for text in (sort, help_text):
        assert "percent change)" not in text and "absolute percent" not in text
        assert "billions" in text and "no FX conversion" in text and "percent change does not rank" in text


def test_unknown_measure_is_documented_where_measures_are_listed():
    assert "``unknown``" in liquidity.__doc__
    assert "growth_rate, index or unknown" in _tool("intel_ecb_series").description
    assert liquidity._ecb_measure("BSI", {"DATA_TYPE": "Z"}) == "unknown"


@pytest.mark.parametrize("bad_key, why", [
    ("M.U2.Y.V.M30.X.1.U2.2300.Z01", "BSI 11"),  # 10 dimensions
    ("M.U2.Y.V.M30.X.1.U2..Z01.E", "none empty, so no wildcards"),
    ("M.U2.Y.V.A+B+C+D.X.1+4+I.U2.2300.Z01.A+B+C+D+E", "at most 50 series combinations"),  # 60
    ("M.U2.Y.V.M30.X.1.U2.2300.Z01.E?x", "letters, digits, '.', '+' and '_' only"),
])
async def test_ecb_key_description_states_the_limits_the_validator_applies(fetcher, bad_key, why):
    rules = _tool("intel_ecb_series").inputSchema["properties"]["key"]["description"]
    assert rules == liquidity.ECB_KEY_RULES
    assert why in rules
    assert "ILM 6" in rules and "up to 200 characters" in rules
    with respx.mock:
        route = respx.get(url__startswith="https://data-api.ecb.europa.eu/").respond(503)
        out = await liquidity.fetch_ecb_series(fetcher, key=bad_key)
    assert out["error"].startswith("key must") and route.call_count == 0


def test_cli_board_prints_ranking_rule_and_row_status(tmp_path, monkeypatch):
    payload = _payloads(tmp_path)["board"]
    monkeypatch.setattr(liquidity, "fetch_liquidity_board", _fake(payload))
    out = _invoke("liquidity").output
    assert f"Ranking: {payload['ranking_rule']}" in _flat(out)
    rows = {r["indicator"]: r for r in payload["indicators"]}
    assert rows["euro_area_m3"]["status"] == "Provisional value"
    assert rows["eurosystem_total_assets"]["status"] == "Normal value"
    lines = {line.split()[0]: line for line in out.splitlines() if line.strip()}
    assert "Provisional value" in lines["euro_area_m3"]
    assert "Normal value" in lines["eurosystem_total_assets"]
    assert "value" not in lines["fed_total_assets"]  # FRED rows carry no status


def test_cli_board_from_stale_copies_keeps_values_dates_and_units_whole(tmp_path, monkeypatch):
    """At 100 columns (a list there: too narrow for the table) the stale
    board cuts no value, date, unit or change; the Status column once
    pushed Units into "USD billi…"."""
    async def build():
        f = Fetcher(cache=Cache(db_path=tmp_path / "stale.db"),
                    breaker=CircuitBreaker(failure_threshold=99), max_retries=0)
        with respx.mock:
            _mock_all()
            await liquidity.fetch_liquidity_board(f)
        conn = f.cache._get_conn()
        conn.execute("UPDATE cache SET expires_at = ?", (1,))
        conn.commit()
        with respx.mock:
            respx.get(url__startswith="https://fred.stlouisfed.org/").respond(503)
            respx.get(url__startswith="https://data-api.ecb.europa.eu/").respond(500)
            board = await liquidity.fetch_liquidity_board(f)
        await f.close()
        f.cache.close()
        return board

    payload = asyncio.run(build())
    assert len(payload["indicators"]) == 9
    assert all(r["served_from_stale_cache"] for r in payload["indicators"])
    monkeypatch.setattr(liquidity, "fetch_liquidity_board", _fake(payload))
    monkeypatch.setenv("COLUMNS", "100")
    table = _invoke("liquidity").output.split("Ranking:")[0]
    assert "…" not in table
    for r in payload["indicators"]:
        assert f"{r['observed_at']} (stale)" in table
        assert r["units"] in table
        assert cli._liq_num(r["latest_value"]) in table
        assert cli._liq_num(r["change"], signed=True) in table


def _stale_board(tmp_path) -> dict:
    """The board served from stale copies: a fresh fetch, every cache row
    expired, then FRED answering 503 and the ECB 500."""
    async def build():
        f = Fetcher(cache=Cache(db_path=tmp_path / "stale-sweep.db"),
                    breaker=CircuitBreaker(failure_threshold=99), max_retries=0)
        with respx.mock:
            _mock_all()
            await liquidity.fetch_liquidity_board(f)
        conn = f.cache._get_conn()
        conn.execute("UPDATE cache SET expires_at = ?", (1,))
        conn.commit()
        with respx.mock:
            respx.get(url__startswith="https://fred.stlouisfed.org/").respond(503)
            respx.get(url__startswith="https://data-api.ecb.europa.eu/").respond(500)
            board = await liquidity.fetch_liquidity_board(f)
        await f.close()
        f.cache.close()
        return board

    return asyncio.run(build())


@pytest.mark.parametrize("served", ["fresh", "stale"])
def test_cli_board_cuts_no_value_and_drops_no_column_at_any_width(tmp_path, monkeypatch, served):
    """From 40 to 200 columns: the table where it fits, else one entry per
    indicator. No ellipsis, no line past the width, and every row keeps its
    value, units, change, date and status (at 80 columns the table once
    dropped Indicator, Measure and Status; at 90 it folded names to one
    letter); a listed row names its indicator whole on its own line."""
    payload = _stale_board(tmp_path) if served == "stale" else _payloads(tmp_path)["board"]
    assert len(payload["indicators"]) == 9
    assert all(bool(r.get("served_from_stale_cache")) == (served == "stale") for r in payload["indicators"])
    monkeypatch.setattr(liquidity, "fetch_liquidity_board", _fake(payload))
    layouts = {}
    for width in range(40, 201):
        monkeypatch.setenv("COLUMNS", str(width))
        board = _invoke("liquidity").output.split("Ranking:")[0]
        assert "…" not in board, width
        assert max(len(line) for line in board.splitlines()) <= width, width
        flat = _flat(board)
        listed = "Change %" not in board
        layouts.setdefault("list" if listed else "table", []).append(width)
        lines = [line.strip() for line in board.splitlines()]
        for r in payload["indicators"]:
            observed = f"{r['observed_at']} (stale)" if served == "stale" else r["observed_at"]
            for whole in (observed, cli._liq_num(r["latest_value"]), r["units"],
                          cli._liq_num(r["change"], signed=True), r["status"] or ""):
                assert whole in flat, (width, whole)
            if listed:
                assert r["indicator"] in lines, width
    assert min(layouts["table"]) > max(layouts["list"])
    assert max(layouts["list"]) >= 100 and min(layouts["table"]) <= 120


def test_changelog_states_the_measured_net_liquidity_start():
    text = _flat((_ROOT / "CHANGELOG.md").read_text())
    match = re.search(
        r"Net liquidity therefore starts at (\d{4}-\d\d-\d\d): of ([\d,]+) WALCL weeks, ([\d,]+) align "
        r"and ([\d,]+) are skipped and counted \(first (\S+), last (\S+)\): (\d+) before RRPONTSYD begins "
        r"on \S+, (\d+) with a blank Wednesday before 2013-09-23, and (\S+), when RRPONTSYD is blank on "
        r"both the Tuesday and the Wednesday\. (\d+) points use the Tuesday value, all on holiday "
        r"Wednesdays \(measured 2026-10-17\)\.",
        text,
    )
    assert match, "dated net liquidity start missing from the CHANGELOG"
    start, total, aligned, skipped, first, last, early, blank, late, tuesday = match.groups()
    as_int = lambda s: int(s.replace(",", ""))  # noqa: E731
    assert as_int(aligned) + as_int(skipped) == as_int(total)
    assert int(early) + int(blank) + 1 == as_int(skipped) and late == last
    assert first < start < "2013-09-23" < last
    assert "ECB 404 and empty responses are final no-data answers" not in text


_RANKING_RULES = {
    "biggest_move": (
        "biggest_move ranks absolute changes in billions of each row currency; no FX conversion, "
        "and observation horizons differ. Percentage changes do not affect ranking."
    ),
    "newest": (
        "newest orders rows by the period end of their latest observation, newest first, and rows "
        "without a period end last; rows that tie are in reverse alphabetical order of indicator. "
        "Changes do not affect the order."
    ),
}


@respx.mock
async def test_each_board_sort_states_the_order_it_applies(fetcher):
    _mock_all()
    by_move = await liquidity.fetch_liquidity_board(fetcher)
    newest = await liquidity.fetch_liquidity_board(fetcher, sort="newest")

    assert by_move["sort"] == "biggest_move" and by_move["ranking_rule"] == _RANKING_RULES["biggest_move"]
    assert newest["sort"] == "newest" and newest["ranking_rule"] == _RANKING_RULES["newest"]
    # Newest first; the three 2026-10-14 rows and the four 2026-08-31 rows tie.
    assert [(r["indicator"], r["period_end"]) for r in newest["indicators"]] == [
        ("reverse_repo", "2026-10-16"),
        ("treasury_general_account", "2026-10-14"), ("net_liquidity", "2026-10-14"),
        ("fed_total_assets", "2026-10-14"),
        ("eurosystem_total_assets", "2026-10-11"),
        ("us_m2", "2026-08-31"), ("euro_area_m3", "2026-08-31"), ("euro_area_m2", "2026-08-31"),
        ("euro_area_m1", "2026-08-31"),
    ]


@respx.mock
async def test_newest_puts_a_row_without_a_period_end_last(fetcher, monkeypatch):
    """A weekly ECB period whose end the parser cannot read (patched here)."""
    real = liquidity._period_end
    monkeypatch.setattr(liquidity, "_period_end", lambda p: None if "-W" in str(p.get("id")) else real(p))
    _mock_all()
    newest = await liquidity.fetch_liquidity_board(fetcher, sort="newest")
    rows = newest["indicators"]
    assert rows[-1]["indicator"] == "eurosystem_total_assets" and rows[-1]["period_end"] is None
    assert all(r["period_end"] for r in rows[:-1])


def test_every_sort_the_surfaces_offer_has_a_ranking_rule():
    tool_sorts = _tool("intel_liquidity_board").inputSchema["properties"]["sort"]["enum"]
    cli_sorts = next(p for p in cli.liquidity_cmd.params if p.name == "sort").type.choices
    assert set(tool_sorts) == set(cli_sorts) == set(liquidity._BOARD_RANKING_RULES) == set(_RANKING_RULES)
    assert liquidity._BOARD_RANKING_RULES == _RANKING_RULES


@pytest.mark.parametrize("sort", ["biggest_move", "newest"])
def test_cli_board_prints_the_ranking_rule_of_its_sort(sort):
    """End to end: CLI, source and payload, upstreams mocked."""
    with respx.mock:
        _mock_all()
        out = _flat(_invoke("liquidity", "--sort", sort).output)
    assert f"Liquidity board (sorted by {sort})" in out
    assert f"Ranking: {_RANKING_RULES[sort]}" in out
    (other,) = set(_RANKING_RULES) - {sort}
    assert _RANKING_RULES[other] not in out
