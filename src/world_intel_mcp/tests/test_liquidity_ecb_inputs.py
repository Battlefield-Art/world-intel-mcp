"""ECB caller input outside the stated rules is refused before any request.

The live portal answers a start after its own date with an HTTP 400 HTML
block page (measured 2026-10-17 at 22:00 UTC: startPeriod 2026-10-19 and
2030-01-01 drew 400, 2026-10-17 and 2026-10-18 drew 200 with an empty
body; the portal clock runs at +02:00). Counted as an outage, three such
calls opened the ECB circuit the liquidity board shares for 300 s. A
start after today (UTC) is therefore refused locally, as is a key outside
ECB_KEY_RULES, with the rule named, no request sent and no breaker
failure recorded.

Gaps / not covered: live portal answers (respx only); a 400 the portal
sends for input these checks pass (a very large limit, a start in year 1)
is still retried and counted as an outage; FRED tools filter a start
locally and are not exercised here.
"""

import re
import string
from unittest.mock import AsyncMock

import httpx
import pytest
import respx
from click.testing import CliRunner

from world_intel_mcp import cli
from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import liquidity
from world_intel_mcp.tests.test_liquidity_surfaces import _liquidity_tools
from world_intel_mcp.tests.test_sources_liquidity import (
    _BSI, _ECB_BSI_URL, _fixed_clock_and_no_throttle, _mock_all,
)

_ECB = "https://data-api.ecb.europa.eu/"
_M3 = "M.U2.Y.V.M30.X.1.U2.2300.Z01.E"  # 11 dimensions, 30 characters
_ECB_ROWS = {"euro_area_m1", "euro_area_m2", "euro_area_m3", "eurosystem_total_assets"}
# Abridged: the live page is 12,174 bytes of text/html with this text.
_BLOCK_PAGE = (
    "<html><head><title>European Central Bank</title></head><body>Your access has been "
    "blocked due to security concerns. HTTP status code: 400</body></html>"
)
_DIMENSIONS = "key must specify every SDMX dimension ({n} for {ds}) without wildcards and at most 50 series combinations"


@pytest.fixture
async def fetcher(tmp_path):
    """Like test_sources_liquidity's fetcher (no retries), but closes its client and cache."""
    f = Fetcher(cache=Cache(db_path=tmp_path / "liq.db"), breaker=CircuitBreaker(), default_timeout=5.0,
                max_retries=0)
    yield f
    await f.close()
    f.cache.close()


@pytest.fixture
def sleep(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    mock = AsyncMock()
    monkeypatch.setattr("world_intel_mcp.fetcher.asyncio.sleep", mock)
    return mock


def _ecb_failures(fetcher) -> int:
    fetcher.breaker.is_available(liquidity.ECB_SOURCE)  # registers the source if no request was ever sent
    return fetcher.breaker.status()[liquidity.ECB_SOURCE]["total_failures"]


# ---------------------------------------------------------------------------
# A start after today
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("start", ["2026-10-18", "2030-01-01"])
@respx.mock
async def test_a_future_start_is_refused_before_any_request(fetcher, sleep, start):
    fetcher.max_retries = 2
    route = respx.get(url__startswith=_ECB).respond(400, text=_BLOCK_PAGE, headers={"content-type": "text/html"})

    out = await liquidity.fetch_ecb_series(fetcher, key=_M3, start=start)

    assert out["error"] == f"start must be on or before today (2026-10-17 UTC), got {start!r}"
    assert route.call_count == 0
    sleep.assert_not_awaited()
    assert _ecb_failures(fetcher) == 0


@respx.mock
async def test_a_start_of_today_is_still_sent(fetcher):
    route = respx.get(url__startswith=_ECB).respond(200, content=b"")

    out = await liquidity.fetch_ecb_series(fetcher, key=_M3, start="2026-10-17")

    assert out["error"] == f"ECB Data Portal: no observations on or after 2026-10-17 for BSI/{_M3}"
    assert route.call_count == 1
    assert route.calls.last.request.url.params["startPeriod"] == "2026-10-17"


@respx.mock
async def test_future_starts_never_open_the_circuit_the_board_shares(fetcher, sleep):
    """The reviewer's scenario: three calls with a future start, then the board."""
    fetcher.max_retries = 2

    def portal(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("startPeriod", "") > "2026-10-18":
            return httpx.Response(400, text=_BLOCK_PAGE, headers={"content-type": "text/html"})
        return httpx.Response(200, json=_BSI)

    _mock_all()
    bsi = respx.get(_ECB_BSI_URL).mock(side_effect=portal)
    for _ in range(fetcher.breaker.failure_threshold):
        out = await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY, start="2030-01-01")
        assert out["error"].startswith("start must be on or before today")
    assert bsi.call_count == 0

    board = await liquidity.fetch_liquidity_board(fetcher)

    assert {r["indicator"] for r in board["indicators"] if r["source"] == liquidity.ECB_SOURCE} == _ECB_ROWS
    assert board["unavailable"] == []
    assert not any(r.get("served_from_stale_cache") for r in board["indicators"])
    state = fetcher.breaker.status()[liquidity.ECB_SOURCE]
    assert state["status"] == "closed" and state["total_failures"] == 0


@pytest.fixture
def cli_cache(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """The CLI's own fetcher on a temp cache, closed afterwards."""
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "cli-cache.db"))
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.setattr(cli, "_fetcher", None)
    yield
    if cli._fetcher is not None:
        cli._fetcher.cache.close()


def test_cli_ecb_series_refuses_a_future_start_without_a_request(cli_cache):
    with respx.mock:
        route = respx.get(url__startswith=_ECB).respond(400, text=_BLOCK_PAGE, headers={"content-type": "text/html"})
        result = CliRunner().invoke(cli.main, ["ecb-series", _M3, "--start", "2030-01-01"], catch_exceptions=False)

    assert "Error: start must be on or before today (2026-10-17 UTC), got '2030-01-01'" in " ".join(result.output.split())
    assert route.call_count == 0
    assert _ecb_failures(cli._fetcher) == 0


def test_the_ecb_surfaces_state_the_start_rule():
    tool = next(t for t in _liquidity_tools().TOOLS if t.name == "intel_ecb_series")
    assert "not after today (UTC)" in tool.inputSchema["properties"]["start"]["description"]
    option = next(p for p in cli.ecb_series_cmd.params if p.name == "start")
    assert "not after today (UTC)" in option.help


# ---------------------------------------------------------------------------
# Keys outside ECB_KEY_RULES
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dataset, key, error", [
    pytest.param("BSI", _M3 + ".X", _DIMENSIONS.format(n=11, ds="BSI"), id="bsi-12-dimensions"),
    pytest.param("BSI", "M.U2.Y.V.M30.X.1.U2.2300.Z01", _DIMENSIONS.format(n=11, ds="BSI"), id="bsi-10-dimensions"),
    pytest.param("ILM", "W.U2.C.T000000.Z5", _DIMENSIONS.format(n=6, ds="ILM"), id="ilm-5-dimensions"),
    pytest.param("ILM", "W.U2.C.T000000.Z5.Z01.X", _DIMENSIONS.format(n=6, ds="ILM"), id="ilm-7-dimensions"),
    pytest.param("BSI", _M3.replace("M30", "M30" + "A" * 171), "key must be at most 200 characters, got 201",
                 id="201-characters"),
    pytest.param("BSI", _M3 + "?x", "key must be an SDMX series key: letters, digits, '.', '+' and '_' only",
                 id="character-outside-the-set"),
])
async def test_a_key_outside_the_rules_is_refused_with_its_rule_and_no_request(fetcher, dataset, key, error):
    with respx.mock:
        route = respx.get(url__startswith=_ECB).respond(503)
        out = await liquidity.fetch_ecb_series(fetcher, dataset=dataset, key=key)
    assert out["error"] == error
    assert route.call_count == 0
    assert _ecb_failures(fetcher) == 0


@pytest.mark.parametrize("dataset, key", [
    pytest.param("BSI", _M3.replace("M30", "M30" + "A" * 170), id="200-characters"),
    pytest.param("ILM", liquidity._ECB_EUROSYSTEM_KEY, id="ilm-6-dimensions"),
])
async def test_a_key_at_the_limits_is_sent(fetcher, dataset, key):
    assert len(key) <= 200
    with respx.mock:
        route = respx.get(url__startswith=_ECB).respond(200, json=_BSI)
        out = await liquidity.fetch_ecb_series(fetcher, dataset=dataset, key=key)
    assert route.call_count == 1
    assert not out.get("error", "").startswith("key must")


def test_key_rules_state_the_characters_and_length_the_validator_applies():
    """Drift check: read the characters and the length out of ECB_KEY_RULES
    and hold the validator (and history tooling that full-matches
    _ECB_KEY_RE) to exactly those."""
    match = re.search(r"letters, digits, (.+) only, up to (\d+) characters$", liquidity.ECB_KEY_RULES)
    assert match, liquidity.ECB_KEY_RULES
    allowed = set(re.findall(r"'(.)'", match.group(1)))
    longest = int(match.group(2))
    assert allowed == {".", "+", "_"} and longest == 200
    for c in string.printable:
        accepted = bool(liquidity._ECB_KEY_RE.fullmatch(f"A{c}B"))
        assert accepted is (c.isalnum() or c in allowed), repr(c)
    assert liquidity._ECB_KEY_RE.fullmatch("A" * longest)
    assert not liquidity._ECB_KEY_RE.fullmatch("A" * (longest + 1))
    assert not liquidity._ECB_KEY_RE.fullmatch("")
