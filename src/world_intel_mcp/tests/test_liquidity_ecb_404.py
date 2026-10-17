"""ECB 404s: the portal's own "no series" answer versus an error page.

The ECB Data Portal answers a key that matches no series with a 404
whose body is empty or a JSON problem object; that stays a final
no-data answer (not retried, not a breaker failure, no stale copy). A
404 with any other body (an HTML or plain-text error page, JSON that is
not an object) came from something in front of the portal: it is
retried per the fetcher's rules, counted as a breaker failure, and the
last good copy is served marked, with the failure named in data_gaps.

Gaps / not covered: live ECB responses (fixtures and respx only). The
residual risk stays: a JSON-object 404 from a moved gateway still reads
as no-data.
"""

import time
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
import respx

from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import liquidity
from world_intel_mcp.tests.test_sources_liquidity import (
    _BSI, _ECB_404, _ECB_BSI_URL, _ECB_ILM_URL,
    _fixed_clock_and_no_throttle, _mock_all,
)

_ECB_ROWS = {"euro_area_m1", "euro_area_m2", "euro_area_m3", "eurosystem_total_assets"}
_BOARD_KEYS = (
    f"liquidity:ecb:BSI:{liquidity._ECB_MONEY_KEY}:2:None",
    f"liquidity:ecb:ILM:{liquidity._ECB_EUROSYSTEM_KEY}:2:None",
)
_NAMED = "upstream fetch failed (HTTP 404)"
_HTML_404 = "<html><body><h1>404 Not Found</h1>nginx</body></html>"
_ERROR_PAGES = [
    pytest.param(httpx.Response(404, text=_HTML_404, headers={"content-type": "text/html"}), id="html"),
    pytest.param(httpx.Response(404, text="Not Found", headers={"content-type": "text/plain"}), id="plain-text"),
    pytest.param(httpx.Response(404, content=b"[]", headers={"content-type": "application/json"}), id="json-array"),
]


def _age(fetcher, seconds: float) -> None:
    """Expire every cached copy and date it ``seconds`` ago."""
    conn = fetcher.cache._get_conn()
    conn.execute("UPDATE cache SET expires_at = ?, created_at = ?", (1, time.time() - seconds))
    conn.commit()


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


@pytest.mark.parametrize("response", _ERROR_PAGES)
@respx.mock
async def test_error_page_404_on_the_board_serves_the_marked_stale_copy(fetcher, sleep, response):
    """The reviewer's probe N1: the board's fixed keys, a 2-hour-old good copy cached."""
    _mock_all()
    await liquidity.fetch_liquidity_board(fetcher)
    _age(fetcher, 7200)
    before = [fetcher.cache.get_stale_meta(k) for k in _BOARD_KEYS]
    fetcher.max_retries = 2
    bsi = respx.get(_ECB_BSI_URL).mock(return_value=response)
    ilm = respx.get(_ECB_ILM_URL).mock(return_value=response)
    sent = (bsi.call_count, ilm.call_count)  # the routes also served the seeding call

    board = await liquidity.fetch_liquidity_board(fetcher)

    ecb = {r["indicator"]: r for r in board["indicators"] if r["source"] == liquidity.ECB_SOURCE}
    assert set(ecb) == _ECB_ROWS
    assert board["unavailable"] == []
    for row in ecb.values():
        assert row["served_from_stale_cache"] is True and row["freshness"]["stale"] is True
        assert row["fetch_failure"] == _NAMED and row["freshness"]["fetch_failure"] == _NAMED
        assert row["freshness"]["cache_age_seconds"] >= 7200
    for name in _ECB_ROWS:
        assert any(g.startswith(f"{name}: {_NAMED}; served from stale cache (") for g in board["data_gaps"]), name
    assert not any("no series matches" in g for g in board["data_gaps"])
    assert (bsi.call_count - sent[0], ilm.call_count - sent[1]) == (3, 3)  # retried per the fetcher's rules
    assert sleep.await_count == 4
    state = fetcher.breaker.status()[liquidity.ECB_SOURCE]
    assert state["total_failures"] == 2 and state["total_stale_serves"] == 2
    assert [fetcher.cache.get_stale_meta(k) for k in _BOARD_KEYS] == before  # never restamped


@pytest.mark.parametrize("response", _ERROR_PAGES)
@respx.mock
async def test_error_page_404_is_named_cold_and_stale_on_a_single_series(fetcher, sleep, response):
    key = liquidity._ECB_MONEY_KEY
    route = respx.get(_ECB_BSI_URL).mock(return_value=response)
    cold = await liquidity.fetch_ecb_series(fetcher, key=key)
    assert cold["error"] == f"ECB Data Portal returned no data for BSI/{key}: {_NAMED}, no cached copy"
    assert cold["data_gaps"] == [cold["error"]]
    assert fetcher.breaker.status()[liquidity.ECB_SOURCE]["total_failures"] == 1

    route.mock(return_value=httpx.Response(200, json=_BSI))
    await liquidity.fetch_ecb_series(fetcher, key=key)
    _age(fetcher, 7200)
    route.mock(return_value=response)
    warm = await liquidity.fetch_ecb_series(fetcher, key=key)
    assert warm["series"] and all(s["served_from_stale_cache"] and s["fetch_failure"] == _NAMED
                                  for s in warm["series"])
    assert any(f": {_NAMED}; served from stale cache (" in g for g in warm["data_gaps"])
    assert fetcher.breaker.status()[liquidity.ECB_SOURCE]["total_failures"] == 2


@respx.mock
async def test_the_portal_problem_404_stays_a_final_no_data_answer(fetcher, sleep):
    """The live problem+json body ("No Series was returned for the query")."""
    _mock_all()
    await liquidity.fetch_liquidity_board(fetcher)
    _age(fetcher, 7200)
    fetcher.max_retries = 2
    problem = httpx.Response(404, text=_ECB_404, headers={"content-type": "application/problem+json"})
    bsi = respx.get(_ECB_BSI_URL).mock(return_value=problem)
    ilm = respx.get(_ECB_ILM_URL).mock(return_value=problem)
    sent = (bsi.call_count, ilm.call_count)  # the routes also served the seeding call

    board = await liquidity.fetch_liquidity_board(fetcher)

    assert not any(r["source"] == liquidity.ECB_SOURCE for r in board["indicators"])  # no stale copy
    errors = {u["indicator"]: u["error"] for u in board["unavailable"]}
    assert set(errors) == _ECB_ROWS
    assert errors["euro_area_m1"] == f"ECB Data Portal: no series matches the key BSI/{liquidity._ECB_MONEY_KEY}"
    assert errors["eurosystem_total_assets"] == (
        f"ECB Data Portal: no series matches the key ILM/{liquidity._ECB_EUROSYSTEM_KEY}"
    )
    assert (bsi.call_count - sent[0], ilm.call_count - sent[1]) == (1, 1)
    sleep.assert_not_awaited()
    state = fetcher.breaker.status()[liquidity.ECB_SOURCE]
    assert state["total_failures"] == 0 and state["total_stale_serves"] == 0


@pytest.mark.parametrize("body, no_data", [
    pytest.param(b"", True, id="empty"),
    pytest.param(b" \n", True, id="whitespace"),
    pytest.param(_ECB_404.encode(), True, id="live-problem-json"),
    pytest.param(b'{"title":"No results"}', True, id="json-object"),
    pytest.param(_HTML_404.encode(), False, id="html"),
    pytest.param(b"Not Found", False, id="plain-text"),
    pytest.param(b"[]", False, id="json-array"),
    pytest.param(b"null", False, id="json-null"),
    pytest.param(b'"No results"', False, id="json-string"),
    pytest.param(b"{broken", False, id="broken-json"),
    pytest.param(b"\x80{}", False, id="not-utf8"),
    pytest.param(b"[" * 100_000, False, id="nesting-past-recursion-limit"),
])
def test_ecb_404_body_classification(body, no_data):
    assert liquidity._ecb_404_is_no_data(body) is no_data


def test_changelog_states_the_ecb_404_rule():
    text = " ".join((Path(__file__).parents[3] / "CHANGELOG.md").read_text().split())
    assert (
        "An ECB 404 whose body is empty or a JSON object (the portal's problem answer) is a final "
        "no-data answer, without outage retries; any other 404, such as an HTML or plain-text error "
        "page, is retried, counted as a breaker failure and takes the outage path."
    ) in text
    assert "ECB 404 responses are final no-data answers" not in text
