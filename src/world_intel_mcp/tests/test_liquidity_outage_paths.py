"""Liquidity outage paths: what each failure kind does to a response.

Every failure that leaves a series without a fresh body is named in the
response (HTTP status, no response, open circuit, an oversized body,
unexpected payload, parse error, including an observation that is not a
finite number); a stale copy is served marked, never re-cached as fresh,
and counted on the circuit breaker. Also pins the ECB key cap and the
"." blank in FRED CSVs. Every fetcher a test opens is closed when it
ends.

Gaps / not covered: live upstreams (fixtures and respx only); concurrent
requests racing the breaker threshold (each test drives one sequence).
"""

import copy
import json
from datetime import date

import httpx
import pytest
import respx

from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import BinaryResult, Fetcher
from world_intel_mcp.sources import liquidity
from world_intel_mcp.tests.test_sources_liquidity import (
    _BSI, _ECB_BSI_URL, _ECB_ILM_URL, _FRED_URL,
    _fixed_clock_and_no_throttle, _fred_router, _mock_all,
)

_ECB_ROWS = {"euro_area_m1", "euro_area_m2", "euro_area_m3", "eurosystem_total_assets"}
_CIRCUIT_OPEN = "upstream fetch failed (circuit open: request not sent after repeated failures)"


@pytest.fixture
async def fetcher(tmp_path):
    """Like test_sources_liquidity's fetcher (no retries), but closes its client and cache."""
    f = Fetcher(cache=Cache(db_path=tmp_path / "liq.db"), breaker=CircuitBreaker(), default_timeout=5.0,
                max_retries=0)
    yield f
    await f.close()
    f.cache.close()


def _expire(fetcher, key=None):
    conn = fetcher.cache._get_conn()
    if key is None:
        conn.execute("UPDATE cache SET expires_at = ?, created_at = ?", (1, 1))
    else:
        conn.execute("UPDATE cache SET expires_at = ?, created_at = ? WHERE key = ?", (1, 1, key))
    conn.commit()


def _respond(route, failure):
    if isinstance(failure, Exception):
        route.mock(side_effect=failure)
    else:
        route.mock(return_value=failure)


# ---------------------------------------------------------------------------
# A stale ECB copy is never restamped (twin of the FRED rule)
# ---------------------------------------------------------------------------


@respx.mock
async def test_only_failed_ecb_dataset_is_stale_and_never_restamped(fetcher):
    _mock_all()
    await liquidity.fetch_liquidity_board(fetcher)
    key = f"liquidity:ecb:BSI:{liquidity._ECB_MONEY_KEY}:2:None"
    _expire(fetcher, key)
    original = fetcher.cache.get_stale_meta(key)
    respx.get(_ECB_BSI_URL).respond(503)
    # Twice: a copy re-cached as fresh on the first call would read as
    # fresh, unmarked data on the second.
    for _ in range(2):
        board = await liquidity.fetch_liquidity_board(fetcher)
        for row in board["indicators"]:
            stale = row["indicator"] in ("euro_area_m1", "euro_area_m2", "euro_area_m3")
            assert bool(row.get("served_from_stale_cache")) is stale, row["indicator"]
    assert fetcher.cache.get_stale_meta(key) == original


# ---------------------------------------------------------------------------
# An empty ECB answer means "no data" only when a start was sent
# ---------------------------------------------------------------------------


@respx.mock
async def test_ecb_empty_answer_without_start_is_an_unexpected_payload(fetcher):
    respx.get(_ECB_BSI_URL).respond(200, content=b"")
    cold = await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY)
    assert cold["error"] == (
        f"ECB Data Portal returned no data for BSI/{liquidity._ECB_MONEY_KEY}: "
        "unexpected payload (empty body, no start requested), no cached copy"
    )
    assert "no observations on or after" not in cold["error"]

    # The board sends no start: an empty answer there takes the outage path
    # and serves the last good copy, marked, instead of dropping the rows.
    _mock_all()
    await liquidity.fetch_liquidity_board(fetcher)
    _expire(fetcher)
    respx.get(_ECB_BSI_URL).respond(200, content=b"")
    respx.get(_ECB_ILM_URL).respond(200, content=b"")
    board = await liquidity.fetch_liquidity_board(fetcher)
    rows = {r["indicator"]: r for r in board["indicators"] if r["source"] == liquidity.ECB_SOURCE}
    assert set(rows) == _ECB_ROWS
    assert board["unavailable"] == []
    for row in rows.values():
        assert row["served_from_stale_cache"] is True and row["freshness"]["stale"] is True
        assert row["fetch_failure"] == "unexpected payload (empty body, no start requested)"
    assert any(
        g.startswith("euro_area_m1: unexpected payload (empty body, no start requested); served from stale cache (")
        for g in board["data_gaps"]
    )


@respx.mock
async def test_ecb_empty_answer_with_start_stays_a_final_no_data_answer(fetcher):
    route = respx.get(_ECB_BSI_URL).respond(200, content=b"")
    out = await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY, start="2026-10-01")
    assert out["error"] == (
        f"ECB Data Portal: no observations on or after 2026-10-01 for BSI/{liquidity._ECB_MONEY_KEY}"
    )
    assert route.calls.last.request.url.params["startPeriod"] == "2026-10-01"


# ---------------------------------------------------------------------------
# JSON that is not an object is an unexpected payload, not an outage
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("body, kind", [(b"[]", "array"), (b"null", "null"), (b'"text"', "string"), (b"7", "number")])
@respx.mock
async def test_ecb_json_that_is_not_an_object_is_an_unexpected_payload(fetcher, body, kind):
    respx.get(_ECB_BSI_URL).respond(200, content=body)
    failure = f"unexpected payload (JSON {kind}, not an object)"
    cold = await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY)
    assert cold["error"] == (
        f"ECB Data Portal returned no data for BSI/{liquidity._ECB_MONEY_KEY}: {failure}, no cached copy"
    )
    assert "circuit open" not in cold["error"] and cold["data_gaps"] == [cold["error"]]

    # With a last good copy it is served marked, and the gap says why.
    respx.get(_ECB_BSI_URL).respond(200, json=_BSI)
    await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY)
    _expire(fetcher)
    respx.get(_ECB_BSI_URL).respond(200, content=body)
    warm = await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY)
    assert warm["series"] and all(s["served_from_stale_cache"] and s["fetch_failure"] == failure
                                  for s in warm["series"])
    assert any(f": {failure}; served from stale cache (" in g for g in warm["data_gaps"])


# ---------------------------------------------------------------------------
# Every stale serve counts on the circuit breaker
# ---------------------------------------------------------------------------


@respx.mock
async def test_every_stale_serve_counts_on_the_breaker(fetcher):
    _mock_all()
    await liquidity.fetch_liquidity_board(fetcher)
    counts = lambda: {src: fetcher.breaker.status()[src]["total_stale_serves"]  # noqa: E731
                      for src in (liquidity.FRED_SOURCE, liquidity.ECB_SOURCE)}
    assert counts() == {liquidity.FRED_SOURCE: 0, liquidity.ECB_SOURCE: 0}
    _expire(fetcher)
    respx.get(_FRED_URL).respond(503)
    respx.get(_ECB_BSI_URL).respond(503)
    respx.get(_ECB_ILM_URL).respond(503)
    board = await liquidity.fetch_liquidity_board(fetcher)
    assert all(r["served_from_stale_cache"] for r in board["indicators"])
    # WALCL, WDTGAL, RRPONTSYD and M2SL; BSI and ILM.
    assert counts() == {liquidity.FRED_SOURCE: 4, liquidity.ECB_SOURCE: 2}
    # Copies served behind an open circuit count too.
    for _ in range(3):
        fetcher.breaker.record_failure(liquidity.FRED_SOURCE)
    assert not fetcher.breaker.is_available(liquidity.FRED_SOURCE)
    single = await liquidity.fetch_fed_liquidity_series(fetcher, "WALCL")
    assert single["served_from_stale_cache"] is True
    assert counts()[liquidity.FRED_SOURCE] == 5


@respx.mock
async def test_fresh_answers_and_unserved_copies_do_not_count(fetcher):
    _mock_all()
    await liquidity.fetch_net_liquidity(fetcher)
    await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY)
    respx.get(_FRED_URL).respond(503)
    cold = await liquidity.fetch_fed_liquidity_series(fetcher, "M2SL")  # nothing cached for M2SL
    assert "no cached copy" in cold["error"]
    assert fetcher.breaker.status()[liquidity.FRED_SOURCE]["total_stale_serves"] == 0
    assert fetcher.breaker.status()[liquidity.ECB_SOURCE]["total_stale_serves"] == 0


# ---------------------------------------------------------------------------
# Gaps and errors name the failure kind
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("failure, named", [
    (httpx.Response(503), "upstream fetch failed (HTTP 503)"),
    (httpx.Response(404, text="<html>Not Found</html>"), "upstream fetch failed (HTTP 404)"),
    (httpx.ReadTimeout("offline"), "upstream fetch failed (no response: ReadTimeout)"),
    (httpx.ConnectError("offline"), "upstream fetch failed (no response: ConnectError)"),
])
@respx.mock
async def test_fred_failure_kind_is_named_cold_and_stale(fetcher, failure, named):
    route = respx.get(_FRED_URL)
    _respond(route, failure)
    cold = await liquidity.fetch_fed_liquidity_series(fetcher, "WALCL")
    assert cold["error"] == f"FRED graph CSV for WALCL unavailable: {named}, no cached copy"
    assert cold["data_gaps"] == [cold["error"]]

    route.mock(side_effect=_fred_router())
    await liquidity.fetch_fed_liquidity_series(fetcher, "WALCL")
    _expire(fetcher)
    _respond(route, failure)
    warm = await liquidity.fetch_fed_liquidity_series(fetcher, "WALCL")
    assert warm["served_from_stale_cache"] is True and warm["fetch_failure"] == named
    assert warm["freshness"]["fetch_failure"] == named
    assert any(g.startswith(f"WALCL: {named}; served from stale cache (") for g in warm["data_gaps"])


@pytest.mark.parametrize("failure, named", [
    (httpx.Response(503), "upstream fetch failed (HTTP 503)"),
    (httpx.Response(500), "upstream fetch failed (HTTP 500)"),
    (httpx.ReadTimeout("offline"), "upstream fetch failed (no response: ReadTimeout)"),
])
@respx.mock
async def test_ecb_failure_kind_is_named_cold_and_stale(fetcher, failure, named):
    route = respx.get(_ECB_BSI_URL)
    _respond(route, failure)
    cold = await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY)
    assert cold["error"] == (
        f"ECB Data Portal returned no data for BSI/{liquidity._ECB_MONEY_KEY}: {named}, no cached copy"
    )
    route.mock(return_value=httpx.Response(200, json=_BSI))
    await liquidity.fetch_money_supply(fetcher, region="euro_area")
    _expire(fetcher)
    _respond(route, failure)
    money = await liquidity.fetch_money_supply(fetcher, region="euro_area")
    for roles in money["euro_area"]["aggregates"].values():
        for s in roles.values():
            assert s["fetch_failure"] == named and s["freshness"]["fetch_failure"] == named
    assert any(g.startswith(f"euro area M3 stock: {named}; served from stale cache (") for g in money["data_gaps"])


@respx.mock
async def test_open_circuit_is_named_on_every_surface(fetcher):
    _mock_all()
    await liquidity.fetch_liquidity_board(fetcher)
    await liquidity.fetch_net_liquidity(fetcher)
    _expire(fetcher)
    for _ in range(3):
        fetcher.breaker.record_failure(liquidity.FRED_SOURCE)
        fetcher.breaker.record_failure(liquidity.ECB_SOURCE)
    routes = [respx.get(url).respond(503) for url in (_FRED_URL, _ECB_BSI_URL, _ECB_ILM_URL)]
    sent_before = sum(r.call_count for r in routes)
    board = await liquidity.fetch_liquidity_board(fetcher)
    net = await liquidity.fetch_net_liquidity(fetcher)
    assert sum(r.call_count for r in routes) == sent_before  # nothing sent while open
    assert {r["indicator"] for r in board["indicators"]} >= _ECB_ROWS | {"net_liquidity"}
    for row in board["indicators"]:
        assert row["fetch_failure"] == _CIRCUIT_OPEN and row["freshness"]["fetch_failure"] == _CIRCUIT_OPEN
    assert net["fetch_failure"] == _CIRCUIT_OPEN
    assert all(c["fetch_failure"] == _CIRCUIT_OPEN for c in net["components"].values())
    for out in (board, net):
        stale_gaps = [g for g in out["data_gaps"] if "served from stale cache" in g]
        assert stale_gaps and all(f": {_CIRCUIT_OPEN}; served from stale cache (" in g for g in stale_gaps)


@respx.mock
async def test_no_gap_says_the_cause_is_unknown(fetcher):
    """The generic "fetch failed or circuit open" sentence named no cause."""
    _mock_all()
    await liquidity.fetch_liquidity_board(fetcher)
    await liquidity.fetch_net_liquidity(fetcher)
    await liquidity.fetch_money_supply(fetcher)
    _expire(fetcher)
    respx.get(_FRED_URL).respond(503)
    respx.get(_ECB_BSI_URL).respond(503)
    respx.get(_ECB_ILM_URL).respond(503)
    outputs = [
        await liquidity.fetch_fed_liquidity_series(fetcher, "M2SL"),
        await liquidity.fetch_net_liquidity(fetcher),
        await liquidity.fetch_money_supply(fetcher),
        await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY),
        await liquidity.fetch_liquidity_board(fetcher),
    ]
    for out in outputs:
        texts = list(out["data_gaps"]) + [out.get("error") or ""]
        assert not any("or circuit open" in t or "fetch failed, no cached copy" in t for t in texts)
        assert any("(HTTP 503)" in t or "(circuit open:" in t for t in texts)


@respx.mock
async def test_parse_errors_are_named(fetcher):
    respx.get(_FRED_URL).mock(side_effect=_fred_router({"WALCL": httpx.Response(200, text="<html>Maintenance</html>")}))
    fred = await liquidity.fetch_fed_liquidity_series(fetcher, "WALCL")
    assert fred["error"].startswith("FRED graph CSV for WALCL not in the expected format (parse error: unexpected header")
    respx.get(_ECB_BSI_URL).respond(200, content=b"{broken")
    ecb = await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY)
    assert ecb["error"].startswith("ECB Data Portal response not in the expected SDMX-JSON shape (parse error: JSONDecodeError(")
    shape = copy.deepcopy(_BSI)
    del shape["structure"]
    respx.get(_ECB_BSI_URL).respond(200, json=shape)
    ecb = await liquidity.fetch_ecb_series(fetcher, key=liquidity._ECB_MONEY_KEY)
    assert ecb["error"] == (
        f"ECB Data Portal response for BSI/{liquidity._ECB_MONEY_KEY} not in the expected "
        "SDMX-JSON shape (parse error: KeyError('structure'))"
    )


# ---------------------------------------------------------------------------
# Boundaries the earlier tests left open
# ---------------------------------------------------------------------------


@respx.mock
async def test_ecb_key_cap_is_exactly_fifty_combinations(fetcher):
    route = respx.get(url__startswith="https://data-api.ecb.europa.eu/").respond(200, json=_BSI)
    items = lambda n: "+".join(f"M{i}" for i in range(n))  # noqa: E731
    fifty = f"M.U2.Y.V.{items(25)}.X.1+4.U2.2300.Z01.E"  # 25 x 2
    fifty_one = f"M.U2.Y.V.{items(17)}.X.1+4+I.U2.2300.Z01.E"  # 17 x 3
    assert len(fifty) <= 200 and len(fifty_one) <= 200  # the cap, not the length rule, decides
    accepted = await liquidity.fetch_ecb_series(fetcher, key=fifty)
    assert "error" not in accepted and accepted["series_count"] == 12
    assert route.call_count == 1
    refused = await liquidity.fetch_ecb_series(fetcher, key=fifty_one)
    assert refused["error"] == (
        "key must specify every SDMX dimension (11 for BSI) without wildcards and at most 50 series combinations"
    )
    assert route.call_count == 1


@respx.mock
async def test_fred_dot_cell_is_a_missing_observation(fetcher):
    text = "observation_date,RRPONTSYD\n2013-09-20,.\n2013-09-23,1.5\n2013-09-24,\n2013-09-25,0.25\n"
    assert liquidity.parse_fred_csv(text, "RRPONTSYD") == [
        (date(2013, 9, 20), None), (date(2013, 9, 23), 1.5),
        (date(2013, 9, 24), None), (date(2013, 9, 25), 0.25),
    ]
    respx.get(_FRED_URL).respond(200, text=text)
    out = await liquidity.fetch_fed_liquidity_series(fetcher, "RRPONTSYD", limit=0)
    assert out["blank_observations"] == 2
    assert [p["value"] for p in out["points"]] == [0.25, 1.5]


# ---------------------------------------------------------------------------
# A whitespace-only ECB body, an oversized body, a value that is no number
# ---------------------------------------------------------------------------


@respx.mock
async def test_ecb_whitespace_only_body_reads_as_an_empty_one(fetcher):
    key = liquidity._ECB_MONEY_KEY
    empty = "unexpected payload (empty body, no start requested)"
    route = respx.get(_ECB_BSI_URL).respond(200, content=b" \n")
    with_start = await liquidity.fetch_ecb_series(fetcher, key=key, start="2026-10-01")
    assert with_start["error"] == f"ECB Data Portal: no observations on or after 2026-10-01 for BSI/{key}"
    cold = await liquidity.fetch_ecb_series(fetcher, key=key)
    assert cold["error"] == f"ECB Data Portal returned no data for BSI/{key}: {empty}, no cached copy"

    route.mock(return_value=httpx.Response(200, json=_BSI))
    await liquidity.fetch_ecb_series(fetcher, key=key)
    _expire(fetcher)
    route.mock(return_value=httpx.Response(200, content=b" \n"))
    warm = await liquidity.fetch_ecb_series(fetcher, key=key)
    assert warm["series"] and all(s["served_from_stale_cache"] and s["fetch_failure"] == empty for s in warm["series"])
    assert fetcher.breaker.status()[liquidity.ECB_SOURCE]["total_failures"] == 0


@pytest.mark.parametrize("error", [
    "body of 300 bytes exceeds cap 100",
    "body exceeds cap 100: stopped after 300 decoded bytes",  # the streamed read's wording on the cyclone branch
])
def test_an_oversized_body_is_named_with_its_cap(error):
    assert liquidity._fetch_failure(BinaryResult(200, None, error)) == f"upstream fetch failed (HTTP 200, {error})"


def _bsi_with_first_value(literal: str) -> bytes:
    """The live BSI response with its first observation value written as ``literal``."""
    data = copy.deepcopy(_BSI)
    first = next(iter(data["dataSets"][0]["series"].values()))
    first["observations"][next(iter(first["observations"]))][0] = 424242.25
    text = json.dumps(data)
    assert text.count("424242.25") == 1
    return text.replace("424242.25", literal).encode()


_NOT_FINITE = [
    pytest.param(str(10**400), id="401-digit-integer"),
    pytest.param("1e400", id="1e400"),
    pytest.param("Infinity", id="infinity"),
    pytest.param("-Infinity", id="minus-infinity"),
    pytest.param("NaN", id="nan"),
]


@pytest.mark.parametrize("literal", _NOT_FINITE)
@respx.mock
async def test_an_ecb_value_that_is_not_a_finite_number_is_a_parse_error(fetcher, literal):
    key = liquidity._ECB_MONEY_KEY
    respx.get(_ECB_BSI_URL).respond(200, content=_bsi_with_first_value(literal))
    out = await liquidity.fetch_ecb_series(fetcher, key=key)
    assert out["error"].startswith(
        f"ECB Data Portal response for BSI/{key} not in the expected SDMX-JSON shape (parse error: "
    )
    assert ("OverflowError" if literal.isdigit() else "is not a finite number") in out["error"]
    assert out["data_gaps"] == [out["error"]]
    assert fetcher.cache.get_stale(f"liquidity:ecb:BSI:{key}:25:None") is None  # never cached
    assert fetcher.breaker.status()[liquidity.ECB_SOURCE]["total_failures"] == 0


@pytest.mark.parametrize("literal", _NOT_FINITE[:2])
@respx.mock
async def test_a_value_that_is_not_a_finite_number_drops_only_its_rows(fetcher, literal):
    """It once raised out of the board and money supply (gather without
    return_exceptions), losing the FRED and ILM rows too."""
    _mock_all(bsi=httpx.Response(200, content=_bsi_with_first_value(literal)))
    board = await liquidity.fetch_liquidity_board(fetcher)
    assert {r["indicator"] for r in board["indicators"]} == {
        "fed_total_assets", "treasury_general_account", "reverse_repo", "net_liquidity", "us_m2",
        "eurosystem_total_assets",
    }
    missing = {u["indicator"]: u["error"] for u in board["unavailable"]}
    assert set(missing) == {"euro_area_m1", "euro_area_m2", "euro_area_m3"}
    assert all("SDMX-JSON shape (parse error: " in e for e in missing.values())

    money = await liquidity.fetch_money_supply(fetcher)
    assert "error" not in money and money["us"]["M2"]["latest"]
    assert "SDMX-JSON shape (parse error: " in money["euro_area"]["error"]
