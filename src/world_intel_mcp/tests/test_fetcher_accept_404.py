"""Tests for Fetcher.get_bytes(accept_404=...).

A 404 is normally a definitive answer: not retried, not a source
failure. An API that answers "no data" with a 404 of a known shape can
pass ``accept_404`` (keyword-only), a predicate on the 404 body: a 404
it accepts stays a definitive answer, a 404 it rejects (an error page
from a proxy in front of the API) is retried and counted as a failure
like any other HTTP error status. Callers that pass no predicate are
unaffected.

Gaps / not covered: a predicate that raises (the contract says it must
not); get_text, get_json and get_xml have no such option.
"""

import inspect
from unittest.mock import AsyncMock

import httpx
import pytest
import respx

from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import BinaryResult, Fetcher

_URL = "https://a.test/data"
_HTML_404 = b"<html><body><h1>404 Not Found</h1>nginx</body></html>"
_PROBLEM_404 = b'{"title":"Not Found","status":404,"detail":"No Series was returned for the query"}'


def _json_object(body: bytes) -> bool:
    return body.lstrip().startswith(b"{")


@pytest.fixture
async def fetcher(tmp_path):
    """Like the conftest fetcher, but closes its client and cache afterwards."""
    f = Fetcher(cache=Cache(db_path=tmp_path / "accept404.db"), breaker=CircuitBreaker(), default_timeout=5.0)
    yield f
    await f.close()
    f.cache.close()


@pytest.fixture
def sleep(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    mock = AsyncMock()
    monkeypatch.setattr("world_intel_mcp.fetcher.asyncio.sleep", mock)
    return mock


@respx.mock
async def test_rejected_404_is_retried_and_counted_as_a_failure(fetcher: Fetcher, sleep: AsyncMock) -> None:
    fetcher.max_retries = 2
    route = respx.get(_URL).mock(
        return_value=httpx.Response(404, content=_HTML_404, headers={"content-type": "text/html"})
    )

    result = await fetcher.get_bytes(_URL, source="t", accept_404=_json_object)

    assert result.status == 404
    assert result.content is None
    assert result.error is not None and result.error.startswith("HTTPStatusError: Client error '404 Not Found'")
    assert route.call_count == 3
    assert sleep.await_count == 2
    state = fetcher.breaker.status()["t"]
    assert state["total_failures"] == 1 and state["total_successes"] == 0


@respx.mock
async def test_rejected_404_failures_open_the_circuit_like_any_error(fetcher: Fetcher, sleep: AsyncMock) -> None:
    fetcher.max_retries = 0
    route = respx.get(_URL).mock(return_value=httpx.Response(404, content=_HTML_404))
    for _ in range(fetcher.breaker.failure_threshold):
        await fetcher.get_bytes(_URL, source="t", accept_404=_json_object)

    assert fetcher.breaker.status()["t"]["status"] == "open"
    blocked = await fetcher.get_bytes(_URL, source="t", accept_404=_json_object)
    assert blocked == BinaryResult(None, None, "circuit open for t")
    assert route.call_count == fetcher.breaker.failure_threshold


@respx.mock
async def test_rejected_404_then_a_body_on_retry_is_a_normal_success(fetcher: Fetcher, sleep: AsyncMock) -> None:
    fetcher.max_retries = 2
    route = respx.get(_URL).mock(
        side_effect=[httpx.Response(404, content=_HTML_404), httpx.Response(200, content=b"body")]
    )

    result = await fetcher.get_bytes(_URL, source="t", accept_404=_json_object)

    assert result == BinaryResult(200, b"body", None)
    assert route.call_count == 2
    assert sleep.await_count == 1
    assert fetcher.breaker.status()["t"]["total_failures"] == 0


@respx.mock
async def test_accepted_404_stays_a_definitive_answer_and_the_predicate_sees_the_body(
    fetcher: Fetcher, sleep: AsyncMock
) -> None:
    fetcher.max_retries = 2
    seen: list[bytes] = []

    def accept(body: bytes) -> bool:
        seen.append(body)
        return True

    route = respx.get(_URL).mock(return_value=httpx.Response(404, content=_PROBLEM_404))

    result = await fetcher.get_bytes(_URL, source="t", accept_404=accept)

    assert result == BinaryResult(404, None, None)
    assert seen == [_PROBLEM_404]
    assert route.call_count == 1
    sleep.assert_not_awaited()
    state = fetcher.breaker.status()["t"]
    assert state["total_failures"] == 0 and state["total_successes"] == 1


@pytest.mark.parametrize("body", [
    pytest.param(_HTML_404, id="html"), pytest.param(_PROBLEM_404, id="problem-json"), pytest.param(b"", id="empty"),
])
@respx.mock
async def test_without_a_predicate_every_404_is_a_definitive_answer(
    fetcher: Fetcher, sleep: AsyncMock, body: bytes
) -> None:
    fetcher.max_retries = 2
    route = respx.get(_URL).mock(return_value=httpx.Response(404, content=body))

    result = await fetcher.get_bytes(_URL, source="t")

    assert result == BinaryResult(404, None, None)
    assert route.call_count == 1
    sleep.assert_not_awaited()
    state = fetcher.breaker.status()["t"]
    assert state["total_failures"] == 0 and state["total_successes"] == 1


@pytest.mark.parametrize("status", [200, 410, 500])
@respx.mock
async def test_the_predicate_is_consulted_only_for_404(fetcher: Fetcher, sleep: AsyncMock, status: int) -> None:
    fetcher.max_retries = 0
    calls: list[bytes] = []
    respx.get(_URL).mock(return_value=httpx.Response(status, content=b"x"))

    result = await fetcher.get_bytes(_URL, source="t", accept_404=lambda body: calls.append(body) or True)

    assert calls == []
    assert result.status == status
    assert result.ok is (status == 200)


@respx.mock
async def test_accept_404_is_keyword_only(fetcher: Fetcher, sleep: AsyncMock) -> None:
    """A parameter another change adds after max_bytes can never be bound
    to the predicate by position, nor the predicate to it."""
    route = respx.get(_URL).mock(return_value=httpx.Response(404, content=_HTML_404))
    param = inspect.signature(Fetcher.get_bytes).parameters["accept_404"]
    assert param.kind is inspect.Parameter.KEYWORD_ONLY and param.default is None

    with pytest.raises(TypeError):
        await fetcher.get_bytes(_URL, "t", None, None, 1024, _json_object)
    assert route.call_count == 0


@respx.mock
async def test_accepted_404_body_is_read_through_the_stream(fetcher: Fetcher, sleep: AsyncMock) -> None:
    """get_bytes streams its response, so the predicate gets the 404 body
    read from the stream, not resp.content (which raises on a stream)."""
    seen: list[bytes] = []

    def accept(body: bytes) -> bool:
        seen.append(body)
        return True

    respx.get(_URL).mock(return_value=httpx.Response(404, content=_PROBLEM_404))

    result = await fetcher.get_bytes(_URL, source="t", accept_404=accept)

    assert result == BinaryResult(404, None, None)
    assert seen == [_PROBLEM_404]


@respx.mock
async def test_404_body_over_the_cap_counts_as_rejected(fetcher: Fetcher, sleep: AsyncMock) -> None:
    """A 404 body is read for the predicate only up to max_bytes; a longer
    one is never handed to it and is handled as a rejected 404."""
    fetcher.max_retries = 0
    seen: list[bytes] = []

    def accept(body: bytes) -> bool:
        seen.append(body)
        return True

    respx.get(_URL).mock(return_value=httpx.Response(404, content=_PROBLEM_404))

    result = await fetcher.get_bytes(_URL, source="t", max_bytes=len(_PROBLEM_404) - 1, accept_404=accept)

    assert seen == []
    assert result.status == 404 and result.content is None
    assert result.error is not None and result.error.startswith("HTTPStatusError: Client error '404 Not Found'")
    assert fetcher.breaker.status()["t"]["total_failures"] == 1
