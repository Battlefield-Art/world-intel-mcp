"""Failed keyed fetches never put the key in a log line or a returned error.

httpx's HTTPStatusError text carries the full request URL, query string
included, so a 401 from a keyed source logged its api_key at WARNING.
NASA FIRMS carries its key in the URL path, which only the environment
value match catches.

Gaps / not covered: errors a source module formats itself without going
through the fetcher; keys shorter than 8 characters held in the
environment (only their query-parameter form is masked).
"""

import logging

import httpx
import pytest

from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher, redact

QUERY_KEY = "-".join(["query", "param", "value"])
PATH_KEY = "-".join(["firms", "path", "value"])


def _fetcher(tmp_path, status: int) -> Fetcher:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text="denied", request=request)

    fetcher = Fetcher(cache=Cache(db_path=tmp_path / "cache.db"), breaker=CircuitBreaker())
    fetcher.max_retries = 0
    fetcher._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return fetcher


def _logged(caplog) -> str:
    return "\n".join(r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("status", [401, 500])
async def test_json_failure_log_masks_query_key(tmp_path, caplog, status) -> None:
    fetcher = _fetcher(tmp_path, status)
    caplog.set_level(logging.DEBUG, logger="world-intel-mcp.fetcher")
    try:
        result = await fetcher.get_json(
            "https://api.example.test/series",
            source="fred",
            params={"series_id": "WALCL", "api_key": QUERY_KEY},
        )
    finally:
        await fetcher._client.aclose()
    assert result is None
    text = _logged(caplog)
    assert "Fetch failed for fred" in text
    assert QUERY_KEY not in text
    assert "api_key=***" in text


async def test_text_failure_log_masks_query_key(tmp_path, caplog) -> None:
    fetcher = _fetcher(tmp_path, 403)
    caplog.set_level(logging.DEBUG, logger="world-intel-mcp.fetcher")
    try:
        await fetcher.get_text(
            "https://api.example.test/flow", source="tomtom", params={"key": QUERY_KEY}
        )
    finally:
        await fetcher._client.aclose()
    text = _logged(caplog)
    assert "Text fetch failed for tomtom" in text
    assert QUERY_KEY not in text


async def test_path_key_from_environment_is_masked(tmp_path, caplog, monkeypatch) -> None:
    monkeypatch.setenv("NASA_FIRMS_API_KEY", PATH_KEY)
    fetcher = _fetcher(tmp_path, 401)
    caplog.set_level(logging.DEBUG, logger="world-intel-mcp.fetcher")
    url = f"https://firms.example.test/api/area/csv/{PATH_KEY}/VIIRS_SNPP_NRT/world/2"
    try:
        await fetcher.get_text(url, source="nasa-firms")
        binary = await fetcher.get_bytes(url, source="nasa-firms")
    finally:
        await fetcher._client.aclose()
    assert binary.error is not None
    assert PATH_KEY not in binary.error
    text = _logged(caplog)
    assert "Binary fetch failed for nasa-firms" in text
    assert PATH_KEY not in text


async def test_stale_serve_log_masks_key_in_cache_key(tmp_path, caplog) -> None:
    fetcher = _fetcher(tmp_path, 500)
    key = f"fred:https://api.example.test/series:{{'api_key': '{QUERY_KEY}'}}"
    fetcher.cache.set(key, {"v": 1}, -1)
    caplog.set_level(logging.DEBUG, logger="world-intel-mcp.fetcher")
    try:
        served = await fetcher.get_json(
            "https://api.example.test/series", source="fred", params={"api_key": QUERY_KEY}
        )
    finally:
        await fetcher._client.aclose()
    assert served is not None and served["_stale"] is True
    text = _logged(caplog)
    assert "Serving stale cache for fred" in text
    assert QUERY_KEY not in text


def test_redact_leaves_ordinary_text_alone() -> None:
    plain = "HTTPStatusError: 404 for url 'https://example.test/a?series_id=WALCL&limit=5'"
    assert redact(plain) == plain
    names = ("token", "apikey")
    query = "x?" + "&".join(f"{name}={QUERY_KEY[:5]}" for name in names)
    assert redact(query) == "x?" + "&".join(f"{name}=***" for name in names)
