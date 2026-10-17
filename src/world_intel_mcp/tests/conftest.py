"""Test configuration — strips proxy env vars so httpx doesn't try SOCKS."""

import asyncio
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest

from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher

_PROXY_VARS = [
    "ALL_PROXY",
    "all_proxy",
    "HTTP_PROXY",
    "http_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "FTP_PROXY",
    "ftp_proxy",
    "GRPC_PROXY",
    "grpc_proxy",
]


@pytest.fixture(autouse=True)
def _strip_proxy_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove system proxy env vars so httpx creates clean connections."""
    for var in _PROXY_VARS:
        monkeypatch.delenv(var, raising=False)


@pytest.fixture(autouse=True)
def _strip_aoi_webhook_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """A developer's real WORLD_INTEL_AOI_WEBHOOK must never receive
    POSTs from tests that run the real sweep path."""
    monkeypatch.delenv("WORLD_INTEL_AOI_WEBHOOK", raising=False)
    monkeypatch.delenv("WORLD_INTEL_AOI_WEBHOOK_FORMAT", raising=False)


@pytest.fixture(autouse=True)
def _reset_fetcher_locks() -> None:
    """Reset global asyncio locks between tests to avoid cross-loop binding."""
    import world_intel_mcp.fetcher as fetcher_mod

    fetcher_mod._yahoo_lock = asyncio.Lock()
    fetcher_mod._yahoo_last_call = 0.0
    fetcher_mod._source_locks.clear()
    fetcher_mod._source_last_call.clear()


@pytest.fixture
def cache(tmp_path: Path) -> Iterator[Cache]:
    cache = Cache(db_path=tmp_path / "test_cache.db")
    try:
        yield cache
    finally:
        cache.close()


@pytest.fixture
async def fetcher(cache: Cache) -> AsyncIterator[Fetcher]:
    breaker = CircuitBreaker()
    fetcher = Fetcher(cache=cache, breaker=breaker, default_timeout=5.0)
    try:
        yield fetcher
    finally:
        await fetcher.close()


@pytest.fixture(scope="module")
async def isolated_runtime():
    """Restore shared infrastructure after a module rebinds it."""
    from world_intel_mcp import runtime

    names = ("cache", "breaker", "fetcher", "aoi_store", "_aoi_store",
             "vector_store", "_vector_store")
    prior = {name: runtime.__dict__[name] for name in names if name in runtime.__dict__}
    try:
        yield runtime
    finally:
        if runtime.__dict__.get("fetcher") is not prior.get("fetcher"):
            await runtime.fetcher.close()
            runtime.aoi_store.close()
            runtime.cache.close()
        for name in names:
            runtime.__dict__.pop(name, None)
        runtime.__dict__.update(prior)


@pytest.fixture(scope="session", autouse=True)
async def _close_shared_infrastructure():
    """Close process-wide resources lazily opened by the tests."""
    import sys

    yield
    runtime = sys.modules.get("world_intel_mcp.runtime")
    if runtime is not None and "cache" in runtime.__dict__:
        await runtime.fetcher.close()
        runtime.aoi_store.close()
        runtime.cache.close()
    for module_name, name in (
        ("world_intel_mcp.sources.intelligence", "_temporal"),
        ("world_intel_mcp.analysis.spikes", "_detector"),
    ):
        module = sys.modules.get(module_name)
        instance = getattr(module, name, None)
        if instance is not None and instance._conn is not None:
            instance._conn.close()


@pytest.fixture
def temporal_baseline(tmp_path):
    from world_intel_mcp.analysis.temporal import TemporalBaseline

    baseline = TemporalBaseline(db_path=str(tmp_path / "t.db"))
    try:
        yield baseline
    finally:
        if baseline._conn is not None:
            baseline._conn.close()


@pytest.fixture(autouse=True)
async def _close_cli_resources(monkeypatch):
    """Close test-owned CLI singletons before monkeypatch restores them."""
    from world_intel_mcp import cli

    yield
    if cli._aoi_store is not None:
        cli._aoi_store.close()
        cli._aoi_store = None
    if cli._fetcher is not None:
        try:
            await cli._fetcher.close()
        finally:
            cli._fetcher.cache.close()
            cli._fetcher = None
