"""The shared runtime is built once, on first access, as one consistent set.

Each test starts from an unbound runtime (as a fresh process sees it) with
the cache pointed at a temp file, and puts the session's objects back
afterwards. The thread test swaps in recording stand-ins for the cache
and AOI store: SQLite connections belong to the thread that opened them,
and what is under test is the build protocol, not SQLite.
"""

import contextlib
import socket
import sqlite3
import threading
import time
from pathlib import Path

import pytest

from world_intel_mcp import runtime
from world_intel_mcp import vector_store as vector_store_mod
from world_intel_mcp.analysis import aoi
from world_intel_mcp.analysis.aoi import AOIStore
from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher

# The names tools and the server read from runtime, spelled out here rather
# than taken from runtime so the tests pin the contract instead of
# mirroring it.
_SHARED_NAMES = (
    "cache", "breaker", "aoi_store", "_aoi_store",
    "vector_store", "_vector_store", "fetcher",
)


@pytest.fixture
def unbound_runtime(monkeypatch, tmp_path: Path):
    """Nothing bound and the cache on a temp file; yields that path."""
    cache_db = tmp_path / "cache.db"
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(cache_db))
    namespace = vars(runtime)
    saved = {name: namespace.pop(name) for name in _SHARED_NAMES if name in namespace}
    try:
        yield cache_db
    finally:
        built = {name: namespace.pop(name, None) for name in _SHARED_NAMES}
        namespace.update(saved)
        for name in ("aoi_store", "cache"):
            if isinstance(built[name], (AOIStore, Cache)):
                built[name].close()


@pytest.fixture
def fake_vector_store(monkeypatch):
    """Vector dependencies reported present; the store records its lifecycle."""
    events: list[str] = []

    class FakeVectorStore:
        def __init__(self, enabled: bool = True):
            events.append("vector store built")

        async def start(self) -> None:
            events.append("vector store started")

        async def stop(self) -> None:
            events.append("vector store stopped")

    monkeypatch.setattr(vector_store_mod, "vector_dependencies_available", lambda: True)
    monkeypatch.setattr(vector_store_mod, "VectorStore", FakeVectorStore)
    return events


@pytest.fixture
def no_network(monkeypatch):
    """Refuse every outbound connection and name lookup; yields the attempts."""
    attempts: list[tuple] = []

    def refuse(*args, **kwargs):
        attempts.append(args)
        raise OSError("network access is blocked in this test")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket.socket, "connect_ex", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(socket, "getaddrinfo", refuse)
    return attempts


def test_first_access_publishes_the_whole_set_wired_together(unbound_runtime) -> None:
    fetcher = runtime.fetcher

    namespace = vars(runtime)
    assert [name for name in _SHARED_NAMES if name not in namespace] == []
    assert isinstance(fetcher, Fetcher)
    assert namespace["fetcher"] is fetcher
    assert runtime.fetcher is fetcher  # bound once; later reads do not rebuild
    assert fetcher.cache is runtime.cache
    assert fetcher.breaker is runtime.breaker
    assert fetcher.vector_store is runtime.vector_store
    assert runtime._aoi_store is runtime.aoi_store
    assert runtime._vector_store is runtime.vector_store
    assert runtime.cache.db_path == unbound_runtime
    assert runtime.aoi_store.db_path == unbound_runtime


def test_concurrent_first_access_builds_one_consistent_set(
    unbound_runtime, monkeypatch,
) -> None:
    caches, stores, breakers, fetchers = [], [], [], []

    class SlowCache:
        def __init__(self):
            caches.append(self)
            # Long enough for every thread to reach runtime before this
            # returns, so an unguarded first access builds once per thread.
            time.sleep(0.3)
            self.db_path = unbound_runtime

        def close(self) -> None:
            pass

    class RecordingAOIStore:
        def __init__(self, db_path):
            stores.append(self)
            self.db_path = db_path

        def close(self) -> None:
            pass

    class RecordingBreaker(CircuitBreaker):
        def __init__(self, *args, **kwargs):
            breakers.append(self)
            super().__init__(*args, **kwargs)

    class RecordingFetcher(Fetcher):
        def __init__(self, *args, **kwargs):
            fetchers.append(self)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(runtime, "Cache", SlowCache)
    monkeypatch.setattr(runtime, "CircuitBreaker", RecordingBreaker)
    monkeypatch.setattr(runtime, "Fetcher", RecordingFetcher)
    monkeypatch.setattr(aoi, "AOIStore", RecordingAOIStore)
    monkeypatch.setattr(vector_store_mod, "vector_dependencies_available", lambda: False)

    names = ("fetcher", "breaker", "cache", "aoi_store", "vector_store", "fetcher")
    barrier = threading.Barrier(len(names))
    seen: list[object] = [None] * len(names)
    errors: list[BaseException] = []

    def first_access(index: int, name: str) -> None:
        try:
            barrier.wait(timeout=10)
            seen[index] = getattr(runtime, name)
        except BaseException as exc:  # re-raised by the assertion below
            errors.append(exc)

    threads = [
        threading.Thread(target=first_access, args=(index, name))
        for index, name in enumerate(names)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert [t.name for t in threads if t.is_alive()] == [], "a first access never returned"
    assert errors == []
    assert (len(caches), len(stores), len(breakers), len(fetchers)) == (1, 1, 1, 1)
    fetcher, breaker, cache, aoi_store, vector_store, fetcher_again = seen
    assert fetcher_again is fetcher
    assert fetcher is runtime.fetcher
    assert breaker is runtime.breaker
    assert cache is runtime.cache
    assert aoi_store is runtime.aoi_store
    assert fetcher.cache is cache
    assert fetcher.breaker is breaker
    assert aoi_store.db_path == cache.db_path
    assert vector_store is None and runtime.vector_store is None


def test_failed_first_access_binds_nothing_and_closes_what_it_opened(
    unbound_runtime, monkeypatch,
) -> None:
    caches: list[Cache] = []
    store_attempts: list[Path] = []

    class RecordingCache(Cache):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            caches.append(self)

    class FlakyAOIStore(AOIStore):
        def __init__(self, db_path):
            store_attempts.append(db_path)
            if len(store_attempts) == 1:
                raise sqlite3.OperationalError("database is locked")
            super().__init__(db_path)

    monkeypatch.setattr(runtime, "Cache", RecordingCache)
    monkeypatch.setattr(aoi, "AOIStore", FlakyAOIStore)

    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        runtime.fetcher
    assert [name for name in _SHARED_NAMES if name in vars(runtime)] == []
    assert len(caches) == 1
    assert caches[0]._conn is None  # the cache that build opened was closed

    fetcher = runtime.fetcher  # the next access starts over
    assert len(caches) == 2
    assert fetcher.cache is runtime.cache is caches[1]
    assert fetcher.breaker is runtime.breaker
    assert runtime.aoi_store.db_path == runtime.cache.db_path == unbound_runtime


def test_server_aliases_forward_to_the_shared_runtime(
    unbound_runtime, fake_vector_store,
) -> None:
    from world_intel_mcp import server

    assert server.cache is runtime.cache
    assert server.breaker is runtime.breaker
    assert server.fetcher is runtime.fetcher
    assert server._aoi_store is runtime.aoi_store
    assert server._vector_store is runtime.vector_store
    assert runtime.vector_store is not None
    assert fake_vector_store == ["vector store built"]


async def test_run_starts_the_vector_store_before_serving_and_stops_it_after(
    unbound_runtime, fake_vector_store, no_network, monkeypatch,
) -> None:
    from world_intel_mcp import server

    events = fake_vector_store

    @contextlib.asynccontextmanager
    async def fake_stdio_server():
        events.append("stdio opened")
        yield object(), object()
        events.append("stdio closed")

    async def fake_serve(read_stream, write_stream, initialization_options):
        bound = all(name in vars(runtime) for name in _SHARED_NAMES)
        events.append(f"serving, runtime bound: {bound}")

    monkeypatch.setattr(server, "stdio_server", fake_stdio_server)
    monkeypatch.setattr(server.server, "run", fake_serve)

    await server._run()

    assert events == [
        "vector store built",
        "vector store started",
        "stdio opened",
        "serving, runtime bound: True",
        "stdio closed",
        "vector store stopped",
    ]
    assert no_network == []
