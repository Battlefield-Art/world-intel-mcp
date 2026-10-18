"""Shared process-wide infrastructure for the MCP server.

Extracted from server.py in the Phase 26 modularization so that the
domain tool modules under ``tools/`` and the server shell can share one
cache, one circuit breaker, one AOI store, and one fetcher without a
circular import.

Nothing is built at import. The first access to any shared name builds
the whole set under a module lock and publishes it in one step, so
concurrent first accesses all get the same objects, and a build that
fails binds nothing and closes whatever it had opened. The SQLite
connections behind the cache and the AOI store belong to the thread
that made that first access; the MCP server makes it on its event-loop
thread in ``server._run()`` before serving.
"""

import logging
import threading

from .analysis import aoi
from .cache import Cache
from .circuit_breaker import CircuitBreaker
from .fetcher import Fetcher

logger = logging.getLogger("world-intel-mcp")

_SHARED = frozenset({
    "cache", "breaker", "aoi_store", "_aoi_store",
    "vector_store", "_vector_store", "fetcher",
})
_init_lock = threading.Lock()


def __getattr__(name: str):
    if name not in _SHARED:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    with _init_lock:
        # Another thread may have published the set while this one waited.
        if not _SHARED.issubset(globals()):
            globals().update(_build())
    return globals()[name]


def _initialize() -> None:
    """Build a new set and publish it in place of any set already bound."""
    with _init_lock:
        globals().update(_build())


def _build() -> dict:
    """Construct every shared object as a local; bind nothing.

    If a step fails, close what the earlier steps opened and re-raise, so
    the module keeps whatever it had and the next access starts over.
    """
    opened = []
    try:
        cache = Cache()
        opened.append(cache)
        breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=300)
        # Same physical SQLite file the Cache above resolved to (fallback path
        # included); see analysis/aoi.py's module docstring for why AOIs live in
        # a dedicated table there rather than as Cache entries.
        aoi_store = aoi.AOIStore(cache.db_path)
        opened.append(aoi_store)
        vector_store = _optional_vector_store()
        fetcher = Fetcher(cache=cache, breaker=breaker, vector_store=vector_store)
    except BaseException:
        for resource in reversed(opened):
            try:
                resource.close()
            except Exception:
                logger.warning(
                    "Could not close %r after a failed runtime build",
                    resource,
                    exc_info=True,
                )
        raise
    return {
        "cache": cache,
        "breaker": breaker,
        "aoi_store": aoi_store,
        "_aoi_store": aoi_store,
        "vector_store": vector_store,
        "_vector_store": vector_store,
        "fetcher": fetcher,
    }


def _optional_vector_store():
    # Vector store — optional, degrades gracefully if Qdrant unavailable.
    try:
        from .vector_store import VectorStore, vector_dependencies_available

        if vector_dependencies_available():
            return VectorStore(enabled=True)
        logger.info(
            "Vector store unavailable (qdrant_client / fastembed not installed)"
        )
    except Exception as exc:
        logger.info("Vector store unavailable: %s", exc)
    return None
