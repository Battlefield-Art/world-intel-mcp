"""Shared process-wide infrastructure for the MCP server.

Extracted from server.py in the Phase 26 modularization so that the
domain tool modules under ``tools/`` and the server shell can share one
cache, one circuit breaker, one AOI store, and one fetcher without a
circular import. Infrastructure is initialized on first access so importing
modules does not open databases.
"""

import logging

from .analysis import aoi
from .cache import Cache
from .circuit_breaker import CircuitBreaker
from .fetcher import Fetcher

logger = logging.getLogger("world-intel-mcp")


def __getattr__(name: str):
    if name not in {"cache", "breaker", "aoi_store", "_aoi_store",
                    "vector_store", "_vector_store", "fetcher"}:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    _initialize()
    return globals()[name]


def _initialize() -> None:
    global cache, breaker, aoi_store, _aoi_store, vector_store, _vector_store, fetcher

    cache = Cache()
    breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=300)
    # Same physical SQLite file the Cache above resolved to (fallback path
    # included); see analysis/aoi.py's module docstring for why AOIs live in
    # a dedicated table there rather than as Cache entries.
    _aoi_store = aoi.AOIStore(cache.db_path)
    aoi_store = _aoi_store

    # Vector store — optional, degrades gracefully if Qdrant unavailable.
    _vector_store = None
    try:
        from .vector_store import VectorStore, vector_dependencies_available

        if vector_dependencies_available():
            _vector_store = VectorStore(enabled=True)
        else:
            logger.info(
                "Vector store unavailable (qdrant_client / fastembed not installed)"
            )
    except Exception as exc:
        logger.info("Vector store unavailable: %s", exc)

    vector_store = _vector_store

    fetcher = Fetcher(cache=cache, breaker=breaker, vector_store=_vector_store)
