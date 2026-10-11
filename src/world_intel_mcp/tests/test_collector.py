"""Tests for collector.py's source map.

The collector daemon dynamically imports every fetch function it feeds
the vector store from string references in ``SOURCES``. Nothing else
executes those references: a renamed or deleted fetch function would
break the 24/7 collector at runtime while the whole test suite stayed
green. This file makes that class of drift a test failure instead.
"""

import inspect

from world_intel_mcp import collector


def test_sources_count_matches_docs() -> None:
    # CLAUDE.md says 53; measured here.
    assert len(collector.SOURCES) == 53


def test_source_names_unique() -> None:
    names = [name for name, _, _, _ in collector.SOURCES]
    assert len(names) == len(set(names))


def test_every_source_reference_resolves_to_an_async_fetcher() -> None:
    """The load-bearing check: every (module, function) string pair in
    SOURCES imports and is an async callable taking ``fetcher`` first."""
    for name, module_path, fn_name, kwargs in collector.SOURCES:
        fn = collector._import_fetch_fn(module_path, fn_name)
        assert callable(fn), f"{name}: {module_path}.{fn_name} not callable"
        assert inspect.iscoroutinefunction(fn), (
            f"{name}: {module_path}.{fn_name} is not async"
        )
        params = list(inspect.signature(fn).parameters)
        assert params and params[0] == "fetcher", (
            f"{name}: {module_path}.{fn_name} does not take fetcher first"
        )
        assert isinstance(kwargs, dict), f"{name}: kwargs must be a dict"


def test_every_source_kwarg_is_accepted_by_its_function() -> None:
    """A typo'd kwarg in SOURCES would raise TypeError at collection
    time in production; catch it here instead."""
    for name, module_path, fn_name, kwargs in collector.SOURCES:
        fn = collector._import_fetch_fn(module_path, fn_name)
        sig = inspect.signature(fn)
        accepts_var_kw = any(
            p.kind is inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
        )
        if accepts_var_kw:
            continue
        for kw in kwargs:
            assert kw in sig.parameters, (
                f"{name}: {module_path}.{fn_name} does not accept kwarg {kw!r}"
            )


def test_aoi_sweep_is_a_collector_source() -> None:
    """Phase 27: the scheduled AOI sweep rides the collector daemon.
    ``aoi_digest`` must reference the fetcher-only sweep wrapper (the
    digest itself needs a store argument the collector can't supply)
    and be reachable via the ``aoi`` domain group."""
    entries = {name: (mod, fn) for name, mod, fn, _ in collector.SOURCES}
    assert entries["aoi_digest"] == ("analysis.aoi", "fetch_aoi_sweep")
    assert collector.DOMAIN_GROUPS["aoi"] == ["aoi_digest"]


def test_aoi_sweep_has_extended_timeout() -> None:
    """Measured 2026-09-01: a single 50 km AOI sweep on a cold cache
    exceeded the flat 45 s per-source budget (the digest fans out to
    ~8 domains per AOI, several rate-floored). The override map must
    give aoi_digest a real budget."""
    assert collector.SOURCE_TIMEOUTS["aoi_digest"] >= 120.0
