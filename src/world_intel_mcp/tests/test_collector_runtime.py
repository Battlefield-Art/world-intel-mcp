"""Runtime tests for collector.py — the run loop test_collector.py leaves out.

test_collector.py verifies the SOURCES map resolves; this file drives the
actual collection machinery: ``collect_once`` (success/failure/timeout/
error-dict accounting and vector-store writes), ``_resolve_source_filter``,
``run_once``/``run_daemon`` lifecycle, and the ``main()`` argparse entry
point. Fetch functions are faked at the ``_import_fetch_fn`` boundary and
infrastructure classes (Cache/CircuitBreaker/VectorStore/Fetcher) at the
collector module boundary, so nothing touches the network, Qdrant, or the
on-disk cache.
"""

import asyncio
import signal as signal_mod

import pytest

from world_intel_mcp import collector

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeVectorStore:
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.stored: list[tuple[str, object]] = []
        self.started = False
        self.stopped = False
        self._store_queue = None

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def store(self, name: str, data: object) -> None:
        self.stored.append((name, data))

    async def collection_stats(self) -> dict:
        return {"points_count": 100 + len(self.stored)}


class FakeFetcher:
    def __init__(self, *args, **kwargs):
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class FakeCache:
    def __init__(self, *args, **kwargs):
        self.evictions = 0

    def close(self) -> None:
        pass

    def evict_expired(self) -> int:
        self.evictions += 1
        return 3


def _importer_for(fakes_by_name: dict):
    """Return an _import_fetch_fn replacement resolving via SOURCES names."""
    name_by_ref = {(mod, fn): name for name, mod, fn, _ in collector.SOURCES}

    def _import(module_path: str, fn_name: str):
        name = name_by_ref[(module_path, fn_name)]
        return fakes_by_name[name]

    return _import


def _returning(payload):
    async def _fake(fetcher, **kwargs):
        return payload

    return _fake


# ---------------------------------------------------------------------------
# _resolve_source_filter
# ---------------------------------------------------------------------------


def test_resolve_filter_none_means_no_filter() -> None:
    assert collector._resolve_source_filter(None) is None
    assert collector._resolve_source_filter("") is None


def test_resolve_filter_expands_domain_group() -> None:
    resolved = collector._resolve_source_filter("markets")
    assert resolved == set(collector.DOMAIN_GROUPS["markets"])
    assert "btc_technicals" in resolved


def test_resolve_filter_mixes_groups_and_raw_names_and_strips_whitespace() -> None:
    resolved = collector._resolve_source_filter(" cyber , earthquakes ")
    assert resolved == {"cyber_threats", "earthquakes"}


def test_resolve_filter_passes_unknown_names_through() -> None:
    # An unrecognized name is kept verbatim (and will simply match zero
    # SOURCES entries at collection time) rather than raising.
    assert collector._resolve_source_filter("no_such_source") == {"no_such_source"}


def test_domain_groups_cover_sources_exactly() -> None:
    """Every group member must be a real source, and the groups together
    must cover all of SOURCES — otherwise a --sources domain filter would
    silently run a subset (or nothing)."""
    source_names = {name for name, _, _, _ in collector.SOURCES}
    grouped: set[str] = set()
    for group, members in collector.DOMAIN_GROUPS.items():
        unknown = set(members) - source_names
        assert unknown == set(), f"group {group!r} references unknown {unknown}"
        grouped.update(members)
    assert grouped == source_names


# ---------------------------------------------------------------------------
# collect_once
# ---------------------------------------------------------------------------


async def test_collect_once_all_success(monkeypatch: pytest.MonkeyPatch) -> None:
    quotes = {"quotes": [{"symbol": "TSTX", "price": 1.0}]}
    quakes = {"count": 3, "earthquakes": []}
    monkeypatch.setattr(
        collector,
        "_import_fetch_fn",
        _importer_for(
            {"market_quotes": _returning(quotes), "earthquakes": _returning(quakes)}
        ),
    )
    vs = FakeVectorStore()

    summary = await collector.collect_once(
        FakeFetcher(), vs, source_filter={"market_quotes", "earthquakes"}
    )

    assert summary["sources_attempted"] == 2
    assert summary["successes"] == 2
    assert summary["failures"] == 0
    assert summary["errors"] == []
    assert summary["vector_store_points"] == 102  # FakeVectorStore: 100 + 2 stored
    assert sorted(vs.stored) == [("earthquakes", quakes), ("market_quotes", quotes)]


async def test_collect_once_partial_failure_accounting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _raises(fetcher, **kwargs):
        raise ValueError("acled exploded")

    async def _returns_none(fetcher, **kwargs):
        return None

    async def _hangs(fetcher, **kwargs):
        await asyncio.sleep(5)

    ok_payload = {"count": 1}
    monkeypatch.setattr(
        collector,
        "_import_fetch_fn",
        _importer_for(
            {
                "earthquakes": _returning(ok_payload),
                "acled_events": _raises,
                "wildfires": _returns_none,
                "nav_warnings": _hangs,
            }
        ),
    )
    vs = FakeVectorStore()

    summary = await collector.collect_once(
        FakeFetcher(),
        vs,
        source_filter={"earthquakes", "acled_events", "wildfires", "nav_warnings"},
        timeout=0.05,
    )

    assert summary["sources_attempted"] == 4
    assert summary["successes"] == 1
    assert summary["failures"] == 3
    # A None return counts as a failure but records NO error message —
    # only the exception and the timeout leave a trace.
    assert len(summary["errors"]) == 2
    assert "acled_events: acled exploded" in summary["errors"]
    assert "nav_warnings: timeout (0.05s)" in summary["errors"]
    # Only the successful source reached the vector store.
    assert vs.stored == [("earthquakes", ok_payload)]


async def test_collect_once_error_dict_is_success_but_not_stored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fetch returning {"error": ...} counts as a *success* in the cycle
    summary (the fetch itself completed) but is excluded from vector-store
    writes by the ``data.get("error")`` guard. Pinned because both halves
    are load-bearing: flipping the first would trip daemon alerting on
    routine upstream errors, flipping the second would pollute semantic
    search with error stubs."""
    monkeypatch.setattr(
        collector,
        "_import_fetch_fn",
        _importer_for({"acled_events": _returning({"error": "ACLED token missing"})}),
    )
    vs = FakeVectorStore()

    summary = await collector.collect_once(
        FakeFetcher(), vs, source_filter={"acled_events"}
    )

    assert summary["successes"] == 1
    assert summary["failures"] == 0
    assert summary["errors"] == []
    assert vs.stored == []


async def test_collect_once_unfiltered_attempts_every_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempted: list[str] = []

    def _import(module_path: str, fn_name: str):
        async def _fake(fetcher, **kwargs):
            attempted.append(fn_name)
            return {"ok": True}

        return _fake

    monkeypatch.setattr(collector, "_import_fetch_fn", _import)
    vs = FakeVectorStore()

    summary = await collector.collect_once(FakeFetcher(), vs, source_filter=None)

    assert summary["sources_attempted"] == len(collector.SOURCES)
    assert summary["successes"] == len(collector.SOURCES)
    assert len(attempted) == len(collector.SOURCES)


# ---------------------------------------------------------------------------
# run_once / run_daemon lifecycle
# ---------------------------------------------------------------------------


@pytest.fixture
def _fake_infra(monkeypatch: pytest.MonkeyPatch) -> dict:
    """Swap collector's infrastructure classes for fakes; return handles."""
    vs = FakeVectorStore()
    cache = FakeCache()
    fetchers: list[FakeFetcher] = []

    def _make_fetcher(*args, **kwargs):
        f = FakeFetcher()
        fetchers.append(f)
        return f

    monkeypatch.setattr(collector, "VectorStore", lambda enabled=True: vs)
    monkeypatch.setattr(collector, "Cache", lambda *a, **kw: cache)
    monkeypatch.setattr(collector, "CircuitBreaker", lambda *a, **kw: object())
    monkeypatch.setattr(collector, "Fetcher", _make_fetcher)
    return {"vs": vs, "cache": cache, "fetchers": fetchers}


async def test_run_once_returns_summary_and_closes_resources(
    monkeypatch: pytest.MonkeyPatch, _fake_infra: dict
) -> None:
    seen: dict = {}

    async def fake_collect_once(
        fetcher, vector_store, source_filter=None, timeout=45.0
    ):
        seen["filter"] = source_filter
        return {"successes": 5, "failures": 0}

    monkeypatch.setattr(collector, "collect_once", fake_collect_once)
    vs = _fake_infra["vs"]
    vs._store_queue = asyncio.Queue()  # empty queue: join() returns immediately

    summary = await collector.run_once(source_filter="markets")

    assert summary == {"successes": 5, "failures": 0}
    assert seen["filter"] == set(collector.DOMAIN_GROUPS["markets"])
    assert vs.started and vs.stopped
    assert _fake_infra["fetchers"][0].closed


async def test_run_daemon_stops_on_signal_after_first_cycle(
    monkeypatch: pytest.MonkeyPatch, _fake_infra: dict
) -> None:
    handlers: dict = {}
    monkeypatch.setattr(
        collector.signal, "signal", lambda sig, h: handlers.setdefault(sig, h)
    )

    cycles: list[set | None] = []

    async def fake_collect_once(
        fetcher, vector_store, source_filter=None, timeout=45.0
    ):
        cycles.append(source_filter)
        # Simulate SIGTERM arriving during the first cycle.
        handlers[signal_mod.SIGTERM](signal_mod.SIGTERM, None)
        return {"successes": 1}

    monkeypatch.setattr(collector, "collect_once", fake_collect_once)

    await collector.run_daemon(interval=300, source_filter="cyber")

    assert set(handlers) == {signal_mod.SIGINT, signal_mod.SIGTERM}
    assert cycles == [{"cyber_threats"}]
    vs = _fake_infra["vs"]
    assert vs.started and vs.stopped
    assert _fake_infra["fetchers"][0].closed


async def test_run_daemon_survives_cycle_exception_and_evicts_cache(
    monkeypatch: pytest.MonkeyPatch, _fake_infra: dict
) -> None:
    """Cycle 1 raises (daemon must log and continue, not die); the loop then
    runs to cycle 12, where the hourly cache eviction fires, and stops."""
    handlers: dict = {}
    monkeypatch.setattr(
        collector.signal, "signal", lambda sig, h: handlers.setdefault(sig, h)
    )

    cycle_count = 0

    async def fake_collect_once(
        fetcher, vector_store, source_filter=None, timeout=45.0
    ):
        nonlocal cycle_count
        cycle_count += 1
        if cycle_count == 1:
            raise RuntimeError("transient blowup")
        if cycle_count == 12:
            handlers[signal_mod.SIGINT](signal_mod.SIGINT, None)
        return {"successes": 1}

    monkeypatch.setattr(collector, "collect_once", fake_collect_once)

    # interval=0 -> the inter-cycle wait times out immediately, so 12
    # cycles complete without real sleeping.
    await collector.run_daemon(interval=0)

    assert cycle_count == 12
    assert _fake_infra["cache"].evictions == 1  # fired at cycle 12 only
    assert _fake_infra["vs"].stopped


# ---------------------------------------------------------------------------
# main() argparse entry point
# ---------------------------------------------------------------------------


def test_main_single_run_success_exits_zero(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    async def fake_run_once(source_filter=None):
        return {
            "successes": 3,
            "sources_attempted": 3,
            "cycle_time_s": 1.2,
            "vector_store_points": 42,
            "errors": [],
            "failures": 0,
        }

    monkeypatch.setattr(collector, "run_once", fake_run_once)
    monkeypatch.setattr(collector.sys, "argv", ["intel-collector"])

    with pytest.raises(SystemExit) as excinfo:
        collector.main()

    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    assert "3/3 succeeded" in out
    assert "42 points" in out
    assert "Errors" not in out


def test_main_single_run_failure_exits_one_and_lists_errors(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    async def fake_run_once(source_filter=None):
        return {
            "successes": 1,
            "sources_attempted": 3,
            "cycle_time_s": 2.0,
            "vector_store_points": 10,
            "errors": ["acled_events: boom", "nav_warnings: timeout (45.0s)"],
            "failures": 2,
        }

    monkeypatch.setattr(collector, "run_once", fake_run_once)
    monkeypatch.setattr(
        collector.sys, "argv", ["intel-collector", "--sources", "conflict"]
    )

    with pytest.raises(SystemExit) as excinfo:
        collector.main()

    assert excinfo.value.code == 1
    out = capsys.readouterr().out
    assert "1/3 succeeded" in out
    assert "Errors (2):" in out
    assert "acled_events: boom" in out


def test_main_daemon_flag_forwards_interval_and_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict = {}

    async def fake_run_daemon(interval=300, source_filter=None):
        seen["interval"] = interval
        seen["source_filter"] = source_filter

    monkeypatch.setattr(collector, "run_daemon", fake_run_daemon)
    monkeypatch.setattr(
        collector.sys,
        "argv",
        ["intel-collector", "--daemon", "--interval", "60", "--sources", "markets"],
    )

    collector.main()  # daemon branch returns without SystemExit

    assert seen == {"interval": 60, "source_filter": "markets"}


async def test_collect_once_honors_per_source_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A SOURCE_TIMEOUTS override must actually reach asyncio.wait_for —
    a slow source with a short override times out even though the
    default budget would have let it finish."""
    import asyncio as _asyncio

    async def _slow(fetcher, **kwargs):
        await _asyncio.sleep(0.3)
        return {"ok": True}

    monkeypatch.setattr(
        collector, "SOURCES", [("slowpoke", "sources.markets", "fetch_market_quotes", {})]
    )
    monkeypatch.setattr(collector, "SOURCE_TIMEOUTS", {"slowpoke": 0.05})
    monkeypatch.setattr(collector, "_import_fetch_fn", lambda m, f: _slow)

    store = FakeVectorStore()
    summary = await collector.collect_once(FakeFetcher(), store, timeout=45.0)
    assert summary["failures"] == 1
    assert any("timeout (0.05s)" in e for e in summary["errors"])
