"""Resource owners close connections even while callers retain references."""

import sqlite3

import pytest

from world_intel_mcp import collector, reports
from world_intel_mcp.analysis import alerts, temporal
from world_intel_mcp.sources import intelligence
from world_intel_mcp.tests.test_collector_runtime import FakeVectorStore


@pytest.mark.parametrize("fail", [False, True])
async def test_standalone_report_closes_resources(tmp_path, monkeypatch, fail):
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "report.db"))
    opened = []

    async def generate(fetcher, *args):
        opened.append((fetcher.cache._conn, await fetcher._get_client()))
        if fail:
            raise RuntimeError("render failed")
        return {"path": "report.html"}

    monkeypatch.setattr(reports, "generate_report", generate)
    try:
        if fail:
            with pytest.raises(RuntimeError, match="render failed"):
                await reports.generate_report_standalone()
        else:
            assert await reports.generate_report_standalone() == {"path": "report.html"}
        conn, client = opened[0]
        assert client.is_closed
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            conn.execute("SELECT 1")
    finally:
        for conn, client in opened:
            conn.close()
            await client.aclose()


@pytest.mark.parametrize("fail", [False, True])
async def test_collector_closes_cache(tmp_path, monkeypatch, fail):
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "collector.db"))
    monkeypatch.setattr(collector, "VectorStore", FakeVectorStore)
    opened = []

    async def collect(fetcher, *args):
        opened.append((fetcher.cache._conn, await fetcher._get_client()))
        if fail:
            raise RuntimeError("collection failed")
        return {"collected": 0}

    monkeypatch.setattr(collector, "collect_once", collect)
    try:
        if fail:
            with pytest.raises(RuntimeError, match="collection failed"):
                await collector.run_once()
        else:
            assert await collector.run_once() == {"collected": 0}
        conn, client = opened[0]
        assert client.is_closed
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            conn.execute("SELECT 1")
    finally:
        for conn, client in opened:
            conn.close()
            await client.aclose()


async def test_weekly_trends_closes_database_on_query_error(tmp_path, monkeypatch):
    path = tmp_path / "no-baselines.db"
    monkeypatch.setattr(temporal, "_default_db_path", lambda: path)
    opened = []
    connect = sqlite3.connect

    def tracked_connect(*args, **kwargs):
        conn = connect(*args, **kwargs)
        opened.append(conn)
        return conn

    async def anomalies(fetcher):
        return {}

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)
    monkeypatch.setattr(intelligence, "fetch_temporal_anomalies", anomalies)
    try:
        await alerts.fetch_weekly_trends(None)
        assert len(opened) == 1
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            opened[0].execute("SELECT 1")
    finally:
        for conn in opened:
            conn.close()


@pytest.mark.parametrize("run", [collector.run_once, collector.run_daemon])
async def test_collector_closes_cache_when_start_fails(tmp_path, monkeypatch, run):
    from world_intel_mcp.cache import Cache

    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "start-failure.db"))
    opened = []

    def cache():
        instance = Cache()
        opened.append(instance._conn)
        return instance

    class FailedStart(FakeVectorStore):
        async def start(self):
            raise RuntimeError("start failed")

    monkeypatch.setattr(collector, "Cache", cache)
    monkeypatch.setattr(collector, "VectorStore", FailedStart)
    try:
        with pytest.raises(RuntimeError, match="start failed"):
            await run()
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            opened[0].execute("SELECT 1")
    finally:
        for conn in opened:
            conn.close()
