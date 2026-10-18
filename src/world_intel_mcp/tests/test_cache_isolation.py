"""Database imports and first-use paths must respect cache isolation."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from world_intel_mcp.analysis import spikes, temporal
from world_intel_mcp.cache import Cache


def test_all_module_imports_open_no_databases(tmp_path: Path) -> None:
    home = tmp_path / "home"
    xdg = tmp_path / "xdg"
    home.mkdir()
    xdg.mkdir()
    env = dict(os.environ, HOME=str(home), XDG_CACHE_HOME=str(xdg),
               WORLD_INTEL_CACHE_DB=str(xdg / "cache.db"))
    script = """
import importlib
import sqlite3
import sys
from pathlib import Path

connections = []
def audit(event, args):
    if event == 'sqlite3.connect':
        connections.append(str(args[0]))
sys.addaudithook(audit)

import world_intel_mcp
root = Path(world_intel_mcp.__file__).parent
for path in sorted(root.rglob('*.py')):
    relative = path.relative_to(root).with_suffix('')
    if 'tests' in relative.parts:
        continue
    parts = relative.parts
    if parts[-1] == '__init__':
        parts = parts[:-1]
    importlib.import_module('.'.join(('world_intel_mcp', *parts)))
assert connections == [], connections
"""
    result = subprocess.run(
        [sys.executable, "-c", script], env=env, capture_output=True, text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert list(home.rglob("*")) == []
    assert list(xdg.rglob("*")) == []


@pytest.mark.parametrize("module,cls,filename", [
    (temporal, temporal.TemporalBaseline, "temporal.db"),
    (spikes, spikes.KeywordSpikeDetector, "keyword_spikes.db"),
])
@pytest.mark.parametrize("location", ["xdg", "~/xdg", "", None])
def test_baseline_path_resolved_on_first_use(
    monkeypatch, tmp_path: Path, module, cls, filename: str, location: str | None,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    old_xdg = tmp_path / "before-first-use"
    monkeypatch.setenv("XDG_CACHE_HOME", str(old_xdg))
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "main-cache.db"))
    baseline = cls()
    assert not old_xdg.exists()
    assert list(home.iterdir()) == []

    if location is None:
        monkeypatch.delenv("XDG_CACHE_HOME")
    else:
        monkeypatch.setenv(
            "XDG_CACHE_HOME", str(tmp_path / location) if location == "xdg" else location
        )
    expected_root = (
        tmp_path / "xdg" if location == "xdg" else
        home / "xdg" if location == "~/xdg" else home / ".cache"
    )
    try:
        if module is temporal:
            assert baseline.check("conflict", "UKR", 10) is None
            baseline.record("conflict", "UKR", 10)
        else:
            assert baseline.detect_spikes({"conflict": 10}) == []
        assert (expected_root / "world-intel-mcp" / filename).is_file()
        assert not old_xdg.exists()
        assert not (tmp_path / "main-cache.db").exists()
        if location:
            assert not (home / ".cache").exists()
    finally:
        if baseline._conn is not None:
            baseline._conn.close()


def test_cache_default_path_follows_home_set_after_import(
    monkeypatch, tmp_path: Path,
) -> None:
    """With neither override set, the cache lands under the HOME in force
    when it is built, beside the baselines, not the HOME of import time."""
    monkeypatch.delenv("WORLD_INTEL_CACHE_DB", raising=False)
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    home = tmp_path / "home-set-after-import"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    expected = home / ".cache" / "world-intel-mcp"

    cache = Cache()
    try:
        assert cache.db_path == expected / "cache.db"
        assert cache.db_path.is_file()
        assert Path(temporal._default_db_path()).parent == expected
        assert Path(spikes._default_db_path()).parent == expected
    finally:
        cache.close()


@pytest.mark.parametrize("module,cls", [
    (temporal, temporal.TemporalBaseline),
    (spikes, spikes.KeywordSpikeDetector),
])
def test_baseline_explicit_path_and_module_override(monkeypatch, tmp_path, module, cls):
    explicit = tmp_path / "explicit.db"
    override = tmp_path / "override.db"
    baseline = cls(str(explicit))
    default = cls()
    monkeypatch.setattr(module, "_DB_PATH", str(override))
    assert not explicit.exists()
    assert not override.exists()
    try:
        baseline._get_conn()
        default._get_conn()
        assert explicit.is_file()
        assert override.is_file()
    finally:
        for instance in (baseline, default):
            if instance._conn is not None:
                instance._conn.close()
