"""History health recognizes the cache's real temp-dir fallback.

Cache isolation moved the fallback into a per-user directory
(``world-intel-mcp-<uid>``); history health still compared against the
old shared ``world-intel-mcp`` path, so a run log living in the fallback
was never named as one. Both now read one path helper.

Gaps / not covered: Windows (no ``os.getuid``), where the fallback stays
``world-intel-mcp`` and only the shared helper keeps the two in step; the
uid is pinned here, not read from a second real account.
"""

import os
import tempfile

from world_intel_mcp.cache import Cache
from world_intel_mcp.history import HistoryStore

T0 = 1_792_000_000.0
FALLBACK_GAP = "this run log is the temp-directory fallback"


def _gaps(db) -> list[str]:
    s = HistoryStore(db)
    try:
        return s.collection_health(["wildfires"], now=T0)["data_gaps"]
    finally:
        s.close()


def test_health_names_the_per_user_fallback(tmp_path, monkeypatch) -> None:
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("")
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(blocker / "cache.db"))
    temp_root = tmp_path / "tmp"
    temp_root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temp_root))
    cache = Cache()
    try:
        assert cache.fell_back is True
        assert cache.db_path == temp_root / f"world-intel-mcp-{os.getuid()}" / "cache.db"
        gaps = _gaps(cache.db_path)
    finally:
        cache.close()
    assert any(gap.startswith(FALLBACK_GAP) for gap in gaps)


def test_health_does_not_call_the_old_shared_path_a_fallback(tmp_path, monkeypatch) -> None:
    temp_root = tmp_path / "tmp"
    shared = temp_root / "world-intel-mcp"
    shared.mkdir(parents=True)
    monkeypatch.setattr(tempfile, "tempdir", str(temp_root))
    monkeypatch.setattr(os, "getuid", lambda: 4242, raising=False)
    assert not any(gap.startswith(FALLBACK_GAP) for gap in _gaps(shared / "cache.db"))
    own = temp_root / "world-intel-mcp-4242"
    own.mkdir()
    assert any(gap.startswith(FALLBACK_GAP) for gap in _gaps(own / "cache.db"))
