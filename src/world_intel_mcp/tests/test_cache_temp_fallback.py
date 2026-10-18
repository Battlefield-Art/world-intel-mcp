"""The cache's temp-directory fallback opens only in a directory private to this user.

Each test points the temp directory at its own folder and makes the
configured cache path unusable (a directory where the database file
should be), so ``Cache()`` takes the fallback.
"""

import os
import re
import sqlite3
import stat
import tempfile
from pathlib import Path

import pytest

from world_intel_mcp.cache import Cache

pytestmark = pytest.mark.skipif(
    not hasattr(os, "getuid"), reason="POSIX owners and mode bits"
)


@pytest.fixture
def shared_tmp(monkeypatch, tmp_path: Path) -> Path:
    shared = tmp_path / "shared-tmp"
    shared.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(shared))
    unusable = tmp_path / "configured-cache.db"
    unusable.mkdir()
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(unusable))
    return shared


def _private_dir(shared: Path, uid: int | None = None) -> Path:
    return shared / f"world-intel-mcp-{os.getuid() if uid is None else uid}"


def test_fallback_creates_and_reuses_a_private_per_user_directory(shared_tmp) -> None:
    directory = _private_dir(shared_tmp)

    cache = Cache()
    try:
        assert cache.db_path == directory / "cache.db"
        info = directory.lstat()
        assert stat.S_ISDIR(info.st_mode)
        assert stat.S_IMODE(info.st_mode) == 0o700
        assert info.st_uid == os.getuid()
        assert stat.S_IMODE(cache.db_path.stat().st_mode) == 0o600
        cache.set("fallback", "ok", ttl_seconds=60)
    finally:
        cache.close()
    # Nothing in the shared directory but this user's own.
    assert [path.name for path in shared_tmp.iterdir()] == [directory.name]

    reopened = Cache()
    try:
        assert reopened.db_path == directory / "cache.db"
        assert reopened.get("fallback") == "ok"
    finally:
        reopened.close()


@pytest.mark.parametrize("mode", [0o750, 0o705, 0o710, 0o777], ids=lambda mode: f"{mode:04o}")
def test_fallback_refuses_a_directory_others_can_enter(shared_tmp, mode: int) -> None:
    directory = _private_dir(shared_tmp)
    directory.mkdir()
    directory.chmod(mode)

    with pytest.raises(PermissionError) as refused:
        Cache()

    assert str(refused.value) == (
        f"Refusing to use {directory} for the cache fallback: its mode {mode:04o} "
        "gives group or other users access. Point WORLD_INTEL_CACHE_DB at a path "
        "only you can write."
    )
    # The configured path's own failure stays attached to the refusal.
    assert isinstance(refused.value.__context__, sqlite3.OperationalError)
    assert list(directory.iterdir()) == []


def test_fallback_refuses_a_directory_another_user_owns(shared_tmp, monkeypatch) -> None:
    # Making a directory that another account owns needs root, so the
    # process's uid is shifted instead: the directory on disk then belongs
    # to a different uid than the one the cache runs as.
    owner = os.getuid()
    other = owner + 1
    directory = _private_dir(shared_tmp, uid=other)
    directory.mkdir(mode=0o700)
    monkeypatch.setattr(os, "getuid", lambda: other)

    with pytest.raises(
        PermissionError,
        match=re.escape(f"it is owned by uid {owner}, not by this user (uid {other})"),
    ):
        Cache()

    assert list(directory.iterdir()) == []


def test_fallback_refuses_a_symlink_in_place_of_the_directory(shared_tmp, tmp_path) -> None:
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir(mode=0o700)
    _private_dir(shared_tmp).symlink_to(elsewhere, target_is_directory=True)

    with pytest.raises(PermissionError, match="for the cache fallback: it is a symlink"):
        Cache()

    assert list(elsewhere.iterdir()) == []


def test_fallback_refuses_a_file_in_place_of_the_directory(shared_tmp) -> None:
    _private_dir(shared_tmp).write_text("")

    with pytest.raises(PermissionError, match="for the cache fallback: it is not a directory"):
        Cache()


@pytest.mark.parametrize(
    "name", ["cache.db", "cache.db-wal", "cache.db-shm", "cache.db-journal"]
)
def test_fallback_refuses_a_symlinked_database_file(shared_tmp, tmp_path, name: str) -> None:
    directory = _private_dir(shared_tmp)
    directory.mkdir(mode=0o700)
    target = tmp_path / "link-target"
    (directory / name).symlink_to(target)

    with pytest.raises(PermissionError, match=re.escape(f"{name} in it is a symlink")):
        Cache()

    assert not target.exists(), "the cache wrote through the symlink"
    assert sorted(path.name for path in directory.iterdir()) == [name]


def test_refusal_closes_the_connection_the_configured_path_opened(
    shared_tmp, tmp_path, monkeypatch,
) -> None:
    # A database whose cache table lacks expires_at opens fine and then
    # fails on the index, so a connection is open when the fallback is
    # refused.
    configured = tmp_path / "old-schema.db"
    with sqlite3.connect(configured) as conn:
        conn.execute("CREATE TABLE cache (key TEXT)")
    conn.close()
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(configured))
    directory = _private_dir(shared_tmp)
    directory.mkdir()
    directory.chmod(0o777)
    opened: list[sqlite3.Connection] = []
    connect = sqlite3.connect

    def recording_connect(*args, **kwargs):
        opened.append(connect(*args, **kwargs))
        return opened[-1]

    monkeypatch.setattr(sqlite3, "connect", recording_connect)

    with pytest.raises(PermissionError, match="gives group or other users access"):
        Cache()

    assert len(opened) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        opened[0].execute("SELECT 1")


def test_explicit_path_still_raises_instead_of_falling_back(shared_tmp, tmp_path) -> None:
    unusable = tmp_path / "explicit-cache.db"
    unusable.mkdir()

    with pytest.raises(sqlite3.OperationalError):
        Cache(db_path=unusable)

    assert list(shared_tmp.iterdir()) == []
