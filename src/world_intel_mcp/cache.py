"""SQLite TTL cache for world-intel-mcp.

Simple key-value cache with per-key TTL, WAL mode, and automatic eviction.
No external deps — just stdlib sqlite3.
"""

import json
import logging
import os
import sqlite3
import tempfile
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("world-intel-mcp.cache")


def _default_db_path() -> Path:
    # Resolved per Cache, not at import, so HOME and XDG_CACHE_HOME are read
    # when the cache opens, the same way the temporal and spike baselines
    # resolve theirs.
    if env_path := os.environ.get("WORLD_INTEL_CACHE_DB"):
        return Path(env_path).expanduser()
    if xdg_cache := os.environ.get("XDG_CACHE_HOME"):
        return Path(xdg_cache).expanduser() / "world-intel-mcp" / "cache.db"
    return Path.home() / ".cache" / "world-intel-mcp" / "cache.db"


class Cache:
    """SQLite-backed TTL cache."""

    def __init__(self, db_path: Path | None = None):
        explicit_path = db_path is not None
        self.db_path = Path(db_path) if db_path is not None else _default_db_path()
        self._conn: sqlite3.Connection | None = None
        try:
            self._init_db()
        except (OSError, sqlite3.OperationalError) as exc:
            if explicit_path:
                raise
            fallback = Path(tempfile.gettempdir()) / "world-intel-mcp" / "cache.db"
            logger.warning(
                "Cache unavailable at %s: %s; falling back to %s",
                self.db_path,
                exc,
                fallback,
            )
            self.close()
            self.db_path = fallback
            self._init_db()

    def _init_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                expires_at REAL NOT NULL,
                created_at REAL NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache(expires_at)"
        )
        conn.commit()
        # Owner-only permissions: the cache is trusted-by-design (whatever
        # is in it gets served as intelligence, including to AOI briefs),
        # so keep other local users from writing — or reading — it. The
        # -wal/-shm sidecars inherit the db file's mode. Best-effort:
        # non-POSIX filesystems may refuse.
        try:
            os.chmod(self.db_path, 0o600)
        except OSError:
            logger.debug("Could not chmod cache db at %s", self.db_path)

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path), timeout=10)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.execute("PRAGMA synchronous=NORMAL")
        return self._conn

    def get(self, key: str) -> Any | None:
        """Get a cached value. Returns None if missing or expired."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT value, expires_at FROM cache WHERE key = ?", (key,)
        ).fetchone()
        if row is None:
            return None
        value, expires_at = row
        if time.time() > expires_at:
            return None
        return json.loads(value)

    def get_stale(self, key: str) -> Any | None:
        """Get cached value even if expired (last-known-good fallback)."""
        meta = self.get_stale_meta(key)
        return meta[0] if meta is not None else None

    def get_stale_meta(self, key: str) -> tuple[Any, float] | None:
        """Get cached value even if expired, plus when it was originally written.

        Returns ``(value, created_at)`` (``created_at`` is a Unix timestamp)
        or ``None`` if the key doesn't exist at all. Callers that need to
        report how old a stale fallback is — or preserve the original
        fetch time instead of restamping "now" — use this instead of
        ``get_stale()``.
        """
        conn = self._get_conn()
        row = conn.execute(
            "SELECT value, created_at FROM cache WHERE key = ?", (key,)
        ).fetchone()
        if row is None:
            return None
        value, created_at = row
        return json.loads(value), created_at

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        """Store a value with TTL in seconds.

        Write failures (readonly DB, locked) are logged and swallowed —
        the cache is best-effort and must never crash the caller.
        """
        try:
            conn = self._get_conn()
            now = time.time()
            conn.execute(
                "INSERT OR REPLACE INTO cache (key, value, expires_at, created_at) VALUES (?, ?, ?, ?)",
                (key, json.dumps(value, default=str), now + ttl_seconds, now),
            )
            conn.commit()
        except sqlite3.OperationalError as exc:
            logger.warning("Cache write failed for %s: %s", key, exc)

    def delete(self, key: str) -> None:
        """Delete a specific key."""
        conn = self._get_conn()
        conn.execute("DELETE FROM cache WHERE key = ?", (key,))
        conn.commit()

    def evict_expired(self) -> int:
        """Remove all expired entries. Returns count removed."""
        try:
            conn = self._get_conn()
            cursor = conn.execute(
                "DELETE FROM cache WHERE expires_at < ?", (time.time(),)
            )
            conn.commit()
            return cursor.rowcount
        except sqlite3.OperationalError as exc:
            logger.warning("Cache evict failed: %s", exc)
            return 0

    def stats(self) -> dict[str, Any]:
        """Cache statistics."""
        conn = self._get_conn()
        now = time.time()
        total = conn.execute("SELECT COUNT(*) FROM cache").fetchone()[0]
        expired = conn.execute(
            "SELECT COUNT(*) FROM cache WHERE expires_at < ?", (now,)
        ).fetchone()[0]
        return {
            "total_entries": total,
            "active_entries": total - expired,
            "expired_entries": expired,
            "db_path": str(self.db_path),
        }

    def freshness(self) -> dict[str, Any]:
        """Per-source freshness report — shows age of newest entry per source prefix."""
        conn = self._get_conn()
        now = time.time()
        rows = conn.execute(
            "SELECT key, created_at, expires_at FROM cache ORDER BY created_at DESC"
        ).fetchall()
        sources: dict[str, dict] = {}
        for key, created_at, expires_at in rows:
            # Extract source prefix (e.g., "markets:quotes:^GSPC" → "markets")
            prefix = key.split(":")[0] if ":" in key else key
            if prefix not in sources:
                age_s = now - created_at
                is_stale = now > expires_at
                sources[prefix] = {
                    "last_updated_s_ago": round(age_s, 1),
                    "is_stale": is_stale,
                    "newest_key": key,
                }
        return sources

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
