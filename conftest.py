"""Isolate cache and temporary files before importing any package modules."""

import os
import tempfile
from pathlib import Path

_test_directory = tempfile.TemporaryDirectory(
    prefix=".test-cache-", dir=Path(__file__).parent
)
_test_root = Path(_test_directory.name)
(_test_root / "home").mkdir()
os.environ["HOME"] = str(_test_root / "home")
os.environ["XDG_CACHE_HOME"] = str(_test_root / "cache")
os.environ["WORLD_INTEL_CACHE_DB"] = str(_test_root / "cache.db")
os.environ["TMPDIR"] = str(_test_root)
tempfile.tempdir = str(_test_root)
