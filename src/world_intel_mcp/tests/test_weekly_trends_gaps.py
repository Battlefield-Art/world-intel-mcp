"""intel_weekly_trends never reads a failed or history-less input as zero.

The baseline path always points under tmp_path. Most tests mock anomaly
detection at the source-function boundary; the two first-use routes run
the real detector (a fresh TemporalBaseline against a mocked live posture
feed), so the database it creates or fails to create is the one trends
then read.
"""

import sqlite3
from pathlib import Path

import pytest

from world_intel_mcp.analysis import temporal
from world_intel_mcp.analysis.alerts import fetch_weekly_trends
from world_intel_mcp.sources import intelligence, military

_TS = "2026-10-12T00:00:00Z"


def _returns(payload):
    async def _fake(*args, **kwargs):
        return payload

    return _fake


def _raises(exc: BaseException):
    async def _fake(*args, **kwargs):
        raise exc

    return _fake


def _seed(db: Path, rows: list[tuple]) -> None:
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db)
    conn.execute(
        """CREATE TABLE baselines (
            key TEXT PRIMARY KEY,
            count INTEGER NOT NULL DEFAULT 0,
            mean REAL NOT NULL DEFAULT 0.0,
            m2 REAL NOT NULL DEFAULT 0.0,
            updated_at TEXT NOT NULL
        )"""
    )
    conn.executemany("INSERT INTO baselines VALUES (?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


@pytest.fixture
def live_detector(monkeypatch):
    """The real anomaly detector on a fresh baseline whose path resolves on
    first use, fed by a live-looking posture feed and ACLED reply."""
    baseline = temporal.TemporalBaseline()
    monkeypatch.setattr(temporal, "_DB_PATH", None)
    monkeypatch.setattr(intelligence, "_temporal", baseline)
    monkeypatch.setattr(
        military,
        "fetch_theater_posture",
        _returns({"theaters": {"european": {"count": 12}}}),
    )
    monkeypatch.setattr(
        intelligence, "acled_query", _returns({"data": [{"country": "Ukraine"}]})
    )
    try:
        yield baseline
    finally:
        if baseline._conn is not None:
            baseline._conn.close()


async def test_missing_baselines_and_failed_anomalies_are_named_not_zero(
    fetcher, tmp_path: Path, monkeypatch,
) -> None:
    db = tmp_path / "temporal.db"
    monkeypatch.setattr(temporal, "_DB_PATH", str(db))
    monkeypatch.setattr(
        intelligence,
        "fetch_temporal_anomalies",
        _raises(PermissionError(13, "Permission denied")),
    )

    result = await fetch_weekly_trends(fetcher)

    assert result["trend_count"] is None
    assert result["current_anomaly_count"] is None
    assert result["trends"] == []
    assert result["current_anomalies"] == []
    assert result["error"] == (
        f"Temporal baselines unavailable: no temporal baseline database at {db}"
    )
    assert result["data_gaps"] == [
        "Current anomalies: PermissionError: [Errno 13] Permission denied",
        f"Baselines: no temporal baseline database at {db}",
    ]
    assert not db.exists(), "reading trends created a database"


@pytest.mark.parametrize("content", ["not a database", "no baselines table"])
async def test_unreadable_baselines_are_an_error_and_anomalies_survive(
    fetcher, tmp_path: Path, monkeypatch, content: str,
) -> None:
    db = tmp_path / "temporal.db"
    if content == "not a database":
        db.write_bytes(b"this is not an SQLite database file. " * 40)
        reason = "file is not a database"
    else:
        sqlite3.connect(db).close()
        reason = "no such table: baselines"
    monkeypatch.setattr(temporal, "_DB_PATH", str(db))
    monkeypatch.setattr(
        intelligence,
        "fetch_temporal_anomalies",
        _returns({"anomalies": [{"event_type": "conflict", "z_score": 2.4}], "data_gaps": []}),
    )

    result = await fetch_weekly_trends(fetcher)

    assert result["trend_count"] is None
    assert result["current_anomaly_count"] == 1
    assert result["data_gaps"] == [f"Baselines: could not read {db}: {reason}"]
    assert result["error"] == f"Temporal baselines unavailable: could not read {db}: {reason}"


async def test_anomaly_reply_without_a_list_is_a_gap_not_zero(
    fetcher, tmp_path: Path, monkeypatch,
) -> None:
    db = tmp_path / "temporal.db"
    _seed(db, [("conflict:UKR:Monday:January", 10, 20.0, 900.0, _TS)])
    monkeypatch.setattr(temporal, "_DB_PATH", str(db))
    monkeypatch.setattr(intelligence, "fetch_temporal_anomalies", _returns({}))

    result = await fetch_weekly_trends(fetcher)

    assert result["trend_count"] == 1
    assert result["current_anomaly_count"] is None
    assert result["current_anomalies"] == []
    assert result["data_gaps"] == [
        "Current anomalies: anomaly detection returned no anomaly list"
    ]
    assert "error" not in result


async def test_anomaly_detections_own_gaps_are_carried(
    fetcher, tmp_path: Path, monkeypatch,
) -> None:
    db = tmp_path / "temporal.db"
    _seed(db, [("conflict:UKR:Monday:January", 10, 20.0, 900.0, _TS)])
    monkeypatch.setattr(temporal, "_DB_PATH", str(db))
    monkeypatch.setattr(
        intelligence,
        "fetch_temporal_anomalies",
        _returns({"anomalies": [], "data_gaps": ["acled_fetch_failed"]}),
    )

    result = await fetch_weekly_trends(fetcher)

    assert result["current_anomaly_count"] == 0
    assert result["data_gaps"] == ["Current anomalies: acled_fetch_failed"]


async def test_healthy_inputs_report_no_gaps(
    fetcher, tmp_path: Path, monkeypatch,
) -> None:
    db = tmp_path / "temporal.db"
    _seed(db, [("conflict:UKR:Monday:January", 10, 20.0, 900.0, _TS)])
    monkeypatch.setattr(temporal, "_DB_PATH", str(db))
    monkeypatch.setattr(
        intelligence, "fetch_temporal_anomalies", _returns({"anomalies": [], "data_gaps": []})
    )

    result = await fetch_weekly_trends(fetcher)

    assert result["trend_count"] == 1
    assert result["current_anomaly_count"] == 0
    assert result["data_gaps"] == []
    assert "error" not in result


async def test_young_baseline_says_it_lacks_history(
    fetcher, tmp_path: Path, monkeypatch,
) -> None:
    db = tmp_path / "temporal.db"
    _seed(
        db,
        [
            ("conflict:UKR:Monday:January", 4, 20.0, 30.0, _TS),
            ("outage:EGY", 1, 5.0, 0.0, _TS),
        ],
    )
    monkeypatch.setattr(temporal, "_DB_PATH", str(db))
    monkeypatch.setattr(
        intelligence, "fetch_temporal_anomalies", _returns({"anomalies": [], "data_gaps": []})
    )

    result = await fetch_weekly_trends(fetcher)

    assert result["trend_count"] == 0
    assert result["data_gaps"] == [
        "Baselines: no tracked metric has the 5 observations volatility needs yet (2 tracked)"
    ]
    assert "error" not in result


async def test_baseline_born_in_this_call_is_flagged_and_old_one_left_alone(
    fetcher, tmp_path: Path, monkeypatch, live_detector,
) -> None:
    """The upgrade route: XDG_CACHE_HOME is set while the history still sits
    in ~/.cache, which is never moved."""
    home = tmp_path / "home"
    legacy = home / ".cache" / "world-intel-mcp" / "temporal.db"
    _seed(legacy, [("military_flights:european:Monday:October", 40, 11.0, 390.0, _TS)])
    xdg = tmp_path / "xdg"
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("XDG_CACHE_HOME", str(xdg))
    created = xdg / "world-intel-mcp" / "temporal.db"

    result = await fetch_weekly_trends(fetcher)

    assert created.is_file()
    assert result["trend_count"] == 0
    assert result["current_anomaly_count"] == 0
    assert result["data_gaps"] == [
        f"Baselines: none existed at {created} before this call, so trends "
        "and anomalies have no history yet"
    ]
    rows = sqlite3.connect(legacy).execute("SELECT key, count FROM baselines").fetchall()
    assert rows == [("military_flights:european:Monday:October", 40)]


async def test_baseline_location_that_cannot_be_created_is_named(
    fetcher, tmp_path: Path, monkeypatch, live_detector,
) -> None:
    """The unwritable route: the baseline directory cannot be made (here
    because the cache home is a regular file), so detection fails and no
    database exists to read."""
    cache_home = tmp_path / "cache-home-is-a-file"
    cache_home.write_text("")
    monkeypatch.setenv("XDG_CACHE_HOME", str(cache_home))
    db = cache_home / "world-intel-mcp" / "temporal.db"

    result = await fetch_weekly_trends(fetcher)

    assert result["trend_count"] is None
    assert result["current_anomaly_count"] is None
    assert result["error"] == (
        f"Temporal baselines unavailable: no temporal baseline database at {db}"
    )
    anomaly_gap, baseline_gap = result["data_gaps"]
    assert anomaly_gap.startswith("Current anomalies: NotADirectoryError: ")
    assert baseline_gap == f"Baselines: no temporal baseline database at {db}"
