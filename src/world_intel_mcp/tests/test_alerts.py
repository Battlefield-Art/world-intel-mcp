"""Tests for analysis/alerts.py — cross-domain alert digest and weekly trends.

Both tools compose source functions, so tests mock at the source-function
boundary. Weekly-trends tests point temporal._DB_PATH at a tmp_path SQLite
file; the coefficient of variation for the seeded row is hand-computed
(n=10, mean=20, M2=900 -> sample variance 100, std 10, cv 50%).
"""

import sqlite3
from pathlib import Path

import pytest

from world_intel_mcp.analysis import temporal
from world_intel_mcp.analysis.alerts import fetch_alert_digest, fetch_weekly_trends
from world_intel_mcp.sources import (
    infrastructure,
    intelligence,
    shipping,
    space_weather,
)


def _make_fake(payload: dict):
    async def _fake(fetcher, *args, **kwargs):
        return payload

    return _fake


def _patch_digest_sources(monkeypatch: pytest.MonkeyPatch, payloads: dict) -> None:
    monkeypatch.setattr(
        space_weather, "fetch_space_weather", _make_fake(payloads["space"])
    )
    monkeypatch.setattr(
        intelligence, "fetch_instability_index", _make_fake(payloads["instability"])
    )
    monkeypatch.setattr(
        intelligence, "fetch_military_surge", _make_fake(payloads["surge"])
    )
    monkeypatch.setattr(
        infrastructure, "fetch_cable_health", _make_fake(payloads["cables"])
    )
    monkeypatch.setattr(
        intelligence, "fetch_hotspot_escalation", _make_fake(payloads["hotspots"])
    )
    monkeypatch.setattr(
        infrastructure, "fetch_internet_outages", _make_fake(payloads["outages"])
    )
    monkeypatch.setattr(
        shipping, "fetch_shipping_index", _make_fake(payloads["shipping"])
    )


_STORMY_WORLD = {
    "space": {"current_kp": 6.0, "kp_level": "Moderate storm"},
    "instability": {
        "countries": [
            {"country_name": "Sudan", "instability_index": 75},
            {"country_name": "Norway", "instability_index": 12},
        ]
    },
    "surge": {"surge_count": 2, "surges": [{"region": "red_sea"}]},
    "cables": {
        "corridors": {
            "red_sea": {"status_score": 2},
            "asia_europe": {"status_score": 1},  # below threshold
        }
    },
    "hotspots": {
        "hotspots": [{"name": "gaza", "score": 82}, {"name": "oslo", "score": 10}]
    },
    "outages": {"outage_count": 7},
    "shipping": {"stress_score": 65, "assessment": "severe"},
}

_QUIET_WORLD = {
    "space": {"current_kp": 2.0, "kp_level": "Quiet"},
    "instability": {"countries": [{"country_name": "Norway", "instability_index": 12}]},
    "surge": {"surge_count": 0},
    "cables": {"corridors": {"red_sea": {"status_score": 1}}},
    "hotspots": {"hotspots": [{"name": "oslo", "score": 10}]},
    "outages": {"outage_count": 2},
    "shipping": {"stress_score": 10, "assessment": "normal"},
}


async def test_alert_digest_fires_on_every_threshold(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_digest_sources(monkeypatch, _STORMY_WORLD)
    result = await fetch_alert_digest(fetcher)

    assert result["alert_count"] == 7
    assert result["by_priority"] == {"critical": 2, "high": 4, "medium": 1}
    # Sorted critical > high > medium.
    priorities = [a["priority"] for a in result["alerts"]]
    assert priorities == sorted(
        priorities, key={"critical": 0, "high": 1, "medium": 2}.get
    )
    assert priorities[0] == "critical"
    assert priorities[-1] == "medium"

    domains = {a["domain"] for a in result["alerts"]}
    assert domains == {
        "space",
        "political",
        "military",
        "infrastructure",
        "security",
        "economic",
    }
    # Only the corridor at status_score >= 2 counts.
    cable_alert = next(a for a in result["alerts"] if "corridors" in a)
    assert cable_alert["corridors"] == ["red_sea"]
    # Only the country at CII >= 70 counts.
    instability_alert = next(a for a in result["alerts"] if "countries" in a)
    assert instability_alert["countries"] == ["Sudan"]
    # Shipping stress 65 escalates from medium to high.
    shipping_alert = next(a for a in result["alerts"] if a["domain"] == "economic")
    assert shipping_alert["priority"] == "high"


async def test_alert_digest_quiet_world_is_silent(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The load-bearing negative: sub-threshold values must produce zero
    alerts, otherwise the digest is noise."""
    _patch_digest_sources(monkeypatch, _QUIET_WORLD)
    result = await fetch_alert_digest(fetcher)
    assert result["alert_count"] == 0
    assert result["alerts"] == []
    assert result["by_priority"] == {}
    assert len(result["domains_checked"]) == 7


async def test_alert_digest_moderate_shipping_stress_is_medium(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    payloads = dict(_QUIET_WORLD)
    payloads["shipping"] = {"stress_score": 40, "assessment": "elevated"}
    _patch_digest_sources(monkeypatch, payloads)
    result = await fetch_alert_digest(fetcher)
    assert result["alert_count"] == 1
    assert result["alerts"][0]["domain"] == "economic"
    assert result["alerts"][0]["priority"] == "medium"


# ---------------------------------------------------------------------------
# Weekly trends
# ---------------------------------------------------------------------------


def _seed_baselines(db_path: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(db_path)
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


async def test_weekly_trends_computes_volatility(
    fetcher, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = tmp_path / "baselines.db"
    ts = "2026-08-31T00:00:00+00:00"
    _seed_baselines(
        db,
        [
            # n=10, mean=20, M2=900: variance 100, std 10, cv 50.0
            ("conflict:UKR:Monday:January", 10, 20.0, 900.0, ts),
            # n=6, mean=5, M2=20: variance 4, std 2, cv 40.0
            ("outage:EGY", 6, 5.0, 20.0, ts),
            # Below the count>=5 floor: excluded.
            ("quake:JPN", 3, 9.0, 10.0, ts),
            # Malformed key without a region: skipped.
            ("nocolon", 9, 1.0, 1.0, ts),
        ],
    )
    monkeypatch.setattr(temporal, "_DB_PATH", str(db))
    monkeypatch.setattr(
        intelligence,
        "fetch_temporal_anomalies",
        _make_fake({"anomalies": [{"key": "conflict:UKR", "z_score": 3.0}]}),
    )

    result = await fetch_weekly_trends(fetcher)
    assert result["trend_count"] == 2
    # Sorted by volatility descending.
    assert [t["volatility_cv"] for t in result["trends"]] == [50.0, 40.0]
    top = result["trends"][0]
    assert top["metric"] == "conflict"
    assert top["region"] == "UKR"
    assert top["weekday"] == "Monday"
    assert top["month"] == "January"
    assert top["mean"] == 20.0
    assert top["std_dev"] == 10.0
    assert top["observations"] == 10
    # A key without weekday/month parts still parses.
    assert result["trends"][1]["weekday"] == ""
    assert result["current_anomaly_count"] == 1


async def test_weekly_trends_missing_database_degrades_gracefully(
    fetcher, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(temporal, "_DB_PATH", str(tmp_path / "does_not_exist.db"))
    monkeypatch.setattr(intelligence, "fetch_temporal_anomalies", _make_fake({}))
    result = await fetch_weekly_trends(fetcher)
    assert result["trends"] == []
    # A missing baseline database is unknown, not zero trends: the count is
    # None and the error names the path that was not there.
    assert result["trend_count"] is None
    assert result["error"] == (
        f"Temporal baselines unavailable: no temporal baseline database at {tmp_path / 'does_not_exist.db'}"
    )
    assert result["current_anomalies"] == []
