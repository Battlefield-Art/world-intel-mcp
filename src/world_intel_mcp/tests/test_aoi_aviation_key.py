"""The AOI brief reads aviation from the payload fetch_domestic_flights
really returns.

The brief scoped aviation by a ``sampled`` list, a key the OpenSky
fetcher never emits (it returns ``positions``), so every live brief read
aviation as "the answer carried no sampled list" while the unit tests
passed on a stub that used the wrong key. These tests drive the real
fetch_domestic_flights through a stand-in fetcher, so the key the brief
reads is the key the source writes.

Gaps / not covered: the aviation list is fetch_domestic_flights'
1-in-10 map sample, so the AOI count is a sample count, not every
aircraft in the area; no live OpenSky request.
"""

import pytest

from world_intel_mcp.analysis import aoi
from world_intel_mcp.analysis.aoi import AOIStore
from world_intel_mcp.sources import aviation
from world_intel_mcp.tests import test_aoi as base

_REAL_DOMESTIC = aviation.fetch_domestic_flights


def _state(icao: str, callsign: str, lat: float, lon: float) -> list:
    # OpenSky state vector: [icao24, callsign, origin, time_pos, last_contact,
    # lon, lat, baro_alt, on_ground, velocity, track, vrate, sensors,
    # geo_alt, squawk, spi, position_source]
    return [icao, callsign, "United States", 0, 0, lon, lat, 10000.0, False,
            200.0, 90.0, 0.0, None, 10000.0, None, False, 0]


class _StatesFetcher:
    """Answers OpenSky's states call with fixed state vectors."""

    def __init__(self, states: list) -> None:
        self.states = states
        self.calls: list[str] = []

    async def get_json(self, url, **kwargs):
        self.calls.append(url)
        return {"time": 0, "states": self.states}


@pytest.fixture
def pgh_store(tmp_path):
    s = AOIStore(tmp_path / "aoi-aviation.db")
    s.define("Pittsburgh", base._PGH_LAT, base._PGH_LON, base._PGH_RADIUS_KM)
    yield s
    s.close()


@pytest.mark.asyncio
async def test_brief_counts_aircraft_from_the_real_aviation_payload(pgh_store, monkeypatch) -> None:
    base._patch_all_domains(monkeypatch)
    monkeypatch.setattr(aviation, "fetch_domestic_flights", _REAL_DOMESTIC)
    # Index 0 is always in the 1-in-10 sample; index 10 is the next sampled one.
    states = [_state("a00001", "UAL1  ", base._NEAR_LAT, base._NEAR_LON)]
    states += [_state(f"a0000{i}", f"N{i}", 0.0, 0.0) for i in range(2, 11)]
    states += [_state("a00011", "UAL2  ", base._FAR_LAT, base._FAR_LON)]
    fetcher = _StatesFetcher(states)

    result = await aoi.fetch_aoi_brief(fetcher, pgh_store, "Pittsburgh")

    assert fetcher.calls == [aviation._OPENSKY_STATES_URL]
    assert result["counts"]["aviation"] == 1
    assert not [gap for gap in result.get("data_gaps", []) if "viation" in gap]


@pytest.mark.asyncio
async def test_aviation_payload_carries_the_list_key_the_brief_reads() -> None:
    fetcher = _StatesFetcher([_state("a00001", "UAL1  ", base._NEAR_LAT, base._NEAR_LON)])
    payload = await _REAL_DOMESTIC(fetcher)
    area = {"lat": base._PGH_LAT, "lon": base._PGH_LON, "radius_km": base._PGH_RADIUS_KM}
    scoped = aoi._scope_to_aoi(payload, "positions", area, "lat", "lon")
    assert "error" not in scoped and len(scoped["items"]) == 1
