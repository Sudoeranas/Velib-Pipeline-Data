from typing import Optional, TypedDict


class GeoCoord(TypedDict):
    lon: float
    lat: float


class AvailabilityRecord(TypedDict):
    stationcode: str
    name: str
    is_installed: str
    capacity: int
    numdocksavailable: int
    numbikesavailable: int
    mechanical: int
    ebike: int
    is_renting: str
    is_returning: str
    duedate: str
    coordonnees_geo: GeoCoord
    nom_arrondissement_communes: str
    code_insee_commune: str
    station_opening_hours: Optional[str]


class StationRecord(TypedDict):
    stationcode: str
    name: str
    capacity: int
    coordonnees_geo: GeoCoord
    station_opening_hours: Optional[str]
