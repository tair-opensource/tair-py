from typing import Dict, List, Optional

from tair.typing import CommandsProtocol, EncodableT, FieldT, KeyT, ResponseT


class TairGisSearchRadius:
    def __init__(
        self,
        longitude: float,
        latitude: float,
        distance: float,
        unit: str,
    ) -> None:
        self.longitude = longitude
        self.latitude = latitude
        self.distance = distance
        self.unit = unit


class TairGisSearchMember:
    def __init__(self, field: FieldT, distance: float, unit) -> None:
        self.field = field
        self.distance = distance
        self.unit = unit


class TairGisCommands(CommandsProtocol):
    def gis_add(self, area: KeyT, mapping: Dict[KeyT, str]) -> ResponseT:
        pieces: List[EncodableT] = [area]

        for name, wkt in mapping.items():
            pieces.append(name)
            pieces.append(wkt)

        return self.execute_command("GIS.ADD", *pieces)

    def gis_get(self, area: KeyT, polygon_name: EncodableT) -> ResponseT:
        return self.execute_command("GIS.GET", area, polygon_name)

    def gis_getall(self, area: KeyT, withoutwkts: bool = False) -> ResponseT:
        if withoutwkts:
            return self.execute_command("GIS.GETALL", area, "WITHOUTWKT")
        return self.execute_command("GIS.GETALL", area)

    def gis_contains(
        self,
        area: KeyT,
        polygon_wkt: EncodableT,
        withoutwkts: bool = False,
    ) -> ResponseT:
        if withoutwkts:
            return self.execute_command(
                "GIS.CONTAINS",
                area,
                polygon_wkt,
                "WITHOUTWKT",
            )
        return self.execute_command("GIS.CONTAINS", area, polygon_wkt)

    def gis_within(
        self,
        area: KeyT,
        polygon_wkt: EncodableT,
        withoutwkts: bool = False,
    ) -> ResponseT:
        if withoutwkts:
            return self.execute_command(
                "GIS.WITHIN",
                area,
                polygon_wkt,
                "WITHOUTWKT",
            )
        return self.execute_command("GIS.WITHIN", area, polygon_wkt)

    def gis_intersects(
        self,
        area: KeyT,
        polygon_wkt: EncodableT,
        withoutwkts: bool = False,
    ) -> ResponseT:
        if withoutwkts:
            return self.execute_command(
                "GIS.INTERSECTS",
                area,
                polygon_wkt,
                "WITHOUTWKT",
            )
        return self.execute_command("GIS.INTERSECTS", area, polygon_wkt)

    def gis_search(
        self,
        area: KeyT,
        radius: Optional[TairGisSearchRadius] = None,
        member: Optional[TairGisSearchMember] = None,
        geom: Optional[EncodableT] = None,
        count: Optional[int] = None,
        asc: bool = True,
        desc: bool = False,
        withdist: bool = False,
        withoutwkts: bool = False,
    ) -> ResponseT:
        pieces: List[EncodableT] = [area]

        if radius:
            pieces.append("RADIUS")
            pieces.append(radius.longitude)
            pieces.append(radius.latitude)
            pieces.append(radius.distance)
            pieces.append(radius.unit)
        if member:
            pieces.append("MEMBER")
            pieces.append(member.field)
            pieces.append(member.distance)
            pieces.append(member.unit)
        if geom:
            pieces.append("GEOM")
            pieces.append(geom)
        if count:
            pieces.append("COUNT")
            pieces.append(count)
        if asc:
            pieces.append("ASC")
        if desc:
            pieces.append("DESC")
        if withdist:
            pieces.append("WITHDIST")
        if withoutwkts:
            pieces.append("WITHOUTWKT")

        return self.execute_command("GIS.SEARCH", *pieces)

    def gis_del(self, area: KeyT, polygen_name) -> ResponseT:
        return self.execute_command("GIS.DEL", area, polygen_name)
