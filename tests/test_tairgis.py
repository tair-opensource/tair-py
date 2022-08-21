import uuid

from tair import Tair, TairGisSearchRadius, TairGisSearchMember


class TestTairGis:
    def test_gis_add(self, t: Tair):
        area = "area_" + str(uuid.uuid4())

        assert (
            t.gis_add(area, {"campus": "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"})
            == 1
        )
        assert t.gis_get(area, "campus") == b"POLYGON((30 10,40 40,20 40,10 20,30 10))"

    def test_gis_getall(self, t: Tair):
        area1 = "area_" + str(uuid.uuid4())
        area2 = "area_" + str(uuid.uuid4())

        assert (
            t.gis_add(
                area1, {"campus": "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"}
            )
            == 1
        )
        assert t.gis_getall(area1) == [
            b"campus",
            b"POLYGON((30 10,40 40,20 40,10 20,30 10))",
        ]
        assert t.gis_getall(area2) is None

    def test_gis_getall_withoutwkts(self, t: Tair):
        area = "area_" + str(uuid.uuid4())

        assert (
            t.gis_add(area, {"campus": "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"})
            == 1
        )
        assert t.gis_getall(area, withoutwkts=True) == [b"campus"]

    def test_gis_contains(self, t: Tair):
        area = "area_" + str(uuid.uuid4())

        assert (
            t.gis_add(area, {"campus": "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"})
            == 1
        )
        assert t.gis_contains(area, "POINT (30 11)") == [
            1,
            [b"campus", b"POLYGON((30 10,40 40,20 40,10 20,30 10))"],
        ]

    def test_gis_contains_withoutwkts(self, t: Tair):
        area = "area_" + str(uuid.uuid4())

        assert (
            t.gis_add(area, {"campus": "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"})
            == 1
        )
        assert t.gis_contains(area, "POINT (30 11)", withoutwkts=True) == [
            1,
            [b"campus"],
        ]

    def test_gis_within(self, t: Tair):
        area = "area_" + str(uuid.uuid4())

        assert (
            t.gis_add(area, {"campus": "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"})
            == 1
        )
        assert t.gis_within(area, "POLYGON ((30 5, 50 50, 20 50, 5 20, 30 5))") == [
            1,
            [b"campus", b"POLYGON((30 10,40 40,20 40,10 20,30 10))"],
        ]

    def test_gis_within_withoutwkts(self, t: Tair):
        area = "area_" + str(uuid.uuid4())

        assert (
            t.gis_add(area, {"campus": "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"})
            == 1
        )
        assert t.gis_within(
            area, "POLYGON ((30 5, 50 50, 20 50, 5 20, 30 5))", withoutwkts=True
        ) == [
            1,
            [b"campus"],
        ]

    def test_gis_intersects(self, t: Tair):
        area = "area_" + str(uuid.uuid4())

        assert (
            t.gis_add(area, {"campus": "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"})
            == 1
        )
        assert t.gis_intersects(area, "LINESTRING (30 10, 40 40)") == [
            1,
            [b"campus", b"POLYGON((30 10,40 40,20 40,10 20,30 10))"],
        ]

    def test_gis_intersects_withoutwkts(self, t: Tair):
        area = "area_" + str(uuid.uuid4())

        assert (
            t.gis_add(area, {"campus": "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"})
            == 1
        )
        assert t.gis_intersects(
            area, "LINESTRING (30 10, 40 40)", withoutwkts=True
        ) == [
            1,
            [b"campus"],
        ]

    def test_gis_search(self, t: Tair):
        radius = TairGisSearchRadius(15, 37, 200, "km")

        assert (
            t.gis_add(
                "Sicily",
                {
                    "Palermo": "POINT(13.361389 38.115556)",
                    "Catania": "POINT(15.087269 37.502669)",
                },
            )
            == 2
        )
        assert t.gis_search(
            "Sicily", radius=radius, withdist=True, count=2, asc=True
        ) == [
            2,
            [
                b"Catania",
                b"POINT(15.087269 37.502669)",
                b"56.4413",
                b"Palermo",
                b"POINT(13.361389 38.115556)",
                b"190.4424",
            ],
        ]

    def test_gis_search_withoutwkts(self, t: Tair):
        radius = TairGisSearchRadius(15, 37, 200, "km")

        assert (
            t.gis_add(
                "Sicily",
                {
                    "Palermo": "POINT(13.361389 38.115556)",
                    "Catania": "POINT(15.087269 37.502669)",
                },
            )
            == 2
        )
        assert t.gis_search(
            "Sicily", radius=radius, withdist=True, count=2, desc=True, withoutwkts=True
        ) == [
            2,
            [
                b"Catania",
                b"56.4413",
                b"Palermo",
                b"190.4424",
            ],
        ]

    def test_gis_search_member(self, t: Tair):
        member = TairGisSearchMember("Palermo", 200, "km")

        assert (
            t.gis_add(
                "Sicily",
                {
                    "Palermo": "POINT(13.361389 38.115556)",
                    "Catania": "POINT(15.087269 37.502669)",
                },
            )
            == 2
        )
        assert t.gis_search("Sicily", member=member, withdist=True) == [
            2,
            [
                b"Palermo",
                b"POINT(13.361389 38.115556)",
                b"0.0000",
                b"Catania",
                b"POINT(15.087269 37.502669)",
                b"166.2743",
            ],
        ]

    def test_gis_search_geom(self, t: Tair):
        geom = "POLYGON((10 30,20 30,20 40,10 40))"

        assert (
            t.gis_add(
                "Sicily",
                {
                    "Palermo": "POINT(13.361389 38.115556)",
                    "Catania": "POINT(15.087269 37.502669)",
                },
            )
            == 2
        )
        assert t.gis_search("Sicily", geom=geom, withdist=True) == [
            2,
            [
                b"Catania",
                b"POINT(15.087269 37.502669)",
                b"4449696.2373",
                b"Palermo",
                b"POINT(13.361389 38.115556)",
                b"4454733.1882",
            ],
        ]

    def test_gis_del(self, t: Tair):
        area = "area_" + str(uuid.uuid4())

        assert (
            t.gis_add(area, {"campus": "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"})
            == 1
        )
        assert t.gis_del(area, "campus")

    def test_tairgis_search_radius_eq(self):
        assert TairGisSearchRadius(10, 20, 30, "km") == TairGisSearchRadius(
            10, 20, 30, "km"
        )
        assert not TairGisSearchRadius(10, 20, 30, "km") == TairGisSearchRadius(
            10, 20, 100, "m"
        )
        assert not TairGisSearchRadius(10, 20, 30, "km") == 1

    def test_tairgis_search_radius_ne(self):
        assert not TairGisSearchRadius(10, 20, 30, "km") != TairGisSearchRadius(
            10, 20, 30, "km"
        )
        assert TairGisSearchRadius(10, 20, 30, "km") != TairGisSearchRadius(
            10, 20, 100, "m"
        )
        assert TairGisSearchRadius(10, 20, 30, "km") != 1

    def test_excas_result_repr(self):
        assert (
            str(TairGisSearchRadius(10, 20, 30, "km"))
            == "{"
            + f"longitude: 10, "
            + f"latitude: 20, "
            + f"distance: 30, "
            + f"unit: km"
            + "}"
        )

    def test_tairgis_search_member_eq(self):
        field = "member_" + str(uuid.uuid4())

        assert TairGisSearchMember(field, 20, "km") == TairGisSearchMember(
            field, 20, "km"
        )
        assert not TairGisSearchMember(field, 20, "km") == TairGisSearchMember(
            field, 10, "m"
        )
        assert not TairGisSearchMember(field, 20, "km") == 1

    def test_tairgis_search_member_ne(self):
        field = "member_" + str(uuid.uuid4())

        assert not TairGisSearchMember(field, 20, "km") != TairGisSearchMember(
            field, 20, "km"
        )
        assert TairGisSearchMember(field, 20, "km") != TairGisSearchMember(
            field, 10, "m"
        )
        assert TairGisSearchMember(field, 20, "km") != 1

    def test_tairgis_search_member_repr(self):
        field = "member_" + str(uuid.uuid4())

        assert (
            str(TairGisSearchMember(field, 20, "km"))
            == f"{{field: {field}, distance: 20, unit: km}}"
        )
