import uuid

from tair import Tair, TairGisSearchRadius


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
        assert t.gis_search("Sicily", radius, withdist=True) == [
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

    def test_gis_del(self, t: Tair):
        area = "area_" + str(uuid.uuid4())

        assert (
            t.gis_add(area, {"campus": "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"})
            == 1
        )
        assert t.gis_del(area, "campus")
