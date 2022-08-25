import uuid

import pytest

from tair import Aggregation, DataError, Tair, TairTsSkeyItem


class TestTairTs:
    def test_exts_p_create(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())

        assert t.exts_p_create(pkey)

    def test_exts_s_create(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())
        skey = "skey_" + str(uuid.uuid4())
        et = 60 * 1000  # 60 seconds

        assert t.exts_s_create(
            pkey, skey, et, chunk_size=256, uncompressed=True, labels={"sensor_id": 1}
        )

    def test_exts_s_alter(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())
        skey = "skey_" + str(uuid.uuid4())
        et1 = 60 * 1000
        et2 = 120 * 1000

        assert t.exts_s_create(
            pkey, skey, et1, chunk_size=256, uncompressed=True, labels={"sensor_id": 1}
        )
        assert t.exts_s_alter(pkey, skey, et2)
        assert t.exts_s_alter(pkey, skey)

    def test_exts_s_add(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())
        skey = "skey_" + str(uuid.uuid4())

        assert t.exts_s_add(
            pkey,
            skey,
            ts=1644310456023,
            value=30.5,
            data_et=60 * 1000,
            chunk_size=256,
            uncompressed=True,
            labels={"sensor_id": 1},
        )

    def test_exts_s_madd(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())

        assert t.exts_s_madd(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 30.2),
                TairTsSkeyItem("pressure", 1661419880203, 2.05),
                TairTsSkeyItem("distance", 1661419880203, 0.5),
            ],
            data_et=60 * 1000,
            chunk_size=256,
            uncompressed=True,
            labels={"sensor_id": 1},
        ) == [True, True, True]

    def test_exts_s_incrby(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())
        skey = "skey_" + str(uuid.uuid4())

        assert t.exts_s_add(pkey, skey, 1644310456023, 30.0)
        assert t.exts_s_incrby(
            pkey,
            skey,
            1644372093031,
            2,
            data_et=60 * 1000,
            chunk_size=256,
            uncompressed=True,
            labels={"sensor_id": 1},
        )

    def test_exts_s_mincrby(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())

        assert t.exts_s_madd(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 30.2),
                TairTsSkeyItem("pressure", 1661419880203, 2.05),
                TairTsSkeyItem("distance", 1661419880203, 0.5),
            ],
        ) == [True, True, True]

        assert t.exts_s_mincrby(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 0.2),
                TairTsSkeyItem("pressure", 1661419880203, -0.1),
                TairTsSkeyItem("distance", 1661419880203, 0.0),
            ],
            data_et=60 * 1000,
            chunk_size=256,
            uncompressed=True,
            labels={"sensor_id": 1},
        )

    def test_exts_s_del(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())

        assert t.exts_s_madd(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 30.2),
                TairTsSkeyItem("pressure", 1661419880203, 2.05),
                TairTsSkeyItem("distance", 1661419880203, 0.5),
            ],
            labels={"sensor_id": 1},
        ) == [True, True, True]
        assert t.exts_s_del(pkey, "temperature")

    def test_exts_s_get(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())

        assert t.exts_s_madd(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 30.2),
                TairTsSkeyItem("pressure", 1661419880203, 2.05),
                TairTsSkeyItem("distance", 1661419880203, 0.5),
            ],
        ) == [True, True, True]
        result = t.exts_s_get(pkey, "temperature")
        assert result[0] == 1661419880203
        assert float(result[1]) == pytest.approx(30.2)

    def test_exts_s_info(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())

        assert t.exts_s_madd(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 30.2),
                TairTsSkeyItem("pressure", 1661419880203, 2.05),
                TairTsSkeyItem("distance", 1661419880203, 0.5),
            ],
            labels={"sensor_id": 1},
        ) == [True, True, True]
        assert t.exts_s_info(pkey, "temperature") == [
            b"totalDataPoints",
            1,
            b"maxDataPoints",
            0,
            b"maxDataPointsPerChunk",
            32,
            b"dataPointsExpireTime",
            0,
            b"lastTimestamp",
            1661419880203,
            b"chunkCount",
            1,
            b"lastValue",
            30,
            b"labels",
            [[b"sensor_id", b"1"]],
        ]

    def test_exts_s_queryindex(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())

        assert t.exts_s_madd(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 30.2),
                TairTsSkeyItem("pressure", 1661419880203, 2.05),
                TairTsSkeyItem("distance", 1661419880203, 0.5),
            ],
            labels={"sensor_id": 1},
        ) == [True, True, True]
        assert t.exts_s_queryindex(pkey, ["sensor_id=1"]) == [
            b"distance",
            b"pressure",
            b"temperature",
        ]

    def test_exts_s_range(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())

        assert t.exts_s_madd(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 30.2),
                TairTsSkeyItem("pressure", 1661419880203, 2.05),
                TairTsSkeyItem("distance", 1661419880203, 0.5),
            ],
        ) == [True, True, True]

        result = t.exts_s_range(
            pkey, "temperature", 1661419880202, 1661419880204, maxcount=10
        )
        assert result[0][0][0] == 1661419880203
        assert float(result[0][0][1].decode()) == pytest.approx(30.2)
        assert result[1] == 0

    def test_exts_s_range_aggregation(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())

        assert t.exts_s_madd(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 30.2),
                TairTsSkeyItem("pressure", 1661419880203, 2.05),
                TairTsSkeyItem("distance", 1661419880203, 0.5),
            ],
        ) == [True, True, True]

        result = t.exts_s_range(
            pkey,
            "temperature",
            1661419880202,
            1661419880204,
            aggregation=Aggregation("MAX", 1),
        )
        assert result[0][0][0] == 1661419880203
        assert float(result[0][0][1].decode()) == pytest.approx(30.2)
        assert result[1] == 0

    def test_exts_s_mrange(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())

        assert t.exts_s_madd(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 1),
                TairTsSkeyItem("pressure", 1661419880203, 2),
                TairTsSkeyItem("distance", 1661419880203, 3),
            ],
            labels={"sensor_id": 1},
        ) == [True, True, True]

        assert t.exts_s_mrange(
            pkey,
            from_ts=0,
            to_ts="*",
            filters=["sensor_id=1"],
            maxcount=10,
            withlabels=True,
        ) == [
            [b"distance", [[b"sensor_id", b"1"]], [[1661419880203, b"3"]], 0],
            [b"pressure", [[b"sensor_id", b"1"]], [[1661419880203, b"2"]], 0],
            [b"temperature", [[b"sensor_id", b"1"]], [[1661419880203, b"1"]], 0],
        ]

    def test_exts_s_mrange_aggregation(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())

        assert t.exts_s_madd(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 1),
                TairTsSkeyItem("pressure", 1661419880203, 2),
                TairTsSkeyItem("distance", 1661419880203, 3),
            ],
            labels={"sensor_id": 1},
        ) == [True, True, True]

        assert t.exts_s_mrange(
            pkey,
            from_ts=0,
            to_ts="*",
            filters=["sensor_id=1"],
            maxcount=10,
            withlabels=True,
            aggregation=Aggregation("MAX", 1),
        ) == [
            [b"distance", [[b"sensor_id", b"1"]], [[1661419880000, b"3"]], 0],
            [b"pressure", [[b"sensor_id", b"1"]], [[1661419880000, b"2"]], 0],
            [b"temperature", [[b"sensor_id", b"1"]], [[1661419880000, b"1"]], 0],
        ]

    def test_exts_p_range(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())

        assert t.exts_s_madd(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 30.2),
                TairTsSkeyItem("pressure", 1661419880203, 2.05),
                TairTsSkeyItem("distance", 1661419880203, 0.5),
            ],
            labels={"sensor_id": 1},
        ) == [True, True, True]

        assert t.exts_p_range(
            pkey,
            1661419880202,
            1661419880204,
            pkey_aggregation="SUM",
            pkey_time_bucket=1,
            filters=["sensor_id=1"],
            maxcount=10,
            aggregation=Aggregation("MAX", 1),
        ) == [[[1661419880000, b"32.75"]], 0]

    def test_exts_p_range_no_pkey_time_bucket(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())

        with pytest.raises(DataError):
            t.exts_p_range(
                pkey,
                1661419880202,
                1661419880204,
                pkey_aggregation="SUM",
                filters=["sensor_id=1"],
                aggregation=Aggregation("MAX", 1),
            )

    def test_exts_s_raw_modify(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())
        skey = "skey_" + str(uuid.uuid4())

        assert t.exts_s_add(pkey, skey, 1644310456023, 30.0)
        assert t.exts_s_raw_modify(
            pkey,
            skey,
            1644310456023,
            31.5,
            data_et=60 * 1000,
            chunk_size=256,
            uncompressed=256,
            labels={"sensor_id": 1},
        )

    def test_exts_s_raw_mmodify(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())

        assert t.exts_s_madd(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 30.2),
                TairTsSkeyItem("pressure", 1661419880203, 2.05),
                TairTsSkeyItem("distance", 1661419880203, 0.5),
            ],
            labels={"sensor_id": 1},
        ) == [True, True, True]
        assert t.exts_s_raw_mmodify(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 1),
                TairTsSkeyItem("pressure", 1661419880203, 2),
                TairTsSkeyItem("distance", 1661419880203, 3),
            ],
            data_et=60 * 1000,
            chunk_size=256,
            uncompressed=256,
            labels={"sensor_id": 1},
        ) == [True, True, True]

    def test_exts_s_raw_incrby(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())
        skey = "skey_" + str(uuid.uuid4())

        assert t.exts_s_add(pkey, skey, 1644310456023, 30.0)
        assert t.exts_s_raw_incrby(
            pkey,
            skey,
            1644310456023,
            2.0,
            data_et=60 * 1000,
            chunk_size=256,
            uncompressed=256,
            labels={"sensor_id": 1},
        )

    def test_exts_s_raw_mincrby(self, t: Tair):
        pkey = "pkey_" + str(uuid.uuid4())

        assert t.exts_s_madd(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 30.2),
                TairTsSkeyItem("pressure", 1661419880203, 2.05),
                TairTsSkeyItem("distance", 1661419880203, 0.5),
            ],
            labels={"sensor_id": 1},
        ) == [True, True, True]
        assert t.exts_s_raw_mincrby(
            pkey,
            [
                TairTsSkeyItem("temperature", 1661419880203, 1),
                TairTsSkeyItem("pressure", 1661419880203, 2),
                TairTsSkeyItem("distance", 1661419880203, 3),
            ],
            data_et=60 * 1000,
            chunk_size=256,
            uncompressed=256,
            labels={"sensor_id": 1},
        ) == [True, True, True]

    def test_tair_ts_skey_item_eq(self):
        skey = "skey_" + str(uuid.uuid4())
        assert TairTsSkeyItem(skey, "*", 1) == TairTsSkeyItem(skey, "*", 1)
        assert not TairTsSkeyItem(skey, "*", 1) == TairTsSkeyItem(skey, "*", 2)
        assert not TairTsSkeyItem(skey, "*", 1) == 1

    def test_tair_ts_skey_item_ne(self):
        skey = "skey_" + str(uuid.uuid4())
        assert not TairTsSkeyItem(skey, "*", 1) != TairTsSkeyItem(skey, "*", 1)
        assert TairTsSkeyItem(skey, "*", 1) != TairTsSkeyItem(skey, "*", 2)
        assert TairTsSkeyItem(skey, "*", 1) != 1

    def test_tair_ts_skey_item_repr(self):
        skey = "skey_" + str(uuid.uuid4())
        assert str(TairTsSkeyItem(skey, "*", 1)) == f"{{skey: {skey}, ts: *, value: 1}}"

    def test_aggregation_eq(self):
        skey = "skey_" + str(uuid.uuid4())
        assert TairTsSkeyItem(skey, "*", 1) == TairTsSkeyItem(skey, "*", 1)
        assert not TairTsSkeyItem(skey, "*", 1) == TairTsSkeyItem(skey, "*", 2)
        assert not TairTsSkeyItem(skey, "*", 1) == 1

    def test_aggregation_ne(self):
        skey = "skey_" + str(uuid.uuid4())
        assert not TairTsSkeyItem(skey, "*", 1) != TairTsSkeyItem(skey, "*", 1)
        assert TairTsSkeyItem(skey, "*", 1) != TairTsSkeyItem(skey, "*", 2)
        assert TairTsSkeyItem(skey, "*", 1) != 1

    def test_tair_ts_skey_item_repr(self):
        skey = "skey_" + str(uuid.uuid4())
        assert str(TairTsSkeyItem(skey, "*", 1)) == f"{{skey: {skey}, ts: *, value: 1}}"

    def test_aggregation_eq(self):
        assert Aggregation("MAX", 1) == Aggregation("MAX", 1)
        assert not Aggregation("MAX", 1) == Aggregation("MAX", 2)
        assert not Aggregation("MAX", 1) == 1

    def test_aggregation_ne(self):
        assert not Aggregation("MAX", 1) != Aggregation("MAX", 1)
        assert Aggregation("MAX", 1) != Aggregation("MAX", 2)
        assert Aggregation("MAX", 1) != 1

    def test_aggregation_repr(self):
        assert (
            str(Aggregation("MAX", 1)) == f"{{aggregation_type: MAX, time_bucket: 1}}"
        )
