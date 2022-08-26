import datetime
import time
import uuid

import pytest

from tair import CpcUpdate2judResult, DataError, Tair

from .conftest import NETWORK_DELAY_CALIBRATION_VALUE, get_server_time


class TestTairCpc:
    @pytest.mark.asyncio
    async def test_cpc_update(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())

        assert await t.cpc_update(key, item)

    @pytest.mark.asyncio
    async def test_cpc_update_ex(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert await t.cpc_update(key, value, ex=10)
        assert 0 < await t.ttl(key) <= 10

        # ex should not be a float.
        with pytest.raises(DataError):
            await t.cpc_update(key, value, ex=10.0)

    @pytest.mark.asyncio
    async def test_cpc_update_ex_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(seconds=10)

        assert await t.cpc_update(key, value, ex=expire_at)
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_update_px(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert await t.cpc_update(key, value, px=10000)
        assert 0 < await t.pttl(key) <= 10000

        # px should not be a float.
        with pytest.raises(DataError):
            await t.cpc_update(key, value, px=10000.0)

    @pytest.mark.asyncio
    async def test_cpc_update_px_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(milliseconds=10000)

        assert await t.cpc_update(key, value, px=expire_at)
        assert 0 < await t.pttl(key) <= 10000

    @pytest.mark.asyncio
    async def test_cpc_update_exat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)
        exat = int(time.mktime(expire_at.timetuple()))

        assert await t.cpc_update(key, value, exat=exat)
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_update_exat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)

        assert await t.cpc_update(key, value, exat=expire_at)
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_update_pxat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(expire_at.timetuple())) * 1000

        assert await t.cpc_update(key, value, pxat=pxat)
        # due to network delay, pttl may be greater than 10000.
        assert 0 < await t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    @pytest.mark.asyncio
    async def test_cpc_update_pxat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)

        assert await t.cpc_update(key, value, pxat=expire_at)
        # due to network delay, pttl may be greater than 10000.
        assert 0 < await t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    @pytest.mark.asyncio
    async def test_cpc_estimate(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())

        assert await t.cpc_update(key, item)
        assert await t.cpc_estimate(key) == pytest.approx(1, 0.001)

    @pytest.mark.asyncio
    async def test_cpc_update2est(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())

        assert await t.cpc_update2est(key, item) == pytest.approx(1, 0.001)

    @pytest.mark.asyncio
    async def test_cpc_update2est_ex(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert await t.cpc_update2est(key, value, ex=10) == pytest.approx(1, 0.001)
        assert 0 < await t.ttl(key) <= 10

        # ex should not be a float.
        with pytest.raises(DataError):
            await t.cpc_update2est(key, value, ex=10.0)

    @pytest.mark.asyncio
    async def test_cpc_update2est_ex_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(seconds=10)

        assert await t.cpc_update2est(key, value, ex=expire_at) == pytest.approx(
            1, 0.001
        )
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_update2est_px(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert await t.cpc_update2est(key, value, px=10000) == pytest.approx(1, 0.001)
        assert 0 < await t.pttl(key) <= 10000

        # px should not be a float.
        with pytest.raises(DataError):
            await t.cpc_update2est(key, value, px=10000.0)

    @pytest.mark.asyncio
    async def test_cpc_update2est_px_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(milliseconds=10000)

        assert await t.cpc_update2est(key, value, px=expire_at) == pytest.approx(
            1, 0.001
        )
        assert 0 < await t.pttl(key) <= 10000

    @pytest.mark.asyncio
    async def test_cpc_update2est_exat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)
        exat = int(time.mktime(expire_at.timetuple()))

        assert await t.cpc_update2est(key, value, exat=exat) == pytest.approx(1, 0.001)
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_update2est_exat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)

        assert await t.cpc_update2est(key, value, exat=expire_at) == pytest.approx(
            1, 0.001
        )
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_update2est_pxat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(expire_at.timetuple())) * 1000

        assert await t.cpc_update2est(key, value, pxat=pxat) == pytest.approx(1, 0.001)
        # due to network delay, pttl may be greater than 10000.
        assert 0 < await t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    @pytest.mark.asyncio
    async def test_cpc_update2est_pxat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)

        assert await t.cpc_update2est(key, value, pxat=expire_at) == pytest.approx(
            1, 0.001
        )
        # due to network delay, pttl may be greater than 10000.
        assert 0 < await t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    @pytest.mark.asyncio
    async def test_cpc_update2jud(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        for i in range(1, 20):
            assert await t.cpc_update(key, f"f{i}")
        result: CpcUpdate2judResult = await t.cpc_update2jud(key, "f20")
        assert result.estimated_value == pytest.approx(20, 0.001)
        assert result.difference == pytest.approx(1, 0.001)

    @pytest.mark.asyncio
    async def test_cpc_update2jud_ex(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        for i in range(1, 20):
            assert await t.cpc_update(key, f"f{i}")
        await t.cpc_update2jud(key, "f20", ex=10)
        assert 0 < await t.ttl(key) <= 10

        # ex should not be a float.
        with pytest.raises(DataError):
            await t.cpc_update2jud(key, "f21", ex=10.0)

    @pytest.mark.asyncio
    async def test_cpc_update2jud_ex_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(seconds=10)

        for i in range(1, 20):
            assert await t.cpc_update(key, f"f{i}")
        await t.cpc_update2jud(key, "f20", ex=expire_at)
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_update2jud_px(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        for i in range(1, 20):
            assert await t.cpc_update(key, f"f{i}")
        await t.cpc_update2jud(key, "f20", px=10000)
        assert 0 < await t.pttl(key) <= 10000

        # px should not be a float.
        with pytest.raises(DataError):
            await t.cpc_update2jud(key, "f21", px=10000.0)

    @pytest.mark.asyncio
    async def test_cpc_update2jud_px_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(milliseconds=10000)

        for i in range(1, 20):
            assert await t.cpc_update(key, f"f{i}")
        await t.cpc_update2jud(key, "f20", px=expire_at)
        assert 0 < await t.pttl(key) <= 10000

    @pytest.mark.asyncio
    async def test_cpc_update2jud_exat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)
        exat = int(time.mktime(expire_at.timetuple()))

        for i in range(1, 20):
            assert await t.cpc_update(key, f"f{i}")
        await t.cpc_update2jud(key, "f20", exat=exat)
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_update2jud_exat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)

        for i in range(1, 20):
            assert await t.cpc_update(key, f"f{i}")
        await t.cpc_update2jud(key, "f20", exat=expire_at)
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_update2jud_pxat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(expire_at.timetuple())) * 1000

        for i in range(1, 20):
            assert await t.cpc_update(key, f"f{i}")
        await t.cpc_update2jud(key, "f20", pxat=pxat)
        # due to network delay, pttl may be greater than 10000.
        assert 0 < await t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    @pytest.mark.asyncio
    async def test_cpc_update2jud_pxat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)

        for i in range(1, 20):
            assert await t.cpc_update(key, f"f{i}")
        await t.cpc_update2jud(key, "f20", pxat=expire_at)
        # due to network delay, pttl may be greater than 10000.
        assert 0 < await t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    @pytest.mark.asyncio
    async def test_cpc_array_update(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())

        assert await t.cpc_array_update(key, 1645584510000, item, size=120, win=10000)

    @pytest.mark.asyncio
    async def test_cpc_array_update_ex(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())

        assert await t.cpc_array_update(
            key, 1645584510000, item, ex=10, size=120, win=10000
        )
        assert 0 < await t.ttl(key) <= 10

        # ex should not be a float.
        with pytest.raises(DataError):
            await t.cpc_array_update(
                key, 1645584510000, item, ex=10.0, size=120, win=10000
            )

    @pytest.mark.asyncio
    async def test_cpc_array_update_ex_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(seconds=10)

        assert await t.cpc_array_update(
            key, 1645584510000, item, ex=expire_at, size=120, win=10000
        )
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_array_update_px(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())

        assert await t.cpc_array_update(
            key, 1645584510000, item, px=10000, size=120, win=10000
        )
        assert 0 < await t.pttl(key) <= 10000

        # px should not be a float.
        with pytest.raises(DataError):
            await t.cpc_array_update(
                key, 1645584510000, item, px=10000.0, size=120, win=10000
            )

    @pytest.mark.asyncio
    async def test_cpc_array_update_px_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(milliseconds=10000)

        assert await t.cpc_array_update(
            key, 1645584510000, item, px=expire_at, size=120, win=10000
        )
        assert 0 < await t.pttl(key) <= 10000

    @pytest.mark.asyncio
    async def test_cpc_array_update_exat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)
        exat = int(time.mktime(expire_at.timetuple()))

        assert await t.cpc_array_update(
            key, 1645584510000, item, exat=exat, size=120, win=10000
        )
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_array_update_exat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)

        assert await t.cpc_array_update(
            key, 1645584510000, item, exat=expire_at, size=120, win=10000
        )
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_array_update_pxat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(expire_at.timetuple())) * 1000

        assert await t.cpc_array_update(
            key, 1645584510000, item, pxat=pxat, size=120, win=10000
        )
        # due to network delay, pttl may be greater than 10000.
        assert 0 < await t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    @pytest.mark.asyncio
    async def test_cpc_array_update_pxat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)

        assert await t.cpc_array_update(
            key, 1645584510000, item, pxat=expire_at, size=120, win=10000
        )
        # due to network delay, pttl may be greater than 10000.
        assert 0 < await t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    @pytest.mark.asyncio
    async def test_cpc_array_estimate(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        timestamp = 1645584510000
        item = "item_" + str(uuid.uuid4())

        assert await t.cpc_array_update(key, timestamp, item, size=120, win=10000)
        assert await t.cpc_array_estimate(key, timestamp) == pytest.approx(1, 0.001)

    @pytest.mark.asyncio
    async def test_cpc_array_estimate_range(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())

        assert await t.cpc_array_update(key, 1645584510000, item, size=120, win=10000)
        assert await t.cpc_array_estimate_range(key, 1645584510000, 1645584550000) == [
            pytest.approx(1, 0.001),
            pytest.approx(0, 0.001),
            pytest.approx(0, 0.001),
            pytest.approx(0, 0.001),
            pytest.approx(0, 0.001),
        ]

    @pytest.mark.asyncio
    async def test_cpc_array_estimate_range_merge(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())

        assert await t.cpc_array_update(key, 1645584510000, item, size=120, win=10000)
        assert await t.cpc_array_estimate_range_merge(
            key, 1645584510000, 100000
        ) == pytest.approx(1, 0.001)

    @pytest.mark.asyncio
    async def test_cpc_array_update2est(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())
        timestamp = 1645584510000

        assert await t.cpc_array_update2est(
            key, timestamp, item, size=120, win=10000
        ) == pytest.approx(1, 0.001)

    @pytest.mark.asyncio
    async def test_cpc_array_update2est_ex(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())

        assert await t.cpc_array_update2est(
            key, 1645584510000, item, ex=10, size=120, win=10000
        ) == pytest.approx(1, 0.001)
        assert 0 < await t.ttl(key) <= 10

        # ex should not be a float.
        with pytest.raises(DataError):
            await t.cpc_array_update2est(
                key, 1645584510000, item, ex=10.0, size=120, win=10000
            ) == pytest.approx(1, 0.001)

    @pytest.mark.asyncio
    async def test_cpc_array_update2est_ex_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(seconds=10)

        assert await t.cpc_array_update2est(
            key, 1645584510000, item, ex=expire_at, size=120, win=10000
        ) == pytest.approx(1, 0.001)
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_array_update2est_px(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())

        assert await t.cpc_array_update2est(
            key, 1645584510000, item, px=10000, size=120, win=10000
        ) == pytest.approx(1, 0.001)
        assert 0 < await t.pttl(key) <= 10000

        # px should not be a float.
        with pytest.raises(DataError):
            await t.cpc_array_update2est(
                key, 1645584510000, item, px=10000.0, size=120, win=10000
            )

    @pytest.mark.asyncio
    async def test_cpc_array_update2est_px_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(milliseconds=10000)

        assert await t.cpc_array_update2est(
            key, 1645584510000, item, px=expire_at, size=120, win=10000
        ) == pytest.approx(1, 0.001)
        assert 0 < await t.pttl(key) <= 10000

    @pytest.mark.asyncio
    async def test_cpc_array_update2est_exat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)
        exat = int(time.mktime(expire_at.timetuple()))

        assert await t.cpc_array_update2est(
            key, 1645584510000, item, exat=exat, size=120, win=10000
        ) == pytest.approx(1, 0.001)
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_array_update2est_exat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)

        assert await t.cpc_array_update2est(
            key, 1645584510000, item, exat=expire_at, size=120, win=10000
        ) == pytest.approx(1, 0.001)
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_array_update2est_pxat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(expire_at.timetuple())) * 1000

        assert await t.cpc_array_update2est(
            key, 1645584510000, item, pxat=pxat, size=120, win=10000
        ) == pytest.approx(1, 0.001)
        # due to network delay, pttl may be greater than 10000.
        assert 0 < await t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    @pytest.mark.asyncio
    async def test_cpc_array_update2est_pxat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)

        assert await t.cpc_array_update2est(
            key, 1645584510000, item, pxat=expire_at, size=120, win=10000
        ) == pytest.approx(1, 0.001)
        # due to network delay, pttl may be greater than 10000.
        assert 0 < await t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    @pytest.mark.asyncio
    async def test_cpc_array_update2jud(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        for i in range(1, 20):
            assert await t.cpc_array_update(
                key, 1645584510000, f"f{i}", size=120, win=10000
            )
        result: CpcUpdate2judResult = await t.cpc_array_update2jud(
            key, 1645584510000, "f20", size=120, win=10000
        )
        assert result.estimated_value == pytest.approx(20, 0.001)
        assert result.difference == pytest.approx(1, 0.001)

    @pytest.mark.asyncio
    async def test_cpc_array_update2jud_ex(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        for i in range(1, 20):
            assert await t.cpc_array_update(
                key, 1645584510000, f"f{i}", size=120, win=10000
            )
        await t.cpc_array_update2jud(
            key, 1645584510000, "f20", ex=10, size=120, win=10000
        )
        assert 0 < await t.ttl(key) <= 10

        # ex should not be a float.
        with pytest.raises(DataError):
            await t.cpc_array_update2jud(key, 1645584510000, "f21", ex=10.0)

    @pytest.mark.asyncio
    async def test_cpc_array_update2jud_ex_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(seconds=10)

        for i in range(1, 20):
            assert await t.cpc_array_update(
                key, 1645584510000, f"f{i}", size=120, win=10000
            )
        await t.cpc_array_update2jud(
            key, 1645584510000, "f20", ex=expire_at, size=120, win=10000
        )
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_array_update2jud_px(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        for i in range(1, 20):
            assert await t.cpc_array_update(
                key, 1645584510000, f"f{i}", size=120, win=10000
            )
        await t.cpc_array_update2jud(
            key, 1645584510000, "f20", px=10000, size=120, win=10000
        )
        assert 0 < await t.pttl(key) <= 10000

        # px should not be a float.
        with pytest.raises(DataError):
            await t.cpc_array_update2jud(key, 1645584510000, "f21", px=10000.0)

    @pytest.mark.asyncio
    async def test_cpc_array_update2jud_px_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(milliseconds=10000)

        for i in range(1, 20):
            assert await t.cpc_array_update(
                key, 1645584510000, f"f{i}", size=120, win=10000
            )
        await t.cpc_array_update2jud(
            key, 1645584510000, "f20", px=expire_at, size=120, win=10000
        )
        assert 0 < await t.pttl(key) <= 10000

    @pytest.mark.asyncio
    async def test_cpc_array_update2jud_exat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)
        exat = int(time.mktime(expire_at.timetuple()))

        for i in range(1, 20):
            assert await t.cpc_array_update(
                key, 1645584510000, f"f{i}", size=120, win=10000
            )
        await t.cpc_array_update2jud(
            key, 1645584510000, "f20", exat=exat, size=120, win=10000
        )
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_array_update2jud_exat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)

        for i in range(1, 20):
            assert await t.cpc_array_update(
                key, 1645584510000, f"f{i}", size=120, win=10000
            )
        await t.cpc_array_update2jud(
            key, 1645584510000, "f20", exat=expire_at, size=120, win=10000
        )
        assert 0 < await t.ttl(key) <= 10

    @pytest.mark.asyncio
    async def test_cpc_array_update2jud_pxat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(expire_at.timetuple())) * 1000

        for i in range(1, 20):
            assert await t.cpc_array_update(
                key, 1645584510000, f"f{i}", size=120, win=10000
            )
        await t.cpc_array_update2jud(
            key, 1645584510000, "f20", pxat=pxat, size=120, win=10000
        )
        # due to network delay, pttl may be greater than 10000.
        assert 0 < await t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    @pytest.mark.asyncio
    async def test_cpc_array_update2jud_pxat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = await get_server_time(t) + datetime.timedelta(seconds=10)

        for i in range(1, 20):
            assert await t.cpc_array_update(
                key, 1645584510000, f"f{i}", size=120, win=10000
            )
        await t.cpc_array_update2jud(
            key, 1645584510000, "f20", pxat=expire_at, size=120, win=10000
        )
        # due to network delay, pttl may be greater than 10000.
        assert 0 < await t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    @pytest.mark.asyncio
    async def test_cpc_update2jud_result_eq(self):
        assert CpcUpdate2judResult(3.14, 3.14) == CpcUpdate2judResult(3.14, 3.14)
        assert not CpcUpdate2judResult(3.14, 3.14) == CpcUpdate2judResult(3.14, 1.2)
        assert not CpcUpdate2judResult(3.14, 3.14) == 1

    @pytest.mark.asyncio
    async def test_cpc_update2jud_result_ne(self):
        assert not CpcUpdate2judResult(3.14, 3.14) != CpcUpdate2judResult(3.14, 3.14)
        assert CpcUpdate2judResult(3.14, 3.14) != CpcUpdate2judResult(3.14, 1.2)
        assert CpcUpdate2judResult(3.14, 3.14) != 1

    @pytest.mark.asyncio
    async def test_cpc_update2jud_result_repr(self):
        assert (
            str(CpcUpdate2judResult(3.14, 3.14))
            == "{estimated_value: 3.14, difference: 3.14}"
        )
