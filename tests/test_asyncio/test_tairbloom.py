import uuid
import pytest

from tair import Tair


class TestTairBloom:
    @pytest.mark.asyncio
    async def test_bf_reserve(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert await t.bf_reserve(key, 0.01, 100)

    @pytest.mark.asyncio
    async def test_bf_add(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())

        assert await t.bf_reserve(key, 0.01, 100)
        assert await t.bf_add(key, item) == 1

    @pytest.mark.asyncio
    async def test_bf_add_maybe_exist(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())

        assert await t.bf_reserve(key, 0.01, 100)
        assert await t.bf_add(key, item) == 1
        assert await t.bf_add(key, item) == 0

    @pytest.mark.asyncio
    async def test_bf_madd(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item1 = "item_" + str(uuid.uuid4())
        item2 = "item_" + str(uuid.uuid4())
        item3 = "item_" + str(uuid.uuid4())

        assert await t.bf_reserve(key, 0.01, 100)
        assert await t.bf_add(key, item2) == 1
        assert await t.bf_madd(key, (item1, item2, item3)) == [1, 0, 1]

    @pytest.mark.asyncio
    async def test_bf_exists(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item = "item_" + str(uuid.uuid4())

        assert await t.bf_reserve(key, 0.01, 100)
        assert await t.bf_exists(key, item) == 0
        assert await t.bf_add(key, item) == 1
        assert await t.bf_exists(key, item) == 1

    @pytest.mark.asyncio
    async def test_bf_mexists(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item1 = "item_" + str(uuid.uuid4())
        item2 = "item_" + str(uuid.uuid4())
        item3 = "item_" + str(uuid.uuid4())

        assert await t.bf_reserve(key, 0.01, 100)
        assert await t.bf_mexists(key, (item1, item2, item3)) == [0, 0, 0]
        assert await t.bf_madd(key, (item1, item2, item3)) == [1, 1, 1]
        assert await t.bf_mexists(key, (item1, item2, item3)) == [1, 1, 1]

    @pytest.mark.asyncio
    async def test_bf_insert(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item1 = "item_" + str(uuid.uuid4())
        item2 = "item_" + str(uuid.uuid4())
        item3 = "item_" + str(uuid.uuid4())

        assert await t.bf_insert(key, [item1, item2, item3], 100, 0.01) == [1, 1, 1]
        assert await t.bf_insert(key, [item1, item2, item3], 100, 0.01) == [0, 0, 0]

    @pytest.mark.asyncio
    async def test_bf_insert_nocreate(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        item1 = "item_" + str(uuid.uuid4())
        item2 = "item_" + str(uuid.uuid4())
        item3 = "item_" + str(uuid.uuid4())

        await t.bf_reserve(key, 0.01, 100)
        assert await t.bf_insert(key, [item1, item2, item3], nocreate=True) == [1, 1, 1]
