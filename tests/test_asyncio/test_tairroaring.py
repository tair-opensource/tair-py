import uuid

import pytest

from tair import Tair, TrScanResult


class TestTairRoaring:
    @pytest.mark.asyncio
    async def test_tr_setbit(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert await t.tr_setbit(key, 3, 1) == 0
        assert await t.tr_getbit(key, 3) == 1
        assert await t.tr_setbit(key, 3, 0) == 1
        assert await t.tr_getbit(key, 3) == 0

    @pytest.mark.asyncio
    async def test_tr_setbits(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        offsets1 = [3, 4, 5]
        offsets2 = [8, 9]

        assert await t.tr_setbits(key, offsets1) == 3
        assert await t.tr_setbits(key, offsets2) == 5

    @pytest.mark.asyncio
    async def test_tr_clearbits(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        offsets1 = [3, 4, 5]
        offsets2 = [4, 5, 6]

        assert await t.tr_setbits(key, offsets1) == 3
        assert await t.tr_clearbits(key, offsets2) == 2

    @pytest.mark.asyncio
    async def test_tr_clearbits_not_exists(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        offsets = [3, 4, 5]

        assert await t.tr_clearbits(key, offsets) == 0

    @pytest.mark.asyncio
    async def test_tr_setrange(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert await t.tr_setrange(key, 1, 3) == 3
        assert await t.tr_getbits(key, [0, 1, 2, 3]) == [0, 1, 1, 1]

    @pytest.mark.asyncio
    async def test_tr_appendbitarray(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        offsets = [0, 1, 2, 3, 4, 5]

        assert await t.tr_setbits(key, offsets) == 6
        assert await t.tr_appendbitarray(key, 1, "1101") == 5
        assert await t.tr_getbits(key, offsets) == [1, 1, 1, 1, 0, 1]

    @pytest.mark.asyncio
    async def test_tr_fliprange(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key, [0, 2, 3, 5]) == 4
        assert await t.tr_fliprange(key, 0, 5) == 2
        assert await t.tr_getbits(key, [0, 1, 2, 3, 4, 5]) == [0, 1, 0, 0, 1, 0]

    @pytest.mark.asyncio
    async def test_tr_appendintarray(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        offsets = [9, 10]

        assert await t.tr_setbit(key, 0, 1) == 0
        assert await t.tr_appendintarray(key, offsets)
        assert await t.tr_getbit(key, 0) == 1
        assert await t.tr_getbits(key, offsets) == [1, 1]

    @pytest.mark.asyncio
    async def test_tr_setintarray(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        offsets = [2, 4, 5, 6]

        assert await t.tr_setbit(key, 0, 1) == 0
        assert await t.tr_setintarray(key, offsets)
        assert await t.tr_getbit(key, 0) == 0
        assert await t.tr_getbits(key, offsets) == [1, 1, 1, 1]

    @pytest.mark.asyncio
    async def test_tr_setbitarray(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        offsets = [i for i in range(8)]

        assert await t.tr_setintarray(key, offsets)
        assert await t.tr_getbits(key, offsets) == [1] * 8
        assert await t.tr_setbitarray(key, "10101001")
        assert await t.tr_getbits(key, offsets) == [1, 0, 1, 0, 1, 0, 0, 1]

    @pytest.mark.asyncio
    async def test_tr_bitop_and(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        key3 = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert await t.tr_setbits(key2, [2, 3]) == 2
        assert await t.tr_bitop(key3, "AND", (key1, key2)) == 2
        assert await t.tr_getbits(key3, [i for i in range(6)]) == [0, 0, 1, 1, 0, 0]

    @pytest.mark.asyncio
    async def test_tr_bitop_or(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        key3 = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert await t.tr_setbits(key2, [1, 4]) == 2
        assert await t.tr_bitop(key3, "OR", (key1, key2)) == 6
        assert await t.tr_getbits(key3, [i for i in range(6)]) == [1, 1, 1, 1, 1, 1]

    @pytest.mark.asyncio
    async def test_tr_bitop_xor(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        key3 = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert await t.tr_setbits(key2, [1, 4]) == 2
        assert await t.tr_bitop(key3, "XOR", (key1, key2)) == 6
        assert await t.tr_getbits(key3, [i for i in range(6)]) == [1, 1, 1, 1, 1, 1]

    @pytest.mark.asyncio
    async def test_tr_bitop_not(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert await t.tr_bitop(key2, "NOT", (key1,)) == 2
        assert await t.tr_getbits(key2, [i for i in range(6)]) == [0, 1, 0, 0, 1, 0]

    @pytest.mark.asyncio
    async def test_tr_bitop_diff(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        key3 = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert await t.tr_setbits(key2, [1, 4]) == 2
        assert await t.tr_bitop(key3, "DIFF", (key1, key2)) == 4
        assert await t.tr_getbits(key3, [i for i in range(6)]) == [1, 0, 1, 1, 0, 1]

    @pytest.mark.asyncio
    async def test_tr_bitopcard_and(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert await t.tr_setbits(key2, [2, 3]) == 2
        assert await t.tr_bitopcard("AND", (key1, key2)) == 2

    @pytest.mark.asyncio
    async def test_tr_bitopcard_or(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert await t.tr_setbits(key2, [1, 4]) == 2
        assert await t.tr_bitopcard("OR", (key1, key2)) == 6

    @pytest.mark.asyncio
    async def test_tr_bitopcard_xor(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert await t.tr_setbits(key2, [1, 4]) == 2
        assert await t.tr_bitopcard("XOR", (key1, key2)) == 6

    @pytest.mark.asyncio
    async def test_tr_bitopcard_not(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key, [0, 2, 3, 5]) == 4
        assert await t.tr_bitopcard("NOT", (key,)) == 2

    @pytest.mark.asyncio
    async def test_tr_bitopcard_diff(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert await t.tr_setbits(key2, [1, 4]) == 2
        assert await t.tr_bitopcard("DIFF", (key1, key2)) == 4

    @pytest.mark.asyncio
    async def test_tr_optimize(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key, [0, 2, 3, 5]) == 4
        assert await t.tr_optimize(key)

    @pytest.mark.asyncio
    async def test_tr_bitcount(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key, [1, 2, 3, 10]) == 4
        assert await t.tr_bitcount(key) == 4
        assert await t.tr_bitcount(key, start=2, end=3) == 2

    @pytest.mark.asyncio
    async def test_tr_bitpos(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key, [1, 2, 3, 10]) == 4
        assert await t.tr_bitpos(key, 1, 2) == 2

    @pytest.mark.asyncio
    async def test_tr_scan(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key, [1, 2, 3, 10]) == 4
        assert await t.tr_scan(key, 0, 2) == TrScanResult(3, [1, 2])

    @pytest.mark.asyncio
    async def test_tr_range(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key, [0, 2, 3, 5]) == 4
        assert await t.tr_range(key, 0, 5) == [0, 2, 3, 5]

    @pytest.mark.asyncio
    async def test_tr_rangebitarray(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key, [0, 2, 3, 5]) == 4
        assert await t.tr_rangebitarray(key, 0, 5) == "101101"

    @pytest.mark.asyncio
    async def test_tr_min(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert await t.tr_min(key) == -1
        assert await t.tr_setbit(key, 0, 0) == 0
        assert await t.tr_min(key) == -1
        assert await t.tr_setbits(key, [1, 2, 3, 10]) == 4
        assert await t.tr_min(key) == 1

    @pytest.mark.asyncio
    async def test_tr_max(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert await t.tr_max(key) == -1
        assert await t.tr_setbit(key, 0, 0) == 0
        assert await t.tr_max(key) == -1
        assert await t.tr_setbits(key, [1, 2, 3, 10]) == 4
        assert await t.tr_max(key) == 10

    @pytest.mark.asyncio
    async def test_tr_stat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key, [1, 2, 3, 5, 8]) == 5
        assert type(await t.tr_stat(key)) == bytes
        assert type(await t.tr_stat(key, json=True)) == bytes

    @pytest.mark.asyncio
    async def test_tr_jaccard(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        key3 = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key1, [1, 2, 3, 5, 8]) == 5
        assert await t.tr_setbits(key2, [1, 2, 3, 5, 8]) == 5
        assert await t.tr_setbits(key3, [1, 3, 5]) == 3
        assert await t.tr_jaccard(key1, key2) == pytest.approx(1.0)
        assert await t.tr_jaccard(key2, key3) == pytest.approx(0.6)

    @pytest.mark.asyncio
    async def test_tr_contains(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key1, [1, 2, 3, 10]) == 4
        assert await t.tr_setbits(key2, [1, 2]) == 2
        assert await t.tr_contains(key1, key2) == 0
        assert await t.tr_contains(key2, key1) == 1

    @pytest.mark.asyncio
    async def test_tr_rank(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert await t.tr_setbits(key, [1, 2, 3, 10]) == 4
        assert await t.tr_rank(key, 10) == 4

    @pytest.mark.asyncio
    async def test_tr_scan_result_eq(self):
        assert TrScanResult(3, [1, 2]) == TrScanResult(3, [1, 2])
        assert not TrScanResult(3, [1, 2]) == TrScanResult(3, [1, 3])
        assert not TrScanResult(3, [1, 2]) == 1

    @pytest.mark.asyncio
    async def test_tr_scan_result_ne(self):
        assert not TrScanResult(3, [1, 2]) != TrScanResult(3, [1, 2])
        assert TrScanResult(3, [1, 2]) != TrScanResult(3, [1, 3])
        assert TrScanResult(3, [1, 2]) != 1

    @pytest.mark.asyncio
    async def test_tr_scan_result_repr(self):
        assert str(TrScanResult(3, [1, 2])) == f"{{start_offset: 3, offsets: [1, 2]}}"
