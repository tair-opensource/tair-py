import uuid

from .conftest import get_tair_client
from pytest import approx

from tair import TrScanResult


class TestTairRoaring:
    def test_tr_setbit(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())

        assert t.tr_setbit(key, 3, 1) == 0
        assert t.tr_getbit(key, 3) == 1
        assert t.tr_setbit(key, 3, 0) == 1
        assert t.tr_getbit(key, 3) == 0

    def test_tr_setbits(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        offsets1 = [3, 4, 5]
        offsets2 = [8, 9]

        assert t.tr_setbits(key, offsets1) == 3
        assert t.tr_setbits(key, offsets2) == 5

    def test_tr_clearbits(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        offsets1 = [3, 4, 5]
        offsets2 = [4, 5, 6]

        assert t.tr_setbits(key, offsets1) == 3
        assert t.tr_clearbits(key, offsets2) == 2

    def test_tr_clearbits_not_exists(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        offsets = [3, 4, 5]

        assert t.tr_clearbits(key, offsets) == 0

    def test_tr_setrange(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())

        assert t.tr_setrange(key, 1, 3) == 3
        assert t.tr_getbits(key, [0, 1, 2, 3]) == [0, 1, 1, 1]

    def test_tr_appendbitarray(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        offsets = [0, 1, 2, 3, 4, 5]

        assert t.tr_setbits(key, offsets) == 6
        assert t.tr_appendbitarray(key, 1, "1101") == 5
        assert t.tr_getbits(key, offsets) == [1, 1, 1, 1, 0, 1]

    def test_tr_fliprange(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key, [0, 2, 3, 5]) == 4
        assert t.tr_fliprange(key, 0, 5) == 2
        assert t.tr_getbits(key, [0, 1, 2, 3, 4, 5]) == [0, 1, 0, 0, 1, 0]

    def test_tr_appendintarray(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        offsets = [9, 10]

        assert t.tr_setbit(key, 0, 1) == 0
        assert t.tr_appendintarray(key, offsets)
        assert t.tr_getbit(key, 0) == 1
        assert t.tr_getbits(key, offsets) == [1, 1]

    def test_tr_setintarray(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        offsets = [2, 4, 5, 6]

        assert t.tr_setbit(key, 0, 1) == 0
        assert t.tr_setintarray(key, offsets)
        assert t.tr_getbit(key, 0) == 0
        assert t.tr_getbits(key, offsets) == [1, 1, 1, 1]

    def test_tr_setbitarray(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        offsets = [i for i in range(8)]

        assert t.tr_setintarray(key, offsets)
        assert t.tr_getbits(key, offsets) == [1] * 8
        assert t.tr_setbitarray(key, "10101001")
        assert t.tr_getbits(key, offsets) == [1, 0, 1, 0, 1, 0, 0, 1]

    def test_tr_bitop_and(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        key3 = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert t.tr_setbits(key2, [2, 3]) == 2
        assert t.tr_bitop(key3, "AND", (key1, key2)) == 2
        assert t.tr_getbits(key3, [i for i in range(6)]) == [0, 0, 1, 1, 0, 0]

    def test_tr_bitop_or(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        key3 = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert t.tr_setbits(key2, [1, 4]) == 2
        assert t.tr_bitop(key3, "OR", (key1, key2)) == 6
        assert t.tr_getbits(key3, [i for i in range(6)]) == [1, 1, 1, 1, 1, 1]

    def test_tr_bitop_xor(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        key3 = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert t.tr_setbits(key2, [1, 4]) == 2
        assert t.tr_bitop(key3, "XOR", (key1, key2)) == 6
        assert t.tr_getbits(key3, [i for i in range(6)]) == [1, 1, 1, 1, 1, 1]

    def test_tr_bitop_not(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert t.tr_bitop(key2, "NOT", (key1,)) == 2
        assert t.tr_getbits(key2, [i for i in range(6)]) == [0, 1, 0, 0, 1, 0]

    def test_tr_bitop_diff(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        key3 = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert t.tr_setbits(key2, [1, 4]) == 2
        assert t.tr_bitop(key3, "DIFF", (key1, key2)) == 4
        assert t.tr_getbits(key3, [i for i in range(6)]) == [1, 0, 1, 1, 0, 1]

    def test_tr_bitopcard_and(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert t.tr_setbits(key2, [2, 3]) == 2
        assert t.tr_bitopcard("AND", (key1, key2)) == 2

    def test_tr_bitopcard_or(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert t.tr_setbits(key2, [1, 4]) == 2
        assert t.tr_bitopcard("OR", (key1, key2)) == 6

    def test_tr_bitopcard_xor(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert t.tr_setbits(key2, [1, 4]) == 2
        assert t.tr_bitopcard("XOR", (key1, key2)) == 6

    def test_tr_bitopcard_not(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key, [0, 2, 3, 5]) == 4
        assert t.tr_bitopcard("NOT", (key,)) == 2

    def test_tr_bitopcard_diff(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key1, [0, 2, 3, 5]) == 4
        assert t.tr_setbits(key2, [1, 4]) == 2
        assert t.tr_bitopcard("DIFF", (key1, key2)) == 4

    def test_tr_optimize(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key, [0, 2, 3, 5]) == 4
        assert t.tr_optimize(key)

    def test_tr_bitcount(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key, [1, 2, 3, 10]) == 4
        assert t.tr_bitcount(key) == 4
        assert t.tr_bitcount(key, start=2, end=3) == 2

    def test_tr_bitpos(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key, [1, 2, 3, 10]) == 4
        assert t.tr_bitpos(key, 1, 2) == 2

    def test_tr_scan(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key, [1, 2, 3, 10]) == 4
        assert t.tr_scan(key, 0, 2) == TrScanResult(3, [1, 2])

    def test_tr_range(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key, [0, 2, 3, 5]) == 4
        assert t.tr_range(key, 0, 5) == [0, 2, 3, 5]

    def test_tr_rangebitarray(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key, [0, 2, 3, 5]) == 4
        assert t.tr_rangebitarray(key, 0, 5) == "101101"

    def test_tr_min(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())

        assert t.tr_min(key) == -1
        assert t.tr_setbit(key, 0, 0) == 0
        assert t.tr_min(key) == -1
        assert t.tr_setbits(key, [1, 2, 3, 10]) == 4
        assert t.tr_min(key) == 1

    def test_tr_max(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())

        assert t.tr_max(key) == -1
        assert t.tr_setbit(key, 0, 0) == 0
        assert t.tr_max(key) == -1
        assert t.tr_setbits(key, [1, 2, 3, 10]) == 4
        assert t.tr_max(key) == 10

    def test_tr_stat(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key, [1, 2, 3, 5, 8]) == 5
        assert type(t.tr_stat(key)) == bytes
        assert type(t.tr_stat(key, json=True)) == bytes

    def test_tr_jaccard(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        key3 = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key1, [1, 2, 3, 5, 8]) == 5
        assert t.tr_setbits(key2, [1, 2, 3, 5, 8]) == 5
        assert t.tr_setbits(key3, [1, 3, 5]) == 3
        assert t.tr_jaccard(key1, key2) == approx(1.0)
        assert t.tr_jaccard(key2, key3) == approx(0.6)

    def test_tr_contains(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key1, [1, 2, 3, 10]) == 4
        assert t.tr_setbits(key2, [1, 2]) == 2
        assert t.tr_contains(key1, key2) == 0
        assert t.tr_contains(key2, key1) == 1

    def test_tr_rank(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())

        assert t.tr_setbits(key, [1, 2, 3, 10]) == 4
        assert t.tr_rank(key, 10) == 4

    def test_tr_scan_result_eq(self):
        assert TrScanResult(3, [1, 2]) == TrScanResult(3, [1, 2])
        assert not TrScanResult(3, [1, 2]) == TrScanResult(3, [1, 3])
        assert not TrScanResult(3, [1, 2]) == 1

    def test_tr_scan_result_ne(self):
        assert not TrScanResult(3, [1, 2]) != TrScanResult(3, [1, 2])
        assert TrScanResult(3, [1, 2]) != TrScanResult(3, [1, 3])
        assert TrScanResult(3, [1, 2]) != 1

    def test_tr_scan_result_repr(self):
        assert str(TrScanResult(3, [1, 2])) == f"{{start_offset: 3, offsets: [1, 2]}}"
