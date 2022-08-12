import pytest
import uuid
from pytest import raises
from tair import DataError, TairZsetItem, Tair


class TestTairZset:
    @pytest.mark.asyncio
    async def test_exzadd_success(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20

        assert await t.exzadd(key, {member1: score1, member2: score2}) == 2
        assert await t.exzrange(key, 0, -1, True) == [
            TairZsetItem(member1.encode(), str(score1)),
            TairZsetItem(member2.encode(), str(score2)),
        ]

    @pytest.mark.asyncio
    async def test_exzadd_add_and_update(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score2_2 = 25
        score3 = 30

        assert await t.exzadd(key, {member1: score1, member2: score2}) == 2
        assert (
            await t.exzadd(key, {member1: score1, member2: score2_2, member3: score3})
            == 1
        )
        assert await t.exzrange(key, 0, -1, True) == [
            TairZsetItem(member1.encode(), str(score1)),
            TairZsetItem(member2.encode(), str(score2_2)),
            TairZsetItem(member3.encode(), str(score3)),
        ]

    @pytest.mark.asyncio
    async def test_exzadd_ch(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score2_2 = 25
        score3 = 30

        assert await t.exzadd(key, {member1: score1, member2: score2}) == 2
        assert (
            await t.exzadd(
                key, {member1: score1, member2: score2_2, member3: score3}, ch=True
            )
            == 2
        )
        assert await t.exzrange(key, 0, -1, True) == [
            TairZsetItem(member1.encode(), str(score1)),
            TairZsetItem(member2.encode(), str(score2_2)),
            TairZsetItem(member3.encode(), str(score3)),
        ]

    @pytest.mark.asyncio
    async def test_exzadd_nx(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score2_2 = 25
        score3 = 30

        assert await t.exzadd(key, {member1: score1, member2: score2}) == 2
        assert (
            await t.exzadd(
                key, {member1: score1, member2: score2_2, member3: score3}, nx=True
            )
            == 1
        )
        assert await t.exzrange(key, 0, -1, True) == [
            TairZsetItem(member1.encode(), str(score1)),
            TairZsetItem(member2.encode(), str(score2)),
            TairZsetItem(member3.encode(), str(score3)),
        ]

    @pytest.mark.asyncio
    async def test_exzadd_xx(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score2_2 = 25
        score3 = 30

        assert await t.exzadd(key, {member1: score1, member2: score2}) == 2
        assert (
            await t.exzadd(
                key, {member1: score1, member2: score2_2, member3: score3}, xx=True
            )
            == 0
        )
        assert await t.exzrange(key, 0, -1, True) == [
            TairZsetItem(member1.encode(), str(score1)),
            TairZsetItem(member2.encode(), str(score2_2)),
        ]

    @pytest.mark.asyncio
    async def test_exzadd_incr(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member = "member_" + str(uuid.uuid4())

        assert await t.exzadd(key, {member: 10}) == 1
        assert await t.exzadd(key, {member: 20}, incr=True) == b"30"
        assert await t.exzrange(key, 0, -1, True) == [
            TairZsetItem(member.encode(), "30"),
        ]

    @pytest.mark.asyncio
    async def test_exzincrby(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member = "member_" + str(uuid.uuid4())

        assert await t.exzadd(key, {member: 10})
        assert await t.exzincrby(key, 20, member) == "30"

    @pytest.mark.asyncio
    async def test_exzscore(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())

        assert await t.exzadd(key1, {member1: 10}) == 1
        assert await t.exzscore(key2, member1) is None
        assert await t.exzscore(key1, member2) is None
        assert await t.exzscore(key1, member1) == "10"

    @pytest.mark.asyncio
    async def test_exzrange(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        member4 = "member_" + str(uuid.uuid4())
        member5 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score3 = 30
        score4 = 40
        score5 = 50

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )
        # index starts from 0.
        assert await t.exzrange(key, 1, 3) == [
            TairZsetItem(member2.encode(), None),
            TairZsetItem(member3.encode(), None),
            TairZsetItem(member4.encode(), None),
        ]

        # index starts from 0.
        assert await t.exzrange(key, 1, 3, True) == [
            TairZsetItem(member2.encode(), str(score2)),
            TairZsetItem(member3.encode(), str(score3)),
            TairZsetItem(member4.encode(), str(score4)),
        ]

    @pytest.mark.asyncio
    async def test_exzrevrange(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        member4 = "member_" + str(uuid.uuid4())
        member5 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score3 = 30
        score4 = 40
        score5 = 50

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )
        # index starts from 0.
        assert await t.exzrevrange(key, 1, 3) == [
            TairZsetItem(member4.encode(), None),
            TairZsetItem(member3.encode(), None),
            TairZsetItem(member2.encode(), None),
        ]

        # index starts from 0.
        assert await t.exzrevrange(key, 1, 3, True) == [
            TairZsetItem(member4.encode(), str(score4)),
            TairZsetItem(member3.encode(), str(score3)),
            TairZsetItem(member2.encode(), str(score2)),
        ]

    @pytest.mark.asyncio
    async def test_exzrangebyscore(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        member4 = "member_" + str(uuid.uuid4())
        member5 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score3 = 30
        score4 = 40
        score5 = 50

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )

        assert await t.exzrangebyscore(key, 20, 40) == [
            TairZsetItem(member2.encode(), None),
            TairZsetItem(member3.encode(), None),
            TairZsetItem(member4.encode(), None),
        ]

        assert await t.exzrangebyscore(key, 20, 40, True) == [
            TairZsetItem(member2.encode(), str(score2)),
            TairZsetItem(member3.encode(), str(score3)),
            TairZsetItem(member4.encode(), str(score4)),
        ]

    @pytest.mark.asyncio
    async def test_exzrangebyscore_limit(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        member4 = "member_" + str(uuid.uuid4())
        member5 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score3 = 30
        score4 = 40
        score5 = 50

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )

        assert await t.exzrangebyscore(key, 20, 40, True, offset=1, count=1) == [
            TairZsetItem(member3.encode(), str(score3)),
        ]

        with raises(DataError):
            await t.exzrangebyscore(key, 20, 40, True, offset=1)

        with raises(DataError):
            await t.exzrangebyscore(key, 20, 40, True, count=1)

    @pytest.mark.asyncio
    async def test_exzrevrangebyscore(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        member4 = "member_" + str(uuid.uuid4())
        member5 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score3 = 30
        score4 = 40
        score5 = 50

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )

        assert await t.exzrevrangebyscore(key, 40, 20) == [
            TairZsetItem(member4.encode(), None),
            TairZsetItem(member3.encode(), None),
            TairZsetItem(member2.encode(), None),
        ]

        assert await t.exzrevrangebyscore(key, 40, 20, True) == [
            TairZsetItem(member4.encode(), str(score4)),
            TairZsetItem(member3.encode(), str(score3)),
            TairZsetItem(member2.encode(), str(score2)),
        ]

    @pytest.mark.asyncio
    async def test_exzrevrangebyscore_limit(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        member4 = "member_" + str(uuid.uuid4())
        member5 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score3 = 30
        score4 = 40
        score5 = 50

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )

        assert await t.exzrevrangebyscore(key, 40, 20, True, offset=1, count=1) == [
            TairZsetItem(member3.encode(), str(score3)),
        ]

        with raises(DataError):
            await t.exzrevrangebyscore(key, 40, 20, True, offset=1)

        with raises(DataError):
            await t.exzrevrangebyscore(key, 40, 20, True, count=1)

    @pytest.mark.asyncio
    async def test_exzrangebylex(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_a"
        member2 = "member_b"
        member3 = "member_c"
        member4 = "member_d"
        member5 = "member_e"
        score1 = 0
        score2 = 0
        score3 = 0
        score4 = 0
        score5 = 0

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )

        assert await t.exzrangebylex(key, f"[{member2}", f"[{member4}") == [
            member2.encode(),
            member3.encode(),
            member4.encode(),
        ]

    @pytest.mark.asyncio
    async def test_exzrangebylex_limit(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_a"
        member2 = "member_b"
        member3 = "member_c"
        member4 = "member_d"
        member5 = "member_e"
        score1 = 0
        score2 = 0
        score3 = 0
        score4 = 0
        score5 = 0

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )

        assert await t.exzrangebylex(
            key, f"[{member2}", f"[{member4}", offset=1, count=1
        ) == [member3.encode()]

        with raises(DataError):
            await t.exzrangebylex(key, f"[{member2}", f"[{member4}", offset=1)

        with raises(DataError):
            await t.exzrangebylex(key, f"[{member2}", f"[{member4}", count=1)

    @pytest.mark.asyncio
    async def test_exzrevrangebylex(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_a"
        member2 = "member_b"
        member3 = "member_c"
        member4 = "member_d"
        member5 = "member_e"
        score1 = 0
        score2 = 0
        score3 = 0
        score4 = 0
        score5 = 0

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )

        assert await t.exzrevrangebylex(key, f"[{member4}", f"[{member2}") == [
            member4.encode(),
            member3.encode(),
            member2.encode(),
        ]

    @pytest.mark.asyncio
    async def test_exzrevrangebylex_limit(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_a"
        member2 = "member_b"
        member3 = "member_c"
        member4 = "member_d"
        member5 = "member_e"
        score1 = 0
        score2 = 0
        score3 = 0
        score4 = 0
        score5 = 0

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )

        assert await t.exzrevrangebylex(
            key, f"[{member4}", f"[{member2}", offset=1, count=1
        ) == [member3.encode()]

        with raises(DataError):
            await t.exzrevrangebylex(key, f"[{member4}", f"[{member2}", offset=1)

        with raises(DataError):
            await t.exzrevrangebylex(key, f"[{member4}", f"[{member2}", count=1)

    @pytest.mark.asyncio
    async def test_exzrem(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score3 = 30

        assert await t.exzadd(
            key,
            {
                member1: score1,
                member2: score2,
                member3: score3,
            },
        )
        assert await t.exzrem(key, [member1, member2, member3]) == 3
        assert await t.exzcard(key) == 0

    @pytest.mark.asyncio
    async def test_exzremrangebyscore(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        member4 = "member_" + str(uuid.uuid4())
        member5 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score3 = 30
        score4 = 40
        score5 = 50

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )
        assert await t.exzremrangebyscore(key, 10, 30) == 3

    @pytest.mark.asyncio
    async def test_exzremrangebyrank(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        member4 = "member_" + str(uuid.uuid4())
        member5 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score3 = 30
        score4 = 40
        score5 = 50

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )
        assert await t.exzremrangebyrank(key, 1, 3) == 3

    @pytest.mark.asyncio
    async def test_exzremrangebylex(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_a"
        member2 = "member_b"
        member3 = "member_c"
        member4 = "member_d"
        member5 = "member_e"
        score1 = 0
        score2 = 0
        score3 = 0
        score4 = 0
        score5 = 0

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )
        assert await t.exzremrangebylex(key, f"[{member2}", f"[{member4}") == 3

    @pytest.mark.asyncio
    async def test_exzrank(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score3 = 30

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                },
            )
            == 3
        )
        assert await t.exzrank(key, member1) == 0
        assert await t.exzrank(key, member2) == 1
        assert await t.exzrank(key, member3) == 2

    @pytest.mark.asyncio
    async def test_exzrevrank(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score3 = 30

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                },
            )
            == 3
        )
        assert await t.exzrevrank(key, member1) == 2
        assert await t.exzrevrank(key, member2) == 1
        assert await t.exzrevrank(key, member3) == 0

    @pytest.mark.asyncio
    async def test_exzcount(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        member4 = "member_" + str(uuid.uuid4())
        member5 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score3 = 30
        score4 = 40
        score5 = 50

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )
        assert await t.exzcount(key, 20, 40) == 3

    @pytest.mark.asyncio
    async def test_exzlexcount(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_a"
        member2 = "member_b"
        member3 = "member_c"
        member4 = "member_d"
        member5 = "member_e"
        score1 = 0
        score2 = 0
        score3 = 0
        score4 = 0
        score5 = 0

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )
        assert await t.exzlexcount(key, f"[{member2}", f"[{member4}") == 3

    @pytest.mark.asyncio
    async def test_exzrankbyscore(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        member4 = "member_" + str(uuid.uuid4())
        member5 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score3 = 30
        score4 = 40
        score5 = 50

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )

        assert await t.exzrankbyscore(key, score1) == 0
        assert await t.exzrankbyscore(key, score2) == 1
        assert await t.exzrankbyscore(key, score3) == 2
        assert await t.exzrankbyscore(key, score4) == 3
        assert await t.exzrankbyscore(key, score5) == 4

    @pytest.mark.asyncio
    async def test_exzrevrankbyscore(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        member1 = "member_" + str(uuid.uuid4())
        member2 = "member_" + str(uuid.uuid4())
        member3 = "member_" + str(uuid.uuid4())
        member4 = "member_" + str(uuid.uuid4())
        member5 = "member_" + str(uuid.uuid4())
        score1 = 10
        score2 = 20
        score3 = 30
        score4 = 40
        score5 = 50

        assert (
            await t.exzadd(
                key,
                {
                    member1: score1,
                    member2: score2,
                    member3: score3,
                    member4: score4,
                    member5: score5,
                },
            )
            == 5
        )

        assert await t.exzrevrankbyscore(key, score1) == 5
        assert await t.exzrevrankbyscore(key, score2) == 4
        assert await t.exzrevrankbyscore(key, score3) == 3
        assert await t.exzrevrankbyscore(key, score4) == 2
        assert await t.exzrevrankbyscore(key, score5) == 1

    @pytest.mark.asyncio
    async def test_tair_zset_item_eq(self):
        member = "member_" + str(uuid.uuid4())
        assert TairZsetItem(member.encode(), 1) == TairZsetItem(member.encode(), 1)
        assert not TairZsetItem(member.encode(), 1) == TairZsetItem(member.encode(), 2)
        assert not TairZsetItem(member.encode(), 1) == 1

    @pytest.mark.asyncio
    async def test_tair_zset_item_ne(self):
        member = "member_" + str(uuid.uuid4())
        assert TairZsetItem(member.encode(), 1) != TairZsetItem(member.encode(), 2)
        assert TairZsetItem(member.encode(), 1) != 1
        assert not (
            TairZsetItem(member.encode(), 1) != TairZsetItem(member.encode(), 1)
        )

    @pytest.mark.asyncio
    async def test_tair_zset_item_repr(self):
        member = "member_" + str(uuid.uuid4())
        assert (
            str(TairZsetItem(member.encode(), 100))
            == f"{{member: {member.encode()}, score: 100}}"
        )
