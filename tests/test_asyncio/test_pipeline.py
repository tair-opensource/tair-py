import uuid

import pytest
from redis.exceptions import RedisClusterException

from tair import ExgetResult
from tair.asyncio import Tair, TairCluster


class TestPipeline:
    @pytest.mark.asyncio
    async def test_pipeline(self, t: Tair):
        async with t.pipeline() as pipe:
            key1 = "key_" + str(uuid.uuid4())
            key2 = "key_" + str(uuid.uuid4())
            key3 = "key_" + str(uuid.uuid4())
            value1 = "value_" + str(uuid.uuid4())
            value2 = "value_" + str(uuid.uuid4())
            value3 = "value_" + str(uuid.uuid4())

            (
                pipe.exset(key1, value1)
                .exset(key2, value2)
                .exset(key3, value3)
                .exget(key1)
                .exget(key2)
                .exget(key3)
            )
            assert await pipe.execute() == [
                True,
                True,
                True,
                ExgetResult(value1.encode(), 1),
                ExgetResult(value2.encode(), 1),
                ExgetResult(value3.encode(), 1),
            ]

    @pytest.mark.asyncio
    async def test_pipeline_cluster(self, tc: TairCluster):
        async with tc.pipeline() as pipe:
            key1 = "key_" + str(uuid.uuid4())
            key2 = "key_" + str(uuid.uuid4())
            key3 = "key_" + str(uuid.uuid4())
            value1 = "value_" + str(uuid.uuid4())
            value2 = "value_" + str(uuid.uuid4())
            value3 = "value_" + str(uuid.uuid4())

            (
                pipe.exset(key1, value1)
                .exset(key2, value2)
                .exset(key3, value3)
                .exget(key1)
                .exget(key2)
                .exget(key3)
            )
            assert await pipe.execute() == [
                True,
                True,
                True,
                ExgetResult(value1.encode(), 1),
                ExgetResult(value2.encode(), 1),
                ExgetResult(value3.encode(), 1),
            ]

    @pytest.mark.asyncio
    async def test_deprecated(self, tc: TairCluster):
        with pytest.raises(RedisClusterException):
            tc.pipeline(transaction=True)
        with pytest.raises(RedisClusterException):
            tc.pipeline(shard_hint=True)
