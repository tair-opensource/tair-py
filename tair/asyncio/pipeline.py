from typing import MutableMapping, Optional, Union

from tair.typing import ResponseCallbackT
from tair.commands import TairCommands

from redis import ConnectionPool
from redis.asyncio.client import Pipeline as RedisPipeline


class Pipeline(RedisPipeline, TairCommands):
    def __init__(
        self,
        connection_pool: ConnectionPool,
        response_callbacks: MutableMapping[Union[str, bytes], ResponseCallbackT],
        transaction: bool,
        shard_hint: Optional[str],
    ):
        RedisPipeline.__init__(
            self,
            connection_pool=connection_pool,
            response_callbacks=response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint,
        )
