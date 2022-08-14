from typing import MutableMapping, Optional, Union

from redis import ConnectionPool
from redis.asyncio.client import Pipeline as RedisPipeline

from tair.commands import TairCommands
from tair.typing import ResponseCallbackT


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
