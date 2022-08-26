from typing import List, Optional

from redis.asyncio.cluster import ClusterNode, RedisCluster

from tair.commands import TairCommands, set_tair_response_callback


class TairCluster(RedisCluster, TairCommands):
    @classmethod
    def from_url(cls, url, **kwargs):
        return cls(url=url, **kwargs)

    def __init__(
        self,
        host: Optional[str] = None,
        port: int = 6379,
        startup_nodes: Optional[List[ClusterNode]] = None,
        require_full_coverage: bool = False,
        read_from_replicas: bool = False,
        cluster_error_retry_attempts: int = 3,
        reinitialize_steps: int = 10,
        url: Optional[str] = None,
        **kwargs,
    ):
        RedisCluster.__init__(
            self,
            host=host,
            port=port,
            startup_nodes=startup_nodes,
            require_full_coverage=require_full_coverage,
            read_from_replicas=read_from_replicas,
            cluster_error_retry_attempts=cluster_error_retry_attempts,
            reinitialize_steps=reinitialize_steps,
            url=url,
            **kwargs,
        )
        set_tair_response_callback(self)
