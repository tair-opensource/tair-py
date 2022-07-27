from .commands import TairCommands, set_tair_response_callback
from .pipeline import ClusterPipeline

from redis import RedisCluster
from redis.exceptions import RedisClusterException


class TairCluster(RedisCluster, TairCommands):
    @classmethod
    def from_url(cls, url, **kwargs):
        return cls(url=url, **kwargs)

    def __init__(
        self,
        host=None,
        port=6379,
        startup_nodes=None,
        cluster_error_retry_attempts=3,
        require_full_coverage=False,
        reinitialize_steps=10,
        read_from_replicas=False,
        url=None,
        **kwargs,
    ):
        RedisCluster.__init__(
            self,
            host=host,
            port=port,
            startup_nodes=startup_nodes,
            cluster_error_retry_attempts=cluster_error_retry_attempts,
            require_full_coverage=require_full_coverage,
            reinitialize_steps=reinitialize_steps,
            read_from_replicas=read_from_replicas,
            url=url,
            **kwargs,
        )
        set_tair_response_callback(self)

    def pipeline(self, transaction=None, shard_hint=None):
        if shard_hint:
            raise RedisClusterException("shard_hint is deprecated in cluster mode")

        if transaction:
            raise RedisClusterException("transaction is deprecated in cluster mode")

        return ClusterPipeline(
            nodes_manager=self.nodes_manager,
            commands_parser=self.commands_parser,
            startup_nodes=self.nodes_manager.startup_nodes,
            result_callbacks=self.result_callbacks,
            cluster_response_callbacks=self.cluster_response_callbacks,
            cluster_error_retry_attempts=self.cluster_error_retry_attempts,
            read_from_replicas=self.read_from_replicas,
            reinitialize_steps=self.reinitialize_steps,
        )
