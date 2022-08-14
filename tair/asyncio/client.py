from typing import Mapping, Optional, Union

from redis import ConnectionPool
from redis.asyncio.client import Redis
from redis.retry import Retry

from tair.commands import TairCommands, set_tair_response_callback
from tair.pipeline import Pipeline


class Tair(Redis, TairCommands):
    @classmethod
    def from_url(cls, url: str, **kwargs):
        connection_pool = ConnectionPool.from_url(url, **kwargs)
        return cls(connection_pool=connection_pool)

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 6379,
        db: Union[str, int] = 0,
        password: Optional[str] = None,
        socket_timeout: Optional[float] = None,
        socket_connect_timeout: Optional[float] = None,
        socket_keepalive: Optional[bool] = None,
        socket_keepalive_options: Optional[Mapping[int, Union[int, bytes]]] = None,
        connection_pool: Optional[ConnectionPool] = None,
        unix_socket_path: Optional[str] = None,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        decode_responses: bool = False,
        retry_on_timeout: bool = False,
        ssl: bool = False,
        ssl_keyfile: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        ssl_cert_reqs: str = "required",
        ssl_ca_certs: Optional[str] = None,
        ssl_ca_data: Optional[str] = None,
        ssl_check_hostname: bool = False,
        max_connections: Optional[int] = None,
        single_connection_client: bool = False,
        health_check_interval: int = 0,
        client_name: Optional[str] = None,
        username: Optional[str] = None,
        retry: Optional[Retry] = None,
        auto_close_connection_pool: bool = True,
        redis_connect_func=None,
    ):
        Redis.__init__(
            self,
            host=host,
            port=port,
            db=db,
            password=password,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            connection_pool=connection_pool,
            unix_socket_path=unix_socket_path,
            encoding=encoding,
            encoding_errors=encoding_errors,
            decode_responses=decode_responses,
            retry_on_timeout=retry_on_timeout,
            ssl=ssl,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            ssl_cert_reqs=ssl_cert_reqs,
            ssl_ca_certs=ssl_ca_certs,
            ssl_ca_data=ssl_ca_data,
            ssl_check_hostname=ssl_check_hostname,
            max_connections=max_connections,
            single_connection_client=single_connection_client,
            health_check_interval=health_check_interval,
            client_name=client_name,
            username=username,
            retry=retry,
            auto_close_connection_pool=auto_close_connection_pool,
            redis_connect_func=redis_connect_func,
        )
        set_tair_response_callback(self)

    async def initialize(self):
        await Redis.initialize(self)

    def pipeline(self, transaction: bool = True, shard_hint: Optional[str] = None):
        return Pipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint,
        )
