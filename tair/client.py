from redis import ConnectionPool, Redis

from tair.commands import TairCommands, set_tair_response_callback
from tair.pipeline import Pipeline


class Tair(Redis, TairCommands):
    @classmethod
    def from_url(cls, url: str, **kwargs):
        connection_pool = ConnectionPool.from_url(url, **kwargs)
        return cls(connection_pool=connection_pool)

    def __init__(
        self,
        host="localhost",
        port=6379,
        db=0,
        password=None,
        socket_timeout=None,
        socket_connect_timeout=None,
        socket_keepalive=None,
        socket_keepalive_options=None,
        connection_pool=None,
        unix_socket_path=None,
        encoding="utf-8",
        encoding_errors="strict",
        charset=None,
        errors=None,
        decode_responses=False,
        retry_on_timeout=False,
        retry_on_error=[],
        ssl=False,
        ssl_keyfile=None,
        ssl_certfile=None,
        ssl_cert_reqs="required",
        ssl_ca_certs=None,
        ssl_ca_path=None,
        ssl_ca_data=None,
        ssl_check_hostname=False,
        ssl_password=None,
        ssl_validate_ocsp=False,
        ssl_validate_ocsp_stapled=False,
        ssl_ocsp_context=None,
        ssl_ocsp_expected_cert=None,
        max_connections=None,
        single_connection_client=False,
        health_check_interval=0,
        client_name=None,
        username=None,
        retry=None,
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
            charset=charset,
            errors=errors,
            decode_responses=decode_responses,
            retry_on_timeout=retry_on_timeout,
            retry_on_error=retry_on_error,
            ssl=ssl,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            ssl_cert_reqs=ssl_cert_reqs,
            ssl_ca_certs=ssl_ca_certs,
            ssl_ca_path=ssl_ca_path,
            ssl_ca_data=ssl_ca_data,
            ssl_check_hostname=ssl_check_hostname,
            ssl_password=ssl_password,
            ssl_validate_ocsp=ssl_validate_ocsp,
            ssl_validate_ocsp_stapled=ssl_validate_ocsp_stapled,
            ssl_ocsp_context=ssl_ocsp_context,
            ssl_ocsp_expected_cert=ssl_ocsp_expected_cert,
            max_connections=max_connections,
            single_connection_client=single_connection_client,
            health_check_interval=health_check_interval,
            client_name=client_name,
            username=username,
            retry=retry,
            redis_connect_func=redis_connect_func,
        )
        set_tair_response_callback(self)

    def pipeline(self, transaction=True, shard_hint=None):
        return Pipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint,
        )
