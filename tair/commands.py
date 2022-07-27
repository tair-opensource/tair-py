from typing import Union

from .tairhash import (
    TairHashCommands,
    parse_exhincrbyfloat,
    parse_exhgetwithver,
    parse_exhmgetwithver,
    parse_exhgetall,
    parse_exhscan,
)

from .tairstring import (
    TairStringCommands,
    parse_exset,
    parse_exget,
    parse_excas,
    parse_exincrbyfloat,
)

from .tairzset import TairZsetCommands, parse_tair_zset_items


from redis import Redis
from redis.asyncio import Redis as AsyncRedis


class TairCommands(
    TairHashCommands,
    TairStringCommands,
    TairZsetCommands,
):
    pass


def str_if_bytes(value: Union[str, bytes]) -> str:
    return (
        value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
    )


def bool_ok(resp) -> bool:
    return str_if_bytes(resp) == "OK"


TAIR_RESPONSE_CALLBACKS = {
    # TairString
    "EXSET": parse_exset,
    "EXGET": parse_exget,
    "EXINCRBYFLOAT": parse_exincrbyfloat,
    "EXCAS": parse_excas,
    # TairHash
    "EXHMSET": bool_ok,
    "EXHINCRBYFLOAT": parse_exhincrbyfloat,
    "EXHGETWITHVER": parse_exhgetwithver,
    "EXHMGETWITHVER": parse_exhmgetwithver,
    "EXHGETALL": parse_exhgetall,
    "EXHSCAN": parse_exhscan,
    # TairZset
    "EXZINCRBY": str_if_bytes,
    "EXZSCORE": str_if_bytes,
    "EXZRANGE": parse_tair_zset_items,
    "EXZREVRANGE": parse_tair_zset_items,
    "EXZRANGEBYSCORE": parse_tair_zset_items,
    "EXZREVRANGEBYSCORE": parse_tair_zset_items,
}


def set_tair_response_callback(redis: Union[Redis, AsyncRedis]):
    for cmd, cb in TAIR_RESPONSE_CALLBACKS.items():
        redis.set_response_callback(cmd, cb)
