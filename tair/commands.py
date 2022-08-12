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
from .tairbloom import TairBloomCommands
from .tairroaring import TairRoaringCommands, parse_tr_scan
from .tairsearch import TairSearchCommands, ScandocidResult

from redis import Redis
from redis.asyncio import Redis as AsyncRedis


class TairCommands(
    TairHashCommands,
    TairStringCommands,
    TairZsetCommands,
    TairBloomCommands,
    TairRoaringCommands,
    TairSearchCommands,
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
    # TairBloom
    "BF.RESERVE": bool_ok,
    # TairRoaring
    "TR.GETBIT": lambda resp: resp if resp == 1 else 0,
    "TR.APPENDINTARRAY": bool_ok,
    "TR.SETINTARRAY": bool_ok,
    "TR.SETBITARRAY": bool_ok,
    "TR.OPTIMIZE": bool_ok,
    "TR.SCAN": parse_tr_scan,
    "TR.RANGEBITARRAY": lambda resp: resp.decode(),
    "TR.JACCARD": lambda resp: float(resp.decode()),
    # TairSearch
    "TFT.CREATEINDEX": bool_ok,
    "TFT.UPDATEINDEX": bool_ok,
    "TFT.GETINDEX": lambda resp: None if resp is None else resp.decode(),
    "TFT.ADDDOC": lambda resp: resp.decode(),
    "TFT.MADDDOC": bool_ok,
    "TFT.DELDOC": lambda resp: int(resp.decode()),
    "TFT.UPDATEDOCFIELD": bool_ok,
    "TFT.INCRFLOATDOCFIELD": lambda resp: float(resp.decode()),
    "TFT.GETDOC": lambda resp: None if resp is None else resp.decode(),
    "TFT.SCANDOCID": lambda resp: ScandocidResult(
        resp[0].decode(), [i.decode() for i in resp[1]]
    ),
    "TFT.DELALL": bool_ok,
    "TFT.SEARCH": lambda resp: resp.decode(),
    "TFT.GETSUG": lambda resp: [i.decode() for i in resp],
    "TFT.GETALLSUGS": lambda resp: [i.decode() for i in resp],
}


def set_tair_response_callback(redis: Union[Redis, AsyncRedis]):
    for cmd, cb in TAIR_RESPONSE_CALLBACKS.items():
        redis.set_response_callback(cmd, cb)
