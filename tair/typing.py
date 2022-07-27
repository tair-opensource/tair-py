from typing import (
    Any,
    Awaitable,
    Union,
)

from redis.typing import (
    AbsExpiryT,
    AnyKeyT,
    CommandsProtocol,
    EncodableT,
    ExpiryT,
    FieldT,
    KeyT,
)

from redis.asyncio.client import ResponseCallbackT

ResponseT = Union[Awaitable, Any]
