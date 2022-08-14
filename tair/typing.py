from typing import Any, Awaitable, Union

from redis.asyncio.client import ResponseCallbackT
from redis.typing import (
    AbsExpiryT,
    AnyKeyT,
    CommandsProtocol,
    EncodableT,
    ExpiryT,
    FieldT,
    KeyT,
)

ResponseT = Union[Awaitable, Any]
