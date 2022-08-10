import pytest_asyncio
from datetime import datetime
from tair.asyncio import Tair

DEFAULT_TAIR_HOST = "localhost"
DEFAULT_TAIR_PORT = 6379
DEFAULT_TAIR_DB = 0

# due to network delay, ttl and pttl are not very accurate,
# so we set a calibration value.
# unit: millisecond
NETWORK_DELAY_CALIBRATION_VALUE = 1000


async def get_tair_client() -> Tair:
    tair = Tair(
        host=DEFAULT_TAIR_HOST,
        port=DEFAULT_TAIR_PORT,
        db=DEFAULT_TAIR_DB,
        username="root",
        password="123456",
    )
    await tair.initialize()
    return tair


@pytest_asyncio.fixture()
async def t():
    tair = await get_tair_client()
    yield tair
    await tair.close()


async def get_server_time(client) -> datetime:
    seconds, milliseconds = await client.time()
    timestamp = float(f"{seconds}.{milliseconds}")
    return datetime.fromtimestamp(timestamp)
