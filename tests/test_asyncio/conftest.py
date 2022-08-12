import pytest_asyncio
from datetime import datetime
from tair.asyncio import Tair
from ..conftest import TAIR_HOST, TAIR_PORT, TAIR_DB, TAIR_USERNAME, TAIR_PASSWORD


# due to network delay, ttl and pttl are not very accurate,
# so we set a calibration value.
# unit: millisecond
NETWORK_DELAY_CALIBRATION_VALUE = 1000


async def get_tair_client() -> Tair:
    tair = Tair(
        host=TAIR_HOST,
        port=TAIR_PORT,
        db=TAIR_DB,
        username=TAIR_USERNAME,
        password=TAIR_PASSWORD,
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
