import os
from datetime import datetime
from typing import Union

import pytest_asyncio

from tair.asyncio import Tair, TairCluster

from ..conftest import (
    TAIR_CLUSERT_HOST,
    TAIR_CLUSERT_PASSWORD,
    TAIR_CLUSERT_PORT,
    TAIR_CLUSERT_USERNAME,
    TAIR_DB,
    TAIR_HOST,
    TAIR_PASSWORD,
    TAIR_PORT,
    TAIR_USERNAME,
)

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


async def get_tair_cluster_client() -> TairCluster:
    tair_cluster = TairCluster(
        host=TAIR_CLUSERT_HOST,
        port=TAIR_CLUSERT_PORT,
        username=TAIR_CLUSERT_USERNAME,
        password=TAIR_CLUSERT_PASSWORD,
    )
    await tair_cluster.initialize()
    return tair_cluster


@pytest_asyncio.fixture()
async def t() -> Union[Tair, TairCluster]:
    if os.environ.get("TEST_TAIR_CLUSTER") is None:
        tair = await get_tair_client()
    else:
        tair = await get_tair_cluster_client()
    yield tair
    await tair.close()


async def get_server_time(client) -> datetime:
    seconds, milliseconds = await client.time()
    timestamp = float(f"{seconds}.{milliseconds}")
    return datetime.fromtimestamp(timestamp)
