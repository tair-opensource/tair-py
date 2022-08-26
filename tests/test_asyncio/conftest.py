import os
from datetime import datetime
from typing import Union

import pytest_asyncio

from tair.asyncio import Tair, TairCluster

from ..conftest import (
    TAIR_CLUSTER_HOST,
    TAIR_CLUSTER_PASSWORD,
    TAIR_CLUSTER_PORT,
    TAIR_CLUSTER_USERNAME,
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
    return tair


async def get_tair_cluster_client() -> TairCluster:
    tair_cluster = TairCluster(
        host=TAIR_CLUSTER_HOST,
        port=TAIR_CLUSTER_PORT,
        username=TAIR_CLUSTER_USERNAME,
        password=TAIR_CLUSTER_PASSWORD,
    )
    return tair_cluster


@pytest_asyncio.fixture()
async def t() -> Union[Tair, TairCluster]:
    if os.environ.get("TEST_TAIR_CLUSTER") is None:
        tair = await get_tair_client()
    else:
        tair = await get_tair_cluster_client()
    yield tair
    await tair.close()


@pytest_asyncio.fixture()
async def tc() -> TairCluster:
    tair = await get_tair_cluster_client()
    yield tair
    await tair.close()


async def get_server_time(client) -> datetime:
    seconds, milliseconds = await client.time()
    timestamp = float(f"{seconds}.{milliseconds}")
    return datetime.fromtimestamp(timestamp)
