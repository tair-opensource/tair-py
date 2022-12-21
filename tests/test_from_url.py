import uuid

import pytest

from tair import ExgetResult, Tair, TairCluster
from tair.asyncio import Tair as AsyncTair
from tair.asyncio import TairCluster as AsyncTairCluster

from .conftest import (
    TAIR_CLUSTER_HOST,
    TAIR_CLUSTER_PASSWORD,
    TAIR_CLUSTER_PORT,
    TAIR_CLUSTER_SCHEME,
    TAIR_CLUSTER_USERNAME,
    TAIR_DB,
    TAIR_HOST,
    TAIR_PASSWORD,
    TAIR_PORT,
    TAIR_SCHEME,
    TAIR_USERNAME,
)


def test_from_url():
    url = f"{TAIR_SCHEME}://{TAIR_HOST}:{TAIR_PORT}/{TAIR_DB}"
    t = Tair.from_url(url, username=TAIR_USERNAME, password=TAIR_PASSWORD)
    key = "key_" + str(uuid.uuid4())
    value = "value_" + str(uuid.uuid4())

    assert t.exset(key, value)
    result: ExgetResult = t.exget(key)
    assert result.value == value.encode()
    assert result.version == 1

    t.close()


def test_from_url_cluster():
    url = f"{TAIR_CLUSTER_SCHEME}://{TAIR_CLUSTER_HOST}:{TAIR_CLUSTER_PORT}"
    tc = TairCluster.from_url(
        url, username=TAIR_CLUSTER_USERNAME, password=TAIR_CLUSTER_PASSWORD
    )
    key = "key_" + str(uuid.uuid4())
    value = "value_" + str(uuid.uuid4())

    assert tc.exset(key, value)
    result: ExgetResult = tc.exget(key)
    assert result.value == value.encode()
    assert result.version == 1

    tc.close()


@pytest.mark.asyncio
async def test_from_url_async():
    url = f"{TAIR_SCHEME}://{TAIR_HOST}:{TAIR_PORT}/{TAIR_DB}"
    t = AsyncTair.from_url(url, username=TAIR_USERNAME, password=TAIR_PASSWORD)
    key = "key_" + str(uuid.uuid4())
    value = "value_" + str(uuid.uuid4())

    assert await t.exset(key, value)
    result: ExgetResult = await t.exget(key)
    assert result.value == value.encode()
    assert result.version == 1

    await t.close()


@pytest.mark.asyncio
async def test_from_url_async_cluster():
    url = f"{TAIR_CLUSTER_SCHEME}://{TAIR_CLUSTER_HOST}:{TAIR_CLUSTER_PORT}"
    tc = AsyncTairCluster.from_url(
        url, username=TAIR_CLUSTER_USERNAME, password=TAIR_CLUSTER_PASSWORD
    )
    key = "key_" + str(uuid.uuid4())
    value = "value_" + str(uuid.uuid4())

    assert await tc.exset(key, value)
    result: ExgetResult = await tc.exget(key)
    assert result.value == value.encode()
    assert result.version == 1

    await tc.close()
