import os
from datetime import datetime

import pytest

from tair import Tair, TairCluster

# change the following configuration for your Tair.
TAIR_HOST = "localhost"
TAIR_PORT = 6379
TAIR_DB = 0
TAIR_USERNAME = None
TAIR_PASSWORD = None

TAIR_CLUSERT_HOST = "localhost"
TAIR_CLUSERT_PORT = 30001
TAIR_CLUSERT_USERNAME = None
TAIR_CLUSERT_PASSWORD = None

# due to network delay, ttl and pttl are not very accurate,
# so we set a calibration value.
# unit: millisecond
NETWORK_DELAY_CALIBRATION_VALUE = 1000


def get_tair_client() -> Tair:
    return Tair(
        host=TAIR_HOST,
        port=TAIR_PORT,
        db=TAIR_DB,
        username=TAIR_USERNAME,
        password=TAIR_PASSWORD,
    )


def get_tair_cluster_client() -> TairCluster:
    return TairCluster(
        host=TAIR_CLUSERT_HOST,
        port=TAIR_CLUSERT_PORT,
        username=TAIR_CLUSERT_USERNAME,
        password=TAIR_CLUSERT_PASSWORD,
    )


# set TEST_TAIR_CLUSTER environment variable to enable cluster mode.
@pytest.fixture()
def t() -> Tair:
    if os.environ.get("TEST_TAIR_CLUSTER") is None:
        tair = get_tair_client()
    else:
        tair = get_tair_cluster_client()
    yield tair
    tair.close()


def get_server_time(client) -> datetime:
    seconds, milliseconds = client.time()
    timestamp = float(f"{seconds}.{milliseconds}")
    return datetime.fromtimestamp(timestamp)
