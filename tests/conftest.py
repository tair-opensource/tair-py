import pytest

from tair import Tair
from datetime import datetime

# change the following configuration for your Tair.
TAIR_HOST = "localhost"
TAIR_PORT = 6379
TAIR_DB = 0
TAIR_USERNAME = "root"
TAIR_PASSWORD = "123456"

# due to network delay, ttl and pttl are not very accurate,
# so we set a calibration value.
# unit: millisecond
NETWORK_DELAY_CALIBRATION_VALUE = 1000


@pytest.fixture()
def t() -> Tair:
    tair = Tair(
        host=TAIR_HOST,
        port=TAIR_PORT,
        db=TAIR_DB,
        username=TAIR_USERNAME,
        password=TAIR_PASSWORD,
    )
    yield tair
    tair.close()


def get_server_time(client) -> datetime:
    seconds, milliseconds = client.time()
    timestamp = float(f"{seconds}.{milliseconds}")
    return datetime.fromtimestamp(timestamp)
