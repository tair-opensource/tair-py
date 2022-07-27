from tair import Tair
from datetime import datetime

DEFAULT_TAIR_HOST = "localhost"
DEFAULT_TAIR_PORT = 6379
DEFAULT_TAIR_DB = 0

# due to network delay, ttl and pttl are not very accurate,
# so we set a calibration value.
# unit: millisecond
NETWORK_DELAY_CALIBRATION_VALUE = 1000


def get_tair_client() -> Tair:
    return Tair(
        host=DEFAULT_TAIR_HOST,
        port=DEFAULT_TAIR_PORT,
        db=DEFAULT_TAIR_DB,
    )


def get_server_time(client) -> datetime:
    seconds, milliseconds = client.time()
    timestamp = float(f"{seconds}.{milliseconds}")
    return datetime.fromtimestamp(timestamp)
