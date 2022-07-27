import pytest

import datetime
import time
import uuid

from tair import (
    ExhscanResult,
    FieldValueItem,
    ValueVersionItem,
    DataError,
    ResponseError,
)

from tair.tairhash import (
    parse_exhincrbyfloat,
    parse_exhgetwithver,
    parse_exhmgetwithver,
    parse_exhgetall,
    parse_exhscan,
)

from .conftest import get_tair_client, get_server_time, NETWORK_DELAY_CALIBRATION_VALUE


class TestTairHash:
    def test_exhset_success(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())

        # if the field does not exist, set it and return 1.
        assert t.exhset(key, field, value1) == 1
        assert t.exhget(key, field) == value1.encode()

        # if the field exists, set it and return 0.
        assert t.exhset(key, field, value2) == 0
        assert t.exhget(key, field) == value2.encode()

    def test_exhset_ex(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exhset(key, field, value, ex=10) == 1
        assert 0 < t.exhttl(key, field) <= 10

        # ex should not be a float.
        with pytest.raises(DataError):
            t.exhset(key, field, value, ex=10.0)

    def test_exhset_ex_timedelta(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        ex = datetime.timedelta(seconds=10)

        assert t.exhset(key, field, value, ex=ex) == 1
        assert 0 < t.exhttl(key, field) <= 10

    def test_exhset_px(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exhset(key, field, value, px=10000) == 1
        assert 0 < t.exhpttl(key, field) <= 10000

        # px should not be a float.
        with pytest.raises(DataError):
            t.exhset(key, field, value, px=10000.0)

    def test_exhset_px_timedelta(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        px = datetime.timedelta(milliseconds=10000)

        assert t.exhset(key, field, value, px=px) == 1
        assert 0 < t.exhpttl(key, field) <= 10000

    def test_exhset_exat(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)
        exat = int(time.mktime(expire_at.timetuple()))

        assert t.exhset(key, field, value, exat=exat) == 1
        assert 0 < t.exhttl(key, field) <= 10

    def test_exhset_exat_timedelta(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        exat = get_server_time(t) + datetime.timedelta(seconds=10)

        assert t.exhset(key, field, value, exat=exat) == 1
        assert 0 < t.exhttl(key, field) <= 10

    def test_exhset_pxat(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        exat = get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(exat.timetuple())) * 1000

        assert t.exhset(key, field, value, pxat=pxat) == 1
        assert 0 < t.exhpttl(key, field) <= 10000

    def test_exhset_pxat_timedelta(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        pxat = get_server_time(t) + datetime.timedelta(seconds=10)

        assert t.exhset(key, field, value, pxat=pxat) == 1
        # due to network delay, pttl may be greater than 10000.
        assert 0 < t.exhpttl(key, field) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    def test_exhset_xx(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())

        # if the field does not exist and xx is True, return -1.
        assert t.exhset(key, field, value1, xx=True) == -1
        assert t.exhset(key, field, value1) == 1
        assert t.exhset(key, field, value2, xx=True) == 0

    def test_exhset_nx(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())

        assert t.exhset(key, field, value1) == 1
        # if the field exists and nx is True, return -1.
        assert t.exhset(key, field, value2, nx=True) == -1

    def test_exhset_ver(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())
        value3 = "value_" + str(uuid.uuid4())

        assert t.exhset(key, field, value1, abs=10) == 1
        assert t.exhset(key, field, value2, ver=10) == 0

        with pytest.raises(ResponseError):
            t.exhset(key, field, value3, ver=100)

    def test_exhset_abs(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())

        assert t.exhset(key, field, value1) == 1
        assert t.exhset(key, field, value2, abs=100) == 0
        assert t.exhver(key, field) == 100

    def test_exhset_keepttl(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())
        exat = get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(exat.timetuple())) * 1000

        assert t.exhset(key, field, value1, pxat=pxat) == 1
        assert t.exhset(key, field, value2, keepttl=True) == 0
        assert 0 < t.exhpttl(key, field) <= 10000

    def test_exhmset(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())

        assert t.exhmset(key, {field1: value1, field2: value2})
        assert t.exhget(key, field1) == value1.encode()
        assert t.exhget(key, field2) == value2.encode()

    def test_exhpexpireat(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        exat = get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(exat.timetuple())) * 1000

        assert t.exhset(key1, field, value) == 1
        # returns 1 on success.
        assert t.exhpexpireat(key1, field, pxat) == 1
        assert 0 < t.exhpttl(key1, field) <= 10000

        # if the field does not exist, return 0.
        assert t.exhpexpireat(key2, field, pxat) == 0

    def test_exhpexpireat_ver(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        exat = get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(exat.timetuple())) * 1000

        assert t.exhset(key, field, value1, abs=10) == 1
        assert t.exhpexpireat(key, field, pxat, ver=10) == 1
        assert 0 < t.exhpttl(key, field) <= 10000

        with pytest.raises(ResponseError):
            t.exhpexpireat(key, field, pxat=pxat, ver=100)

    def test_exhpexpireat_abs(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        exat = get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(exat.timetuple())) * 1000

        assert t.exhset(key, field, value) == 1
        assert t.exhpexpireat(key, field, pxat=pxat, abs=10) == 1
        assert 0 < t.exhpttl(key, field) <= 10000
        assert t.exhver(key, field) == 10

    def test_exhpexpire(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exhset(key1, field, value) == 1
        # returns 1 on success.
        assert t.exhpexpire(key1, field, 10000) == 1
        assert 0 < t.exhpttl(key1, field) <= 10000

        # if the field does not exist, return 0.
        assert t.exhpexpire(key2, field, 10000) == 0

    def test_exhpexpire_ver(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())

        assert t.exhset(key, field, value1, abs=10) == 1
        assert t.exhpexpire(key, field, px=10000, ver=10) == 1
        assert 0 < t.exhpttl(key, field) <= 10000

        with pytest.raises(ResponseError):
            t.exhpexpire(key, field, px=10000, ver=100)

    def test_exhpexpire_abs(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exhset(key, field, value) == 1
        assert t.exhpexpire(key, field, 10000, abs=10) == 1
        assert 0 < t.exhpttl(key, field) <= 10000
        assert t.exhver(key, field) == 10

    def test_exhexpireat(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)
        exat = int(time.mktime(expire_at.timetuple()))

        assert t.exhset(key, field, value) == 1
        assert t.exhexpireat(key, field, exat) == 1
        assert 0 < t.exhttl(key, field) <= 10

    def test_exhexpireat_ver(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)
        exat = int(time.mktime(expire_at.timetuple()))

        assert t.exhset(key, field, value, abs=10) == 1
        assert t.exhexpireat(key, field, exat=exat, ver=10) == 1
        assert 0 < t.exhttl(key, field) <= 10

        with pytest.raises(ResponseError):
            t.exhexpireat(key, field, exat=exat, ver=100)

    def test_exhexpireat_abs(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)
        exat = int(time.mktime(expire_at.timetuple()))

        assert t.exhset(key, field, value) == 1
        assert t.exhexpireat(key, field, exat=exat, abs=10) == 1
        assert 0 < t.exhttl(key, field) <= 10
        assert t.exhver(key, field) == 10

    def test_exhexpire(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exhset(key, field, value) == 1
        assert t.exhexpire(key, field, 10) == 1
        assert 0 < t.exhttl(key, field) <= 10

    def test_exhexpire_ver(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())

        assert t.exhset(key, field, value1, abs=10) == 1
        assert t.exhexpire(key, field, ex=10, ver=10) == 1
        assert 0 < t.exhttl(key, field) <= 10

        with pytest.raises(ResponseError):
            t.exhexpire(key, field, ex=10, ver=100)

    def test_exhexpire_abs(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exhset(key, field, value) == 1
        assert t.exhexpire(key, field, ex=10, abs=10) == 1
        assert 0 < t.exhttl(key, field) <= 10
        assert t.exhver(key, field) == 10

    def test_exhver(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exhset(key, field, value) == 1
        assert t.exhver(key, field) == 1

    def test_exhsetver(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exhset(key, field, value) == 1
        assert t.exhsetver(key, field, 10) == 1
        assert t.exhget(key, field) == value.encode()

    def test_exhincrby(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())

        assert t.exhset(key, field, 10) == 1
        assert t.exhincrby(key, field, 20) == 30
        assert t.exhget(key, field) == b"30"

    def test_exhincrby_ex(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())

        assert t.exhset(key, field, 10) == 1
        assert t.exhincrby(key, field, 20, ex=10) == 30
        assert 0 < t.exhttl(key, field) <= 10

        # ex should not be a float.
        with pytest.raises(DataError):
            t.exhincrby(key, field, 20, ex=10.0)

    def test_exhincrby_ex_timedelta(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        ex = datetime.timedelta(seconds=10)

        assert t.exhset(key, field, 10) == 1
        assert t.exhincrby(key, field, 20, ex=ex) == 30
        assert 0 < t.exhttl(key, field) <= 10

    def test_exhincrby_px(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())

        assert t.exhset(key, field, 10) == 1
        assert t.exhincrby(key, field, 20, px=10000) == 30
        assert 0 < t.exhpttl(key, field) <= 10000

        # px should not be a float.
        with pytest.raises(DataError):
            t.exhincrby(key, field, 20, px=10000.0)

    def test_exhincrby_px_timedelta(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        px = datetime.timedelta(milliseconds=10000)

        assert t.exhset(key, field, 10) == 1
        assert t.exhincrby(key, field, 20, px=px) == 30
        assert 0 < t.exhpttl(key, field) <= 10000

    def test_exhincrby_exat(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)
        exat = int(time.mktime(expire_at.timetuple()))

        assert t.exhset(key, field, 10) == 1
        assert t.exhincrby(key, field, 20, exat=exat) == 30
        assert 0 < t.exhttl(key, field) <= 10

        # ex should not be a float.
        with pytest.raises(DataError):
            t.exhincrby(key, field, 20, ex=10.0)

    def test_exhincrby_exat_timedelta(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)

        assert t.exhset(key, field, 10) == 1
        assert t.exhincrby(key, field, 20, exat=expire_at) == 30
        assert 0 < t.exhttl(key, field) <= 10

    def test_exhincrby_pxat(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(expire_at.timetuple())) * 1000

        assert t.exhset(key, field, 10) == 1
        assert t.exhincrby(key, field, 20, pxat=pxat) == 30
        assert 0 < t.exhpttl(key, field) <= 10000

    def test_exhincrby_pxat_timedelta(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        pxat = get_server_time(t) + datetime.timedelta(seconds=10)

        assert t.exhset(key, field, 10) == 1
        assert t.exhincrby(key, field, 20, pxat=pxat) == 30
        # due to network delay, pttl may be greater than 10000.
        assert 0 < t.exhpttl(key, field) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    def test_exhincrby_ver(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())

        assert t.exhset(key, field1, 10) == 1
        assert t.exhincrby(key, field1, 20, ver=1) == 30
        assert t.exhget(key, field1) == b"30"
        assert t.exhver(key, field1) == 2

        assert t.exhset(key, field2, 10) == 1
        with pytest.raises(ResponseError):
            t.exhincrby(key, field2, 20, ver=10)

    def test_exhincrby_abs(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())

        assert t.exhset(key, field, 10) == 1
        assert t.exhincrby(key, field, 20, abs=100) == 30
        assert t.exhget(key, field) == b"30"
        assert t.exhver(key, field) == 100

    def test_exhincrby_overflow(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())

        assert t.exhset(key, field, 10)
        with pytest.raises(ResponseError):
            t.exhincrby(key, field, 20, maxval=10)
        with pytest.raises(ResponseError):
            t.exhincrby(key, field, 20, minval=100)

    def test_exhincrby_keepttl(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        exat = get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(exat.timetuple())) * 1000

        assert t.exhset(key, field, 10, pxat=pxat) == 1
        assert t.exhincrby(key, field, 20, keepttl=True) == 30
        assert 0 < t.exhpttl(key, field) <= 10000

    def test_exhincrbyfloat(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())

        assert t.exhset(key, field, 1.1) == 1
        assert t.exhincrbyfloat(key, field, 2.2) == 3.3
        assert t.exhget(key, field) == b"3.3"

    def test_exhincrbyfloat_ex(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())

        assert t.exhset(key, field, 1.1) == 1
        assert t.exhincrbyfloat(key, field, 2.2, ex=10) == 3.3
        assert 0 < t.exhttl(key, field) <= 10

        # ex should not be a float.
        with pytest.raises(DataError):
            t.exhincrbyfloat(key, field, 2.2, ex=10.0)

    def test_exhincrbyfloat_ex_timedelta(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        ex = datetime.timedelta(seconds=10)

        assert t.exhset(key, field, 1.1) == 1
        assert t.exhincrbyfloat(key, field, 2.2, ex=ex) == 3.3
        assert 0 < t.exhttl(key, field) <= 10

    def test_exhincrbyfloat_px(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())

        assert t.exhset(key, field, 1.1) == 1
        assert t.exhincrbyfloat(key, field, 2.2, px=10000) == 3.3
        assert 0 < t.exhpttl(key, field) <= 10000

        # px should not be a float.
        with pytest.raises(DataError):
            t.exhincrbyfloat(key, field, 2.2, px=10000.0)

    def test_exhincrbyfloat_px_timedelta(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        px = datetime.timedelta(milliseconds=10000)

        assert t.exhset(key, field, 1.1) == 1
        assert t.exhincrbyfloat(key, field, 2.2, px=px) == 3.3
        assert 0 < t.exhpttl(key, field) <= 10000

    def test_exhincrbyfloat_exat(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)
        exat = int(time.mktime(expire_at.timetuple()))

        assert t.exhset(key, field, 1.1) == 1
        assert t.exhincrbyfloat(key, field, 2.2, exat=exat) == 3.3
        assert 0 < t.exhttl(key, field) <= 10

    def test_exhincrbyfloat_exat_timedelta(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)

        assert t.exhset(key, field, 1.1) == 1
        assert t.exhincrbyfloat(key, field, 2.2, exat=expire_at) == 3.3
        assert 0 < t.exhttl(key, field) <= 10

    def test_exhincrbyfloat_pxat(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(expire_at.timetuple())) * 1000

        assert t.exhset(key, field, 1.1) == 1
        assert t.exhincrbyfloat(key, field, 2.2, pxat=pxat) == 3.3
        assert 0 < t.exhpttl(key, field) <= 10000

    def test_exhincrbyfloat_pxat_timedelta(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        pxat = get_server_time(t) + datetime.timedelta(seconds=10)

        assert t.exhset(key, field, 1.1) == 1
        assert t.exhincrbyfloat(key, field, 2.2, pxat=pxat) == 3.3
        # due to network delay, pttl may be greater than 10000.
        assert 0 < t.exhpttl(key, field) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    def test_exhincrbyfloat_ver(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())

        assert t.exhset(key, field1, 1.1) == 1
        assert t.exhincrbyfloat(key, field1, 2.2, ver=1) == 3.3
        assert t.exhget(key, field1) == b"3.3"
        assert t.exhver(key, field1) == 2

        assert t.exhset(key, field2, 10) == 1
        with pytest.raises(ResponseError):
            t.exhincrbyfloat(key, field2, 2.2, ver=10)

    def test_exhincrbyfloat_abs(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())

        assert t.exhset(key, field, 1.1) == 1
        assert t.exhincrbyfloat(key, field, 2.2, abs=100) == 3.3
        assert t.exhget(key, field) == b"3.3"
        assert t.exhver(key, field) == 100

    def test_exhincrbyfloat_overflow(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())

        assert t.exhset(key, field, 9.1)
        with pytest.raises(ResponseError):
            t.exhincrbyfloat(key, field, 1.1, maxval=10.0)
        with pytest.raises(ResponseError):
            t.exhincrbyfloat(key, field, 2.2, minval=100.0)

    def test_exhincrbyfloat_keepttl(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        exat = get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(exat.timetuple())) * 1000

        assert t.exhset(key, field, 1.1, pxat=pxat) == 1
        assert t.exhincrbyfloat(key, field, 2.2, keepttl=True) == 3.3
        assert 0 < t.exhpttl(key, field) <= 10000

    def test_exhgetwithver(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exhset(key1, field1, value) == 1
        assert t.exhgetwithver(key1, field1) == ValueVersionItem(value.encode(), 1)

        assert t.exhgetwithver(key2, field1) is None
        assert t.exhgetwithver(key1, field2) is None

    def test_exhmget(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())

        assert t.exhmset(key, {field1: value1, field2: value2})
        assert t.exhmget(key, [field1, field2]) == [value1.encode(), value2.encode()]

    def test_exhmgetwithver_success(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())

        assert t.exhmset(key, {field1: value1, field2: value2})
        assert t.exhmgetwithver(key, [field1, field2]) == [
            ValueVersionItem(value1.encode(), 1),
            ValueVersionItem(value2.encode(), 1),
        ]

    def test_exhmgetwithver_key_not_exists(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())

        assert t.exhmgetwithver(key, [field1, field2]) == [None, None]

    def test_exhmgetwithver_field_not_exist(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        field3 = "field_" + str(uuid.uuid4())
        value1 = "field_" + str(uuid.uuid4())
        value3 = "field_" + str(uuid.uuid4())

        assert t.exhmset(key, {field1: value1, field3: value3})
        assert t.exhmgetwithver(key, [field1, field2, field3]) == [
            ValueVersionItem(value1.encode(), 1),
            None,
            ValueVersionItem(value3.encode(), 1),
        ]

    def test_exhlen(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())

        assert t.exhmset(key1, {field1: value1, field2: value2})
        assert t.exhlen(key1) == 2
        assert t.exhlen(key2) == 0
        assert t.exhlen(key1, noexp=True) == 2

    def test_exhexists(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exhset(key1, field, value) == 1
        assert t.exhexists(key1, field) == 1
        assert t.exhexists(key2, field) == 0

    def test_exhstrlen(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exhset(key1, field1, value) == 1
        assert t.exhstrlen(key1, field1) == len(value)
        assert t.exhstrlen(key2, field1) == 0
        assert t.exhstrlen(key1, field2) == 0

    def test_exhkeys(self):
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())

        assert t.exhmset(key, {field1: value1, field2: value2})
        assert sorted(t.exhkeys(key)) == sorted([field1.encode(), field2.encode()])

    def test_exhvals(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())

        assert t.exhmset(key1, {field1: value1, field2: value2})
        assert sorted(t.exhvals(key1)) == sorted([value1.encode(), value2.encode()])

        key2 = "key_" + str(uuid.uuid4())
        assert t.exhvals(key2) == []

    def test_exhgetall(self):
        t = get_tair_client()
        key1 = "key_" + str(uuid.uuid4())
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())

        assert t.exhmset(key1, {field1: value1, field2: value2})
        ret = t.exhgetall(key1)
        # pairs may be out of order.
        assert ret == [
            FieldValueItem(field1.encode(), value1.encode()),
            FieldValueItem(field2.encode(), value2.encode()),
        ] or ret == [
            FieldValueItem(field2.encode(), value2.encode()),
            FieldValueItem(field1.encode(), value1.encode()),
        ]

        key2 = "key_" + str(uuid.uuid4())
        assert t.exhgetall(key2) == []

    # the open source version of exhscan is inconsistent with the enterprise version,
    # so this test sample is temporarily commented out.

    # def test_exhscan(self):
    #     t = get_tair_client()
    #     key1 = "key_" + str(uuid.uuid4())
    #     key2 = "key_" + str(uuid.uuid4())
    #     field1 = "field_1_" + str(uuid.uuid4())
    #     field2 = "field_2_" + str(uuid.uuid4())
    #     field3 = "field_3_" + str(uuid.uuid4())
    #     field4 = "field_4_" + str(uuid.uuid4())
    #     field5 = "field_5_" + str(uuid.uuid4())
    #     field6 = "field_6_" + str(uuid.uuid4())
    #     value1 = "value_" + str(uuid.uuid4())
    #     value2 = "value_" + str(uuid.uuid4())
    #     value3 = "value_" + str(uuid.uuid4())
    #     value4 = "value_" + str(uuid.uuid4())
    #     value5 = "value_" + str(uuid.uuid4())
    #     value6 = "value_" + str(uuid.uuid4())

    #     assert t.exhmset(
    #         key1,
    #         {
    #             field1: value1,
    #             field2: value2,
    #             field3: value3,
    #             field4: value4,
    #             field5: value5,
    #             field6: value6,
    #         },
    #     )

    #     result = ExhscanResult(
    #         field5.encode(),
    #         [
    #             FieldValueItem(field2.encode(), value2.encode()),
    #             FieldValueItem(field3.encode(), value3.encode()),
    #             FieldValueItem(field4.encode(), value4.encode()),
    #         ],
    #     )

    #     assert t.exhscan(key1, ">=", field2, match="*", count=3) == result
    #     assert t.exhscan(key2, ">=", field2, count=3) is None

    def test_exhdel(self):
        # NOTE: exhdel actually returns the number of keys it deleted,
        # but in the documentation exhdel returns 1 on success.
        # this should be a error in the documentation.
        # see https://help.aliyun.com/document_detail/145970.html
        t = get_tair_client()
        key = "key_" + str(uuid.uuid4())
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        field3 = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())
        value3 = "value_" + str(uuid.uuid4())

        assert t.exhmset(key, {field1: value1, field2: value2, field3: value3})
        assert t.exhdel(key, [field1, field2, field3]) == 3
        assert t.exhexists(key, field1) == 0
        assert t.exhexists(key, field2) == 0

        field4 = "field_" + str(uuid.uuid4())
        field5 = "field_" + str(uuid.uuid4())
        field6 = "field_" + str(uuid.uuid4())
        value4 = "value_" + str(uuid.uuid4())
        value5 = "value_" + str(uuid.uuid4())

        assert t.exhmset(key, {field4: value4, field5: value5})
        # field6 does not exist, so this should return 2 instead of 3.
        assert t.exhdel(key, [field4, field5, field6]) == 2
        assert t.exhexists(key, field4) == 0
        assert t.exhexists(key, field5) == 0
        assert t.exhexists(key, field6) == 0

    def test_value_version_item_eq(self):
        value = "value_" + str(uuid.uuid4())
        assert ValueVersionItem(value.encode(), 1) == ValueVersionItem(
            value.encode(), 1
        )
        assert not ValueVersionItem(value.encode(), 1) == ValueVersionItem(
            value.encode(), 2
        )
        assert not ValueVersionItem(value.encode(), 1) == 1

    def test_value_version_item_ne(self):
        value = "value_" + str(uuid.uuid4())
        assert not ValueVersionItem(value.encode(), 1) != ValueVersionItem(
            value.encode(), 1
        )
        assert ValueVersionItem(value.encode(), 1) != ValueVersionItem(
            value.encode(), 2
        )
        assert ValueVersionItem(value.encode(), 1) != 1

    def test_value_version_item_repr(self):
        value = "value_" + str(uuid.uuid4())
        assert (
            str(ValueVersionItem(value.encode(), 100))
            == f"{{value: {value}, version: 100}}"
        )

    def test_field_value_item_eq(self):
        field = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = value1 + "a"
        assert FieldValueItem(field.encode(), value1.encode()) == FieldValueItem(
            field.encode(), value1.encode()
        )
        assert not FieldValueItem(field.encode(), value1.encode()) == FieldValueItem(
            field.encode(), value2.encode()
        )
        assert not FieldValueItem(field.encode(), value1.encode()) == 1

    def test_field_value_item_ne(self):
        field = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = value1 + "a"
        assert not FieldValueItem(field.encode(), value1.encode()) != FieldValueItem(
            field.encode(), value1.encode()
        )
        assert FieldValueItem(field.encode(), value1.encode()) != FieldValueItem(
            field.encode(), value2.encode()
        )
        assert FieldValueItem(field.encode(), value1.encode()) != 1

    def test_field_value_item_repr(self):
        field = "field_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        assert (
            str(FieldValueItem(field.encode(), value.encode()))
            == f"{{field: {field}, value: {value}}}"
        )

    def test_exhscan_result_eq(self):
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        field3 = "field_" + str(uuid.uuid4())
        field4 = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())
        value3 = "value_" + str(uuid.uuid4())

        assert ExhscanResult(
            field4.encode(),
            (
                FieldValueItem(field1.encode(), value1.encode()),
                FieldValueItem(field2.encode(), value2.encode()),
                FieldValueItem(field3.encode(), value3.encode()),
            ),
        ) == ExhscanResult(
            field4.encode(),
            [
                FieldValueItem(field1.encode(), value1.encode()),
                FieldValueItem(field2.encode(), value2.encode()),
                FieldValueItem(field3.encode(), value3.encode()),
            ],
        )

        assert not ExhscanResult(
            field4.encode(),
            [
                FieldValueItem(field1.encode(), value1.encode()),
                FieldValueItem(field2.encode(), value2.encode()),
                FieldValueItem(field3.encode(), value3.encode()),
            ],
        ) == ExhscanResult(
            field4.encode(),
            [
                FieldValueItem(field3.encode(), value3.encode()),
                FieldValueItem(field2.encode(), value2.encode()),
                FieldValueItem(field1.encode(), value1.encode()),
            ],
        )

        assert (
            not ExhscanResult(
                field4.encode(),
                [
                    FieldValueItem(field1.encode(), value1.encode()),
                    FieldValueItem(field2.encode(), value2.encode()),
                    FieldValueItem(field3.encode(), value3.encode()),
                ],
            )
            == "xxx"
        )

    def test_exhscan_result_ne(self):
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        field3 = "field_" + str(uuid.uuid4())
        field4 = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())
        value3 = "value_" + str(uuid.uuid4())

        assert not ExhscanResult(
            field4.encode(),
            (
                FieldValueItem(field1.encode(), value1.encode()),
                FieldValueItem(field2.encode(), value2.encode()),
                FieldValueItem(field3.encode(), value3.encode()),
            ),
        ) != ExhscanResult(
            field4.encode(),
            [
                FieldValueItem(field1.encode(), value1.encode()),
                FieldValueItem(field2.encode(), value2.encode()),
                FieldValueItem(field3.encode(), value3.encode()),
            ],
        )

        assert ExhscanResult(
            field4.encode(),
            [
                FieldValueItem(field1.encode(), value1.encode()),
                FieldValueItem(field2.encode(), value2.encode()),
                FieldValueItem(field3.encode(), value3.encode()),
            ],
        ) != ExhscanResult(
            field4.encode(),
            [
                FieldValueItem(field3.encode(), value3.encode()),
                FieldValueItem(field2.encode(), value2.encode()),
                FieldValueItem(field1.encode(), value1.encode()),
            ],
        )

        assert (
            ExhscanResult(
                field4.encode(),
                [
                    FieldValueItem(field1.encode(), value1.encode()),
                    FieldValueItem(field2.encode(), value2.encode()),
                    FieldValueItem(field3.encode(), value3.encode()),
                ],
            )
            != "xxx"
        )

    def test_exhscan_result_repr(self):
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        field3 = "field_" + str(uuid.uuid4())
        field4 = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())
        value3 = "value_" + str(uuid.uuid4())

        items = [
            FieldValueItem(field1.encode(), value1.encode()),
            FieldValueItem(field2.encode(), value2.encode()),
            FieldValueItem(field3.encode(), value3.encode()),
        ]

        result = ExhscanResult(
            field4.encode(),
            items,
        )

        assert str(result) == f"{{next_field: {field4}, items: {items}}}"

    def test_parse_exhincrbyfloat(self):
        assert parse_exhincrbyfloat(None) is None
        assert parse_exhincrbyfloat(b"3.14") == 3.14

    def test_parse_exhgetwithver(self):
        value = "value_" + str(uuid.uuid4())
        assert parse_exhgetwithver(None) is None
        assert parse_exhgetwithver([value.encode(), 100]) == ValueVersionItem(
            value.encode(), 100
        )

    def test_parse_exhmgetwithver(self):
        value = "value_" + str(uuid.uuid4())
        assert parse_exhmgetwithver(None) is None
        assert parse_exhmgetwithver([[value.encode(), 100], None]) == [
            ValueVersionItem(value.encode(), 100),
            None,
        ]

    def test_parse_exhgetall(self):
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())
        assert parse_exhgetall(
            [
                field1.encode(),
                value1.encode(),
                field2.encode(),
                value2.encode(),
            ]
        ) == [
            FieldValueItem(field1.encode(), value1.encode()),
            FieldValueItem(field2.encode(), value2.encode()),
        ]

    def test_parse_exhscan(self):
        field1 = "field_" + str(uuid.uuid4())
        field2 = "field_" + str(uuid.uuid4())
        field3 = "field_" + str(uuid.uuid4())
        field4 = "field_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())
        value3 = "value_" + str(uuid.uuid4())

        assert parse_exhscan([b"", []]) is None
        assert parse_exhscan(
            [
                field4.encode(),
                [
                    field1.encode(),
                    value1.encode(),
                    field2.encode(),
                    value2.encode(),
                    field3.encode(),
                    value3.encode(),
                ],
            ]
        ) == ExhscanResult(
            field4.encode(),
            [
                FieldValueItem(field1.encode(), value1.encode()),
                FieldValueItem(field2.encode(), value2.encode()),
                FieldValueItem(field3.encode(), value3.encode()),
            ],
        )
