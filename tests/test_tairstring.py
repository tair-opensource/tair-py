import datetime
import time
import uuid

import pytest

from tair import DataError, ExcasResult, ExgetResult, ResponseError, Tair

from .conftest import NETWORK_DELAY_CALIBRATION_VALUE, compare_str, get_server_time


class TestTairString:
    def test_exset_success(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exset(key, value)
        result = t.exget(key)
        assert compare_str(result.value, value.encode())
        assert result.version == 1

    def test_exset_ex(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exset(key, value, ex=10)
        assert 0 < t.ttl(key) <= 10

        # ex should not be a float.
        with pytest.raises(DataError):
            t.exset(key, value, ex=10.0)

    def test_exset_ex_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(seconds=10)

        assert t.exset(key, value, ex=expire_at)
        assert 0 < t.ttl(key) <= 10

    def test_exset_px(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exset(key, value, px=10000)
        assert 0 < t.pttl(key) <= 10000

        # px should not be a float.
        with pytest.raises(DataError):
            t.exset(key, value, px=10000.0)

    def test_exset_px_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(milliseconds=10000)

        assert t.exset(key, value, px=expire_at)
        assert 0 < t.pttl(key) <= 10000

    def test_exset_exat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)
        exat = int(time.mktime(expire_at.timetuple()))

        assert t.exset(key, value, exat=exat)
        assert 0 < t.ttl(key) <= 10

    def test_exset_exat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)

        assert t.exset(key, value, exat=expire_at)
        assert 0 < t.ttl(key) <= 10

    def test_exset_pxat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(expire_at.timetuple())) * 1000

        assert t.exset(key, value, pxat=pxat)
        # due to network delay, pttl may be greater than 10000.
        assert 0 < t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    def test_exset_pxat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)

        assert t.exset(key, value, pxat=expire_at)
        # due to network delay, pttl may be greater than 10000.
        assert 0 < t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    def test_exset_nx(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exset(key, value, nx=True)
        # if the key exists and nx is True, exset will return None.
        assert t.exset(key, value, nx=True) is None

    def test_exset_xx(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())

        # if the key does not exists and xx is True, exset will return None.
        assert t.exset(key, value1, xx=True) is None
        assert t.exset(key, value1)
        assert t.exset(key, value2, xx=True)

    def test_exset_ver(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())

        assert t.exset(key, value1)
        assert t.exset(key, value2, ver=1)
        result = t.exget(key)
        assert compare_str(result.value, value2)
        assert result.version == 2

    def test_exset_abs(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exset(key, value, abs=10)
        result = t.exget(key)
        assert compare_str(result.value, value)
        assert result.version == 10

    def test_exsetver(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exset(key1, value)
        assert t.exsetver(key1, 10) == 1
        result = t.exget(key1)
        assert compare_str(result.value, value)
        assert result.version == 10

        # if the key does not exists, exsetver will return 0.
        assert t.exsetver(key2, 10) == 0

    def test_exincrby_success(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.exset(key, 10)
        assert t.exincrby(key, 20) == 30
        result = t.exget(key)
        assert compare_str(result.value, "30")
        assert result.version == 2

    def test_exincrby_ex(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.exset(key, 10)
        assert t.exincrby(key, 20, ex=10) == 30
        assert 0 < t.ttl(key) <= 10

        # ex should not be a float.
        with pytest.raises(DataError):
            t.exincrby(key, 20, ex=10.0)

    def test_exincrby_ex_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(seconds=10)

        assert t.exset(key, 10)
        assert t.exincrby(key, 20, ex=expire_at) == 30
        assert 0 < t.ttl(key) <= 10

    def test_exincrby_px(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.exset(key, 10)
        assert t.exincrby(key, 20, px=10000) == 30
        assert 0 < t.pttl(key) <= 10000

        # px should not be a float.
        with pytest.raises(DataError):
            t.exincrby(key, 20, px=10000.0)

    def test_exincrby_px_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(milliseconds=10000)

        assert t.exset(key, 10)
        assert t.exincrby(key, 20, px=expire_at) == 30
        assert 0 < t.pttl(key) <= 10000

    def test_exincrby_exat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)
        exat = int(time.mktime(expire_at.timetuple()))

        assert t.exset(key, 10)
        assert t.exincrby(key, 20, exat=exat) == 30
        assert 0 < t.ttl(key) <= 10

    def test_exincrby_exat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)

        assert t.exset(key, 10)
        assert t.exincrby(key, 20, exat=expire_at) == 30
        assert 0 < t.ttl(key) <= 10

    def test_exincrby_pxat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(expire_at.timetuple())) * 1000

        assert t.exset(key, 10)
        assert t.exincrby(key, 20, pxat=pxat) == 30
        # due to network delay, pttl may be greater than 10000.
        assert 0 < t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    def test_exincrby_pxat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)

        assert t.exset(key, 10)
        assert t.exincrby(key, 20, pxat=expire_at) == 30
        # due to network delay, pttl may be greater than 10000.
        assert 0 < t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    def test_exincrby_nx(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert t.exincrby(key1, 20, nx=True) == 20
        result = t.exget(key1)
        assert compare_str(result.value, "20")
        assert result.version == 1

        assert t.exset(key2, 10)
        assert t.exincrby(key2, 20, nx=True) is None
        result = t.exget(key2)
        assert compare_str(result.value, "10")
        assert result.version == 1

    def test_exincrby_xx(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert t.exincrby(key1, 20, xx=True) is None
        assert not t.exists(key1)

        assert t.exset(key2, 10)
        assert t.exincrby(key2, 20, xx=True) == 30
        result = t.exget(key2)
        assert compare_str(result.value, "30")
        assert result.version == 2

    def test_exincrby_ver(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.exset(key, 10)
        assert t.exincrby(key, 20, ver=1) == 30
        result = t.exget(key)
        assert compare_str(result.value, "30")
        assert result.version == 2

        assert t.exset(key, 10)
        with pytest.raises(ResponseError):
            t.exincrby(key, 20, ver=10)

    def test_exincrby_abs(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.exset(key, 10)
        assert t.exincrby(key, 20, abs=100) == 30
        result = t.exget(key)
        assert compare_str(result.value, "30")
        assert result.version == 100

    def test_exincrby_overflow(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.exset(key, 10)
        with pytest.raises(ResponseError):
            t.exincrby(key, 20, maxval=10)
        with pytest.raises(ResponseError):
            t.exincrby(key, 20, minval=100)

    def test_exincrbyfloat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.exset(key, 1.1)
        assert t.exincrbyfloat(key, 2.2) == pytest.approx(3.3)
        result = t.exget(key)
        assert float(result.value) == pytest.approx(3.3)
        assert result.version == 2

    def test_exincrbyfloat_ex(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.exset(key, 1.1)
        assert t.exincrbyfloat(key, 2.2, ex=10) == pytest.approx(3.3)
        assert 0 < t.ttl(key) <= 10

        # ex should not be a float.
        with pytest.raises(DataError):
            t.exincrbyfloat(key, 20, ex=10.0)

    def test_exincrbyfloat_ex_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(seconds=10)

        assert t.exset(key, 1.1)
        assert t.exincrbyfloat(key, 2.2, ex=expire_at) == pytest.approx(3.3)
        assert 0 < t.ttl(key) <= 10

    def test_exincrbyfloat_px(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.exset(key, 1.1)
        assert t.exincrbyfloat(key, 2.2, px=10000) == pytest.approx(3.3)
        assert 0 < t.pttl(key) <= 10000

        # px should not be a float.
        with pytest.raises(DataError):
            t.exincrbyfloat(key, 20, px=10000.0)

    def test_exincrbyfloat_px_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = datetime.timedelta(milliseconds=10000)

        assert t.exset(key, 1.1)
        assert t.exincrbyfloat(key, 2.2, px=expire_at) == pytest.approx(3.3)
        assert 0 < t.pttl(key) <= 10000

    def test_exincrbyfloat_exat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)
        exat = int(time.mktime(expire_at.timetuple()))

        assert t.exset(key, 1.1)
        assert t.exincrbyfloat(key, 2.2, exat=exat) == pytest.approx(3.3)
        assert 0 < t.ttl(key) <= 10

    def test_exincrbyfloat_exat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)

        assert t.exset(key, 1.1)
        assert t.exincrbyfloat(key, 2.2, exat=expire_at) == pytest.approx(3.3)
        assert 0 < t.ttl(key) <= 10

    def test_exincrbyfloat_pxat(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)
        pxat = int(time.mktime(expire_at.timetuple())) * 1000

        assert t.exset(key, 1.1)
        assert t.exincrbyfloat(key, 2.2, pxat=pxat) == pytest.approx(3.3)
        # due to network delay, pttl may be greater than 10000.
        assert 0 < t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    def test_exincrbyfloat_pxat_timedelta(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        expire_at = get_server_time(t) + datetime.timedelta(seconds=10)

        assert t.exset(key, 1.1)
        assert t.exincrbyfloat(key, 2.2, pxat=expire_at) == pytest.approx(3.3)
        # due to network delay, pttl may be greater than 10000.
        assert 0 < t.pttl(key) <= (10000 + NETWORK_DELAY_CALIBRATION_VALUE)

    def test_exincrbyfloat_nx(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert t.exincrbyfloat(key1, 1.1, nx=True) == 1.1
        result = t.exget(key1)
        assert float(result.value) == pytest.approx(1.1)
        assert result.version == 1

        assert t.exset(key2, 1.1)
        assert t.exincrbyfloat(key2, 2.2, nx=True) is None
        result = t.exget(key2)
        assert float(result.value) == pytest.approx(1.1)
        assert result.version == 1

    def test_exincrbyfloat_xx(self, t: Tair):
        key1 = "key_" + str(uuid.uuid4())
        key2 = "key_" + str(uuid.uuid4())

        assert t.exincrbyfloat(key1, 1.1, xx=True) is None
        assert not t.exists(key1)

        assert t.exset(key2, 1.1)
        assert t.exincrbyfloat(key2, 2.2, xx=True) == pytest.approx(3.3)
        result = t.exget(key2)
        assert float(result.value) == pytest.approx(3.3)
        assert result.version == 2

    def test_exincrbyfloat_ver(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.exset(key, 1.1)
        assert t.exincrbyfloat(key, 2.2, ver=1) == pytest.approx(3.3)
        result = t.exget(key)
        assert float(result.value) == pytest.approx(3.3)
        assert result.version == 2

        assert t.exset(key, 1.1)
        with pytest.raises(ResponseError):
            t.exincrbyfloat(key, 2.2, ver=10)

    def test_exincrbyfloat_abs(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.exset(key, 1.1)
        assert t.exincrbyfloat(key, 2.2, abs=100) == pytest.approx(3.3)
        result = t.exget(key)
        assert float(result.value) == pytest.approx(3.3)
        assert result.version == 100

    def test_exincrbyfloat_overflow(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.exset(key, 9.1)
        with pytest.raises(ResponseError):
            t.exincrbyfloat(key, 1.1, maxval=10)
        with pytest.raises(ResponseError):
            t.exincrbyfloat(key, 0.1, minval=100)

    def test_excas_success(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())

        assert t.exset(key, value1)
        excas_result = t.excas(key, value2, 1)
        assert compare_str(excas_result.msg, "OK")
        assert compare_str(excas_result.value, "")
        assert excas_result.version == 2
        exget_result = t.exget(key)
        assert compare_str(exget_result.value, value2)
        assert exget_result.version == 2

    def test_excas_failed(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value1 = "value_" + str(uuid.uuid4())
        value2 = "value_" + str(uuid.uuid4())

        assert t.exset(key, value1)
        result = t.excas(key, value2, 100)
        assert compare_str(result.msg, "CAS_FAILED")
        assert compare_str(result.value, value1)
        assert result.version == 1

    def test_excas_not_exists(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        # if the key does not exist, return -1.
        assert t.excas(key, value, 1) == -1

    def test_excad(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.exset(key, value)
        assert t.excad(key, 1) == 1
        assert not t.exists(key)

    def test_excad_not_exists(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        # if the key does not exist, return -1.
        assert t.excad(key, 1) == -1

    def test_cas_success(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.set(key, value) is True
        assert t.cas(key, value, "newval") == 1

    def test_cas_fail(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.set(key, value) is True
        assert t.cas(key, "oldkey", "newval") == 0

    def test_cad_success(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.setnx(key, value) is True
        assert t.cad(key, value) == 1

    def test_cad_fail(self, t: Tair):
        key = "key_" + str(uuid.uuid4())
        value = "value_" + str(uuid.uuid4())

        assert t.setnx(key, value) is True
        assert t.cad(key, "newval") == 0

    def test_exget_result_eq(self):
        value = "value_" + str(uuid.uuid4())
        assert ExgetResult(value.encode(), 1) == ExgetResult(value.encode(), 1)
        assert not ExgetResult(value.encode(), 1) == ExgetResult(value.encode(), 2)
        assert not ExgetResult(value.encode(), 1) == 1

    def test_exget_result_ne(self):
        value = "value_" + str(uuid.uuid4())
        assert not ExgetResult(value.encode(), 1) != ExgetResult(value.encode(), 1)
        assert ExgetResult(value.encode(), 1) != ExgetResult(value.encode(), 2)
        assert ExgetResult(value.encode(), 1) != 1

    def test_exget_result_repr(self):
        value = "value_" + str(uuid.uuid4())
        assert (
            str(ExgetResult(value.encode(), 100))
            == f"{{value: {value.encode()}, version: 100}}"
        )

    def test_excas_result_eq(self):
        value = "value_" + str(uuid.uuid4())
        assert ExcasResult("OK", value.encode(), 1) == ExcasResult(
            "OK", value.encode(), 1
        )
        assert not ExcasResult("OK", value.encode(), 1) == ExcasResult(
            "OK", value.encode(), 2
        )
        assert not ExcasResult("OK", value.encode(), 1) == 1

    def test_excas_result_ne(self):
        value = "value_" + str(uuid.uuid4())
        assert not ExcasResult("OK", value.encode(), 1) != ExcasResult(
            "OK", value.encode(), 1
        )
        assert ExcasResult("OK", value.encode(), 1) != ExcasResult(
            "OK", value.encode(), 2
        )
        assert ExcasResult("OK", value.encode(), 1) != 1

    def test_excas_result_repr(self):
        value = "value_" + str(uuid.uuid4())
        assert (
            str(ExcasResult("OK", value.encode(), 100))
            == f"{{msg: OK, value: {value.encode()}, version: 100}}"
        )
