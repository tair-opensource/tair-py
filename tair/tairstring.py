import datetime
import time
from typing import List, Optional, Union

from .exceptions import DataError
from .typing import (
    AbsExpiryT,
    CommandsProtocol,
    EncodableT,
    ExpiryT,
    KeyT,
    ResponseT,
)


class ExgetResult:
    def __init__(self, value: bytes, version: int) -> None:
        self.value = value
        self.version = version

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ExgetResult):
            return False
        return self.value == other.value and self.version == other.version

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __repr__(self) -> str:
        return f"{{value: {self.value}, version: {self.version}}}"


class ExcasResult:
    def __init__(self, msg: str, value: bytes, version: int) -> None:
        self.msg = msg
        self.value = value
        self.version = version

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ExcasResult):
            return False
        return (
            self.msg == other.msg
            and self.value == other.value
            and self.version == other.version
        )

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __repr__(self) -> str:
        return f"{{msg: {self.msg}, value: {self.value}, version: {self.version}}}"


class TairStringCommands(CommandsProtocol):
    def exset(
        self,
        key: KeyT,
        value: EncodableT,
        ex: Optional[ExpiryT] = None,
        px: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        pxat: Optional[AbsExpiryT] = None,
        nx: bool = False,
        xx: bool = False,
        ver: Optional[int] = None,
        abs: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, value]

        if ex is not None:
            pieces.append("EX")
            if isinstance(ex, datetime.timedelta):
                pieces.append(int(ex.total_seconds()))
            elif isinstance(ex, int):
                pieces.append(ex)
            else:
                raise DataError("ex must be datetime.timedelta or int")
        if px is not None:
            pieces.append("PX")
            if isinstance(px, datetime.timedelta):
                pieces.append(int(px.total_seconds() * 1000))
            elif isinstance(px, int):
                pieces.append(px)
            else:
                raise DataError("px must be datetime.timedelta or int")
        if exat is not None:
            pieces.append("EXAT")
            if isinstance(exat, datetime.datetime):
                s = int(exat.microsecond / 1000000)
                exat = int(time.mktime(exat.timetuple())) + s
            pieces.append(exat)
        if pxat is not None:
            pieces.append("PXAT")
            if isinstance(pxat, datetime.datetime):
                ms = int(pxat.microsecond / 1000)
                pxat = int(time.mktime(pxat.timetuple())) * 1000 + ms
            pieces.append(pxat)

        if nx:
            pieces.append("NX")
        if xx:
            pieces.append("XX")

        if ver is not None:
            pieces.append("VER")
            pieces.append(ver)
        if abs is not None:
            pieces.append("ABS")
            pieces.append(abs)

        return self.execute_command("EXSET", *pieces)

    def exget(self, key: KeyT) -> ResponseT:
        return self.execute_command("EXGET", key)

    def exsetver(self, key: KeyT, version: int) -> ResponseT:
        return self.execute_command("EXSETVER", key, version)

    def exincrby(
        self,
        key: KeyT,
        num: int,
        ex: Optional[ExpiryT] = None,
        px: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        pxat: Optional[AbsExpiryT] = None,
        nx: bool = False,
        xx: bool = False,
        ver: Optional[int] = None,
        abs: Optional[int] = None,
        minval: Optional[int] = None,
        maxval: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, num]

        if ex is not None:
            pieces.append("EX")
            if isinstance(ex, datetime.timedelta):
                pieces.append(int(ex.total_seconds()))
            elif isinstance(ex, int):
                pieces.append(ex)
            else:
                raise DataError("ex must be datetime.timedelta or int")
        if px is not None:
            pieces.append("PX")
            if isinstance(px, datetime.timedelta):
                pieces.append(int(px.total_seconds() * 1000))
            elif isinstance(px, int):
                pieces.append(px)
            else:
                raise DataError("px must be datetime.timedelta or int")
        if exat is not None:
            pieces.append("EXAT")
            if isinstance(exat, datetime.datetime):
                s = int(exat.microsecond / 1000000)
                exat = int(time.mktime(exat.timetuple())) + s
            pieces.append(exat)
        if pxat is not None:
            pieces.append("PXAT")
            if isinstance(pxat, datetime.datetime):
                ms = int(pxat.microsecond / 1000)
                pxat = int(time.mktime(pxat.timetuple())) * 1000 + ms
            pieces.append(pxat)

        if nx:
            pieces.append("NX")
        if xx:
            pieces.append("XX")

        if ver is not None:
            pieces.append("VER")
            pieces.append(ver)
        if abs is not None:
            pieces.append("ABS")
            pieces.append(abs)

        if minval is not None:
            pieces.append("MIN")
            pieces.append(minval)
        if maxval is not None:
            pieces.append("MAX")
            pieces.append(maxval)

        return self.execute_command("EXINCRBY", *pieces)

    def exincrbyfloat(
        self,
        key: KeyT,
        num: float,
        ex: Optional[ExpiryT] = None,
        px: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        pxat: Optional[AbsExpiryT] = None,
        nx: bool = False,
        xx: bool = False,
        ver: Optional[int] = None,
        abs: Optional[int] = None,
        minval: Optional[float] = None,
        maxval: Optional[float] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, num]

        if ex is not None:
            pieces.append("EX")
            if isinstance(ex, datetime.timedelta):
                pieces.append(int(ex.total_seconds()))
            elif isinstance(ex, int):
                pieces.append(ex)
            else:
                raise DataError("ex must be datetime.timedelta or int")
        if px is not None:
            pieces.append("PX")
            if isinstance(px, datetime.timedelta):
                pieces.append(int(px.total_seconds() * 1000))
            elif isinstance(px, int):
                pieces.append(px)
            else:
                raise DataError("px must be datetime.timedelta or int")
        if exat is not None:
            pieces.append("EXAT")
            if isinstance(exat, datetime.datetime):
                s = int(exat.microsecond / 1000000)
                exat = int(time.mktime(exat.timetuple())) + s
            pieces.append(exat)
        if pxat is not None:
            pieces.append("PXAT")
            if isinstance(pxat, datetime.datetime):
                ms = int(pxat.microsecond / 1000)
                pxat = int(time.mktime(pxat.timetuple())) * 1000 + ms
            pieces.append(pxat)

        if nx:
            pieces.append("NX")
        if xx:
            pieces.append("XX")

        if ver is not None:
            pieces.append("VER")
            pieces.append(ver)
        if abs is not None:
            pieces.append("ABS")
            pieces.append(abs)

        if minval is not None:
            pieces.append("MIN")
            pieces.append(minval)
        if maxval is not None:
            pieces.append("MAX")
            pieces.append(maxval)

        return self.execute_command("EXINCRBYFLOAT", *pieces)

    def excas(self, key: KeyT, value: EncodableT, version: int) -> ResponseT:
        return self.execute_command("EXCAS", key, value, version)

    def excad(self, key: KeyT, version: int) -> ResponseT:
        return self.execute_command("EXCAD", key, version)

    def cas(self, key: KeyT, oldval: EncodableT, newval: EncodableT) -> ResponseT:
        return self.execute_command("CAS", key, oldval, newval)

    def cad(self, key: KeyT, value: EncodableT) -> ResponseT:
        return self.execute_command("CAD", key, value)


def parse_exset(resp) -> Union[bool, None]:
    if resp is None:
        return None
    return resp == b"OK"


def parse_exget(resp) -> ExgetResult:
    return ExgetResult(resp[0], resp[1])


def parse_excas(resp) -> ExcasResult:
    if isinstance(resp, int):
        return resp
    return ExcasResult(resp[0].decode(), resp[1], resp[2])


def parse_exincrbyfloat(resp) -> Union[float, None]:
    if resp is None:
        return resp
    return float(resp.decode())
