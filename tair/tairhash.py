import datetime
import time
from typing import Dict, Iterable, List, Optional, Union

from tair.exceptions import DataError
from tair.typing import (
    AbsExpiryT,
    CommandsProtocol,
    EncodableT,
    ExpiryT,
    FieldT,
    KeyT,
    ResponseT,
)


class ValueVersionItem:
    def __init__(self, value: Union[bytes, str], version: int) -> None:
        self.value = value
        self.version = version

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ValueVersionItem):
            return False
        return self.value == other.value and self.version == other.version

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __repr__(self) -> str:
        return f"{{value: {self.value}, version: {self.version}}}"


class FieldValueItem:
    def __init__(self, field: Union[bytes, str], value: Union[bytes, str]) -> None:
        self.field = field
        self.value = value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FieldValueItem):
            return False
        return self.field == other.field and self.value == other.value

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, FieldValueItem):
            raise TypeError(
                "Cannot compare 'FieldValueItem' with non-FieldValueItem objects."
            )
        return self.field < other.field or (
            self.field == other.field and self.value < other.value
        )

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __repr__(self) -> str:
        return f"{{field: {self.field}, value: {self.value}}}"


class ExhscanResult:
    def __init__(
        self, next_field: Union[bytes, str], items: Iterable[FieldValueItem]
    ) -> None:
        self.next_field = next_field
        self.items = list(items)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ExhscanResult):
            return False
        return self.next_field == other.next_field and self.items == other.items

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __repr__(self) -> str:
        return f"{{next_field: {self.next_field}, items: {self.items}}}"


class TairHashCommands(CommandsProtocol):
    def exhset(
        self,
        key: KeyT,
        field: FieldT,
        value: EncodableT,
        ex: Optional[ExpiryT] = None,
        px: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        pxat: Optional[AbsExpiryT] = None,
        nx: bool = False,
        xx: bool = False,
        ver: Optional[int] = None,
        abs: Optional[int] = None,
        keepttl: bool = False,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, field, value]

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

        if keepttl:
            pieces.append("KEEPTTL")

        return self.execute_command("EXHSET", *pieces)

    def exhget(self, key: KeyT, field: FieldT) -> ResponseT:
        return self.execute_command("EXHGET", key, field)

    def exhmset(self, key: KeyT, mapping: Dict[FieldT, EncodableT]) -> ResponseT:
        pieces: List[EncodableT] = [key]

        for field, value in mapping.items():
            pieces.append(field)
            pieces.append(value)

        return self.execute_command("EXHMSET", *pieces)

    def exhpexpireat(
        self,
        key: KeyT,
        field: FieldT,
        pxat: AbsExpiryT,
        ver: Optional[int] = None,
        abs: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, field, pxat]

        if ver is not None:
            pieces.append("VER")
            pieces.append(ver)
        if abs is not None:
            pieces.append("ABS")
            pieces.append(abs)

        return self.execute_command("EXHPEXPIREAT", *pieces)

    def exhpexpire(
        self,
        key: KeyT,
        field: FieldT,
        px: ExpiryT,
        ver: Optional[int] = None,
        abs: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, field, px]

        if ver is not None:
            pieces.append("VER")
            pieces.append(ver)
        if abs is not None:
            pieces.append("ABS")
            pieces.append(abs)

        return self.execute_command("EXHPEXPIRE", *pieces)

    def exhexpireat(
        self,
        key: KeyT,
        field: FieldT,
        exat: AbsExpiryT,
        ver: Optional[int] = None,
        abs: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, field, exat]

        if ver is not None:
            pieces.append("VER")
            pieces.append(ver)
        if abs is not None:
            pieces.append("ABS")
            pieces.append(abs)

        return self.execute_command("EXHEXPIREAT", *pieces)

    def exhexpire(
        self,
        key: KeyT,
        field: FieldT,
        ex: ExpiryT,
        ver: Optional[int] = None,
        abs: Optional[int] = None,
    ) -> ResponseT:
        pieces = [key, field, ex]

        if ver is not None:
            pieces.append("VER")
            pieces.append(ver)
        if abs is not None:
            pieces.append("ABS")
            pieces.append(abs)

        return self.execute_command("EXHEXPIRE", *pieces)

    def exhpttl(self, key: KeyT, field: FieldT) -> ResponseT:
        return self.execute_command("EXHPTTL", key, field)

    def exhttl(self, key: KeyT, field: FieldT) -> ResponseT:
        return self.execute_command("EXHTTL", key, field)

    def exhver(self, key: KeyT, field: FieldT) -> ResponseT:
        return self.execute_command("EXHVER", key, field)

    def exhsetver(self, key: KeyT, field: FieldT, version: int) -> ResponseT:
        return self.execute_command("EXHSETVER", key, field, version)

    def exhincrby(
        self,
        key: KeyT,
        field: FieldT,
        num: int,
        ex: Optional[ExpiryT] = None,
        px: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        pxat: Optional[AbsExpiryT] = None,
        ver: Optional[int] = None,
        abs: Optional[int] = None,
        minval: Optional[int] = None,
        maxval: Optional[int] = None,
        keepttl: bool = False,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, field, num]

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

        if keepttl:
            pieces.append("KEEPTTL")

        return self.execute_command("EXHINCRBY", *pieces)

    def exhincrbyfloat(
        self,
        key: KeyT,
        field: FieldT,
        num: float,
        ex: Optional[ExpiryT] = None,
        px: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        pxat: Optional[AbsExpiryT] = None,
        ver: Optional[int] = None,
        abs: Optional[int] = None,
        minval: Optional[float] = None,
        maxval: Optional[float] = None,
        keepttl: bool = False,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, field, num]

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

        if keepttl:
            pieces.append("KEEPTTL")

        return self.execute_command("EXHINCRBYFLOAT", *pieces)

    def exhgetwithver(self, ket: KeyT, field: FieldT) -> ResponseT:
        return self.execute_command("EXHGETWITHVER", ket, field)

    def exhmget(self, key: KeyT, fields: Iterable[FieldT]) -> ResponseT:
        return self.execute_command("EXHMGET", key, *fields)

    def exhmgetwithver(self, key: KeyT, fields: Iterable[FieldT]) -> ResponseT:
        return self.execute_command("EXHMGETWITHVER", key, *fields)

    def exhlen(self, key: KeyT, noexp: bool = False) -> ResponseT:
        if noexp:
            return self.execute_command("EXHLEN", key, "NOEXP")
        return self.execute_command("EXHLEN", key)

    def exhexists(self, key: KeyT, field: FieldT) -> ResponseT:
        return self.execute_command("EXHEXISTS", key, field)

    def exhstrlen(self, key: KeyT, field: FieldT) -> ResponseT:
        return self.execute_command("EXHSTRLEN", key, field)

    def exhkeys(self, key: KeyT) -> ResponseT:
        return self.execute_command("EXHKEYS", key)

    def exhvals(self, key: KeyT) -> ResponseT:
        return self.execute_command("EXHVALS", key)

    def exhgetall(self, key: KeyT) -> ResponseT:
        return self.execute_command("EXHGETALL", key)

    # the open source version of exhscan is inconsistent with the enterprise version,
    # so this method is temporarily commented out.

    # def exhscan(
    #     self,
    #     key: KeyT,
    #     op: str,
    #     subkey: KeyT,
    #     match: Optional[str] = None,
    #     count: Optional[int] = None,
    # ) -> ResponseT:
    #     pieces: List[EncodableT] = [key, op, subkey]

    #     if match is not None:
    #         pieces.append("MATCH")
    #         pieces.append(match)
    #     if count is not None:
    #         pieces.append("COUNT")
    #         pieces.append(count)

    #     return self.execute_command("EXHSCAN", *pieces)

    def exhdel(self, key: KeyT, fields: Iterable[FieldT]) -> ResponseT:
        return self.execute_command("EXHDEL", key, *fields)


def parse_exhincrbyfloat(resp) -> Union[float, None]:
    if resp is None:
        return resp
    return float(resp)


def parse_exhgetwithver(resp) -> Union[ValueVersionItem, None]:
    if resp is None:
        return None
    return ValueVersionItem(resp[0], resp[1])


def parse_exhmgetwithver(resp) -> Union[List[Union[ValueVersionItem, None]], None]:
    if resp is None:
        return None
    result: List[ValueVersionItem] = []
    for i in resp:
        if i is None:
            result.append(None)
            continue
        result.append(ValueVersionItem(i[0], i[1]))
    return result


def parse_exhgetall(resp) -> List[FieldValueItem]:
    result: List[FieldValueItem] = []
    for i in range(0, len(resp), 2):
        result.append(FieldValueItem(resp[i], resp[i + 1]))
    return result


def parse_exhscan(resp) -> Union[ExhscanResult, None]:
    if resp == [b"", []]:
        return None
    items: List[FieldValueItem] = []
    for i in range(0, len(resp[1]), 2):
        items.append(FieldValueItem(resp[1][i], resp[1][i + 1]))
    return ExhscanResult(resp[0], items)
