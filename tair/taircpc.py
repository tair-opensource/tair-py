import datetime
import time
from typing import List, Optional

from tair.exceptions import DataError
from tair.typing import (
    AbsExpiryT,
    CommandsProtocol,
    EncodableT,
    ExpiryT,
    KeyT,
    ResponseT,
)


class CpcUpdate2judResult:
    def __init__(self, estimated_value: float, difference: float) -> None:
        self.estimated_value = estimated_value
        self.difference = difference

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CpcUpdate2judResult):
            return False
        return (
            self.estimated_value == other.estimated_value
            and self.difference == other.difference
        )

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __repr__(self) -> str:
        return f"{{estimated_value: {self.estimated_value}, difference: {self.difference}}}"


class TairCpcCommands(CommandsProtocol):
    def cpc_update(
        self,
        key: KeyT,
        item: EncodableT,
        ex: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        px: Optional[float] = None,
        pxat: Optional[AbsExpiryT] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, item]

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

        return self.execute_command("CPC.UPDATE", *pieces)

    def cpc_estimate(self, key: KeyT) -> ResponseT:
        return self.execute_command("CPC.ESTIMATE", key)

    def cpc_update2est(
        self,
        key: KeyT,
        item: EncodableT,
        ex: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        px: Optional[float] = None,
        pxat: Optional[AbsExpiryT] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, item]

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

        return self.execute_command("CPC.UPDATE2EST", *pieces)

    def cpc_update2jud(
        self,
        key: KeyT,
        item: EncodableT,
        ex: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        px: Optional[float] = None,
        pxat: Optional[AbsExpiryT] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, item]

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

        return self.execute_command("CPC.UPDATE2JUD", *pieces)

    def cpc_array_update(
        self,
        key: KeyT,
        timestamp: int,
        item: EncodableT,
        ex: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        px: Optional[float] = None,
        pxat: Optional[AbsExpiryT] = None,
        size: Optional[int] = None,
        win: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, timestamp, item]

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

        if size is not None:
            pieces.append("SIZE")
            pieces.append(size)
        if win is not None:
            pieces.append("WIN")
            pieces.append(win)

        return self.execute_command("CPC.ARRAY.UPDATE", *pieces)

    def cpc_array_estimate(self, key: KeyT, timestamp: int) -> ResponseT:
        return self.execute_command("CPC.ARRAY.ESTIMATE", key, timestamp)

    def cpc_array_estimate_range(
        self,
        key: KeyT,
        start_time: int,
        end_time: int,
    ) -> ResponseT:
        return self.execute_command(
            "CPC.ARRAY.ESTIMATE.RANGE", key, start_time, end_time
        )

    def cpc_array_estimate_range_merge(
        self,
        key: KeyT,
        timestamp: int,
        range: int,
    ) -> ResponseT:
        return self.execute_command(
            "CPC.ARRAY.ESTIMATE.RANGE.MERGE", key, timestamp, range
        )

    def cpc_array_update2est(
        self,
        key: KeyT,
        timestamp: int,
        item: EncodableT,
        ex: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        px: Optional[float] = None,
        pxat: Optional[AbsExpiryT] = None,
        size: Optional[int] = None,
        win: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, timestamp, item]

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

        if size is not None:
            pieces.append("SIZE")
            pieces.append(size)
        if win is not None:
            pieces.append("WIN")
            pieces.append(win)

        return self.execute_command("CPC.ARRAY.UPDATE2EST", *pieces)

    def cpc_array_update2jud(
        self,
        key: KeyT,
        timestamp: int,
        item: EncodableT,
        ex: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        px: Optional[float] = None,
        pxat: Optional[AbsExpiryT] = None,
        size: Optional[int] = None,
        win: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, timestamp, item]

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

        if size is not None:
            pieces.append("SIZE")
            pieces.append(size)
        if win is not None:
            pieces.append("WIN")
            pieces.append(win)

        return self.execute_command("CPC.ARRAY.UPDATE2JUD", *pieces)
