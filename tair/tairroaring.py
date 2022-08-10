from typing import Iterable, List, Optional

from .typing import (
    CommandsProtocol,
    EncodableT,
    KeyT,
    ResponseT,
)


class TrScanResult:
    def __init__(self, start_offset: int, offsets: Iterable[int]) -> None:
        self.start_offset = start_offset
        self.offsets = list(offsets)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TrScanResult):
            return False
        return self.start_offset == other.start_offset and self.offsets == other.offsets

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __repr__(self) -> str:
        return f"{{start_offset: {self.start_offset}, offsets: {self.offsets}}}"


class TairRoaringCommands(CommandsProtocol):
    def tr_setbit(self, key: KeyT, offset: int, value: int) -> ResponseT:
        return self.execute_command("TR.SETBIT", key, offset, value)

    def tr_setbits(self, key: KeyT, offsets: Iterable[int]) -> ResponseT:
        return self.execute_command("TR.SETBITS", key, *offsets)

    def tr_clearbits(self, key: KeyT, offsets: Iterable[int]) -> ResponseT:
        return self.execute_command("TR.CLEARBITS", key, *offsets)

    def tr_setrange(self, key: KeyT, start: int, end: int) -> ResponseT:
        return self.execute_command("TR.SETRANGE", key, start, end)

    def tr_appendbitarray(self, key: KeyT, offset: int, bitarray: str) -> ResponseT:
        return self.execute_command("TR.APPENDBITARRAY", key, offset, bitarray)

    def tr_fliprange(self, key: KeyT, start: int, end: int) -> ResponseT:
        return self.execute_command("TR.FLIPRANGE", key, start, end)

    def tr_appendintarray(self, key: KeyT, values: Iterable[int]) -> ResponseT:
        return self.execute_command("TR.APPENDINTARRAY", key, *values)

    def tr_setintarray(self, key: KeyT, values: Iterable[int]) -> ResponseT:
        return self.execute_command("TR.SETINTARRAY", key, *values)

    def tr_setbitarray(self, key: KeyT, value: str) -> ResponseT:
        return self.execute_command("TR.SETBITARRAY", key, value)

    def tr_bitop(
        self,
        destkey: KeyT,
        operation: str,
        keys: Iterable[KeyT],
    ) -> ResponseT:
        return self.execute_command("TR.BITOP", destkey, operation, *keys)

    def tr_bitopcard(self, operation: str, keys: Iterable[KeyT]) -> ResponseT:
        return self.execute_command("TR.BITOPCARD", operation, *keys)

    def tr_optimize(self, key: KeyT) -> ResponseT:
        return self.execute_command("TR.OPTIMIZE", key)

    def tr_getbit(self, key: KeyT, offset: int) -> ResponseT:
        return self.execute_command("TR.GETBIT", key, offset)

    def tr_getbits(self, key: KeyT, offsets: Iterable[int]) -> ResponseT:
        return self.execute_command("TR.GETBITS", key, *offsets)

    def tr_bitcount(
        self,
        key: KeyT,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key]

        if start is not None:
            pieces.append(start)
        if end is not None:
            pieces.append(end)

        return self.execute_command("TR.BITCOUNT", *pieces)

    def tr_bitpos(
        self,
        key: KeyT,
        value: int,
        count: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, value]

        if count is not None:
            pieces.append(count)

        return self.execute_command("TR.BITPOS", *pieces)

    def tr_scan(
        self,
        key: KeyT,
        start_offset: int,
        count: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, start_offset]

        if count is not None:
            pieces.append("COUNT")
            pieces.append(count)

        return self.execute_command("TR.SCAN", *pieces)

    def tr_range(self, key: KeyT, start: int, end: int) -> ResponseT:
        return self.execute_command("TR.RANGE", key, start, end)

    def tr_rangebitarray(self, key: KeyT, start: int, end: int) -> ResponseT:
        return self.execute_command("TR.RANGEBITARRAY", key, start, end)

    def tr_min(self, key: KeyT) -> ResponseT:
        return self.execute_command("TR.MIN", key)

    def tr_max(self, key: KeyT) -> ResponseT:
        return self.execute_command("TR.MAX", key)

    def tr_stat(self, key: KeyT, json: bool = False) -> ResponseT:
        if json:
            return self.execute_command("TR.STAT", key, "JSON")
        return self.execute_command("TR.STAT", key)

    def tr_jaccard(self, key1: KeyT, key2: KeyT) -> ResponseT:
        return self.execute_command("TR.JACCARD", key1, key2)

    def tr_contains(self, key1: KeyT, key2: KeyT) -> ResponseT:
        return self.execute_command("TR.CONTAINS", key1, key2)

    def tr_rank(self, key: KeyT, offset: int) -> ResponseT:
        return self.execute_command("TR.RANK", key, offset)


def parse_tr_scan(resp) -> TrScanResult:
    return TrScanResult(resp[0], resp[1])
