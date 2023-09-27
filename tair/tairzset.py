from typing import Iterable, List, Mapping, Optional, Union

from redis.typing import CommandsProtocol
from redis.utils import str_if_bytes

from tair.exceptions import DataError
from tair.typing import AnyKeyT, EncodableT, KeyT, ResponseT


class TairZsetItem:
    def __init__(self, member: Union[bytes, str], score: str) -> None:
        self.member = member
        self.score = score

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TairZsetItem):
            return False
        return self.member == other.member and self.score == other.score

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __repr__(self) -> str:
        return f"{{member: {self.member}, score: {self.score}}}"


class TairZsetCommands(CommandsProtocol):
    def exzadd(
        self,
        key: KeyT,
        mapping: Mapping[AnyKeyT, EncodableT],
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        incr: bool = False,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key]

        if nx:
            pieces.append("NX")
        if xx:
            pieces.append("XX")
        if ch:
            pieces.append("CH")
        if incr:
            pieces.append("INCR")

        for member, score in mapping.items():
            pieces.append(score)
            pieces.append(member)

        return self.execute_command("EXZADD", *pieces)

    def exzincrby(
        self,
        key: KeyT,
        increment: float,
        member: EncodableT,
    ) -> ResponseT:
        return self.execute_command("EXZINCRBY", key, increment, member)

    def exzscore(self, key: KeyT, member: EncodableT) -> ResponseT:
        return self.execute_command("EXZSCORE", key, member)

    def exzrange(
        self,
        key: KeyT,
        minval: EncodableT,
        maxval: EncodableT,
        withscores: bool = False,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, minval, maxval]
        if withscores:
            pieces.append("WITHSCORES")
        return self.execute_command("EXZRANGE", *pieces, withscores=withscores)

    def exzrevrange(
        self,
        key: KeyT,
        minval: EncodableT,
        maxval: EncodableT,
        withscores: bool = False,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, minval, maxval]
        if withscores:
            pieces.append("WITHSCORES")
        return self.execute_command("EXZREVRANGE", *pieces, withscores=withscores)

    def exzrangebyscore(
        self,
        key: KeyT,
        minval: EncodableT,
        maxval: EncodableT,
        withscores: bool = False,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, minval, maxval]

        if withscores:
            pieces.append("WITHSCORES")

        if offset is None and count is None:
            pass
        elif offset is not None and count is not None:
            pieces.append("LIMIT")
            pieces.append(offset)
            pieces.append(count)
        else:
            raise DataError("offset and count must be neither or both none")

        return self.execute_command("EXZRANGEBYSCORE", *pieces, withscores=withscores)

    def exzrevrangebyscore(
        self,
        key: KeyT,
        minval: EncodableT,
        maxval: EncodableT,
        withscores: bool = False,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, minval, maxval]

        if withscores:
            pieces.append("WITHSCORES")

        if offset is None and count is None:
            pass
        elif offset is not None and count is not None:
            pieces.append("LIMIT")
            pieces.append(offset)
            pieces.append(count)
        else:
            raise DataError("offset and count must be neither or both none")

        return self.execute_command(
            "EXZREVRANGEBYSCORE", *pieces, withscores=withscores
        )

    def exzrangebylex(
        self,
        key: KeyT,
        minval: EncodableT,
        maxval: EncodableT,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, minval, maxval]

        if offset is None and count is None:
            pass
        elif offset is not None and count is not None:
            pieces.append("LIMIT")
            pieces.append(offset)
            pieces.append(count)
        else:
            raise DataError("offset and count must be neither or both none")

        return self.execute_command("EXZRANGEBYLEX", *pieces)

    def exzrevrangebylex(
        self,
        key: KeyT,
        maxval: EncodableT,
        minval: EncodableT,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, maxval, minval]

        if offset is None and count is None:
            pass
        elif offset is not None and count is not None:
            pieces.append("LIMIT")
            pieces.append(offset)
            pieces.append(count)
        else:
            raise DataError("offset and count must be neither or both none")

        return self.execute_command("EXZREVRANGEBYLEX", *pieces)

    def exzrem(self, key: KeyT, members: Iterable[EncodableT]) -> ResponseT:
        return self.execute_command("EXZREM", key, *members)

    def exzremrangebyscore(
        self,
        key: KeyT,
        minval: EncodableT,
        maxval: EncodableT,
    ) -> ResponseT:
        return self.execute_command("EXZREMRANGEBYSCORE", key, minval, maxval)

    def exzremrangebyrank(self, key: KeyT, start: int, stop: int) -> ResponseT:
        return self.execute_command("EXZREMRANGEBYRANK", key, start, stop)

    def exzremrangebylex(
        self,
        key: KeyT,
        minval: EncodableT,
        maxval: EncodableT,
    ) -> ResponseT:
        return self.execute_command("EXZREMRANGEBYLEX", key, minval, maxval)

    def exzcard(self, key: KeyT) -> ResponseT:
        return self.execute_command("EXZCARD", key)

    def exzrank(self, key: KeyT, member: EncodableT) -> ResponseT:
        return self.execute_command("EXZRANK", key, member)

    def exzrevrank(self, key: KeyT, member: EncodableT) -> ResponseT:
        return self.execute_command("EXZREVRANK", key, member)

    def exzcount(
        self,
        key: KeyT,
        minval: EncodableT,
        maxval: EncodableT,
    ) -> ResponseT:
        return self.execute_command("EXZCOUNT", key, minval, maxval)

    def exzlexcount(
        self,
        key: KeyT,
        minval: EncodableT,
        maxval: EncodableT,
    ) -> ResponseT:
        return self.execute_command("EXZLEXCOUNT", key, minval, maxval)

    def exzrankbyscore(self, key: KeyT, score: EncodableT) -> ResponseT:
        return self.execute_command("EXZRANKBYSCORE", key, score)

    def exzrevrankbyscore(self, key: KeyT, score: EncodableT) -> ResponseT:
        return self.execute_command("EXZREVRANKBYSCORE", key, score)


def parse_tair_zset_items(resp, **options):
    result: List[TairZsetItem] = []
    if options.get("withscores"):
        for i in range(0, len(resp), 2):
            result.append(TairZsetItem(resp[i], str_if_bytes(resp[i + 1])))
    else:
        for i in resp:
            result.append(TairZsetItem(i, None))
    return result
