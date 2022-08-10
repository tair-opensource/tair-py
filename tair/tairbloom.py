from typing import Iterable, List, Optional

from .typing import (
    CommandsProtocol,
    EncodableT,
    KeyT,
    ResponseT,
)


class TairBloomCommands(CommandsProtocol):
    def bf_reserve(
        self,
        key: KeyT,
        error_rate: float,
        capacity: int,
    ) -> ResponseT:
        return self.execute_command("BF.RESERVE", key, error_rate, capacity)

    def bf_add(self, key: KeyT, item: EncodableT) -> ResponseT:
        return self.execute_command("BF.ADD", key, item)

    def bf_madd(self, key: KeyT, items: Iterable[EncodableT]) -> ResponseT:
        return self.execute_command("BF.MADD", key, *items)

    def bf_exists(self, key: KeyT, item: EncodableT) -> ResponseT:
        return self.execute_command("BF.EXISTS", key, item)

    def bf_mexists(self, key: KeyT, items: Iterable[EncodableT]) -> ResponseT:
        return self.execute_command("BF.MEXISTS", key, *items)

    def bf_insert(
        self,
        key: KeyT,
        items: Iterable[EncodableT],
        capacity: Optional[int] = None,
        error_rate: Optional[float] = None,
        nocreate: bool = False,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key]

        if capacity is not None:
            pieces.append("CAPACITY")
            pieces.append(capacity)
        if error_rate is not None:
            pieces.append("ERROR")
            pieces.append(error_rate)
        if nocreate:
            pieces.append("NOCREATE")

        pieces.append("ITEMS")
        for item in items:
            pieces.append(item)

        return self.execute_command("BF.INSERT", *pieces)
