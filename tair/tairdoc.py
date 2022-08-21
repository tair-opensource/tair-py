from typing import Iterable, List, Optional, Union

from tair.typing import CommandsProtocol, EncodableT, KeyT, ResponseT


class TairDocCommands(CommandsProtocol):
    def json_set(
        self,
        key: KeyT,
        path: str,
        json: str,
        nx: bool = False,
        xx: bool = False,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, path, json]

        if nx:
            pieces.append("NX")
        elif xx:
            pieces.append("XX")

        return self.execute_command("JSON.SET", *pieces)

    def json_get(
        self,
        key: KeyT,
        path: str,
        format: Optional[str] = None,
        rootname: Optional[str] = None,
        arrname: Optional[str] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, path]

        if format is not None:
            pieces.append("FORMAT")
            pieces.append(format)
        if rootname is not None:
            pieces.append("ROOTNAME")
            pieces.append(rootname)
        if arrname is not None:
            pieces.append("ARRNAME")
            pieces.append(arrname)

        return self.execute_command("JSON.GET", *pieces)

    def json_del(self, key: KeyT, path: str) -> ResponseT:
        return self.execute_command("JSON.DEL", key, path)

    def json_type(self, key: KeyT, path: str) -> ResponseT:
        return self.execute_command("JSON.TYPE", key, path)

    def json_numincrby(
        self,
        key: KeyT,
        path: str,
        value: Union[int, float],
    ) -> ResponseT:
        return self.execute_command("JSON.NUMINCRBY", key, path, value)

    def json_strappend(
        self,
        key: KeyT,
        path: str,
        json_string: str,
    ) -> ResponseT:
        return self.execute_command("JSON.STRAPPEND", key, path, json_string)

    def json_strlen(self, key: KeyT, path: str) -> ResponseT:
        return self.execute_command("JSON.STRLEN", key, path)

    def json_arrappend(
        self,
        key: KeyT,
        path: str,
        json: Iterable[str],
    ) -> ResponseT:
        return self.execute_command("JSON.ARRAPPEND", key, path, *json)

    def json_arrpop(
        self,
        key: KeyT,
        path: str,
        index: Optional[int] = None,
    ) -> ResponseT:
        if index is not None:
            return self.execute_command("JSON.ARRPOP", key, path, index)
        return self.execute_command("JSON.ARRPOP", key, path)

    def json_arrinsert(
        self,
        key: KeyT,
        path: str,
        json: Iterable[str],
        index: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [key, path]

        if index is not None:
            pieces.append(index)

        pieces.extend(json)

        return self.execute_command("JSON.ARRINSERT", *pieces)

    def json_arrlen(
        self,
        key: KeyT,
        path: str,
    ) -> ResponseT:
        return self.execute_command("JSON.ARRLEN", key, path)

    def json_arrtrim(
        self,
        key: KeyT,
        path: str,
        start: int,
        stop: int,
    ) -> ResponseT:
        return self.execute_command("JSON.ARRTRIM", key, path, start, stop)
