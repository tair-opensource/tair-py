from typing import Dict, Iterable, List, Optional

import tair
from tair.typing import CommandsProtocol, EncodableT, KeyT, ResponseT


class ScandocidResult:
    def __init__(self, cursor: str, doc_ids: Iterable[str]):
        self.cursor = cursor
        self.doc_ids = list(doc_ids)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ScandocidResult):
            return False
        return self.cursor == other.cursor and self.doc_ids == other.doc_ids

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __repr__(self) -> str:
        return f"{{cursor: {self.cursor}, doc_ids: {self.doc_ids}}}"


class TairSearchCommands(CommandsProtocol):
    def tft_createindex(self, index: KeyT, mappings: str) -> ResponseT:
        return self.execute_command("TFT.CREATEINDEX", index, mappings)

    def tft_updateindex(self, index: KeyT, mappings: str) -> ResponseT:
        return self.execute_command("TFT.UPDATEINDEX", index, mappings)

    def tft_getindex(self, index: KeyT) -> ResponseT:
        return self.execute_command("TFT.GETINDEX", index)

    def tft_getindex_mappings(self, index: KeyT) -> ResponseT:
        return self.execute_command("TFT.GETINDEX", index, "mappings")

    def tft_getindex_settings(self, index: KeyT) -> ResponseT:
        return self.execute_command("TFT.GETINDEX", index, "settings")

    def tft_adddoc(
        self,
        index: KeyT,
        document: str,
        doc_id: Optional[str] = None,
    ) -> ResponseT:
        if doc_id is not None:
            return self.execute_command(
                "TFT.ADDDOC", index, document, "WITH_ID", doc_id
            )
        return self.execute_command("TFT.ADDDOC", index, document)

    def tft_madddoc(self, index: KeyT, mapping: Dict[str, str]) -> ResponseT:
        pieces: List[EncodableT] = [index]

        for document, doc_id in mapping.items():
            pieces.append(document)
            pieces.append(doc_id)

        return self.execute_command("TFT.MADDDOC", *pieces)

    def tft_updatedocfield(
        self,
        index: KeyT,
        doc_id: str,
        document: str,
    ) -> ResponseT:
        return self.execute_command(
            "TFT.UPDATEDOCFIELD",
            index,
            doc_id,
            document,
        )

    def tft_deldocfield(
        self,
        index: KeyT,
        doc_id: str,
        fields: Iterable,
    ) -> ResponseT:
        return self.execute_command(
            "TFT.DELDOCFIELD",
            index,
            doc_id,
            *fields,
        )

    def tft_incrlongdocfield(
        self,
        index: KeyT,
        doc_id: str,
        field: str,
        increment: int,
    ) -> ResponseT:
        return self.execute_command(
            "TFT.INCRLONGDOCFIELD",
            index,
            doc_id,
            field,
            increment,
        )

    def tft_incrfloatdocfield(
        self,
        index: KeyT,
        doc_id: str,
        field: str,
        increment: float,
    ) -> ResponseT:
        return self.execute_command(
            "TFT.INCRFLOATDOCFIELD",
            index,
            doc_id,
            field,
            increment,
        )

    def tft_getdoc(self, index: KeyT, doc_id: str) -> ResponseT:
        return self.execute_command("TFT.GETDOC", index, doc_id)

    def tft_exists(self, index: KeyT, doc_id: str) -> ResponseT:
        return self.execute_command("TFT.EXISTS", index, doc_id)

    def tft_docnum(self, index: KeyT) -> ResponseT:
        return self.execute_command("TFT.DOCNUM", index)

    def tft_scandocid(
        self,
        index: KeyT,
        cursor: int,
        match: Optional[str] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [index, cursor]

        if match is not None:
            pieces.append("MATCH")
            pieces.append(match)
        if count is not None:
            pieces.append("COUNT")
            pieces.append(count)

        return self.execute_command("TFT.SCANDOCID", *pieces)

    def tft_deldoc(self, index: KeyT, doc_id: Iterable[str]) -> ResponseT:
        return self.execute_command("TFT.DELDOC", index, *doc_id)

    def tft_delall(self, index: KeyT) -> ResponseT:
        return self.execute_command("TFT.DELALL", index)

    def tft_search(self, index: KeyT, query: str, use_cache: bool = False) -> ResponseT:
        pieces: List[EncodableT] = [index, query]
        if use_cache:
            pieces.append("use_cache")
        return self.execute_command("TFT.SEARCH", *pieces)

    def tft_msearch(self, index_count: int, index: Iterable[KeyT], query: str) -> ResponseT:
        return self.execute_command("TFT.MSEARCH", index_count, *index, query)

    def tft_analyzer(self, analyzer_name: str, text: str, index: Optional[KeyT] = None,
                     show_time: Optional[bool] = False) -> ResponseT:
        pieces: List[EncodableT] = [analyzer_name, text]
        if index is not None:
            pieces.append("INDEX")
            pieces.append(index)
        if show_time:
            pieces.append("show_time")
        if isinstance(self, tair.TairCluster):
            if index is None:
                return self.execute_command("TFT.ANALYZER", *pieces, target_nodes='random')
            slot = self.keyslot(index)
            node = self.nodes_manager.get_node_from_slot(slot)
            return self.execute_command("TFT.ANALYZER", *pieces, target_nodes=node)
        return self.execute_command("TFT.ANALYZER", *pieces)

    def tft_addsug(self, index: KeyT, mapping: Dict[str, int]) -> ResponseT:
        pieces: List[EncodableT] = [index]

        for text, weight in mapping.items():
            pieces.append(text)
            pieces.append(weight)

        return self.execute_command("TFT.ADDSUG", *pieces)

    def tft_delsug(self, index: KeyT, text: Iterable[str]) -> ResponseT:
        return self.execute_command("TFT.DELSUG", index, *text)

    def tft_sugnum(self, index: KeyT) -> ResponseT:
        return self.execute_command("TFT.SUGNUM", index)

    def tft_getsug(
        self,
        index: KeyT,
        prefix: str,
        max_count: Optional[int] = None,
        fuzzy: bool = False,
    ) -> ResponseT:
        pieces: List[EncodableT] = [index, prefix]

        if max_count is not None:
            pieces.append("MAX_COUNT")
            pieces.append(max_count)
        if fuzzy:
            pieces.append("FUZZY")

        return self.execute_command("TFT.GETSUG", *pieces)

    def tft_getallsugs(self, index: KeyT) -> ResponseT:
        return self.execute_command("TFT.GETALLSUGS", index)
