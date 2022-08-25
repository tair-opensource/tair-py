from typing import Dict, Iterable, List, Optional, Union

from tair.exceptions import DataError
from tair.typing import CommandsProtocol, EncodableT, KeyT, ResponseT


class TairTsSkeyItem:
    def __init__(self, skey: KeyT, ts: Union[int, str], value: float) -> None:
        self.skey = skey
        self.ts = ts
        self.value = value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TairTsSkeyItem):
            return False
        return (
            self.skey == other.skey
            and self.ts == other.ts
            and self.value == other.value
        )

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __repr__(self) -> str:
        return f"{{skey: {self.skey}, ts: {self.ts}, value: {self.value}}}"


class Aggregation:
    def __init__(self, aggregation_type: str, time_bucket: int) -> None:
        self.aggregation_type = aggregation_type
        self.time_bucket = time_bucket

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Aggregation):
            return False
        return (
            self.aggregation_type == other.aggregation_type
            and self.time_bucket == other.time_bucket
        )

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __repr__(self) -> str:
        return f"{{aggregation_type: {self.aggregation_type}, time_bucket: {self.time_bucket}}}"


class TairTsCommands(CommandsProtocol):
    def exts_p_create(self, pkey: KeyT) -> ResponseT:
        return self.execute_command("EXTS.P.CREATE", pkey)

    def exts_s_create(
        self,
        pkey: KeyT,
        skey: KeyT,
        data_et: Optional[int] = None,
        chunk_size: Optional[int] = None,
        uncompressed: bool = False,
        labels: Optional[Dict[str, str]] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [pkey, skey]

        if data_et is not None:
            pieces.append("DATA_ET")
            pieces.append(data_et)
        if chunk_size is not None:
            pieces.append("CHUNK_SIZE")
            pieces.append(chunk_size)
        if uncompressed:
            pieces.append("UNCOMPRESSED")

        if labels is not None:
            for label, val in labels.items():
                pieces.append("LABELS")
                pieces.append(label)
                pieces.append(val)

        return self.execute_command("EXTS.S.CREATE", *pieces)

    def exts_s_alter(
        self,
        pkey: KeyT,
        skey: KeyT,
        data_et: Optional[int] = None,
    ) -> ResponseT:
        if data_et is not None:
            return self.execute_command("EXTS.S.ALTER", pkey, skey, "DATA_ET", data_et)
        return self.execute_command("EXTS.S.ALTER", pkey, skey)

    def exts_s_add(
        self,
        pkey: KeyT,
        skey: KeyT,
        ts: Union[int, str],
        value: float,
        data_et: Optional[int] = None,
        chunk_size: Optional[int] = None,
        uncompressed: bool = False,
        labels: Optional[Dict[str, str]] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [pkey, skey, ts, value]

        if data_et is not None:
            pieces.append("DATA_ET")
            pieces.append(data_et)
        if chunk_size is not None:
            pieces.append("CHUNK_SIZE")
            pieces.append(chunk_size)
        if uncompressed:
            pieces.append("UNCOMPRESSED")

        if labels is not None:
            pieces.append("LABELS")
            for label, val in labels.items():
                pieces.append(label)
                pieces.append(val)

        return self.execute_command("EXTS.S.ADD", *pieces)

    def exts_s_madd(
        self,
        pkey: KeyT,
        items: Iterable[TairTsSkeyItem],
        data_et: Optional[int] = None,
        chunk_size: Optional[int] = None,
        uncompressed: bool = False,
        labels: Optional[Dict[str, str]] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [pkey, len(items)]

        for item in items:
            pieces.append(item.skey)
            pieces.append(item.ts)
            pieces.append(item.value)

        if data_et is not None:
            pieces.append("DATA_ET")
            pieces.append(data_et)
        if chunk_size is not None:
            pieces.append("CHUNK_SIZE")
            pieces.append(chunk_size)
        if uncompressed:
            pieces.append("UNCOMPRESSED")

        if labels is not None:
            pieces.append("LABELS")
            for label, val in labels.items():
                pieces.append(label)
                pieces.append(val)

        return self.execute_command("EXTS.S.MADD", *pieces)

    def exts_s_incrby(
        self,
        pkey: KeyT,
        skey: KeyT,
        ts: Union[int, str],
        value: float,
        data_et: Optional[int] = None,
        chunk_size: Optional[int] = None,
        uncompressed: bool = False,
        labels: Optional[Dict[str, str]] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [pkey, skey, ts, value]

        if data_et is not None:
            pieces.append("DATA_ET")
            pieces.append(data_et)
        if chunk_size is not None:
            pieces.append("CHUNK_SIZE")
            pieces.append(chunk_size)
        if uncompressed:
            pieces.append("UNCOMPRESSED")

        if labels is not None:
            pieces.append("LABELS")
            for label, val in labels.items():
                pieces.append(label)
                pieces.append(val)

        return self.execute_command("EXTS.S.INCRBY", *pieces)

    def exts_s_mincrby(
        self,
        pkey: KeyT,
        items: Iterable[TairTsSkeyItem],
        data_et: Optional[int] = None,
        chunk_size: Optional[int] = None,
        uncompressed: bool = False,
        labels: Optional[Dict[str, str]] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [pkey, len(items)]

        for item in items:
            pieces.append(item.skey)
            pieces.append(item.ts)
            pieces.append(item.value)

        if data_et is not None:
            pieces.append("DATA_ET")
            pieces.append(data_et)
        if chunk_size is not None:
            pieces.append("CHUNK_SIZE")
            pieces.append(chunk_size)
        if uncompressed:
            pieces.append("UNCOMPRESSED")

        if labels is not None:
            pieces.append("LABELS")
            for label, val in labels.items():
                pieces.append(label)
                pieces.append(val)

        return self.execute_command("EXTS.S.MINCRBY", *pieces)

    def exts_s_del(self, pkey: KeyT, skey: KeyT) -> ResponseT:
        return self.execute_command("EXTS.S.DEL", pkey, skey)

    def exts_s_get(self, pkey: KeyT, skey: KeyT) -> ResponseT:
        return self.execute_command("EXTS.S.GET", pkey, skey)

    def exts_s_info(self, pkey: KeyT, skey: KeyT) -> ResponseT:
        return self.execute_command("EXTS.S.INFO", pkey, skey)

    def exts_s_queryindex(
        self,
        pkey: KeyT,
        filters: Iterable[str],
    ) -> ResponseT:
        return self.execute_command("EXTS.S.QUERYINDEX", pkey, *filters)

    def exts_s_range(
        self,
        pkey: KeyT,
        skey: KeyT,
        from_ts: int,
        to_ts: Union[int, str],
        maxcount: Optional[int] = None,
        aggregation: Optional[Aggregation] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [pkey, skey, from_ts, to_ts]

        if maxcount is not None:
            pieces.append("MAXCOUNT")
            pieces.append(maxcount)

        if aggregation is not None:
            pieces.append("AGGREGATION")
            pieces.append(aggregation.aggregation_type)
            pieces.append(aggregation.time_bucket)

        return self.execute_command("EXTS.S.RANGE", *pieces)

    def exts_s_mrange(
        self,
        pkey: KeyT,
        from_ts: int,
        to_ts: Union[int, str],
        filters: Iterable[str],
        maxcount: Optional[int] = None,
        aggregation: Optional[Aggregation] = None,
        withlabels: bool = False,
    ) -> ResponseT:
        pieces: List[EncodableT] = [pkey, from_ts, to_ts]

        if maxcount is not None:
            pieces.append("MAXCOUNT")
            pieces.append(maxcount)

        if aggregation is not None:
            pieces.append("AGGREGATION")
            pieces.append(aggregation.aggregation_type)
            pieces.append(aggregation.time_bucket)

        if withlabels:
            pieces.append("WITHLABELS")

        pieces.append("FILTER")
        pieces.extend(filters)

        return self.execute_command("EXTS.S.MRANGE", *pieces)

    def exts_p_range(
        self,
        pkey: KeyT,
        from_ts: int,
        to_ts: Union[int, str],
        pkey_aggregation: str = None,
        pkey_time_bucket: int = None,
        filters: Iterable[str] = None,
        maxcount: Optional[int] = None,
        aggregation: Optional[Aggregation] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [pkey, from_ts, to_ts]

        if pkey_aggregation is not None:
            pieces.append(pkey_aggregation)
            if pkey_time_bucket is not None:
                pieces.append(pkey_time_bucket)
            else:
                raise DataError(
                    "pkey_time_bucket is required when pkey_aggregation is specified"
                )
        if maxcount is not None:
            pieces.append("MAXCOUNT")
            pieces.append(maxcount)
        if aggregation is not None:
            pieces.append("AGGREGATION")
            pieces.append(aggregation.aggregation_type)
            pieces.append(aggregation.time_bucket)

        pieces.append("FILTER")
        for i in filters:
            pieces.append(i)

        return self.execute_command("EXTS.P.RANGE", *pieces)

    def exts_s_raw_modify(
        self,
        pkey: KeyT,
        skey: KeyT,
        ts: Union[int, str],
        value: float,
        data_et: Optional[int] = None,
        chunk_size: Optional[int] = None,
        uncompressed: bool = False,
        labels: Optional[Dict[str, str]] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [pkey, skey, ts, value]

        if data_et is not None:
            pieces.append("DATA_ET")
            pieces.append(data_et)
        if chunk_size is not None:
            pieces.append("CHUNK_SIZE")
            pieces.append(chunk_size)
        if uncompressed:
            pieces.append("UNCOMPRESSED")

        if labels is not None:
            pieces.append("LABELS")
            for label, val in labels.items():
                pieces.append(label)
                pieces.append(val)

        return self.execute_command("EXTS.S.RAW_INCRBY", *pieces)

    def exts_s_raw_mmodify(
        self,
        pkey: KeyT,
        items: Iterable[TairTsSkeyItem],
        data_et: Optional[int] = None,
        chunk_size: Optional[int] = None,
        uncompressed: bool = False,
        labels: Optional[Dict[str, str]] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [pkey, len(items)]

        for item in items:
            pieces.append(item.skey)
            pieces.append(item.ts)
            pieces.append(item.value)

        if data_et is not None:
            pieces.append("DATA_ET")
            pieces.append(data_et)
        if chunk_size is not None:
            pieces.append("CHUNK_SIZE")
            pieces.append(chunk_size)
        if uncompressed:
            pieces.append("UNCOMPRESSED")

        if labels is not None:
            pieces.append("LABELS")
            for label, val in labels.items():
                pieces.append(label)
                pieces.append(val)

        return self.execute_command("EXTS.S.RAW_MMODIFY", *pieces)

    def exts_s_raw_incrby(
        self,
        pkey: KeyT,
        skey: KeyT,
        ts: Union[int, str],
        value: float,
        data_et: Optional[int] = None,
        chunk_size: Optional[int] = None,
        uncompressed: bool = False,
        labels: Optional[Dict[str, str]] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [pkey, skey, ts, value]

        if data_et is not None:
            pieces.append("DATA_ET")
            pieces.append(data_et)
        if chunk_size is not None:
            pieces.append("CHUNK_SIZE")
            pieces.append(chunk_size)
        if uncompressed:
            pieces.append("UNCOMPRESSED")

        if labels is not None:
            pieces.append("LABELS")
            for label, val in labels.items():
                pieces.append(label)
                pieces.append(val)

        return self.execute_command("EXTS.S.RAW_INCRBY", *pieces)

    def exts_s_raw_mincrby(
        self,
        pkey: KeyT,
        items: Iterable[TairTsSkeyItem],
        data_et: Optional[int] = None,
        chunk_size: Optional[int] = None,
        uncompressed: bool = False,
        labels: Optional[Dict[str, str]] = None,
    ) -> ResponseT:
        pieces: List[EncodableT] = [pkey, len(items)]

        for item in items:
            pieces.append(item.skey)
            pieces.append(item.ts)
            pieces.append(item.value)

        if data_et is not None:
            pieces.append("DATA_ET")
            pieces.append(data_et)
        if chunk_size is not None:
            pieces.append("CHUNK_SIZE")
            pieces.append(chunk_size)
        if uncompressed:
            pieces.append("UNCOMPRESSED")

        if labels is not None:
            pieces.append("LABELS")
            for label, val in labels.items():
                pieces.append(label)
                pieces.append(val)

        return self.execute_command("EXTS.S.RAW_MINCRBY", *pieces)
