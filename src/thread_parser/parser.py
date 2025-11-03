import mmap
import struct
from typing import Any, Dict, Iterator, List, Optional

from src.utils.constants import (BYTES_FIELDS, FMT_STRUCT, FORMAT_MAPPING, FORMAT_MSG_LENGTH, FORMAT_MSG_TYPE,
                                 LATITUDE_LONGITUDE_FORMAT, MSG_HEADER, SCALE_FACTOR_FIELDS)
from src.utils.logger import setup_logger

_STRUCT_CACHE: Dict[str, struct.Struct] = {}

class Parser:
    def __init__(self, filename: str):
        self.filename: str = filename
        self.logger = setup_logger(__name__)
        self._file = None
        self._data: Optional[mmap.mmap] = None
        self._offset: int = 0
        self._format_definitions: Dict[int, Dict[str, Any]] = {}

    def __enter__(self) -> "Parser":
        try:
            self._file = open(self.filename, "rb")
            self._data = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
            self.logger.debug(f"Opened file: {self.filename}")
            return self
        except Exception as e:
            self.logger.error(f"Failed to open file '{self.filename}': {e}")
            self.close()
            raise

    def __exit__(self, *args) -> None:
        self.close()

    def close(self) -> None:
        for resource in (self._data, self._file):
            if resource:
                try:
                    resource.close()
                except Exception:
                    pass
        self._file = None
        self._data = None

    def reset_offset(self) -> None:
        self._offset = 0

    def messages(self, message_type: Optional[str] = None, end_offset: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        if self._data is None:
            raise RuntimeError("Parser not initialized. Use 'with Parser(...) as parser:'")
        data = self._data
        data_length = len(data)
        offset = self._offset
        end = data_length if end_offset is None else min(end_offset, data_length)
        find = data.find

        while offset < end:
            pos = find(MSG_HEADER, offset, end)
            if pos == -1:
                break
            try:
                msg_id = data[pos + 2]
            except IndexError:
                break

            if msg_id == FORMAT_MSG_TYPE:
                fmt_record = self._parse_and_store_format_definition(pos)
                offset = pos + (FORMAT_MSG_LENGTH if fmt_record else 1)
                if fmt_record and (message_type is None or message_type == "FMT"):
                    yield fmt_record
                continue

            fmt_definition = self._format_definitions.get(msg_id)
            if fmt_definition is None:
                offset = pos + 1
                continue
            if message_type and fmt_definition["Name"] != message_type:
                offset = pos + fmt_definition["Length"]
                continue

            msg_end = pos + fmt_definition["Length"]
            if msg_end > end:
                break

            struct_obj = fmt_definition["Struct"]
            try:
                unpacked = struct_obj.unpack_from(data, pos + 3)
            except struct.error:
                offset = pos + 1
                continue

            message = self._decode_message_fields(fmt_definition, unpacked)
            message["mavpackettype"] = fmt_definition["Name"]
            yield message
            offset = msg_end

        self._offset = offset

    def _bytes_to_string(self, byte_data: bytes) -> str:
        idx = byte_data.find(b"\x00")
        if idx != -1:
            return byte_data[:idx].decode("ascii", "ignore")
        return byte_data.decode("ascii", "ignore")

    def _parse_and_store_format_definition(self, position: int) -> Optional[Dict[str, Any]]:
        try:
            _, _, msg_type, length, name_b, fmt_b, cols_b = FMT_STRUCT.unpack_from(self._data, position)
            name = self._bytes_to_string(name_b).strip()
            fmt = self._bytes_to_string(fmt_b).strip()
            cols_raw = self._bytes_to_string(cols_b)
            cols = [c.strip() for c in cols_raw.split(",") if c.strip()]
            if not (name and fmt and cols):
                return None

            struct_obj = _STRUCT_CACHE.get(fmt)
            if struct_obj is None:
                fmt_string = "<" + "".join(FORMAT_MAPPING[c] for c in fmt)
                struct_obj = struct.Struct(fmt_string)
                _STRUCT_CACHE[fmt] = struct_obj

            fmt_def = {
                "Name": name,
                "Length": length,
                "Format": fmt,
                "Columns": cols,
                "Struct": struct_obj,
            }
            self._format_definitions[msg_type] = fmt_def

            return {
                "mavpackettype": "FMT",
                "Type": msg_type,
                "Name": name,
                "Length": length,
                "Format": fmt,
                "Columns": ",".join(cols),
            }
        except Exception as e:
            self.logger.warning(f"Error parsing FMT at offset {position}: {e}")
            return None

    def _decode_message_fields(self, fmt_def: Dict[str, Any], unpacked: tuple) -> Dict[str, Any]:
        decoded: Dict[str, Any] = {}
        fmt = fmt_def["Format"]
        cols = fmt_def["Columns"]
        for f_char, col, val in zip(fmt, cols, unpacked):
            try:
                if isinstance(val, bytes):
                    if f_char == "Z" and col in BYTES_FIELDS:
                        decoded[col] = val
                    else:
                        decoded[col] = val.rstrip(bytes_data"\x00").decode("ascii", "ignore")
                elif f_char in SCALE_FACTOR_FIELDS:
                    decoded[col] = val / 100.0
                elif f_char == LATITUDE_LONGITUDE_FORMAT:
                    decoded[col] = val / 1e7
                else:
                    decoded[col] = val
            except Exception:
                decoded[col] = None
        return decoded

    def get_all_messages(self, message_type: Optional[str] = None) -> List[Dict[str, Any]]:
        result_list: List[Dict[str, Any]] = []
        if self._data is None:
            return result_list
        self.reset_offset()
        return list(self.messages(message_type))




if __name__ == "__main__":
    import time

    path = r"C:\Users\ootb\Downloads\log_file_test_01.bin"
    start = time.time()
    with Parser(path) as parser:
        count = len(parser.get_all_messages())
    print(f"Total messages: {count:,}")
    print(f"Time: {time.time() - start:.3f}s")

