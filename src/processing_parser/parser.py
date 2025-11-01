import struct
import mmap
from typing import Optional, Dict, Any, Iterator, List
from src.utils.constants import (
    MSG_HEADER,
    FORMAT_MAPPING,
    FORMAT_MSG_TYPE,
    FORMAT_MSG_LENGTH,
    SCALE_FACTOR_FIELDS,
    LATITUDE_LONGITUDE_FORMAT,
    BYTES_FIELDS,FMT_STRUCT
)
from src.utils.logger import setup_logger


class Parser:
    """
    MAVLink Binary Log Parser (.BIN)
    Parses ArduPilot-style MAVLink binary log files using mmap for memory efficiency.
    """
    _STRUCT_CACHE: Dict[str, struct.Struct] = {}

    def __init__(self, filename: str):
        self.filename: str = filename
        self.logger = setup_logger(__name__)
        self._file: Optional[Any] = None
        self._data: Optional[mmap.mmap] = None
        self._offset: int = 0
        self._format_definitions: Dict[int, Dict[str, Any]] = {}

    def __enter__(self) -> "Parser":
        """Open and memory-map the MAVLink log file."""
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
        """Safely close resources."""
        for resource in (self._data, self._file):
            if resource:
                try:
                    resource.close()
                except Exception:
                    pass
        self._file = self._data = None

    def reset(self):
        """Reset parser offset to beginning."""
        self._offset = 0

    def messages(self, message_type: Optional[str] = None, end_offset: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """
        Generator yielding MAVLink messages as dictionaries.
        """
        if not self._data:
            raise RuntimeError("Parser not initialized. Use 'with MavlogParser(...) as parser:'")

        data_length = len(self._data)

        while self._offset < data_length and (end_offset is None or self._offset < end_offset):
            position = self._data.find(MSG_HEADER, self._offset)
            if position == -1:
                break

            try:
                message_id = self._data[position + 2]
                if message_id == FORMAT_MSG_TYPE:
                    fmt = self._parse_and_store_format_definition(position)
                    self._offset = position + (FORMAT_MSG_LENGTH if fmt else 1)
                    if fmt and (message_type in (None, "FMT")):
                        yield fmt
                    continue

                fmt_def = self._format_definitions.get(message_id)
                if not fmt_def:
                    self._offset = position + 1
                    continue

                if message_type and fmt_def["Name"] != message_type:
                    self._offset = position + fmt_def["Length"]
                    continue

                message_end = position + fmt_def["Length"]
                if message_end > data_length:
                    break

                unpacked = fmt_def["Struct"].unpack_from(self._data, position + 3)
                message = self._decode_message_fields(fmt_def, unpacked)
                message["mavpackettype"] = fmt_def["Name"]

                yield message
                self._offset = message_end
            except IndexError:
                break
            except Exception as e:
                self.logger.warning(f"Error parsing message at offset {position}: {e}")
                self._offset = position + 1
                continue


    def _bytes_to_str(self, b: bytes) -> str:
        idx = b.find(0)
        if idx != -1:
            b = b[:idx]
        return b.decode("ascii", "ignore")

    def _parse_and_store_format_definition(self, position: int) -> Optional[Dict[str, Any]]:
        """Parse and store an FMT (Format Definition) message."""
        try:
            _, _, msg_type, length, name_b, fmt_b, cols_b = FMT_STRUCT.unpack_from(self._data, position)

            name = self._bytes_to_str(name_b).strip()
            fmt  = self._bytes_to_str(fmt_b).strip()
            cols_raw = self._bytes_to_str(cols_b)
            cols = [c for c in (cols_raw.split(",") if cols_raw else []) if c.strip()]

            if not (name and fmt and cols):
                return None

            struct_obj = self._STRUCT_CACHE.get(fmt)
            if struct_obj is None:
                struct_obj = struct.Struct("<" + "".join(FORMAT_MAPPING[c] for c in fmt))
                self._STRUCT_CACHE[fmt] = struct_obj

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


    def _decode_message_fields(self, fmt_def: dict, unpacked: tuple) -> dict:
        """Decode fields according to format definition."""
        decoded: Dict[str, Any] = {}
        fmt = fmt_def["Format"]
        cols = fmt_def["Columns"]
        for f_char, col, val in zip(fmt, cols, unpacked):
            try:
                if isinstance(val, bytes):
                    decoded[col] = (
                        val if (f_char == "Z" and col in BYTES_FIELDS)
                        else val.rstrip(b"\x00").decode("ascii", "ignore")
                    )
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
        """Return all messages of the specified type (or all messages if None)."""
        return list(self.messages(message_type))


if __name__ == "__main__":
    import time

    start = time.time()
    path = r"C:\Users\ootb\Downloads\log_file_test_01.bin"

    with Parser(path) as parser:
        count = len(parser.get_all_messages())

    print(f"\nFormats: {len(parser._format_definitions)}")
    print(f"Total messages: {count:,}")
    print(f"Time: {time.time() - start:.3f}s")
