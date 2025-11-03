"""MAVLink Binary Log Parser (.BIN) using mmap for memory efficiency."""
import os
from struct import Struct
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
    _STRUCT_CACHE: Dict[str, Struct] = {}

    def __init__(self, filename: str):
        """Initialize the parser with the given filename."""
        self.filename: str = filename
        self.logger = setup_logger(os.path.basename(__file__))
        self._file: Optional[Any] = None
        self.data: Optional[mmap.mmap] = None
        self.offset: int = 0
        self.format_definitions: Dict[int, Dict[str, Any]] = {}
        
    def __enter__(self) -> "Parser":
        """Open and memory-map the MAVLink log file."""
        try:
            self._file = open(self.filename, "rb")
            self.data = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
            self.logger.info(f"Opened file: {self.filename}")
            return self
        except Exception as e:
            self.logger.error(f"Failed to open file '{self.filename}': {e}")
            raise

    def __exit__(self, *args) -> None:
        """Close the memory-mapped file and underlying file."""
        try:
            if self.data:
                self.data.close()
            if self._file:
                self._file.close()
        except Exception as e:
            self.logger.error(f"Failed to close resource: {e}")
        self.data = self._file = None

   
    def reset(self):
        """Reset parser offset to beginning."""
        self.offset: int = 0

    def messages(self, message_type: Optional[str] = None, end_offset: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """
        Generator yielding MAVLink messages as dictionaries.
        """
        if not self.data:
            raise RuntimeError("Parser not initialized. Use 'with MavlogParser(...) as parser:'")

        data_length : int = len(self.data)

        while self.offset < data_length and (end_offset is None or self.offset < end_offset):
            position: int = self.data.find(MSG_HEADER, self.offset)
            if position == -1:
                break

            try:
                message_id: int = self.data[position + 2]
                if message_id == FORMAT_MSG_TYPE:
                    fmt: Optional[Dict[str, Any]] = self._parse_and_store_format_definition(position)
                    self.offset = position + (FORMAT_MSG_LENGTH if fmt else 1)
                    if fmt and (message_type in (None, "FMT")):
                        yield fmt
                    continue

                fmt_def: Optional[Dict[str, Any]] = self.format_definitions.get(message_id)
                if not fmt_def:
                    self.offset = position + 1
                    continue

                if message_type and fmt_def["Name"] != message_type:
                    self.offset = position + fmt_def["Length"]
                    continue

                message_end: int = position + fmt_def["Length"]
                if message_end > data_length:
                    break

                unpacked: tuple = fmt_def["Struct"].unpack_from(self.data, position + 3)
                message: Dict[str, Any] = self._decode_message_fields(fmt_def, unpacked)
                message["mavpackettype"] = fmt_def["Name"]

                yield message
                self.offset = message_end
            except IndexError:
                break
            except Exception as e:
                self.logger.error(f"Error parsing message at offset {position}: {e}")
                self.offset = position + 1
                continue


    def _bytes_to_str(self, bytes_data: bytes) -> str:
        """Convert bytes to ASCII string, stopping at null byte."""
        position: int = bytes_data.find(0)
        if position != -1:
            bytes_data = bytes_data[:position]
        return bytes_data.decode("ascii", "ignore")

    def _parse_and_store_format_definition(self, position: int) -> Optional[Dict[str, Any]]:
        """Parse and store an FMT (Format Definition) message."""
        try:
            if self.data is None:
                raise RuntimeError("Parser not initialized. Use 'with Parser(...) as parser:'")
            _, _, msg_type, length, name_bin, format_def_bin, columns_bin = FMT_STRUCT.unpack_from(self.data, position)

            name: str = self._bytes_to_str(name_bin).strip()
            format_def: str = self._bytes_to_str(format_def_bin).strip()
            columns_raw: str = self._bytes_to_str(columns_bin)
            cols: List[str] = [c for c in (columns_raw.split(",") if columns_raw else []) if c.strip()]

            if not (name and format_def and cols):
                return None

            struct_obj: Optional[Struct] = self._STRUCT_CACHE.get(format_def)
            if struct_obj is None:
                struct_obj = Struct("<" + "".join(FORMAT_MAPPING[c] for c in format_def))
                self._STRUCT_CACHE[format_def] = struct_obj

            fmt_def = {
                "Name": name,
                "Length": length,
                "Format": format_def,
                "Columns": cols,
                "Struct": struct_obj,
            }

            self.format_definitions[msg_type] =  fmt_def

            return {
                "mavpackettype": "FMT",
                "Type": msg_type,
                "Name": name,
                "Length": length,
                "Format": format_def,
                "Columns": ",".join(cols),
            }
        
        except Exception as e:
            self.logger.error(f"Error parsing FMT at offset {position}: {e}")
            return None


    # def _decode_message_fields(self, fmt_def: dict, unpacked: tuple) -> dict:
    #     """Decode fields according to format definition."""
    #     decoded: Dict[str, Any] = {}
    #     fmt: str = fmt_def["Format"]
    #     cols: List[str] = fmt_def["Columns"]
    #     for f_char, col, val in zip(fmt, cols, unpacked):
    #         try:
    #             if isinstance(val, bytes):
    #                 decoded[col] = (
    #                     val if (f_char == "Z" and col in BYTES_FIELDS)
    #                     else val.rstrip(b"\x00").decode("ascii", "ignore")
    #                 )
    #             elif f_char in SCALE_FACTOR_FIELDS:
    #                 decoded[col] = val / 100.0
    #             elif f_char == LATITUDE_LONGITUDE_FORMAT:
    #                 decoded[col] = val / 1e7
    #             else:
    #                 decoded[col] = val
    #         except Exception:
    #             decoded[col] = None
    #     return decoded
    def _decode_message_fields(self, fmt_def: dict, unpacked: tuple) -> dict:
        """Decode fields according to format definition (cleaner dict comprehension)."""
        fmt = fmt_def["Format"]
        cols = fmt_def["Columns"]

        return {
            col:
                val if isinstance(val, bytes) and f_char == "Z" and col in BYTES_FIELDS
                else val[: val.find(b"\x00")].decode("ascii", "ignore") if isinstance(val, bytes)
                else val / 100.0 if f_char in SCALE_FACTOR_FIELDS
                else val / 1e7 if f_char == LATITUDE_LONGITUDE_FORMAT
                else val
            for f_char, col, val in zip(fmt, cols, unpacked)
        }



    def get_all_messages(self, message_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return all messages of the specified type (or all messages if None)."""
        return list(self.messages(message_type))



if __name__ == "__main__":
    import time

    path = r"C:\Users\ootb\Downloads\log_file_test_01.bin"
    start = time.time()
    with Parser(path) as parser:
        count = len(parser.get_all_messages())
    print(f"\nFormats: {len(parser.format_definitions)}")
    print(f"Total messages: {count:,}")
    print(f"Time: {time.time() - start:.3f}s")
