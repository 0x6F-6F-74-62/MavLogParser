"""MAVLink Binary Log Parser (.BIN) using mmap for memory efficiency."""

import mmap
import os
import struct
from typing import Any, Dict, Iterator, List, Optional, Type

from business_logic.utils.constants import (
    BYTES_FIELDS,
    FMT_STRUCT,
    FORMAT_MAPPING,
    FORMAT_MSG_LENGTH,
    FORMAT_MSG_TYPE,
    LATITUDE_LONGITUDE_FORMAT,
    MSG_HEADER,
    SCALE_FACTOR_FIELDS,
)
from business_logic.utils.logger import setup_logger


class Parser:
    """
    MAVLink Binary Log Parser (.BIN)
    Parses ArduPilot-style MAVLink binary log files using mmap for memory efficiency.
    """

    def __init__(self, filename: str):
        self.filename: str = filename
        self.logger = setup_logger(os.path.basename(__file__))
        self._file: Optional[Any] = None
        self.data: Optional[mmap.mmap] = None
        self.offset: int = 0
        self.format_defs: Dict[int, Dict[str, Any]] = {}

    def __enter__(self) -> "Parser":
        """Open and memory-map the MAVLink log file."""
        try:
            self._file = open(self.filename, "rb")
            file_size = os.path.getsize(self.filename)
            if file_size == 0:
                self.logger.warning(f"File '{self.filename}' is empty.")
                self.data = b""
            else:
                self.data = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
            self.logger.info(f"Opened file: {self.filename}")
            return self
        except Exception as e:
            self.logger.error(f"Failed to open file '{self.filename}': {e}")
            raise

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        try:
            if self.data:
                self.data.close()
            if self._file:
                self._file.close()
            self.logger.info(f"Closed file: {self.filename}")
        except Exception as e:
            self.logger.error(f"Failed to close resource: {e}")
        self.data = self._file = None

    def messages(self, message_type: Optional[str] = None, end_index: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """
        Generator yielding MAVLink messages as dictionaries.
        """
        if self.data is None:
            raise RuntimeError("Parser not initialized. Use 'with MavlogParser(...) as parser:'")

        data_len: int = len(self.data)
        while (self.offset < data_len) and ((not end_index) or (self.offset < end_index)):
            position: int = self.data.find(MSG_HEADER, self.offset)
            if position == -1:
                break

            try:
                message_id: int = self.data[position + 2]
                if message_id == FORMAT_MSG_TYPE:
                    format_defs: Optional[Dict[str, Any]] = self._extract_format_def(position)
                    self.offset = position + (FORMAT_MSG_LENGTH if format_defs else 1)
                    if format_defs and (message_type in (None, "FMT")):
                        yield format_defs
                    continue

                msg_format: Optional[Dict[str, Any]] = self.format_defs.get(message_id)
                if not msg_format:
                    self.offset = position + 1
                    continue

                if message_type and msg_format["Name"] != message_type:
                    self.offset = position + msg_format["Length"]
                    continue

                message_end: int = position + msg_format["Length"]
                if message_end > data_len:
                    break

                unpacked: tuple = msg_format["Struct"].unpack_from(self.data, position + 3)
                message: Dict[str, Any] = self._decode_messages(msg_format["Name"], msg_format, unpacked)

                yield message
                self.offset = message_end
            except IndexError:
                break
            except Exception as e:
                self.logger.error(f"Error parsing message at offset {position}: {e}")
                self.offset = position + 1
                continue

    def get_all_messages(self, message_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return all messages of the specified type (or all messages if None)."""
        return list(self.messages(message_type))

    def _extract_format_def(self, position: int) -> Optional[Dict[str, Any]]:
        """Parse and store an FMT (Format Definition) message."""
        try:
            if self.data is None:
                raise RuntimeError("Parser not initialized. Use 'with Parser(...) as parser:'")
            _, _, msg_type, length, name_bin, format_def_bin, columns_bin = struct.unpack_from(
                FMT_STRUCT, self.data, position
            )
            name: str = self._bytes_to_ascii(name_bin)
            format_def: str = self._bytes_to_ascii(format_def_bin)
            cols: List[str] = [
                c.strip() for c in columns_bin.split(b"\x00", 1)[0].decode("ascii", "ignore").split(",") if c.strip()
            ]

            if not (name and format_def and cols):
                return None

            format_defs = {
                "Name": name,
                "Length": length,
                "Format": format_def,
                "Columns": cols,
                "Struct": struct.Struct("<" + "".join(FORMAT_MAPPING[c] for c in format_def)),
            }

            self.format_defs[msg_type] = format_defs

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

    @staticmethod
    def _bytes_to_ascii(bytes_data: bytes) -> str:
        """Convert null-terminated bytes to ASCII string."""
        null = bytes_data.find(0)
        return bytes_data[: null if null != -1 else None].decode("ascii", "ignore").strip()

    @staticmethod
    def _decode_messages(msg_type: str, format_defs: dict, unpacked: tuple) -> dict:
        """Decode fields according to format definition."""
        decoded: Dict[str, Any] = {"mavpackettype": msg_type}
        for fmt, col, val in zip(format_defs["Format"], format_defs["Columns"], unpacked):
            try:
                if isinstance(val, bytes):
                    decoded[col] = (
                        val if (fmt == "Z" and col in BYTES_FIELDS) else val.rstrip(b"\x00").decode("ascii", "ignore")
                    )
                elif fmt in SCALE_FACTOR_FIELDS:
                    decoded[col] = val / 100.0
                elif fmt == LATITUDE_LONGITUDE_FORMAT:
                    decoded[col] = val / 1e7
                else:
                    decoded[col] = val
            except Exception:
                decoded[col] = None
        return decoded
