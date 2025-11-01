# parser.py (optimized)
import struct
import mmap
from typing import Optional, Dict, Any, Iterator, List, Tuple
from src.utils.constants import (
    MSG_HEADER,
    FORMAT_MAPPING,
    FORMAT_MSG_TYPE,
    FORMAT_MSG_LENGTH,
    SCALE_FACTOR_FIELDS,
    LATITUDE_LONGITUDE_FORMAT,
    BYTES_FIELDS,
)
from src.utils.logger import setup_logger


class Parser:
    """
    MAVLink Binary Log Parser (.BIN) - optimized for concurrent parsing via memoryview buffers.
    NOTE: instance methods that mutate state (like __enter__/__exit__) are unchanged,
    but heavy parsing work for threads is performed by parse_buffer_chunk (stateless).
    """

    def __init__(self, filename: str):
        self.filename: str = filename
        self.logger = setup_logger(__name__)
        self._file = None
        self._data: Optional[mmap.mmap] = None
        self._format_definitions: Dict[int, Dict[str, Any]] = {}
        self._offset: int = 0

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
        self._file = self._data = None

    def reset_offset(self) -> None:
        self._offset = 0

    # Legacy generator that uses internal mmap (single-thread usage)
    def messages(self, message_type: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        if not self._data:
            raise RuntimeError("Parser not initialized. Use 'with Parser(...) as parser:'")

        data_length = len(self._data)
        while self._offset < data_length:
            position = self._data.find(MSG_HEADER, self._offset)
            if position == -1:
                break

            try:
                message_id = self._data[position + 2]
                if message_id == FORMAT_MSG_TYPE:
                    fmt = self._parse_and_store_format_definition(position)
                    self._offset = position + (FORMAT_MSG_LENGTH if fmt else 1)
                    if fmt:
                        yield fmt
                    continue

                fmt_def = self._format_definitions.get(message_id)
                if not fmt_def:
                    self._offset = position + 1
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

    def _parse_and_store_format_definition(self, position: int) -> Optional[Dict[str, Any]]:
        try:
            _, _, msg_type, length, name_b, fmt_b, cols_b = struct.unpack_from(
                "<2sBBB4s16s64s", self._data, position
            )

            name = name_b.split(b"\x00", 1)[0].decode("ascii", "ignore").strip()
            fmt = fmt_b.split(b"\x00", 1)[0].decode("ascii", "ignore").strip()
            cols = [
                c.strip() for c in cols_b.split(b"\x00", 1)[0].decode("ascii", "ignore").split(",") if c.strip()
            ]

            if not (name and fmt and cols):
                return None

            fmt_def = {
                "Name": name,
                "Length": length,
                "Format": fmt,
                "Columns": cols,
                "Struct": struct.Struct("<" + "".join(FORMAT_MAPPING[c] for c in fmt)),
            }

            self._format_definitions[msg_type] = fmt_def

            return {
                "mavpackettype": "FMT",
                "Type": msg_type,
                **{k: v if not isinstance(v, list) else ','.join(v) for k, v in fmt_def.items() if k != 'Struct'}
            }

        except Exception as e:
            self.logger.warning(f"Error parsing FMT at offset {position}: {e}")
            return None

    def _decode_message_fields(self, fmt_def: dict, unpacked: tuple) -> dict:
        decoded: Dict[str, Any] = {}
        for f_char, col, val in zip(fmt_def["Format"], fmt_def["Columns"], unpacked):
            try:
                if isinstance(val, (bytes, bytearray)):
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

    # ----------------------------
    # New: stateless chunk parser
    # ----------------------------
    @staticmethod
    def parse_buffer_chunk(
        buffer_view: memoryview,
        chunk_start: int,
        chunk_end: int,
        format_definitions: Dict[int, Dict[str, Any]],
        requested_message_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Parse a slice of the buffer (memoryview) from chunk_start (inclusive) to chunk_end (exclusive).
        This method is stateless and thread-safe — suitable to run in ThreadPoolExecutor workers.
        format_definitions: mapping of msg_id -> dict containing Name, Length, Format, Columns, Struct (precompiled).
        """
        results: List[Dict[str, Any]] = []
        local_buffer = buffer_view  # memoryview
        local_find = local_buffer.tobytes().find  # not ideal to recreate bytes, but to use .find relative to entire file we will use memoryview.find if available

        # Use memoryview.toreadonly for safety? memoryview already read-only for mmap with ACCESS_READ.
        offset = chunk_start
        data_length = chunk_end

        # For performance: bind locals
        msg_header = MSG_HEADER
        fmt_defs_local = format_definitions
        FORMAT_MSG_TYPE_LOCAL = FORMAT_MSG_TYPE
        FORMAT_MSG_LENGTH_LOCAL = FORMAT_MSG_LENGTH

        # We can't directly call memoryview.find with a start on all python versions — use buffer slicing approach
        # We'll manually use bytes.find on memoryview slice to find MSG_HEADER occurrences efficiently.
        mv = local_buffer

        # iterate searching for header
        while offset < data_length:
            # slice is cheap: memoryview supports slicing without copy
            relative_slice = mv[offset: data_length]
            pos_relative = relative_slice.tobytes().find(msg_header)
            if pos_relative == -1:
                break
            position = offset + pos_relative

            # quick bounds and id extraction
            # ensure we can read message id (position+2)
            if position + 3 > data_length:
                break

            message_id = mv[position + 2]

            if message_id == FORMAT_MSG_TYPE_LOCAL:
                # parse FMT inline (we can do a lightweight parse here)
                try:
                    # struct.unpack_from expects a buffer that supports buffer protocol; memoryview works
                    # But struct.unpack_from needs absolute offset relative to the original bytes object.
                    # We'll use struct.unpack_from on the underlying memoryview cast to bytes via memoryview.obj and offset.
                    # Simpler: convert the small slice to bytes and parse (FMT messages are small) — acceptable cost.
                    fmt_raw_slice = mv[position: position + FORMAT_MSG_LENGTH_LOCAL].tobytes()
                    _, _, msg_type, length, name_b, fmt_b, cols_b = struct.unpack_from("<2sBBB4s16s64s", fmt_raw_slice, 0)
                    name = name_b.split(b"\x00", 1)[0].decode("ascii", "ignore").strip()
                    fmt = fmt_b.split(b"\x00", 1)[0].decode("ascii", "ignore").strip()
                    cols = [c.strip() for c in cols_b.split(b"\x00", 1)[0].decode("ascii", "ignore").split(",") if c.strip()]

                    if name and fmt and cols:
                        struct_obj = struct.Struct("<" + "".join(FORMAT_MAPPING[c] for c in fmt))
                        fmt_def = {
                            "Name": name,
                            "Length": length,
                            "Format": fmt,
                            "Columns": cols,
                            "Struct": struct_obj,
                        }
                        fmt_defs_local[msg_type] = fmt_def
                        if requested_message_type in (None, "FMT"):
                            msg_out = {"mavpackettype": "FMT", "Type": msg_type, "Name": name, "Length": length, "Format": fmt, "Columns": ",".join(cols)}
                            results.append(msg_out)
                    offset = position + FORMAT_MSG_LENGTH_LOCAL
                    continue
                except Exception:
                    offset = position + 1
                    continue

            fmt_def = fmt_defs_local.get(int(message_id))
            if not fmt_def:
                offset = position + 1
                continue

            # check if we should filter out by message type
            if requested_message_type and fmt_def["Name"] != requested_message_type:
                offset = position + fmt_def["Length"]
                continue

            message_end = position + fmt_def["Length"]
            if message_end > data_length:
                break

            try:
                unpacked = fmt_def["Struct"].unpack_from(mv, position + 3)
                # decode fields
                decoded: Dict[str, Any] = {}
                for f_char, col, val in zip(fmt_def["Format"], fmt_def["Columns"], unpacked):
                    try:
                        if isinstance(val, (bytes, bytearray)):
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

                decoded["mavpackettype"] = fmt_def["Name"]
                results.append(decoded)
            except Exception:
                # If unpacking failed, step forward by 1 to try find next header
                offset = position + 1
                continue

            offset = message_end

        return results
