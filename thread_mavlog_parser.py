import mmap
import os
import heapq
import concurrent.futures
from typing import List, Dict, Any, Optional, Tuple, Iterator
from struct import Struct
from src.utils.constants import MSG_HEADER, FORMAT_MAPPING
from src.utils.logger import setup_logger


class Parser:
    """
    MAVLink Binary Log Parser (.BIN)
    Parses ArduPilot-style MAVLink binary log files using mmap for memory efficiency.
    """

    def __init__(self, filename: str):
        self.filename: str = filename
        self.logger = setup_logger(self.__class__.__name__)
        self._file = None
        self._data: Optional[mmap.mmap] = None
        self._offset: int = 0
        self._format_definitions: Dict[int, Dict[str, Any]] = {}
        self._struct_cache: Dict[str, Struct] = {}

    def __enter__(self) -> "Parser":
        """Open and memory-map the MAVLink log file."""
        self._file = open(self.filename, "rb")
        self._data = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
        self.logger.debug(f"Opened file: {self.filename}")
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def close(self) -> None:
        """Safely close resources."""
        if self._data:
            self._data.close()
        if self._file:
            self._file.close()
        self._data = None
        self._file = None

    def messages(self, message_type: Optional[str] = None, end_offset: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """
        Generator yielding MAVLink messages as dictionaries.
        """
        if not self._data:
            raise RuntimeError("Parser not initialized. Use 'with Parser(...) as parser:'")

        data_length = len(self._data)

        while self._offset < data_length and (end_offset is None or self._offset < end_offset):
            position = self._data.find(MSG_HEADER, self._offset)
            if position == -1:
                break

            try:
                message_id = self._data[position + 2]
                if message_id in self._format_definitions:
                    fmt_def = self._format_definitions[message_id]
                    if message_type and fmt_def["Name"] != message_type:
                        self._offset = position + fmt_def["Length"]
                        continue

                    message = self._decode_message(fmt_def, position)
                    yield message
                    self._offset = position + fmt_def["Length"]
                else:
                    self._offset = position + 1

            except Exception as e:
                self.logger.warning(f"Error parsing message at offset {position}: {e}")
                self._offset = position + 1

    def _decode_message(self, fmt_def: Dict[str, Any], position: int) -> Dict[str, Any]:
        """Decode a single message based on its format definition."""
        struct_obj = self._get_struct(fmt_def["Format"])
        unpacked = struct_obj.unpack_from(self._data, position + 3)
        return {col: val for col, val in zip(fmt_def["Columns"], unpacked)}

    def _get_struct(self, fmt: str) -> Struct:
        """Retrieve or create a Struct object based on the format string."""
        if fmt not in self._struct_cache:
            self._struct_cache[fmt] = Struct("<" + "".join(FORMAT_MAPPING[c] for c in fmt))
        return self._struct_cache[fmt]


class ParallelLogProcessor:
    """
    Processes large MAVLink binary log files (.BIN) in parallel.
    Splits the file into aligned chunks and uses multiple processes for faster parsing.
    """

    def __init__(self, filename: str, chunk_size_mb: int = 100, max_workers: int = os.cpu_count(), overlap_bytes: int = 128):
        self.filename: str = filename
        self.chunk_size_mb: int = chunk_size_mb
        self.max_workers: int = max_workers
        self.overlap_bytes: int = overlap_bytes
        self.logger = setup_logger(self.__class__.__name__)

    def process_all(self, message_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Process the entire log file in parallel and return sorted messages."""
        with Parser(self.filename) as parser:
            list(parser.messages("FMT"))  # Preload format definitions
            chunks = self._split_into_chunks(parser)
            fmt_defs = parser._format_definitions

        if not chunks:
            return []

        self.logger.info(f"Processing {len(chunks)} chunks with {self.max_workers} workers...")
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            sorted_chunks = executor.map(
                self._process_chunk,
                [self.filename] * len(chunks),
                chunks,
                [fmt_defs] * len(chunks),
                [message_type] * len(chunks),
            )

        results = list(heapq.merge(*sorted_chunks, key=lambda x: x.get("TimeUS", 0)))
        self.logger.info(f"Total messages parsed: {len(results):,}")
        return results

    def _split_into_chunks(self, parser: Parser) -> List[Tuple[int, int]]:
        """Split the file into aligned chunks based on message headers."""
        file_size = os.path.getsize(self.filename)
        chunk_size = self.chunk_size_mb * 1024 * 1024
        chunks = []

        start = 0
        while start < file_size:
            end = min(start + chunk_size, file_size)
            next_header = parser._data.find(MSG_HEADER, end, min(end + self.overlap_bytes, file_size))
            end = file_size if next_header == -1 else next_header
            chunks.append((start, end))
            start = end

        return chunks

    @staticmethod
    def _process_chunk(filename: str, chunk: Tuple[int, int], fmt_defs: Dict[int, Dict[str, Any]], message_type: Optional[str]) -> List[Dict[str, Any]]:
        """Process a single chunk of the log file."""
        start, end = chunk
        try:
            with open(filename, "rb") as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                parser = Parser(filename)
                parser._data = mm
                parser._offset = start
                parser._format_definitions = fmt_defs

                messages = [msg for msg in parser.messages(message_type, end_offset=end)]
                messages.sort(key=lambda x: x.get("TimeUS", 0))
                return messages
        except Exception as e:
            print(f"[Worker {start}-{end}] Error: {e}")
            return []