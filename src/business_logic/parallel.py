"""Parallel MAVLink Binary Log Parser."""

import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from struct import Struct
from typing import Any, Dict, List, Literal, Optional, Tuple, Type
from itertools import repeat
from itertools import chain

from src.business_logic.parser import Parser
from src.utils.constants import FORMAT_MAPPING, MSG_HEADER
from src.utils.helpers import is_valid_message_header
from src.utils.logger import setup_logger


class ParallelParser:
    """
    Processes large MAVLink binary log files (.BIN) using multiprocessing or multithreading.
    Splits the file into aligned chunks and uses multiple processes or threads for faster parsing.
    """

    def __init__(
        self, filename: str, max_workers: Optional[int] = None
    ):
        """Initialize the ParallelParser."""
        self.filename: str = filename
        self.max_workers: int = max_workers or os.cpu_count() or 1
        self.logger = setup_logger(os.path.basename(__file__))

    def process_all(self, message_type: Optional[str] = None, executor_type: Literal["process", "thread"] = "process") -> List[Dict[str, Any]]:
        """Process the entire log file in parallel and return sorted messages."""
        try:
            with Parser(self.filename) as parser:
                for _ in parser.messages("FMT"): pass
                chunks = ParallelParser._split_to_chunks(parser, self.max_workers)

                fmt_def = {
                    msg_id: {
                        "Name": fmt["Name"],
                        "Length": fmt["Length"],
                        "Format": fmt["Format"],
                        "Columns": fmt["Columns"],
                        "StructStr": "<" + "".join(FORMAT_MAPPING[c] for c in fmt["Format"])
                    }
                    for msg_id, fmt in parser.format_defs.items()
                } if executor_type == "process" else parser.format_defs

                need_struct_rebuild = executor_type == "process"

            if not chunks:
                raise RuntimeError("No chunks to process.")

            chunks_count = len(chunks)
            self.logger.info(f"Processing {chunks_count} chunks with {self.max_workers} workers...")

            executor_class = ProcessPoolExecutor if executor_type == "process" else ThreadPoolExecutor

            results = self._run_executor(
                executor_class, chunks_count, chunks, fmt_def, message_type, need_struct_rebuild
            )

            self.logger.info(f"Total messages parsed: {len(results):,}")
            return results

        except Exception as e:
            self.logger.error(f"Error in parallel processing: {e}")
            raise RuntimeError(f"Error in parallel processing: {e}") from e

    def _run_executor(
        self,
        executor_class: Type[ProcessPoolExecutor] | Type[ThreadPoolExecutor],
        chunks_count: int,
        chunks: List[Tuple[int, int]],
        fmt_def: Dict[int, Dict[str, Any]],
        message_type: Optional[str],
        need_struct_rebuild: bool = True,
    ) -> List[Dict[str, Any]]:
        """Process chunks using executor and merge results."""
        with executor_class(max_workers=self.max_workers) as executor:
            chunk_results = executor.map(
                ParallelParser._process_chunk,
                repeat(self.filename, chunks_count),
                chunks,
                repeat(fmt_def, chunks_count),
                repeat(message_type, chunks_count),
                repeat(need_struct_rebuild, chunks_count),
            )

        results = list(chain.from_iterable(chunk_results))

        return results

    @staticmethod
    def _process_chunk(
        filename: str,
        chunk_range: Tuple[int, int],
        format_defs: Dict[int, Dict[str, Any]],
        message_type: Optional[str],
        need_struct_rebuild: bool = True,
    ) -> List[Dict[str, Any]]:
        """Process a chunk of the log file and return messages."""
        try:
            if need_struct_rebuild:
                for fmt in format_defs.values():
                    fmt["Struct"] = Struct(fmt["StructStr"])
            with Parser(filename) as parser:
                parser.format_defs = format_defs
                parser.offset = chunk_range[0]
                messages = list(parser.messages(message_type, end_index=chunk_range[1]))
                return messages

        except Exception as e:
            raise RuntimeError(f"Error processing chunk {chunk_range}: {e}") from e

    @staticmethod
    def _split_to_chunks(parser: Parser, max_workers: int) -> List[Tuple[int, int]]:
        """Split the file into valid message-aligned chunks."""
        try:
            if parser.data is None:
                raise RuntimeError("File must be opened before splitting.")

            data = parser.data
            fmt_defs = parser.format_defs
            size = len(data)

            if size == 0:
                raise RuntimeError("Log file is empty.")

            chunk_size = max(size // max_workers, 10 * 1024 * 1024)
            chunks, pos = [], 0
            header_len = len(MSG_HEADER)
            while True:
                pos = data.find(MSG_HEADER, pos)
                if pos == -1:
                    raise RuntimeError("No valid message headers found in file.")
                if is_valid_message_header(data, pos, fmt_defs):
                    break
                pos += header_len

            while pos < size:
                start = pos
                end = min(start + chunk_size, size)

                next_pos = data.find(MSG_HEADER, end)
                while next_pos != -1 and not is_valid_message_header(data, next_pos, fmt_defs):
                    next_pos = data.find(MSG_HEADER, next_pos + header_len)

                pos = next_pos if next_pos != -1 else size
                chunks.append((start, pos))

            return chunks
        except Exception as e:
            raise RuntimeError(f"Error splitting to chunks: {e}") from e
