"""Parallel MAVLink Binary Log Parser."""

import mmap
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import os
from struct import Struct
from typing import Any, Dict, List, Optional, Tuple, Literal, Type

from business_logic.parser import Parser
from business_logic.utils.logger import setup_logger
from business_logic.utils.constants import MSG_HEADER, FORMAT_MAPPING, FORMAT_MSG_TYPE


class ParallelParser:
    """
    Processes large MAVLink binary log files (.BIN) using multiprocessing or multithreading.
    Splits the file into aligned chunks and uses multiple processes or threads for faster parsing.
    """

    def __init__(
        self, filename: str, executor_type: Literal["process", "thread"] = "process", max_workers: Optional[int] = None
    ):
        """Initialize the ParallelParser."""
        self.filename: str = filename
        self.executor_type: Literal["process", "thread"] = executor_type
        self.max_workers: int = 0
        if executor_type == "process":
            self.max_workers = max_workers if max_workers else os.cpu_count() or 4
        else:
            self.max_workers = max_workers if max_workers else 16
        self.logger = setup_logger(os.path.basename(__file__))

    def process_all(self, message_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Process the entire log file in parallel and return sorted messages."""
        try:
            with Parser(self.filename) as parser:
                list(parser.messages("FMT"))
                chunks = ParallelParser._split_to_chunks(parser, self.max_workers)

                fmt_def = (
                    {
                        msg_id: {
                            "Name": fmt["Name"],
                            "Length": fmt["Length"],
                            "Format": fmt["Format"],
                            "Columns": fmt["Columns"],
                        }
                        for msg_id, fmt in parser.format_defs.items()
                    }
                    if self.executor_type == "process"
                    else parser.format_defs
                )

                need_struct_rebuild = self.executor_type == "process"

            if not chunks:
                raise RuntimeError("No chunks to process.")

            chunks_count = len(chunks)
            self.logger.info(f"Processing {chunks_count} chunks with {self.max_workers} workers...")

            executor_class = ProcessPoolExecutor if self.executor_type == "process" else ThreadPoolExecutor

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
                [self.filename] * chunks_count,
                chunks,
                [fmt_def] * chunks_count,
                [message_type] * chunks_count,
                [need_struct_rebuild] * chunks_count,
            )

        results = [msg for chunk in chunk_results for msg in chunk]

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
                    fmt["Struct"] = Struct("<" + "".join(FORMAT_MAPPING[char] for char in fmt["Format"]))

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

            while True:
                pos = data.find(MSG_HEADER, pos)
                if pos == -1:
                    raise RuntimeError("No valid message headers found in file.")
                if ParallelParser._is_valid_message_header(data, pos, fmt_defs):
                    break
                pos += 1

            while pos < size:
                start = pos
                end = min(start + chunk_size, size)

                next_pos = data.find(MSG_HEADER, end)
                while next_pos != -1 and not ParallelParser._is_valid_message_header(data, next_pos, fmt_defs):
                    next_pos = data.find(MSG_HEADER, next_pos + 1)

                pos = next_pos if next_pos != -1 else size
                chunks.append((start, pos))

            return chunks
        except Exception as e:
            raise RuntimeError(f"Error splitting to chunks: {e}") from e

    @staticmethod
    def _is_valid_message_header(data: bytes | mmap.mmap, pos: int, fmt_defs: Dict[int, Dict[str, Any]]) -> bool:
        """Check if MSG_HEADER at pos marks a valid message start."""
        if pos + 2 >= len(data) or data[pos : pos + 2] != MSG_HEADER:
            return False

        msg_id = data[pos + 2]
        if msg_id == FORMAT_MSG_TYPE:
            return True

        fmt = fmt_defs.get(msg_id)
        return bool(fmt and pos + fmt["Length"] <= len(data))

