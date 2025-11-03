"""Parallel MAVLink Binary Log Parser."""

import concurrent.futures
import heapq
import os
from struct import Struct
from typing import Any, Dict, List, Optional, Tuple

from src.bin_parser.parser import Parser
from src.utils.constants import FORMAT_MAPPING, MSG_HEADER
from src.utils.logger import setup_logger


class MultiprocessParser:
    """
    Processes large MAVLink binary log files (.BIN) in parallel.
    Splits the file into aligned chunks and uses multiple processes for faster parsing.
    """

    def __init__(self, filename: str, max_workers: Optional[int] = None):
        """Initialize the parallel parser with given parameters."""
        self.filename: str = filename
        self.max_workers: int = max_workers if max_workers else os.cpu_count() or 4
        self.logger = setup_logger(os.path.basename(__file__))

    def _split_to_chunks(self, parser: Parser) -> List[Tuple[int, int]]:
        """Split the file into chunks aligned to message headers."""
        file_size: int = os.path.getsize(self.filename)
        chunk_size: int = max(1, file_size // self.max_workers)
        chunks: List[Tuple[int, int]] = []
        start_index: int = 0
        while start_index < file_size:
            tentative_end: int = min(start_index + chunk_size, file_size)
            next_header: int = parser.data.find(MSG_HEADER, tentative_end)
            end_index: int = next_header if next_header != -1 else file_size
            chunks.append((start_index, end_index))
            start_index = end_index
        return chunks

    @staticmethod
    def _process_chunk(
        filename: str, chunk_range: Tuple[int, int], format_defs: Dict[int, Dict[str, Any]], message_type: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Process a chunk of the log file and return messages."""
        try:
            for _, fmt in format_defs.items():
                fmt["Struct"] = Struct("<" + "".join(FORMAT_MAPPING[char] for char in fmt["Format"]))

            with Parser(filename) as parser:
                parser.offset = chunk_range[0]
                parser.format_defs = format_defs
                messages = list(parser.messages(message_type, end_index=chunk_range[1]))
            return messages
        except Exception as e:
            raise RuntimeError(f"Error processing chunk {chunk_range}: {e}")

    def process_all(self, message_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Process the entire log file in parallel and return sorted messages."""
        try:
            with Parser(self.filename) as parser:
                list(parser.messages("FMT"))
                chunks = self._split_to_chunks(parser)
                fmt_defs = {
                    msg_id: {
                        "Name": fmt["Name"],
                        "Length": fmt["Length"],
                        "Format": fmt["Format"],
                        "Columns": fmt["Columns"],
                    }
                    for msg_id, fmt in parser.format_defs.items()
                }

            if not chunks:
                raise RuntimeError("No chunks to process.")

            chunks_count = len(chunks)
            self.logger.info(f"Processing {chunks_count} chunks with {self.max_workers} workers...")

            with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                sorted_chunks = executor.map(
                    MultiprocessParser._process_chunk,
                    [self.filename] * chunks_count,
                    chunks,
                    [fmt_defs] * chunks_count,
                    [message_type] * chunks_count,
                    chunksize=max(1, chunks_count // self.max_workers),
                )

                results = list(heapq.merge(*sorted_chunks, key=lambda x: x.get("TimeUS", 0)))

            self.logger.info(f"Total messages parsed: {len(results):,}")
            return results

        except Exception as e:
            self.logger.error(f"Error in parallel processing: {e}")
            raise RuntimeError(f"Error in parallel processing: {e}")


if __name__ == "__main__":
    import time

    start_time = time.time()
    processor = MultiprocessParser(r"C:\Users\ootb\Downloads\log_file_test_01.bin")
    my_messages = processor.process_all()
    print(f"Total messages: {len(my_messages)}")
    print(f"TIME: {time.time() - start_time}")
