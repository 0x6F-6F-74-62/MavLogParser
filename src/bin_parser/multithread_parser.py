"""Module for multithreaded parsing of binary log files."""
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple
import os
import heapq

from src.bin_parser.parser import Parser
from src.utils.parser_utils import split_to_chunks
from src.utils.logger import setup_logger


class ThreadParser:
    """
    Processes large MAVLink binary log files (.BIN) using multithreading.
    Splits the file into aligned chunks and uses multiple threads for faster parsing.
    """
    def __init__(self, filename: str, max_threads: int = 4):
        self.filename: str = filename
        self.max_threads: int = max_threads
        self.logger = setup_logger(os.path.basename(__file__))


    @staticmethod
    def _process_chunk(
            filename: str, chunk_range: Tuple[int, int], format_defs: Dict[int, Dict[str, Any]],
            message_type: Optional[str]) -> List[Dict[str, Any]]:
        """
        Process a chunk of the log file and return messages.
        """
        try:
            with Parser(filename) as parser:
                parser.offset = chunk_range[0]
                parser.format_defs = format_defs
                messages: List[Dict[str, Any]] = list(parser.messages(message_type, end_index=chunk_range[1]))
                return messages
        except Exception as e:
            print(f"[Thread chunk {chunk_range[0]}-{chunk_range[1]}] Error: {e}")
            raise RuntimeError(f"Error processing chunk {chunk_range}: {e}")

    def process_all(self, message_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Process the entire log file in parallel and return sorted messages.
        """
        try:
            with Parser(self.filename) as parser:
                list(parser.messages("FMT"))
                chunks = split_to_chunks(self.filename, self.max_threads, parser)
                fmt_defs = {
                    msg_id: {
                        "Name": fmt["Name"],
                        "Length": fmt["Length"],
                        "Format": fmt["Format"],
                        "Columns": fmt["Columns"],
                        "Struct": fmt["Struct"],
                    }
                    for msg_id, fmt in parser.format_defs.items()}

            if not chunks:
                raise RuntimeError("No chunks to parse")
            chunks_count = len(chunks)
            self.logger.info(f"Processing {len(chunks)} chunks using {self.max_threads} threads...")

            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                sorted_chunks = executor.map(
                    ThreadParser._process_chunk,
                    [self.filename] * chunks_count,
                    chunks,
                    [fmt_defs] * chunks_count,
                    [message_type] * chunks_count,
                    chunksize=max(1, chunks_count // self.max_threads),
                )

            results = list(heapq.merge(*sorted_chunks, key=lambda x: x.get("TimeUS", 0)))

            self.logger.info(f"Total messages parsed: {len(results):,}")
            return results
        except Exception as e:
            self.logger.error(f"Error in threaded processing: {e}")
            raise RuntimeError(f"Error in threaded processing: {e}")


if __name__ == "__main__":
    import time

    start = time.time()

    processor = ThreadParser(
        r"/Users/shlomo/Downloads/log_file_test_01.bin", max_threads=8
    )
    messages = processor.process_all()
    print(f"Total messages: {len(messages)}")
    print(f"TIME: {time.time() - start:.3f}s")
