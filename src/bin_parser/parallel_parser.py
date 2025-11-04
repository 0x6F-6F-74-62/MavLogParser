"""Parallel MAVLink Binary Log Parser."""

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import heapq
import os
from struct import Struct
from typing import Any, Dict, List, Optional, Tuple, Literal

from src.bin_parser.parser import Parser
from src.utils.logger import setup_logger
from src.utils.constants import MSG_HEADER, FORMAT_MAPPING


class ParallelParser:
    """
    Processes large MAVLink binary log files (.BIN) using multiprocessing or multithreading.
    Splits the file into aligned chunks and uses multiple processes or threads for faster parsing.
    """

    def __init__(
        self, filename: str, executor_type: Literal["process", "thread"] = "process", max_workers: Optional[int] = None
    ):
        """
        Initialize the ParallelParser.
        """
        self.filename: str = filename
        self.executor_type: Literal["process", "thread"] = executor_type
        if executor_type == "process":
            self.max_workers: int = max_workers if max_workers else os.cpu_count() or 4
        else:
            self.max_workers: int = max_workers if max_workers else 8
        self.logger = setup_logger(os.path.basename(__file__))

    @staticmethod
    def split_to_chunks(filename: str, max_workers: int, parser: Parser) -> List[Tuple[int, int]]:
        """Split the file into chunks aligned to message headers."""
        file_size: int = os.path.getsize(filename)
        chunk_size: int = max(1, file_size // max_workers)
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
        filename: str,
        chunk_range: Tuple[int, int],
        format_defs: Dict[int, Dict[str, Any]],
        message_type: Optional[str],
        build_struct: bool = True,
    ) -> List[Dict[str, Any]]:
        """Process a chunk of the log file and return messages."""
        try:
            if build_struct:
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
                chunks = ParallelParser.split_to_chunks(self.filename, self.max_workers, parser)

                fmt_def = {
                    msg_id: {
                        "Name": fmt["Name"],
                        "Length": fmt["Length"],
                        "Format": fmt["Format"],
                        "Columns": fmt["Columns"],
                    }
                    for msg_id, fmt in parser.format_defs.items()
                } if self.executor_type == "process" else parser.format_defs



            if not chunks:
                raise RuntimeError("No chunks to process.")

            chunks_count = len(chunks)

            self.logger.info(f"Processing {chunks_count} chunks with {self.max_workers} workers...")

            executor_class = ThreadPoolExecutor if self.executor_type == "thread" else ProcessPoolExecutor
            results = self._run_executor(executor_class, chunks_count, chunks, fmt_def, message_type)


            self.logger.info(f"Total messages parsed: {len(results):,}")
            return results

        except Exception as e:
            self.logger.error(f"Error in parallel processing: {e}")
            raise RuntimeError(f"Error in parallel processing: {e}")


    def _run_executor(
        self,
        executor_class,
        chunks_count: int,
        chunks: List[Tuple[int, int]],
        fmt_def: Dict[int, Dict[str, Any]],
        message_type: Optional[str],
    ) -> List[Dict[str, Any]]:
        """Process chunks using ThreadPoolExecutor."""
        with executor_class(max_workers=self.max_workers) as executor:
            sorted_chunks = executor.map(
                ParallelParser._process_chunk,
                [self.filename] * chunks_count,
                chunks,
                [fmt_def] * chunks_count,
                [message_type] * chunks_count,
                chunksize=max(1, chunks_count // self.max_workers),
            )

        results = list(heapq.merge(*sorted_chunks, key=lambda x: x.get("TimeUS", 0)))
        return results



if __name__ == "__main__":
    import time

    start_time = time.time()
    processor = ParallelParser(r"/Users/shlomo/Downloads/log_file_test_01.bin", executor_type="thread")
    my_messages = processor.process_all()
    print(f"Total messages: {len(my_messages)}")
    print(f"TIME: {time.time() - start_time}")
