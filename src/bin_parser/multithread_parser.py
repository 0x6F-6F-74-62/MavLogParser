import mmap
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple
import os

from src.bin_parser.parser import Parser
from src.utils.constants import MSG_HEADER
from src.utils.logger import setup_logger


class ThreadedLogProcessor:
    def __init__(self, filename: str, max_threads: int = 4):
        self.filename: str = filename
        self.max_threads: int = max_threads
        self.logger = setup_logger(os.path.basename(__file__))

    def _split_file_into_chunks(self, parser: Parser) -> List[Tuple[int, int]]:
        if parser._data is None:
            raise RuntimeError("File must be opened before splitting.")
        data = parser._data
        total_size = len(data)
        chunks: List[Tuple[int, int]] = []
        start_offset = 0
        find = data.find

        while start_offset < total_size:
            header_pos = find(MSG_HEADER, start_offset)
            if header_pos == -1:
                break
            end_offset = header_pos + self.chunk_size_bytes
            if end_offset > total_size:
                end_offset = total_size
            else:
                next_header = find(MSG_HEADER, end_offset)
                if next_header != -1:
                    end_offset = next_header
            chunks.append((header_pos, end_offset))
            start_offset = end_offset
        return chunks

    @staticmethod
    def _process_chunk_segment(
        filename: str,
        start_offset: int,
        end_offset: int,
        fmt_defs: Dict[int, Dict[str, Any]],
        requested_message_type: Optional[str],
    ) -> List[Dict[str, Any]]:
        try:
            with open(filename, "rb") as fobj, mmap.mmap(fobj.fileno(), 0, access=mmap.ACCESS_READ) as mem_map:
                parser = Parser(filename)
                parser._data = mem_map
                parser._offset = start_offset
                parser._format_definitions = fmt_defs
                messages: List[Dict[str, Any]] = []
                for msg in parser.messages(requested_message_type, end_offset):
                    messages.append(msg)
                messages.sort(key=lambda m: m.get("TimeUS", 0))
                return messages
        except Exception as err:
            print(f"[Thread chunk {start_offset}-{end_offset}] Error: {err}")
            return []

    def process_all(self, message_type: Optional[str] = None) -> List[Dict[str, Any]]:
        try:
            with Parser(self.filename) as parser:
                for _ in parser.messages("FMT"):
                    pass
                chunks = self._split_file_into_chunks(parser)
                fmt_defs = {
                    msg_id: {
                        "Name": fd["Name"],
                        "Length": fd["Length"],
                        "Format": fd["Format"],
                        "Columns": fd["Columns"],
                        "Struct": fd["Struct"],
                    }
                    for msg_id, fd in parser._format_definitions.items()
                }

            if not chunks:
                return []

            self.logger.info(f"Processing {len(chunks)} chunks using {self.max_threads} threads...")

            results_per_chunk: List[List[Dict[str, Any]]] = []
            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                futures = [
                    executor.submit(self._process_chunk_segment, self.filename, start, end, fmt_defs, message_type)
                    for start, end in chunks
                ]
                for future in futures:
                    results_per_chunk.append(future.result())

            merged: List[Dict[str, Any]] = []
            for segment in results_per_chunk:
                merged.extend(segment)
            merged.sort(key=lambda m: m.get("TimeUS", 0))

            self.logger.info(f"Total messages parsed: {len(merged):,}")
            return merged
        except Exception as e:
            self.logger.error(f"Error in threaded processing: {e}")
            return []


if __name__ == "__main__":
    import time

    start = time.time()

    processor = ThreadedLogProcessor(
        r"/Users/shlomo/Downloads/log_file_test_01.bin",
        chunk_size_mb=50,
    )
    messages = processor.process_all()
    print(f"Total messages: {len(messages)}")
    print(f"TIME: {time.time() - start:.3f}s")
