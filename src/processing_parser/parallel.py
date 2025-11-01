import mmap
import concurrent.futures
from typing import List, Dict, Any, Optional, Tuple
from struct import Struct
import heapq

from src.processing_parser.parser import Parser
from src.utils.constants import MSG_HEADER, FORMAT_MAPPING
from src.utils.logger import setup_logger


class ParallelLogProcessor:
    """
    Processes large MAVLink binary log files (.BIN) in parallel.
    Splits the file into aligned chunks and uses multiple processes for faster parsing.
    """

    def __init__(self, filename: str, chunk_size_mb: int = 100, max_workers: int = 4, overlap_bytes: int = 128):
        self.filename = filename
        self.chunk_size_mb = chunk_size_mb
        self.max_workers = max_workers
        self.overlap_bytes = overlap_bytes
        self.logger = setup_logger(__name__)

   
    def _split_into_chunks(self, parser: Parser) -> List[Tuple[int, int]]:
        if not parser._data:
            raise RuntimeError("File must be opened before splitting.")

        file_size = len(parser._data)
        chunk_size = self.chunk_size_mb * 1024 * 1024
        chunks = []

        start = 0
        while start < file_size:
            start = parser._data.find(MSG_HEADER, start)
            if start == -1:
                break

            end = min(start + chunk_size, file_size)

            next_header = parser._data.find(MSG_HEADER, end)
            if next_header == -1:
                end = file_size
            else:
                end = next_header

            chunks.append((start, end))
            start = end

        return chunks
    
      
              
    @staticmethod
    def _process_chunk(filename: str, start: int, end: int, fmt_defs: Dict[int, Dict[str, Any]], message_type: Optional[str]) -> List[Dict[str, Any]]:
        """Process a chunk of the log file and return sorted messages."""
        try:
            for msg_id, fmt in fmt_defs.items():
                fmt["Struct"] = Struct("<" + "".join(FORMAT_MAPPING[c] for c in fmt["Format"]))

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
   
   
    def process_all(self, message_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Process the entire log file in parallel and return sorted messages."""
        try:
            with Parser(self.filename) as parser:
                list(parser.messages("FMT"))
                chunks = self._split_into_chunks(parser)
                fmt_defs = {
                    msg_id: {
                        "Name": fmt["Name"],
                        "Length": fmt["Length"],
                        "Format": fmt["Format"],
                        "Columns": ",".join(fmt["Columns"]),
                    }
                    for msg_id, fmt in parser._format_definitions.items()
                }

            if not chunks:
                return []

            self.logger.info(f"Processing {len(chunks)} chunks with {self.max_workers} workers...")
            starts, ends = zip(*chunks)

            with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as ex:
                sorted_chunks = ex.map(
                    ParallelLogProcessor._process_chunk,
                    [self.filename] * len(starts),
                    starts,
                    ends,
                    [fmt_defs] * len(starts),
                    [message_type] * len(starts),
                    chunksize=max(1, len(starts) // (self.max_workers * 2)),
                )

                results = list(heapq.merge(*sorted_chunks, key=lambda x: x.get("TimeUS", 0)))

            self.logger.info(f"Total messages parsed: {len(results):,}")
            return results

        except Exception as e:
            self.logger.error(f"Error in parallel processing: {e}")
            return []
   
if __name__ == "__main__":
    import time
    start = time.time()
    processor = ParallelLogProcessor(r"C:\Users\ootb\Downloads\log_file_test_01.bin", chunk_size_mb=50, max_workers=4)
    messages = processor.process_all()
    print(f"Total messages: {len(messages)}")
    print(f"TIME: {time.time() - start}")
    # print(messages[:500:15])


