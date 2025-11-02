import mmap
import concurrent.futures
from typing import List, Dict, Any, Optional, Tuple
from struct import Struct
import heapq
import os
from src.processing_parser.parser import Parser
from src.utils.constants import MSG_HEADER, FORMAT_MAPPING
from src.utils.logger import setup_logger


class ParallelLogProcessor:
    """
    Processes large MAVLink binary log files (.BIN) in parallel.
    Splits the file into aligned chunks and uses multiple processes for faster parsing.
    """

    def __init__(self, filename: str, chunk_size_mb: int = 100, max_workers: Optional[int] = None, overlap_bytes: int = 128):
        self.filename: str = filename
        self.chunk_size_mb: int = chunk_size_mb
        self.max_workers: int = max_workers if max_workers else os.cpu_count() or 4
        self.overlap_bytes: int = overlap_bytes
        self.logger = setup_logger(os.path.basename(__file__))
    
    def _split_into_chunks(self, parser: Parser) -> List[Tuple[int, int]]:
        """"""
        file_size: int = os.path.getsize(self.filename)
        chunk_size: int = max(1, file_size // self.max_workers) 
        chunks: List[Tuple[int, int]] = []
        data: Optional[mmap.mmap] = parser.data
        if data is None:
            raise RuntimeError("Parser data is not initialized before splitting.")
        start_pos: int = 0
        while start_pos < file_size:
            tentative_end: int = min(start_pos + chunk_size, file_size)

            next_header: int = data.find(MSG_HEADER, tentative_end, min(tentative_end + self.overlap_bytes, file_size)) 

            end_pos: int = file_size if next_header == -1 else next_header            

            chunks.append((start_pos, end_pos))
            start_pos = end_pos
        return chunks
    
              
    @staticmethod
    def _process_chunk(filename: str, start: int, end: int, fmt_defs: Dict[int, Dict[str, Any]], message_type: Optional[str]) -> List[Dict[str, Any]]:
        """Process a chunk of the log file and return sorted messages."""
        try:
            for _, fmt in fmt_defs.items():
                fmt["Struct"] = Struct("<" + "".join(FORMAT_MAPPING[c] for c in fmt["Format"]))

            with open(filename, "rb") as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                parser = Parser(filename)
                parser.data = mm
                parser.offset = start
                parser.format_definitions = fmt_defs

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
                    for msg_id, fmt in parser.format_definitions.items()
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
    start_time = time.time()
    processor = ParallelLogProcessor(r"C:\Users\ootb\Downloads\log_file_test_01.bin")
    my_messages = processor.process_all()
    print(f"Total messages: {len(my_messages)}")
    print(f"TIME: {time.time() - start_time}")
    # print(messages[:500:15])

