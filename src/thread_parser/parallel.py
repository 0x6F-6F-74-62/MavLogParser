# parallel.py (optimized, threads)
import mmap
from typing import List, Dict, Any, Optional, Tuple
import concurrent.futures
from src.processing_parser.parser import Parser
from src.utils.constants import MSG_HEADER, FORMAT_MAPPING
from src.utils.logger import setup_logger

class ParallelLogProcessor:
    """
    Processes large MAVLink binary log files (.BIN) in parallel using ThreadPoolExecutor.
    The file is memory-mapped once and shared (read-only) between threads via memoryview.
    """

    def __init__(self, filename: str, chunk_size_mb: int = 100, max_workers: int = 4, overlap_bytes: int = 128):
        self.filename = filename
        self.chunk_size_bytes = chunk_size_mb * 1024 * 1024
        self.max_workers = max_workers
        self.overlap_bytes = overlap_bytes
        self.logger = setup_logger(__name__)

    def _split_into_chunks_by_header(self, buffer_view: memoryview) -> List[Tuple[int, int]]:
        """
        Split the file into chunks aligned to MSG_HEADER boundaries.
        Returns list of (start, end) offsets.
        """
        file_size = len(buffer_view)
        header = MSG_HEADER
        chunks: List[Tuple[int, int]] = []

        start = 0
        while start < file_size:
            # find first header at or after start
            # memoryview slice then tobytes().find used for robust find
            relative = buffer_view[start:file_size].tobytes()
            found = relative.find(header)
            if found == -1:
                break
            header_pos = start + found

            # determine end candidate
            end_candidate = min(header_pos + self.chunk_size_bytes, file_size)

            # find next header after end_candidate to align end
            relative_after = buffer_view[end_candidate:file_size].tobytes()
            next_header_found = relative_after.find(header)
            if next_header_found == -1:
                end = file_size
            else:
                end = end_candidate + next_header_found

            # optionally extend backwards to include overlap (small safety)
            end = min(end + self.overlap_bytes, file_size)
            chunks.append((header_pos, end))
            start = end

        if not chunks:
            # fallback single chunk
            chunks.append((0, file_size))
        return chunks

    @staticmethod
    def _worker_parse_chunk(
        buffer_view: memoryview,
        start: int,
        end: int,
        format_definitions: Dict[int, Dict[str, Any]],
        message_type: Optional[str],
    ) -> List[Dict[str, Any]]:
        # delegate to Parser.parse_buffer_chunk (stateless)
        return Parser.parse_buffer_chunk(buffer_view, start, end, format_definitions, message_type)

    def process_all(self, message_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Main entry: memory-map file once, build format definitions, split into chunks,
        and parse chunks in parallel with threads.
        """
        try:
            # open and mmap once
            with open(self.filename, "rb") as fh, mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                buffer_view = memoryview(mm)

                # Use a Parser in main thread to collect initial FMT messages and format definitions
                with Parser(self.filename) as parser:
                    # iterate all FMT messages to populate parser._format_definitions
                    for _ in parser.messages("FMT"):
                        pass

                    # copy format definitions but replace Struct with compiled struct.Struct
                    fmt_defs_for_workers: Dict[int, Dict[str, Any]] = {}
                    for msg_id, fmt_def in parser._format_definitions.items():
                        # ensure we create fresh dicts so threads can read them without side-effects
                        compiled_struct = fmt_def["Struct"]  # already struct.Struct from parser
                        fmt_defs_for_workers[msg_id] = {
                            "Name": fmt_def["Name"],
                            "Length": fmt_def["Length"],
                            "Format": fmt_def["Format"],
                            "Columns": fmt_def["Columns"],
                            "Struct": compiled_struct,
                        }

                # split into chunks aligned on header
                chunks = self._split_into_chunks_by_header(buffer_view)
                self.logger.info(f"Processing {len(chunks)} chunks using {self.max_workers} threads...")

                results: List[Dict[str, Any]] = []

                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self._worker_parse_chunk, buffer_view, start, end, fmt_defs_for_workers, message_type)
                        for start, end in chunks
                    ]

                    for future in concurrent.futures.as_completed(futures):
                        chunk_results = future.result() or []
                        results.extend(chunk_results)
                        self.logger.debug(f"Chunk processed: {len(chunk_results)} messages")

                # final sort by TimeUS where present
                results.sort(key=lambda x: x.get("TimeUS", 0))
                self.logger.info(f"Total messages parsed: {len(results):,}")
                return results

        except Exception as e:
            self.logger.error(f"Error in parallel processing: {e}")
            return []
