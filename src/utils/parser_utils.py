from typing import List, Tuple
import os

from src.bin_parser.parser import Parser
from src.utils.constants import MSG_HEADER

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