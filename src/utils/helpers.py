import mmap
from typing import Any, Dict

from src.utils.constants import FORMAT_MSG_TYPE, MSG_HEADER


def is_valid_message_header(data: bytes | mmap.mmap, pos: int, fmt_defs: Dict[int, Dict[str, Any]]) -> bool:
    """Check if MSG_HEADER at pos marks a valid message start."""
    if pos + 2 >= len(data) or data[pos : pos + 2] != MSG_HEADER:
        return False

    msg_id = data[pos + 2]
    if msg_id == FORMAT_MSG_TYPE:
        return True

    fmt = fmt_defs.get(msg_id)
    return bool(fmt and pos + fmt["Length"] <= len(data))


def bytes_to_ascii(bytes_data: bytes) -> str:
    """Convert null-terminated bytes to ASCII string."""
    null = bytes_data.find(0)
    return bytes_data[: null if null != -1 else None].decode("ascii", "ignore").strip()
