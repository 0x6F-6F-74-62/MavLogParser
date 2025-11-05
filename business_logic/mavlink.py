from pymavlink import mavutil
import os
from typing import Any, Dict, Iterator, Optional

class PymavlinkParser:
    """
    MAVLink Binary Log Parser (.BIN) using pymavlink.
    Streams MAVLink messages as dictionaries using a generator.
    """

    def __init__(self, filename: str):
        self.filename: str = filename
        self._mavlog = None

    def __enter__(self) -> "PymavlinkParser":
        """Open the MAVLink log file using pymavlink."""
        if not os.path.exists(self.filename):
            raise FileNotFoundError(f"File not found: {self.filename}")
        self._mavlog = mavutil.mavlink_connection(self.filename)
        return self

    def __exit__(self, *args) -> None:
        """Close the MAVLink log."""
        if self._mavlog:
            self._mavlog.close()
        self._mavlog = None

    def messages(self, message_type: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        """
        Generator yielding MAVLink messages as dictionaries.
        :param message_type: Filter by message type (e.g. 'IMU', 'GPS', etc.)
        """
        if not self._mavlog:
            raise RuntimeError("Parser not initialized. Use 'with PymavlinkParser(...) as parser:'")

        while True:
            msg = self._mavlog.recv_match(type=message_type, blocking=False)
            if msg is None:
                break
            yield msg.to_dict()

    def get_all_messages(self, message_type: Optional[str] = None):
        """Return all messages as a list (uses the generator internally)."""
        return list(self.messages(message_type))
