from pymavlink import mavutil
import os
from typing import Any, Dict, Iterator, Optional, Type, List


class Mavlink:
    """
    MAVLink Binary Log Parser (.BIN) using pymavlink.
    Streams MAVLink messages as dictionaries using a generator.
    """

    def __init__(self, filename: str):
        self.filename: str = filename
        self._mavlog = None

    def __enter__(self) -> "Mavlink":
        """Open the MAVLink log file using pymavlink."""
        if not os.path.exists(self.filename):
            raise FileNotFoundError(f"File not found: {self.filename}")
        self._mavlog = mavutil.mavlink_connection(self.filename, dialect="ardupilotmega")
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[Any]
    ) -> None:
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

    def get_all_messages(self, message_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return all messages as a list (uses the generator internally)."""
        return [msg for msg in self.messages(message_type)]
