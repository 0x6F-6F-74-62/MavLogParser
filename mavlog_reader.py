import struct
import mmap
from typing import Optional, Dict, Any, Iterator
from mavlink_constants import HEADER, FMT_TYPE, FMT_LENGTH, SCALED, LATLON, ALT_MM, FMT_MAPPING
from logger import setup_logger


class MavlogParser:
    def __init__(self, filename: str):
        self.filename = filename
        self.logger = setup_logger(__name__)
        self._file = None
        self._data = None
        self._offset = 0
        self._fmt_defs: Dict[int, Dict] = {}
        self._open_file()

    def _open_file(self):
        self._file = open(self.filename, 'rb')
        self._data = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
        self.logger.debug(f"Opened file: {self.filename}")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self._data:
            self._data.close()
            self._data = None
        if self._file:
            self._file.close()
            self._file = None

  
    def messages(self, msg_type: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        data_len = len(self._data)
        while self._offset < data_len:
            pos = self._data.find(HEADER, self._offset)
            if pos == -1 or pos + 3 > data_len:
                break

            msg_id = self._data[pos + 2]

            if msg_id == FMT_TYPE:
                if pos + FMT_LENGTH <= data_len:
                    self._parse_and_store_fmt(pos)
                self._offset = pos + FMT_LENGTH
                continue

            fmt_def = self._fmt_defs.get(msg_id)

            if msg_type and fmt_def['name'] != msg_type:
                self._offset = pos + fmt_def['length']
                continue

            msg_len = fmt_def['length']
            if pos + msg_len > data_len:
                break

            try:
                unpacked = fmt_def['struct'].unpack_from(self._data, pos + 3)
            except struct.error:
                self._offset = pos + 1
                continue

            msg = self._scale_values(fmt_def, unpacked)
            msg['TypeName'] = fmt_def['name']
            self._offset = pos + msg_len

            if msg_type and msg['TypeName'] != msg_type:
                continue

            yield msg  

    def _parse_and_store_fmt(self, pos: int):
        try:
            _, _, msg_type, length, name_b, fmt_b, cols_b = struct.unpack_from(
                '<2sBBB4s16s64s', self._data, pos)
        except struct.error:
            return

        name = name_b.split(b'\x00', 1)[0].decode('ascii', 'ignore').strip()
        fmt = fmt_b.split(b'\x00', 1)[0].decode('ascii', 'ignore').strip()
        cols = [c.strip() for c in cols_b.split(b'\x00', 1)[0].decode('ascii', 'ignore').split(',') if c.strip()]

        if not (name and fmt and cols):
            return

        try:
            struct_obj = struct.Struct('<' + ''.join(FMT_MAPPING[c] for c in fmt))
        except KeyError:
            return

        self._fmt_defs[msg_type] = {
            'name': name,
            'length': length,
            'format': fmt,
            'columns': cols,
            'struct': struct_obj
        }

    def _scale_values(self, fmt_def: dict, unpacked: tuple) -> dict:
        msg = {}
        fmt_chars = fmt_def['format']
        for i, (col, val) in enumerate(zip(fmt_def['columns'], unpacked)):
            c = fmt_chars[i]
            if isinstance(val, bytes):
                msg[col] = val.rstrip(b'\x00').decode('ascii', 'ignore')
            elif c in SCALED:
                msg[col] = val / 100.0
            elif c == LATLON:
                msg[col] = val / 1e7
            elif c in ALT_MM:
                msg[col] = val / 1000.0
            else:
                msg[col] = val
        return msg
    
if __name__ == "__main__":
    import time
    start = time.time()

    with MavlogParser(r"C:\Users\ootb\Downloads\log_file_test_01.bin") as parser:
        n = 0
        for msg in parser.messages():
            n += 1
            if n % 10000 == 0:
                print(f"Parsed {n:,} GPS messages...")

    print(f"\nTotal GPS: {n:,}")
    print(f"Time: {time.time() - start:.3f}s")