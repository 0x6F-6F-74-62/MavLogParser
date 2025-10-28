
import struct
import mmap
import os
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor
from mavlink_constants import HEADER, FMT_TYPE, FMT_LENGTH, SCALED, LATLON, ALT_MM, FMT_MAPPING
from logger import setup_logger


class ThreadedMavlogParser:
    def __init__(self, filename: str, max_workers: int = None):
        self.filename = filename
        self.max_workers = max_workers or min(32, os.cpu_count() + 4)
        self.logger = setup_logger(__name__)
        self._fmt_defs: Dict[int, Dict] = {}
        self._parse_fmt_definitions()  

    
    def _parse_fmt_definitions(self):
        with open(self.filename, 'rb') as f:
            data = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            offset = 0
            data_len = len(data)

            while offset < data_len:
                pos = data.find(HEADER, offset)
                if pos == -1 or pos + 3 >= data_len:
                    break
                if data[pos + 2] == FMT_TYPE and pos + FMT_LENGTH <= data_len:
                    try:
                        _, _, msg_type, length, name_b, fmt_b, cols_b = struct.unpack_from(
                            '<2sBBB4s16s64s', data, pos)
                        name = name_b.split(b'\x00', 1)[0].decode('ascii', 'ignore').strip()
                        fmt = fmt_b.split(b'\x00', 1)[0].decode('ascii', 'ignore').strip()
                        cols = [c.strip() for c in cols_b.split(b'\x00', 1)[0].decode('ascii', 'ignore').split(',') if c.strip()]
                        if name and fmt and cols:
                            struct_obj = struct.Struct('<' + ''.join(FMT_MAPPING[c] for c in fmt))
                            self._fmt_defs[msg_type] = {
                                'name': name, 'length': length, 'format': fmt,
                                'columns': cols, 'struct': struct_obj
                            }
                    except:
                        pass
                offset = pos + 1
            data.close()
        self.logger.info(f"Extracted {len(self._fmt_defs)} FMT definitions")

    
    def _parse_chunk(self, args):
        start, end, filename, msg_type_filter, nsats_min = args
        msgs = []
        try:
            with open(filename, 'rb') as f:
                data = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                offset = start

                while offset < end:
                    pos = data.find(HEADER, offset)
                    if pos == -1 or pos >= end:
                        break
                    msg_id = data[pos + 2]
                    if msg_id == FMT_TYPE:
                        offset = pos + FMT_LENGTH
                        continue

                    fmt_def = self._fmt_defs.get(msg_id)
                    if not fmt_def:
                        offset = pos + 1
                        continue

                    if msg_type_filter and fmt_def['name'] != msg_type_filter:
                        offset = pos + fmt_def['length']
                        continue

                    if pos + fmt_def['length'] > len(data):
                        break

                    try:
                        unpacked = fmt_def['struct'].unpack_from(data, pos + 3)
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
                        msg['TypeName'] = fmt_def['name']

                        if nsats_min is not None and msg.get('NSats', 0) < nsats_min:
                            offset = pos + fmt_def['length']
                            continue

                        msgs.append(msg)
                    except:
                        pass
                    offset = pos + fmt_def['length']
        except Exception as e:
            self.logger.error(f"Chunk error: {e}")
        return msgs

   
    def read_messages(
        self,
        msg_type: Optional[str] = None,
        nsats_min: Optional[int] = None,
        progress_callback: Optional = None
    ) -> List[Dict]:
        file_size = os.path.getsize(self.filename)
        chunk_size = file_size // self.max_workers
        chunks = []

        for i in range(self.max_workers):
            start = max(0, i * chunk_size - 2048)
            end = file_size if i == self.max_workers - 1 else (i + 1) * chunk_size
            chunks.append((start, end, self.filename, msg_type, nsats_min))

        start_time = time.time()
        all_msgs = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._parse_chunk, chunk) for chunk in chunks]
            completed = 0
            for future in ThreadPoolExecutor(futures):
                msgs = future.result()
                all_msgs.extend(msgs)
                completed += 1
                if progress_callback:
                    progress_callback(completed, len(futures))

        self.logger.info(f"Parsed {len(all_msgs)} messages in {time.time() - start_time:.2f}s (threads)")
        return all_msgs
    
if __name__ == "__main__":
    import time

    parser = ThreadedMavlogParser(r"C:\Users\ootb\Downloads\log_file_test_01.bin", max_workers=8)
    
    start = time.time()
    gps_msgs = parser.read_messages(
        msg_type="GPS",
        nsats_min=6,
        progress_callback=lambda done, total: print(f"Thread progress: {done}/{total}")
    )
    
    print(f"GPS messages: {len(gps_msgs):,}")
    print(f"Time: {time.time() - start:.3f}s")