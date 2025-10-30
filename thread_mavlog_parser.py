# import struct
# import mmap
# import time
# from typing import Optional, Dict, Any, List, Tuple
# from mavlink_constants import MSG_HEADER, FORMAT_MSG_TYPE, FORMAT_MSG_LENGTH, SCALE_FACTOR_FIELDS, LATITUDE_LONGITUDE_FORMAT, ALTITUDE_MM_FIELDS, FORMAT_MAPPING
# from logger import setup_logger
# from concurrent.futures import ThreadPoolExecutor, as_completed


# class ThreadMavlogParser:
#     def __init__(self, filename: str):
#         self.filename = filename
#         self.logger = setup_logger(__name__)
#         self._file = None
#         self._data = None
#         self._offset = 0
#         self._fmt_defs: Dict[int, Dict] = {}
#         self._open_file()
#         self._extract_fmt()

#     def _open_file(self):
#         self._file = open(self.filename, 'rb')
#         self._data = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
#         self.logger.debug(f"Opened file: {self.filename}")

#     def __enter__(self):
#         return self

#     def __exit__(self, *args):
#         self.close()

#     def close(self):
#         if self._data:
#             self._data.close()
#             self._data = None
#         if self._file:
#             self._file.close()
#             self._file = None


#     def _extract_fmt(self):
#         """Scan the file once to populate self._fmt_defs"""
#         old_offset = getattr(self, "_offset", 0)
#         self._offset = 0
#         data_len = len(self._data)
#         while self._offset < data_len:
#             pos = self._data.find(MSG_HEADER, self._offset)
#             if pos == -1 or pos + 3 > data_len:
#                 break
#             msg_id = self._data[pos + 2]
#             if msg_id == FORMAT_MSG_TYPE:
#                 self._parse_and_store_fmt(pos)
#                 self._offset = pos + FORMAT_MSG_LENGTH
#             else:
#                 self._offset = pos + 1
#         self._offset = old_offset

#     def _parse_and_store_fmt(self, pos: int) -> Optional[Dict[str, Any]]:
#         """
#         Parse an FMT record at pos, store it in self._fmt_defs and
#         return a dict representation of the FMT (for including in results).
#         If parsing fails, return None.
#         """
#         try:
#             _, _, msg_type, length, name_b, fmt_b, cols_b = struct.unpack_from(
#                 '<2sBBB4s16s64s', self._data, pos)
#         except struct.error:
#             return None

#         name = name_b.split(b'\x00', 1)[0].decode('ascii', 'ignore').strip()
#         fmt = fmt_b.split(b'\x00', 1)[0].decode('ascii', 'ignore').strip()
#         cols_raw = cols_b.split(b'\x00', 1)[0].decode('ascii', 'ignore').strip()
#         cols = [c.strip() for c in cols_raw.split(',') if c.strip()]

#         if not (name and fmt and cols):
#             return None

#         try:
#             struct_obj = struct.Struct('<' + ''.join(FORMAT_MAPPING[c] for c in fmt))
#         except KeyError:
#             return None

#         self._fmt_defs[msg_type] = {
#             'name': name,
#             'length': length,
#             'format': fmt,
#             'columns': cols,
#             'struct': struct_obj
#         }

#         fmt_dict = {
#             'mavpackettype': 'FMT',
#             'Columns': cols_raw,
#             'Format': fmt,
#             'Length': length,
#             'Name': name,
#             'Type': msg_type
#         }
#         return fmt_dict

   
#     def _find_next_valid_header(self, pos: int) -> int:
#         """
#         Return the offset of the next occurrence of HEADER that appears to
#         be the start of a real message (i.e., the msg_id after header exists in self._fmt_defs
#         or is FMT_TYPE). If none found, return len(self._data).
#         """
#         data_len = len(self._data)
#         if pos < 0:
#             pos = 0
#         while pos < data_len - 3:
#             hdr_pos = self._data.find(MSG_HEADER, pos)
#             if hdr_pos == -1:
#                 return data_len
#             if hdr_pos + 2 >= data_len:
#                 return data_len
#             msg_id = self._data[hdr_pos + 2]
#             if msg_id == FORMAT_MSG_TYPE or msg_id in self._fmt_defs:
#                 return hdr_pos
#             pos = hdr_pos + 1
#         return data_len

#     def _split_to_chunks(self, num_threads: int) -> List[Tuple[int, int]]:
#         """
#         Split file into num_threads chunks. Returns list of (start, end) offsets.
#         Each start is aligned to a valid message header. Each end is aligned to
#         start of next valid header (i.e., exclusive).
#         """
#         if num_threads <= 0:
#             raise ValueError("num_threads must be > 0")

#         data_len = len(self._data)
#         if data_len == 0:
#             return []

#         approx = data_len // num_threads
#         chunks: List[Tuple[int, int]] = []
#         start = 0
#         start = self._find_next_valid_header(0)
#         if start >= data_len:
#             return []

#         for i in range(num_threads):
#             if i == num_threads - 1:
#                 end = data_len
#             else:
#                 candidate = (i + 1) * approx
#                 end = self._find_next_valid_header(candidate)
#                 if end > data_len:
#                     end = data_len
#             if end <= start:
#                 end = data_len
#             chunks.append((start, end))
#             start = end
#             if start >= data_len:
#                 break

#         return chunks

   
#     def _read_range_worker(self, start: int, end: int, msg_type: Optional[str] = None,
#                            max_messages: Optional[int] = None) -> List[Dict[str, Any]]:
#         """
#         Read messages from [start, end) (start aligned to valid header).
#         Returns list of dict messages (including FMT dicts).
#         """
#         data_len = len(self._data)
#         if start < 0:
#             start = 0
#         if end is None or end > data_len:
#             end = data_len
#         if start >= end:
#             return []

#         msgs: List[Dict[str, Any]] = []
#         pos = self._find_next_valid_header(start)
#         read_count = 0

#         while pos < data_len and pos < end:
#             if pos + 3 > data_len:
#                 break
#             msg_id = self._data[pos + 2]

#             if msg_id == FORMAT_MSG_TYPE:
#                 if pos + FORMAT_MSG_LENGTH > data_len:
#                     break
#                 fmt_dict = self._parse_and_store_fmt(pos) 
#                 pos += FORMAT_MSG_LENGTH
#                 if msg_type is None or msg_type == 'FMT':
#                     if fmt_dict:
#                         msgs.append(fmt_dict)
#                         read_count += 1
#                         if max_messages and read_count >= max_messages:
#                             break
#                 continue

#             fmt_def = self._fmt_defs.get(msg_id)
#             if not fmt_def:
#                 pos += 1
#                 continue

#             msg_len = fmt_def['length']
#             if pos + msg_len > data_len:
#                 break

#             try:
#                 unpacked = fmt_def['struct'].unpack_from(self._data, pos + 3)
#             except struct.error:
#                 pos += 1
#                 continue

#             msg: Dict[str, Any] = {}
#             fmt_chars = fmt_def['format']
#             for i, (col, val) in enumerate(zip(fmt_def['columns'], unpacked)):
#                 c = fmt_chars[i]
#                 if isinstance(val, bytes):
#                     msg[col] = val.rstrip(b'\x00').decode('ascii', 'ignore')
#                 elif c in SCALE_FACTOR_FIELDS:
#                     msg[col] = val / 100.0
#                 elif c == LATITUDE_LONGITUDE_FORMAT:
#                     msg[col] = val / 1e7
#                 elif c in ALTITUDE_MM_FIELDS:
#                     msg[col] = val / 1000.0
#                 else:
#                     msg[col] = val

#             msg['mavpackettype'] = fmt_def['name']

#             if msg_type is None or msg['mavpackettype'] == msg_type:
#                 msgs.append(msg)
#                 read_count += 1
#                 if max_messages and read_count >= max_messages:
#                     break

#             pos += msg_len
#         return msgs

#     def get_messages_parallel(self, num_threads: int = 4,
#                               msg_type: Optional[str] = None,
#                               max_per_thread: Optional[int] = None) -> List[Dict[str, Any]]:
#         """
#         High-level API: read file in parallel using num_threads threads.
#         Returns a merged list of messages (dicts) in file order.
#         """
#         if num_threads <= 0:
#             raise ValueError("num_threads must be > 0")

#         data_len = len(self._data)
#         if data_len == 0:
#             return []

#         chunks = self._split_to_chunks(num_threads)
#         if not chunks:
#             return []

#         results: List[List[Dict[str, Any]]] = [None] * len(chunks)
#         with ThreadPoolExecutor(max_workers=len(chunks)) as exe:
#             futures = {exe.submit(self._read_range_worker, s, e, msg_type, max_per_thread): idx
#                        for idx, (s, e) in enumerate(chunks)}
#             for fut in as_completed(futures):
#                 idx = futures[fut]
#                 try:
#                     res = fut.result()
#                 except Exception as exc:
#                     self.logger.warning(f"Worker {idx} failed: {exc}")
#                     res = []
#                 results[idx] = res

#         merged: List[Dict[str, Any]] = []
#         for part in results:
#             if part:
#                 merged.extend(part)
#         return merged

   
#     def get_all_messages(self, msg_type: Optional[str] = None) -> List[Dict[str, Any]]:
#         """Backward-compatible: read entire file single-threaded."""
#         self._offset = 0
#         return self._read_range_worker(0, len(self._data), msg_type=msg_type, max_messages=None)


# if __name__ == "__main__":
#     start = time.time()
#     parser = ThreadMavlogParser(r"C:\Users\ootb\Downloads\log_file_test_01.bin")
#     msgs = parser.get_messages_parallel(num_threads=4)
#     print(f"Read {len(msgs)} messages in {time.time() - start:.2f}s")
#     parser.close()



import struct
import mmap
import os
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor
from src.utils.constants import HEADER, FMT_TYPE, FMT_LENGTH, SCALED, LATLON, ALT_MM, FMT_MAPPING
from src.utils.logger import setup_logger


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
            for future in futures:  
                msgs = future.result()
                all_msgs.extend(msgs)
                completed += 1
                if progress_callback:
                    progress_callback(completed, len(futures))

        self.logger.info(f"Parsed {len(all_msgs)} messages in {time.time() - start_time:.2f}s (threads)")
        return all_msgs
    
if __name__ == "__main__":
    import time

    parser = ThreadedMavlogParser(r"C:\Users\ootb\Downloads\log_file_test_01.bin", max_workers=4)
    
    start = time.time()
    msgs = parser.read_messages(
        nsats_min=4,
        progress_callback=lambda done, total: print(f"Thread progress: {done}/{total}")
    )
    
    print(f"messages: {len(msgs):,}")
    print(f"Time: {time.time() - start:.3f}s")
