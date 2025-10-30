import math
from pymavlink import mavutil
import pytest

from src.synchronous_parser.mavlog_parser import MavlogParser



LOG_FILE_PATH = r"C:\Users\ootb\Downloads\log_file_test_01.bin"



def mavlink_messages(file_path):
    mlog = mavutil.mavlink_connection(file_path, dialect="ardupilotmega")
    while True:
        msg = mlog.recv_match(blocking=False)
        if msg is None:
            break
        yield msg.to_dict()


def equal_ignore_nan(v1, v2):
    if isinstance(v1, float) and isinstance(v2, float):
        if math.isnan(v1) and math.isnan(v2):
            return True
    return v1 == v2


def dicts_equal(d1, d2):
    if d1.keys() != d2.keys():
        return False
    for k in d1:
        if not equal_ignore_nan(d1[k], d2[k]):
            return False
    return True

def test_mavlog_parser_matches_pymavlink():
    with MavlogParser(LOG_FILE_PATH) as parser:
        parser_msgs = parser.get_all_messages()

    pymav_msgs = list(mavlink_messages(LOG_FILE_PATH))

    assert len(parser_msgs) == len(pymav_msgs), \
        f"Message count mismatch: parser={len(parser_msgs)}, pymavlink={len(pymav_msgs)}"

    mismatches = []
    for i, (pmsg, pymav) in enumerate(zip(parser_msgs, pymav_msgs)):
        if not dicts_equal(pmsg, pymav):
            mismatches.append((i, pmsg, pymav))

    if mismatches:
        idx, pmsg, pymav = mismatches[0]
        print(f"\nMismatch at index {idx}:")
        for k in sorted(set(pmsg.keys()) | set(pymav.keys())):
            if k not in pmsg:
                print(f"  Missing in parser: {k}={pymav[k]}")
            elif k not in pymav:
                print(f"  Missing in pymavlink: {k}={pmsg[k]}")
            elif not equal_ignore_nan(pmsg[k], pymav[k]):
                print(f"  {k}: parser={pmsg[k]} | pymav={pymav[k]}")
        pytest.fail(f"{len(mismatches)} message(s) differ. First mismatch at index {idx}.")

    print(f"\nAll {len(parser_msgs)} messages matched pymavlink output.")

