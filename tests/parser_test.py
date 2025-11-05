import math
import pytest
import json
import time

from business_logic.parser import Parser
from business_logic.mavlink import Mavlink



with open("config.json", "r") as f:
    config_data = json.load(f)


LOG_FILE_PATH = rf"{config_data.get("LOG_FILE_PATH")}"




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

def test_parser_matches_pymavlink():
    parser_start = time.time()
    with Parser(LOG_FILE_PATH) as parser:
        parser_msgs = parser.get_all_messages()
    parser_end = time.time()

    mavlink_start = time.time()
    with Mavlink(LOG_FILE_PATH) as pymav_parser:
        pymav_msgs = pymav_parser.get_all_messages()
    mavlink_end = time.time()

    print(f"\nParser processed {len(parser_msgs)} messages in {parser_end - parser_start:.2f} seconds.")
    print(f"Pymavlink processed {len(pymav_msgs)} messages in {mavlink_end - mavlink_start:.2f} seconds.")

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

