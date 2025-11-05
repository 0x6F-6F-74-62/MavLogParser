import math
import pytest
import json
import time

from business_logic.parallel import ParallelParser
from business_logic.mavlink import Mavlink



with open("config.json", "r") as f:
    config_data = json.load(f)

LOG_FILE_PATH = rf"{config_data.get("LOG_FILE_PATH")}"



def equal_ignore_nan(v1, v2):
    """
    Return True when v1 == v2, except treat NaN == NaN as True.
    Exact equality for all other types.
    """
    if isinstance(v1, float) and isinstance(v2, float):
        if math.isnan(v1) and math.isnan(v2):
            return True
    return v1 == v2


def dicts_equal(d1, d2):
    """
    Full equality of keys and values, except NaN==NaN.
    Returns True only if keys sets are equal and every corresponding value equals.
    """
    if set(d1.keys()) != set(d2.keys()):
        return False
    for k in d1:
        if not equal_ignore_nan(d1[k], d2[k]):
            return False
    return True


def _print_mismatch(idx, pmsg, pymav):
    print(f"\nMismatch at index {idx}:")
    all_keys = sorted(set(pmsg.keys()) | set(pymav.keys()))
    for k in all_keys:
        in_p = k in pmsg
        in_m = k in pymav
        if not in_p:
            print(f"  Missing in parser: {k} = {pymav[k]!r}")
            continue
        if not in_m:
            print(f"  Missing in pymavlink: {k} = {pmsg[k]!r}")
            continue
        v1 = pmsg[k]
        v2 = pymav[k]
        if not equal_ignore_nan(v1, v2):
            print(f"  {k}: parser={v1!r} | pymav={v2!r}")


def test_mavlog_parser_matches_pymavlink_exact_nan_equal():
    pareser_start = time.time()
    parser = ParallelParser(LOG_FILE_PATH)
    parser_msgs = parser.process_all()
    parser_end = time.time()

    mavlink_start = time.time()
    with Mavlink(LOG_FILE_PATH) as pymav_parser:
        pymav_msgs = pymav_parser.get_all_messages()
    mavlink_end = time.time()

    print(f"\nParser processed {len(parser_msgs)} messages in {parser_end - pareser_start:.2f} seconds.")
    print(f"Pymavlink processed {len(pymav_msgs)} messages in {mavlink_end - mavlink_start:.2f} seconds.")

    assert len(parser_msgs) == len(pymav_msgs), (
        f"Message count mismatch: parser={len(parser_msgs)}, pymavlink={len(pymav_msgs)}"
    )

    mismatches = []
    for i, (pmsg, pymav) in enumerate(zip(parser_msgs, pymav_msgs)):
        if not dicts_equal(pmsg, pymav):
            mismatches.append((i, pmsg, pymav))

    if mismatches:
        idx, pmsg, pymav = mismatches[0]
        _print_mismatch(idx, pmsg, pymav)
        pytest.fail(f"{len(mismatches)} message(s) differ. First mismatch at index {idx}.")

    print(f"\nAll {len(parser_msgs)} messages matched pymavlink output (NaN treated equal).")
