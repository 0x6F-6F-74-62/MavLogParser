import struct
import pytest
import json
from src.utils.constants import (
    MSG_HEADER, FORMAT_MSG_TYPE, FORMAT_MSG_LENGTH, FORMAT_MAPPING,
    SCALE_FACTOR_FIELDS, LATITUDE_LONGITUDE_FORMAT, BYTES_FIELDS, FMT_STRUCT
)

from src.business_logic.mavlink import Mavlink


@pytest.fixture()
def log_file_path():
    """Load log path once per module."""
    with open("config.json", "r") as f:
        return json.load(f)["LOG_FILE_PATH"]


@pytest.fixture()
def reference_messages(log_file_path):
    """Reference output from pymavlink."""
    with Mavlink(log_file_path) as mav:
        return mav.get_all_messages()

@pytest.fixture
def mock_config():
    """Mock configuration data."""
    return {
        "MSG_HEADER": MSG_HEADER,
        "FORMAT_MSG_TYPE": FORMAT_MSG_TYPE,
        "FORMAT_MSG_LENGTH": FORMAT_MSG_LENGTH,
        "FORMAT_MAPPING": FORMAT_MAPPING,
        "SCALE_FACTOR_FIELDS": SCALE_FACTOR_FIELDS,
        "LATITUDE_LONGITUDE_FORMAT": LATITUDE_LONGITUDE_FORMAT,
        "BYTES_FIELDS": BYTES_FIELDS,
        "FMT_STRUCT": FMT_STRUCT,
    }


@pytest.fixture
def sample_fmt_message():
    """Create a valid FMT message."""
    header = b"\xa3\x95"
    msg_id = 128
    type_id = 1
    length = 10
    name = b"TEST"
    format_str = b"BHI\x00" + b"\x00" * 13
    columns = b"A,B,C\x00" + b"\x00" * 59

    return struct.pack("<2sBBB4s16s64s", header, msg_id, type_id, length, name, format_str, columns)


@pytest.fixture
def sample_data_message(sample_fmt_message):
    """Create a valid data message matching the FMT."""
    header = b"\xa3\x95"
    msg_id = 1
    data = struct.pack("<BHI", 255, 1000, 100000)
    return header + struct.pack("B", msg_id) + data


@pytest.fixture
def valid_log_file(tmp_path, sample_fmt_message, sample_data_message):
    """Create a temporary valid log file."""
    log_file = tmp_path / "test.bin"
    with open(log_file, "wb") as f:
        f.write(sample_fmt_message)
        f.write(sample_data_message)
        f.write(sample_data_message)
    return str(log_file)


@pytest.fixture
def empty_log_file(tmp_path):
    """Create an empty log file."""
    log_file = tmp_path / "empty.bin"
    log_file.touch()
    return str(log_file)


@pytest.fixture
def corrupted_log_file(tmp_path):
    """Create a corrupted log file (no valid headers)."""
    log_file = tmp_path / "corrupted.bin"
    with open(log_file, "wb") as f:
        f.write(b"\x00" * 1000)
    return str(log_file)


@pytest.fixture
def minimal_log_file(tmp_path):
    """Create a minimal valid log file with just one FMT message."""
    log_file = tmp_path / "minimal.bin"
    header = b"\xa3\x95"
    msg_id = 128
    fmt_data = struct.pack(
        "<BB4s16s64s",
        1,
        10,
        b"GPS\x00",
        b"BH\x00" + b"\x00" * 13,
        b"A,B\x00" + b"\x00" * 61
    )
    with open(log_file, "wb") as f:
        f.write(header + struct.pack("B", msg_id) + fmt_data)
    return str(log_file)
