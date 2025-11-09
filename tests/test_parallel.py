import pytest
import struct

from src.business_logic.parser import Parser
from src.business_logic.parallel import ParallelParser
from src.utils.helpers import is_valid_message_header
from src.utils.constants import FORMAT_MAPPING


def test_initialization_custom_workers():
    """Test initialization with custom worker count."""
    parser = ParallelParser("test.bin", max_workers=8)
    assert parser.max_workers == 8

def test_process_all_with_valid_file(valid_log_file):
    """Test process_all with a valid log file."""
    parser = ParallelParser(valid_log_file, max_workers=2)
    results = parser.process_all()

    assert isinstance(results, list)
    assert len(results) >= 1

def test_process_all_with_message_filter(valid_log_file):
    """Test process_all with message type filter."""
    parser = ParallelParser(valid_log_file, max_workers=2)
    results = parser.process_all(message_type="FMT")

    for msg in results:
        if "mavpackettype" in msg:
            assert msg["mavpackettype"] == "FMT"

def test_process_all_empty_file(empty_log_file):
    """Test process_all with empty file."""
    parser = ParallelParser(empty_log_file, max_workers=2)

    with pytest.raises(RuntimeError):
        parser.process_all()

def test_process_all_thread_executor(valid_log_file):
    """Test process_all with thread executor."""
    parser = ParallelParser(valid_log_file, max_workers=4)
    results = parser.process_all(executor_type="thread")

    assert isinstance(results, list)
    assert len(results) >= 1

def test_split_to_chunks_basic(valid_log_file):
    """Test splitting file into chunks."""
    with Parser(valid_log_file) as parser:
        list(parser.messages("FMT"))  # Load format definitions
        chunks = ParallelParser._split_to_chunks(parser, max_workers=2)

        assert isinstance(chunks, list)
        assert len(chunks) >= 1

        for i, (start, end) in enumerate(chunks):
            assert start < end
            if i > 0:
                assert start >= chunks[i - 1][1]

def test_split_to_chunks_single_worker(valid_log_file):
    """Test splitting with single worker."""
    with Parser(valid_log_file) as parser:
        list(parser.messages("FMT"))
        chunks = ParallelParser._split_to_chunks(parser, max_workers=1)

        assert len(chunks) >= 1

def test_split_to_chunks_many_workers(valid_log_file):
    """Test splitting with many workers."""
    with Parser(valid_log_file) as parser:
        list(parser.messages("FMT"))
        chunks = ParallelParser._split_to_chunks(parser, max_workers=10)

        assert len(chunks) >= 1

def test_split_to_chunks_empty_file(empty_log_file):
    """Test splitting empty file raises error."""
    with pytest.raises(RuntimeError, match="Empty MAVLink log file"):
        with Parser(empty_log_file) as parser:
            ParallelParser._split_to_chunks(parser, max_workers=2)

def test_split_to_chunks_no_headers(corrupted_log_file):
    """Test splitting file with no valid headers."""
    with Parser(corrupted_log_file) as parser:
        with pytest.raises(RuntimeError, match="No valid message headers"):
            ParallelParser._split_to_chunks(parser, max_workers=2)

def test_is_valid_message_header():
    """Test validating message headers."""
    # Create mock data with header
    data = b"\xa3\x95\x01" + b"\x00" * 100
    format_defs = {
        1: {"Length": 10}
    }

    assert is_valid_message_header(data, 0, format_defs)

    assert not is_valid_message_header(data, 99, format_defs)

    bad_data = b"\x00\x00\x01" + b"\x00" * 100
    assert not is_valid_message_header(bad_data, 0, format_defs)

def test_is_valid_message_header_fmt():
    """Test validating FMT message headers."""
    data = b"\xa3\x95\x80" + b"\x00" * 100  # 128 = FORMAT_MSG_TYPE
    format_defs = {}

    assert is_valid_message_header(data, 0, format_defs)

def test_is_valid_message_header_unknown_type():
    """Test validating headers with unknown message type."""
    data = b"\xa3\x95\xFF" + b"\x00" * 100
    format_defs = {}

    assert not is_valid_message_header(data, 0, format_defs)

def test_process_chunk_basic(valid_log_file):
    """Test processing a single chunk."""
    with Parser(valid_log_file) as parser:
        list(parser.messages("FMT"))
        format_defs = parser.format_definitions
        file_size = len(parser.data)

    serializable_defs = {}
    for msg_id, fmt in format_defs.items():
        serializable_defs[msg_id] = {
            "Name": fmt["Name"],
            "Length": fmt["Length"],
            "Format": fmt["Format"],
            "Columns": fmt["Columns"],
            "StructStr": "<" + "".join(FORMAT_MAPPING[c] for c in fmt["Format"])
        }

    messages = ParallelParser._process_chunk(
        valid_log_file,
        (0, file_size),
        serializable_defs,
        None,
        need_struct_rebuild=True
    )

    assert isinstance(messages, list)
    assert len(messages) >= 1

def test_process_chunk_with_filter(valid_log_file):
    """Test processing chunk with message type filter."""
    with Parser(valid_log_file) as parser:
        list(parser.messages("FMT"))
        format_defs = parser.format_definitions
        file_size = len(parser.data)

    serializable_defs = {}
    for msg_id, fmt in format_defs.items():
        serializable_defs[msg_id] = {
            "Name": fmt["Name"],
            "Length": fmt["Length"],
            "Format": fmt["Format"],
            "Columns": fmt["Columns"],
            "StructStr": "<" + "".join(FORMAT_MAPPING[c] for c in fmt["Format"])
        }

    messages = ParallelParser._process_chunk(
        valid_log_file,
        (0, file_size),
        serializable_defs,
        "FMT",
        need_struct_rebuild=True
    )

    for msg in messages:
        if "mavpackettype" in msg:
            assert msg["mavpackettype"] == "FMT"

def test_process_chunk_error_handling():
    """Test error handling in chunk processing."""
    with pytest.raises(RuntimeError):
        ParallelParser._process_chunk(
            "nonexistent.bin",
            (0, 100),
            {},
            None,
            need_struct_rebuild=True
        )

def test_struct_rebuild(valid_log_file):
    """Test that struct rebuilding works correctly."""
    with Parser(valid_log_file) as parser:
        list(parser.messages("FMT"))

        format_defs = {}
        for msg_id, fmt in parser.format_definitions.items():
            format_defs[msg_id] = {
                "Name": fmt["Name"],
                "Length": fmt["Length"],
                "Format": fmt["Format"],
                "Columns": fmt["Columns"],
                "StructStr": "<" + "".join(FORMAT_MAPPING[c] for c in fmt["Format"])
            }

        messages = ParallelParser._process_chunk(
            valid_log_file,
            (0, len(parser.data)),
            format_defs,
            None,
            need_struct_rebuild=True
        )

        assert len(messages) >= 0  # Should not crash


def test_large_file_chunking(tmp_path):
    """Test handling of larger files with multiple chunks."""
    # Create a larger test file
    log_file = tmp_path / "large.bin"
    with open(log_file, "wb") as f:
        # Write FMT
        fmt_header = b"\xa3\x95\x80"
        fmt_data = struct.pack(
            "<BB4s16s64s",
            1, 10,
            b"TST\x00",
            b"BHI\x00" + b"\x00" * 12,
            b"A,B,C\x00" + b"\x00" * 58
        )
        f.write(fmt_header + fmt_data)

        for _ in range(1000):
            msg_header = b"\xa3\x95\x01"
            msg_data = struct.pack("<BHI", 1, 100, 1000)
            f.write(msg_header + msg_data)

    parallel_parser = ParallelParser(str(log_file), max_workers=4)
    messages = parallel_parser.process_all()

    assert len(messages) >= 1000