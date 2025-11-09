import struct
from src.business_logic.parser import Parser
from src.business_logic.parallel import ParallelParser



def test_parse_empty_file(empty_log_file):
    """Test parsing an empty file."""
    with Parser(empty_log_file) as parser:
        messages = list(parser.messages())
        assert len(messages) == 0

def test_parse_corrupted_file(corrupted_log_file):
    """Test parsing a file with no valid headers."""
    with Parser(corrupted_log_file) as parser:
        messages = list(parser.messages())
        assert len(messages) == 0

def test_parse_fmt_messages(valid_log_file):
    """Test parsing FMT messages."""
    with Parser(valid_log_file) as parser:
        fmt_messages = list(parser.messages("FMT"))
        assert len(fmt_messages) >= 1
        assert fmt_messages[0]["mavpackettype"] == "FMT"
        assert "Name" in fmt_messages[0]
        assert "Format" in fmt_messages[0]
        assert "Columns" in fmt_messages[0]

def test_parse_specific_message_type(valid_log_file):
    """Test filtering messages by type."""
    with Parser(valid_log_file) as parser:
        list(parser.messages("FMT"))
        parser.offset = 0

        test_messages = list(parser.messages("TEST"))
        for msg in test_messages:
            if msg.get("mavpackettype") != "FMT":
                assert msg["mavpackettype"] == "TEST"

def test_bytes_to_ascii():
    """Test _bytes_to_ascii static method."""
    result = Parser._bytes_to_ascii(b"TEST\x00\x00")
    assert result == "TEST"

    result = Parser._bytes_to_ascii(b"TEST")
    assert result == "TEST"

    result = Parser._bytes_to_ascii(b"\x00")
    assert result == ""

def test_decode_messages_with_bytes_field():
    """Test decoding messages with byte fields."""
    format_defs = {
        "Format": "Z",
        "Columns": ["Data"],
    }
    unpacked = (b"test_data\x00\x00",)

    result = Parser._decode_messages("TEST", format_defs, unpacked)
    assert result["mavpackettype"] == "TEST"
    assert isinstance(result["Data"], bytes)

def test_decode_messages_with_scale_factor():
    """Test decoding messages with scale factor fields."""
    format_defs = {
        "Format": "c",
        "Columns": ["Alt"],
    }
    unpacked = (1000,)

    result = Parser._decode_messages("TEST", format_defs, unpacked)
    assert result["Alt"] == 10.0

def test_decode_messages_with_lat_lon():
    """Test decoding messages with latitude/longitude."""
    format_defs = {
        "Format": "L",
        "Columns": ["Lat"],
    }
    unpacked = (376543210,)

    result = Parser._decode_messages("GPS", format_defs, unpacked)
    assert abs(result["Lat"] - 37.6543210) < 0.0000001

def test_decode_messages_error_handling():
    """Test that decode handles errors gracefully."""
    format_defs = {
        "Format": "B",
        "Columns": ["Field"],
    }
    unpacked = (None,)

    result = Parser._decode_messages("TEST", format_defs, unpacked)
    assert "Field" in result

def test_messages_with_end_index(valid_log_file):
    """Test messages generator with end_index parameter."""
    with Parser(valid_log_file) as parser:
        all_msgs = list(parser.messages())
        total = len(all_msgs)

        parser.offset = 0
        partial_msgs = list(parser.messages(end_index=50))

        assert len(partial_msgs) <= total

def test_format_defs_persistence(valid_log_file):
    """Test that format definitions persist across message parsing."""
    with Parser(valid_log_file) as parser:
        list(parser.messages("FMT"))

        assert len(parser.format_defs) > 0

        parser.offset = 0
        messages = list(parser.messages())

        assert len(parser.format_defs) > 0

def test_malformed_fmt_message(tmp_path):
    """Test handling of malformed FMT messages."""
    log_file = tmp_path / "malformed.bin"
    with open(log_file, "wb") as f:
        f.write(b"\xa3\x95\x80")
        f.write(b"\x00" * 10)

    with Parser(str(log_file)) as parser:
        messages = list(parser.messages())


def test_single_byte_file(tmp_path):
    """Test file with just one byte."""
    log_file = tmp_path / "single.bin"
    with open(log_file, "wb") as f:
        f.write(b"\x00")

    with Parser(str(log_file)) as parser:
        messages = list(parser.messages())
        assert len(messages) == 0

def test_header_at_end_of_file(tmp_path):
    """Test file ending with incomplete message."""
    log_file = tmp_path / "incomplete.bin"
    with open(log_file, "wb") as f:
        f.write(b"\xa3\x95\x01")  # Header but no data

    with Parser(str(log_file)) as parser:
        messages = list(parser.messages())

def test_multiple_consecutive_headers(tmp_path):
    """Test multiple headers without complete messages."""
    log_file = tmp_path / "multi_header.bin"
    with open(log_file, "wb") as f:
        f.write(b"\xa3\x95\xa3\x95\xa3\x95")

    with Parser(str(log_file)) as parser:
        messages = list(parser.messages())

def test_zero_length_message_type(tmp_path):
    """Test handling of message type with zero length."""
    log_file = tmp_path / "zero_len.bin"
    with open(log_file, "wb") as f:
        # FMT with length 0
        fmt_header = b"\xa3\x95\x80"
        fmt_data = struct.pack(
            "<BB4s16s64s",
            1, 0,  # length = 0
            b"TST\x00",
            b"B\x00\x00" + b"\x00" * 13,
            b"A\x00" + b"\x00" * 62
        )
        f.write(fmt_header + fmt_data)

    with Parser(str(log_file)) as parser:
        messages = list(parser.messages())

def test_unicode_in_field_names(tmp_path):
    """Test handling of non-ASCII characters in field names."""
    log_file = tmp_path / "unicode.bin"
    with open(log_file, "wb") as f:
        fmt_header = b"\xa3\x95\x80"
        fmt_data = struct.pack(
            "<BB4s16s64s",
            1, 10,
            b"TST\x00",
            b"B\x00\x00" + b"\x00" * 13,
            b"Fi\xc3\xa9ld\x00" + b"\x00" * 57  # "Fiéld"
        )
        f.write(fmt_header + fmt_data)

    with Parser(str(log_file)) as parser:
        messages = list(parser.messages())

def test_chunk_size_edge_cases(valid_log_file):
    """Test chunking with various worker counts."""
    with Parser(valid_log_file) as parser:
        list(parser.messages("FMT"))
        chunks = ParallelParser._split_to_chunks(parser, 1)
        assert len(chunks) == 1

    with Parser(valid_log_file) as parser:
        list(parser.messages("FMT"))
        chunks = ParallelParser._split_to_chunks(parser, 100)
        assert len(chunks) >= 1
