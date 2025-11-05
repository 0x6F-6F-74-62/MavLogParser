import pytest
import json
from business_logic.parser import Parser
from business_logic.mavlink import Mavlink
from business_logic.parallel import ParallelParser
from tests.test_utils import compare_parser_outputs


@pytest.fixture(scope="module")
def log_file_path():
    """Load log path once per module."""
    with open("config.json", "r") as f:
        return json.load(f)["LOG_FILE_PATH"]


@pytest.fixture(scope="module")
def reference_messages(log_file_path):
    """Reference output from pymavlink."""
    with Mavlink(log_file_path) as mav:
        return mav.get_all_messages()


def test_parser_matches_pymavlink(log_file_path, reference_messages):
    with Parser(log_file_path) as parser:
        parser_msgs = parser.get_all_messages()
    compare_parser_outputs(reference_messages, parser_msgs, "Pymavlink", "Parser")


def test_process_parser_matches_pymavlink(log_file_path, reference_messages):
    parser = ParallelParser(log_file_path, executor_type="process")
    compare_parser_outputs(reference_messages, parser.process_all(), "Pymavlink", "ProcessParser")


def test_thread_parser_matches_pymavlink(log_file_path, reference_messages):
    parser = ParallelParser(log_file_path, executor_type="thread")
    compare_parser_outputs(reference_messages, parser.process_all(), "Pymavlink", "ThreadParser")
