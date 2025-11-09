from src.business_logic.parser import Parser
from src.business_logic.parallel import ParallelParser
from tests.test_utils import compare_parser_outputs




def test_parser_matches_pymavlink(log_file_path, reference_messages):
    with Parser(log_file_path) as parser:
        parser_msgs = parser.get_all_messages()
    compare_parser_outputs(reference_messages, parser_msgs, "Pymavlink", "Parser")


def test_process_parser_matches_pymavlink(log_file_path, reference_messages):
    parser = ParallelParser(log_file_path)
    compare_parser_outputs(reference_messages, parser.process_all(executor_type="process"), "Pymavlink", "ProcessParser")


def test_thread_parser_matches_pymavlink(log_file_path, reference_messages):
    parser = ParallelParser(log_file_path)
    compare_parser_outputs(reference_messages, parser.process_all(executor_type="thread"), "Pymavlink", "ThreadParser")
