import math
import pytest


def equal_ignore_nan(value_a, value_b):
    """Compare two values, treating NaN as equal."""
    if isinstance(value_a, float) and isinstance(value_b, float):
        if math.isnan(value_a) and math.isnan(value_b):
            return True
    return value_a == value_b


def dicts_equal(dict_a, dict_b):
    """Check if two dicts are equal (key set and values), ignoring NaN differences."""
    if dict_a.keys() != dict_b.keys():
        return False
    for key in dict_a:
        if not equal_ignore_nan(dict_a[key], dict_b[key]):
            return False
    return True


def compare_parser_outputs(reference_messages, test_messages, reference_name="Reference", test_name="Test"):
    """Compare two lists of parsed messages for equality."""
    if len(test_messages) != len(reference_messages):
        pytest.fail(
            f"Message count mismatch: {test_name}={len(test_messages)}, {reference_name}={len(reference_messages)}"
        )

    for index, (test_msg, ref_msg) in enumerate(zip(test_messages, reference_messages)):
        if not dicts_equal(test_msg, ref_msg):
            _report_mismatch(index, test_msg, ref_msg, reference_name, test_name)


def _report_mismatch(index, test_msg, ref_msg, reference_name, test_name):
    """Detailed mismatch report when one message differs."""
    details = [f"\nMismatch at index {index}:"]
    for key in sorted(set(test_msg.keys()) | set(ref_msg.keys())):
        if key not in test_msg:
            details.append(f"  Missing in {test_name}: {key}={ref_msg[key]}")
        elif key not in ref_msg:
            details.append(f"  Missing in {reference_name}: {key}={test_msg[key]}")
        elif not equal_ignore_nan(test_msg[key], ref_msg[key]):
            details.append(f"  {key}: {test_name}={test_msg[key]} | {reference_name}={ref_msg[key]}")
    pytest.fail("\n".join(details))
