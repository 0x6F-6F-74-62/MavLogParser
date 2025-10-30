
from src.synchronous_parser.mavlog_parser import MavlogParser
from pymavlink import mavutil
import math



def mavlink_parser(file_path, type=None):
    try:
        file_reader = mavutil.mavlink_connection(file_path, dialect="ardupilotmega")
    except Exception as e:
        raise RuntimeError(f"Failed to open MAVLink file: {e}") from e

    mssagess = []

    try:

        while True:
            msg = file_reader.recv_match(type=type, blocking=False)
            
            if msg is None:
                break

            msg_to_dict = msg.to_dict()
            mssagess.append(msg_to_dict)
    except Exception as e:
        raise RuntimeError(f"Error occurred while reading MAVLink file: {e}") from e


    return mssagess


def dicts_equal_ignore_nan(d1, d2):
    if d1.keys() != d2.keys():
        return False
    for k in d1:
        v1, v2 = d1[k], d2[k]
        if isinstance(v1, float) and isinstance(v2, float):
            if math.isnan(v1) and math.isnan(v2):
                continue
        if v1 != v2:
            return False
    return True



file_path = r"C:\Users\ootb\Downloads\log_file_test_01.bin"
mav_lib = mavlink_parser(file_path)
with MavlogParser(file_path) as parser:

    my_lib_data = parser.get_all_messages()

    for i in range(len(my_lib_data)):
        if dicts_equal_ignore_nan(my_lib_data[i], mav_lib[i]):
            print(i)
            continue
        else:
            print(f"Error in msg {i}")
            print("Un match")
            print(f"Mavlink: {mav_lib[i]}\n")
            print("--------")
            print(f"Parser: {my_lib_data[i]}")
