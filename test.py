from platform import processor

from pymavlink import mavutil
from src.bin_parser.parser import Parser
from src.bin_parser.parallel_parser import ParallelParser
if __name__ == "__main__":
    path = r"/Users/shlomo/Downloads/log_file_test_01.bin"
    pyma=[]
    log = mavutil.mavlink_connection(r"/Users/shlomo/Downloads/log_file_test_01.bin",dialect="ardupilotmega")
    while True:
        msg = log.recv_match()  # חסימה – לא מפספסים הודעות
        if msg is None:
            break
        pyma.append(msg.to_dict())

    counter =0
    processor = ParallelParser(r"/Users/shlomo/Downloads/log_file_test_01.bin")
    my_messages = processor.process_all()
    # processor = Parser(path)
    # with processor as p:
    #     my_messages = p.get_all_messages()
    binr=my_messages

    assert len(pyma)==len(binr)
    for i in range(len(pyma)):
        if pyma[i]!=binr[i]:
            counter +=1
            print(f"Difference at message {i}:")
            print("pymavlink:", pyma[i])
            print("BinReader:", binr[i])
    print(counter)
