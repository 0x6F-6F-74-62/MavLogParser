

from pymavlink import mavutil
from src.bin_parser.parallel_parser import MultiprocessParser

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
    processor = MultiprocessParser(r"/Users/shlomo/Downloads/log_file_test_01.bin")
    my_messages = processor.process_all()
    binr=my_messages
    # with ReaderProcess(path,like_pymav=True) as reader:
    #     reader.parse_fmt_messages()
    #     msgs = reader.parse_messages()
    # binr=msgs
    print(len(binr))
    assert len(pyma)==len(binr)
    for i in range(len(pyma)):
        if pyma[i]!=binr[i]:
            counter +=1
            print(f"Difference at message {i}:")
            print("pymavlink:", pyma[i])
            print("BinReader:", binr[i])
            break
    print(counter)