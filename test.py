
from pymavlink import mavutil
from business_logic.bin_parser.parallel import ParallelParser

if __name__ == "__main__":
    path = r"/Users/shlomo/Downloads/log_file_test_01.bin"
    pyma=[]
    log = mavutil.mavlink_connection(path,dialect="ardupilotmega")
    while True:
        msg = log.recv_match()
        if msg is None:
            break
        pyma.append(msg.to_dict())
    counter =0
    processor = ParallelParser(path, executor_type="process")
    my_messages = processor.process_all()
    binr=my_messages

    assert len(pyma)==len(binr)
    for i in range(len(pyma)):
        if pyma[i]!=binr[i]:
            counter +=1
            print(f"Difference at message {i}:")
            print("pymavlink:", pyma[i])
            print("BinReader:", binr[i])
    print(counter)
