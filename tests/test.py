if __name__ == "__main__":
    from src.business_logic import ParallelParser
    from pymavlink import mavutil
    path = r"/Users/shlomo/Downloads/log_file_test_01.bin"
    pyma=[]
    log = mavutil.mavlink_connection(path,dialect="ardupilotmega")
    while True:
        msg = log.recv_match()  # חסימה – לא מפספסים הודעות
        if msg is None:
            break
        pyma.append(msg.to_dict())

    counter =0
    processor = ParallelParser(path, executor_type="process", max_workers=20)
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