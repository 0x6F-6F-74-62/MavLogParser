import json



with open("config.json", "r") as f:
    config_data = json.load(f)


MSG_HEADER = bytes.fromhex(config_data.get("MSG_HEADER"))
FORMAT_MSG_TYPE = config_data.get("FORMAT_MSG_TYPE")
FORMAT_MSG_LENGTH = config_data.get("FORMAT_MSG_LENGTH")
FORMAT_MAPPING = config_data.get("FORMAT_MAPPING")
SCALE_FACTOR_FIELDS = set(config_data.get("SCALE_FACTOR_FIELDS"))
LATITUDE_LONGITUDE_FORMAT = config_data.get("LATITUDE_LONGITUDE_FORMAT")
BYTES_FIELDS = set(config_data.get("BYTES_FIELDS"))
FMT_STRUCT = config_data.get("FMT_STRUCT")



# MSG_HEADER = b"\xa3\x95"
# FORMAT_MSG_TYPE = 0x80
# FORMAT_MSG_LENGTH = 89
# FORMAT_MAPPING = {
#     "a": "32h",
#     "b": "b",
#     "B": "B",
#     "h": "h",
#     "H": "H",
#     "i": "i",
#     "I": "I",
#     "f": "f",
#     "d": "d",
#     "n": "4s",
#     "N": "16s",
#     "Z": "64s",
#     "c": "h",
#     "C": "H",
#     "e": "i",
#     "E": "I",
#     "L": "i",
#     "M": "B",
#     "q": "q",
#     "Q": "Q",
# }
# SCALE_FACTOR_FIELDS = {"c", "C", "e", "E"}
# LATITUDE_LONGITUDE_FORMAT = "L"
# BYTES_FIELDS = {"Data", "Blob", "Payload"}
# FMT_STRUCT = "<2sBBB4s16s64s"