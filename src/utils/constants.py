import json
from pathlib import Path

config_path = Path(__file__).resolve().parent.parent.parent / "config.json"

with open(config_path, "r", encoding="utf-8") as f:
    config_data = json.load(f)


MSG_HEADER = bytes.fromhex(config_data.get("MSG_HEADER"))
FORMAT_MSG_TYPE = config_data.get("FORMAT_MSG_TYPE")
FORMAT_MSG_LENGTH = config_data.get("FORMAT_MSG_LENGTH")
FORMAT_MAPPING = config_data.get("FORMAT_MAPPING")
SCALE_FACTOR_FIELDS = set(config_data.get("SCALE_FACTOR_FIELDS"))
LATITUDE_LONGITUDE_FORMAT = config_data.get("LATITUDE_LONGITUDE_FORMAT")
BYTES_FIELDS = set(config_data.get("BYTES_FIELDS"))
FMT_STRUCT = config_data.get("FMT_STRUCT")
