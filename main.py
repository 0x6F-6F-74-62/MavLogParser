import json

from src.business_logic.parallel import ParallelParser


with open("config.json", "r", encoding="utf-8") as f:
    CONFIG = json.load(f)
    FILE_PATH = CONFIG["LOG_FILE_PATH"]





def main():
    parser = ParallelParser(FILE_PATH)
    parser.process_all()


if __name__ == "__main__":
    main()

