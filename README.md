# MAVLink Binary Log Parser

A Python module for parsing MAVLink Binary Log (`.BIN`) files with support for serial and parallel processing.

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Detailed Guide](#detailed-guide)
- [Configuration](#configuration)
- [Testing](#testing)
---

## Overview

This module provides three approaches for parsing MAVLink Binary Log files:

1. **`Parser`** - Efficient basic parser using memory-mapping (mmap)
2. **`ParallelParser`** - Parallel parser with multiprocessing or multithreading support
3. **`PymavlinkParser`** - Wrapper for the official pymavlink library

---

## Project Structure

```
project/
├── business_logic/
│   ├── bin_parser/
│   │   ├── parser.py           # Basic parser with mmap
│   │   ├── parallel.py         # Parallel parser
│   │   └── mavlink.py          # pymavlink wrapper
│   └── utils/
│       ├── constants.py        # MAVLink constants
│       └── logger.py           # Logging system
├── config.json                 # MAVLink configuration
└── logs/                       # Log directory
```

---

## Installation

### Prerequisites

```bash
pip install pymavlink
```

---

## Quick Start

### Quick Example - Parser

```python
from business_logic.parser import Parser

# Read all messages from file
with Parser("flight_log.BIN") as parser:
    messages = parser.get_all_messages()
    print(f"Total messages: {len(messages)}")

# Filter by message type
with Parser("flight_log.BIN") as parser:
    gps_messages = parser.get_all_messages(message_type="GPS")
    for msg in gps_messages:
        print(f"Lat: {msg['Lat']}, Lon: {msg['Lng']}")
```

### Quick Example - ParallelParser

```python
from business_logic.parallel import ParallelParser

# Read all messages from file
parser = ParallelParser("large_flight_log.BIN", executor_type="process")
messages = parser.process_all()
print(f"Parsed {len(messages)} messages")

# Filter by message type
parser = ParallelParser("large_flight_log.BIN", executor_type="process")
messages = parser.process_all(message_type="IMU")
print(f"Parsed {len(messages)} IMU messages")
```

### Quick Example - PymavlinkParser

```python
from business_logic.mavlink import Mavlink

with Mavlink("flight_log.BIN") as parser:
    # Using generator (memory efficient)
    for msg in parser.messages(message_type="ATTITUDE"):
        print(f"Roll: {msg['Roll']}, Pitch: {msg['Pitch']}")
```

---

## Detailed Guide

### Parser - Basic Parser

#### Key Features

- **Memory-mapped I/O**: Reads file directly from disk without unnecessary copies
- **Generator-based**: Returns messages one at a time (memory efficient)
- **Type filtering**: Filter messages by type
- **Context manager**: Automatic resource management

#### API

```python
class Parser:
    def __init__(self, filename: str)
    
    # Use as context manager
    def __enter__(self) -> "Parser"
    def __exit__(self, *args) -> None
    
    # Generator - returns one message at a time
    def messages(
        self, 
        message_type: Optional[str] = None,
        end_index: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]
    
    # Returns list of all messages
    def get_all_messages(
        self, 
        message_type: Optional[str] = None
    ) -> List[Dict[str, Any]]
```

#### Usage Examples

**1. Read all messages**
```python
with Parser("log.BIN") as parser:
    all_messages = parser.get_all_messages()
```

**2. Filter by message type**
```python
with Parser("log.BIN") as parser:
    # GPS messages only
    gps_data = parser.get_all_messages(message_type="GPS")
    
    # IMU messages only
    imu_data = parser.get_all_messages(message_type="IMU")
```

**3. Using generator (memory efficient)**
```python
with Parser("log.BIN") as parser:
    for message in parser.messages(message_type="BARO"):
        altitude = message.get("Alt", 0)
        if altitude > 1000:
            print(f"High altitude detected: {altitude}m")
```

**4. Partial file processing**
```python
with Parser("log.BIN") as parser:
    # Read only first 1MB
    for msg in parser.messages(end_index=1024*1024):
        process_message(msg)
```

---

### ParallelParser - Parallel Parser

#### Key Features

- **Multiprocessing/Multithreading**: Full utilization of multiple CPUs
- **Automatic chunking**: Automatic file splitting into aligned chunks
- **Configurable workers**: Control over number of processes/threads
- **Message alignment**: Ensures correct splitting between messages

#### API

```python
class ParallelParser:
    def __init__(
        self,
        filename: str,
        executor_type: Literal["process", "thread"] = "process",
        max_workers: Optional[int] = None
    )
    
    # Process entire file in parallel
    def process_all(
        self, 
        message_type: Optional[str] = None
    ) -> List[Dict[str, Any]]
```

#### Parameters

- **`executor_type`**: 
  - `"process"` (default) - multiprocessing, recommended for large files
  - `"thread"` - multithreading, recommended when I/O bottleneck exists
  
- **`max_workers`**: 
  - `None` (default) - number of CPUs in system (process) or 16 (thread)
  - Custom number

#### Usage Examples

**1. Basic usage**
```python
parser = ParallelParser("large_log.BIN")
messages = parser.process_all()
```

**2. Configure number of workers**
```python
# Use 8 processes
parser = ParallelParser("log.BIN", max_workers=8)
messages = parser.process_all()
```

**3. Choose executor type**
```python
# Threading instead of multiprocessing
parser = ParallelParser("log.BIN", executor_type="thread", max_workers=16)
messages = parser.process_all(message_type="GPS")
```

**4. Custom processing**
```python
# For huge files (>2GB)
parser = ParallelParser(
    "huge_log.BIN",
    executor_type="process",
    max_workers=16  # Aggressive CPU usage
)
imu_data = parser.process_all(message_type="IMU")
```

#### How It Works

1. **File splitting**: File is divided into chunks aligned to message boundaries
2. **Parallel processing**: Each chunk is processed in a separate process/thread
3. **Result merging**: Messages are collected and merged into a single list
4. **Order preservation**: Messages appear in original chronological order

---

### PymavlinkParser - Pymavlink Wrapper

#### Key Features

- **Full compatibility** with pymavlink
- **Simple API** similar to Parser
- **Official support** for all message types

#### API

```python
class PymavlinkParser:
    def __init__(self, filename: str)
    
    def __enter__(self) -> "PymavlinkParser"
    def __exit__(self, *args) -> None
    
    def messages(
        self, 
        message_type: Optional[str] = None
    ) -> Iterator[Dict[str, Any]]
    
    def get_all_messages(
        self, 
        message_type: Optional[str] = None
    ) -> List[Dict[str, Any]]
```

#### Usage Examples

```python
with PymavlinkParser("log.BIN") as parser:
    # Generator
    for msg in parser.messages(message_type="GLOBAL_POSITION_INT"):
        print(msg)
    
    # Full list
    all_msgs = parser.get_all_messages()
```

---

## Configuration

### config.json File

The module uses `config.json` to define MAVLink constants:

```json
{
  "MSG_HEADER": "a395",
  "FORMAT_MSG_TYPE": 128,
  "FORMAT_MSG_LENGTH": 89,
  "FORMAT_MAPPING": {
    "b": "b",
    "B": "B",
    "h": "h",
    "H": "H",
    "i": "i",
    "I": "I",
    "f": "f",
    "d": "d",
    "L": "i",
    ...
  },
  "SCALE_FACTOR_FIELDS": ["c", "C", "e", "E"],
  "LATITUDE_LONGITUDE_FORMAT": "L",
  "BYTES_FIELDS": ["Data", "Blob", "Payload"],
  "FMT_STRUCT": "<2sBBB4s16s64s"
}
```

### Important Settings

- **`MSG_HEADER`**: Unique identifier for message start (hex)
- **`FORMAT_MAPPING`**: Mapping between format characters and struct types
- **`SCALE_FACTOR_FIELDS`**: Fields requiring division by 100
- **`LATITUDE_LONGITUDE_FORMAT`**: Fields requiring division by 10^7

---

## Message Structure

Each message is returned as a dictionary with the following structure:

```python
{
    "mavpackettype": "GPS",    # Message type
    "TimeUS": 1234567890,      # Timestamp (microseconds)
    "Lat": 32.0853,            # Latitude
    "Lng": 34.7818,            # Longitude
    "Alt": 50.5,               # Altitude
    ...                        # Additional fields by message type
}
```
---

## Testing

The module includes comprehensive tests that validate parser accuracy by comparing results against the official pymavlink library.

### Test Files

- **`test.py`** - Tests for the basic Parser, ParallelParser with both executor types
- **`test_utils`** - Utility functions for testing

### Running Tests

```bash
# Run all tests
python -m pytest tests/test.py 

# Run with verbose output
python -m pytest tests/test.py -v

# Run with detailed output
python -m pytest tests/test.py -s
```

### Test Configuration

Tests use `config.json` for the log file path:

```json
{
  "LOG_FILE_PATH": "path/to/your/test_log.BIN",
  ...
}
```

Make sure to set a valid `.BIN` file path before running tests.

### What the Tests Validate

The tests perform comprehensive validation:

1. **Message Count**: Ensures parser extracts the exact same number of messages as pymavlink
2. **Message Content**: Validates every field in every message matches pymavlink output
3. **Field Names**: Checks all keys/field names are identical
4. **Field Values**: Compares all values with special handling for:
   - **NaN values**: Treats `NaN == NaN` as `True` (mathematically different but logically equivalent)
   - **All data types**: Strings, integers, floats, bytes
5. **Message Order**: Confirms messages are returned in chronological order


