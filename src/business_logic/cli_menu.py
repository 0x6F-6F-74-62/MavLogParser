"""Main entry point for MAVLink Binary Log Parser."""
import json
import os
import sys
from typing import List, Dict, Any, Optional, Literal

from src.business_logic.parallel import ParallelParser
from src.business_logic.parser import Parser
from src.utils.logger import setup_logger


class CLIMenu:
    """Command-line interface for MAVLink log parsing."""

    def __init__(self):
        """Initialize the CLI with configuration."""
        self.logger = setup_logger(os.path.basename(__file__))
        self.file_path: Optional[str] = None

    def _get_file_path(self) -> str:
        """Prompt user for file path and validate it."""
        while True:
            file_path = input("Enter file path (or 'q' to quit): ").strip()

            if file_path.lower() == 'q':
                sys.exit(0)

            if not file_path:
                print("Error: File path cannot be empty.")
                continue

            if not os.path.exists(file_path):
                print(f"Error: File '{file_path}' does not exist.")
                continue

            if not os.path.isfile(file_path):
                print(f"Error: '{file_path}' is not a file.")
                continue

            return file_path

    def _get_message_type(self) -> str:
        """Prompt user for message type."""
        return input("Enter message type: ").strip().upper()

    def _parse_synchronous(self, message_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Parse log file synchronously."""
        try:
            with Parser(self.file_path) as parser:
                messages = parser.get_all_messages(message_type)
                self.logger.info(f"Parsed {len(messages):,} messages synchronously")
                return messages
        except Exception as e:
            self.logger.error(f"Synchronous parsing failed: {e}")
            raise

    def _parse_parallel(
            self,
            executor_type: Literal["process", "thread"],
            message_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Parse log file in parallel."""
        try:
            parser = ParallelParser(self.file_path, executor_type=executor_type)
            messages = parser.process_all(message_type)
            self.logger.info(f"Parsed {len(messages):,} messages using {executor_type} pool")
            return messages
        except Exception as e:
            self.logger.error(f"Parallel parsing ({executor_type}) failed: {e}")
            raise

    def _display_main_menu(self) -> None:
        """Display the main menu."""
        print("\n" + "=" * 40)
        print("   MAVLink Log Parser")
        print("=" * 40)
        print("1. Synchronous parsing")
        print("2. Parallel parsing (processes)")
        print("3. Parallel parsing (threads)")
        print("0. Exit")
        print("=" * 40)

    def _display_filter_menu(self) -> None:
        """Display the filter selection menu."""
        print("\n" + "-" * 40)
        print("   Parsing Options")
        print("-" * 40)
        print("1. Parse all messages")
        print("2. Parse specific message type")
        print("0. Back to main menu")
        print("-" * 40)

    def _get_filter_choice(self) -> Optional[bool]:
        """
        Get user's filter choice.
        """
        while True:
            self._display_filter_menu()
            choice = input("Enter your choice: ").strip()

            if choice == "1":
                return False
            elif choice == "2":
                return True
            elif choice == "0":
                return None
            else:
                print("Invalid choice. Please try again.")

    def _handle_parsing_option(
            self,
            parse_method: Literal["sync", "process", "thread"]
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Handle a parsing option selected by the user.
        """
        filter_choice = self._get_filter_choice()

        if filter_choice is None:
            return None

        message_type = self._get_message_type() if filter_choice else None

        try:
            if parse_method == "sync":
                return self._parse_synchronous(message_type)
            elif parse_method == "process":
                return self._parse_parallel("process", message_type)
            elif parse_method == "thread":
                return self._parse_parallel("thread", message_type)
        except Exception as e:
            print(f"\nError during parsing: {e}")
            return None

    def run_menu(self) -> None:
        """Run the main CLI loop."""
        print("\nWelcome to MAVLink Log Parser!")
        self.file_path = self._get_file_path()
        self.logger.info(f"Selected file: {self.file_path}")

        while True:
            self._display_main_menu()
            choice = input("Enter your choice: ").strip()

            if choice == "1":
                messages = self._handle_parsing_option("sync")
                if messages:
                    print(f"\nSuccessfully parsed {len(messages):,} messages")

            elif choice == "2":
                messages = self._handle_parsing_option("process")
                if messages:
                    print(f"\nSuccessfully parsed {len(messages):,} messages")

            elif choice == "3":
                messages = self._handle_parsing_option("thread")
                if messages:
                    print(f"\nSuccessfully parsed {len(messages):,} messages")

            elif choice == "0":
                print("\nExiting. Goodbye!")
                break

            else:
                print("Invalid choice. Please try again.")

