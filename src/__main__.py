import sys

from src.business_logic.cli_menu import CLIMenu


def main() -> None:
    """Main entry point."""
    try:
        cli = CLIMenu()
        cli.run_menu()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
