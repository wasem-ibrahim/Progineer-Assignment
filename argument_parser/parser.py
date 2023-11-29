
import argparse
from argparse import Namespace


class ArgumentParser:
    """
    Handles the parsing of command line arguments for a script. This class simplifies the setup and 
    retrieval of command line options and flags.

    The ArgumentParser provides a structured way to define and parse command line arguments, 
    making scripts more user-friendly and configurable.
    """

    def __init__(self):
        """
        Initializes an ArgumentParser instance with descriptions and setups of the expected arguments.
        """
        self.parser = argparse.ArgumentParser(
            description="""Split a CSV file into many files based on a specified 
            column number, with options for custom output file naming and logging."""
        )
        self._setup_arguments()

    def _setup_arguments(self) -> None:
        """
        Defines and adds all the necessary command line arguments to the parser.
        """
        self.parser.add_argument(
            "column_index",
            type=int,
            help="The index of the column to split the file by (1-based index).",
        )
        self.parser.add_argument(
            "-f", "--file",
            type=str,
            default="datafile.csv",
            help="Path to the CSV file to split. Default is datafile.csv.",
        )
        self.parser.add_argument(
            "-o", "--output",
            type=str,
            default="split_output",
            help="Output folder for the split files. Default is split_output.",
        )
        self.parser.add_argument(
            "-c", "--chunk_size",
            type=int,
            default=300,
            help="Number of rows per split file. Default is 300.",
        )
        self.parser.add_argument(
            "-n", "--name_pattern",
            type=str,
            default="datafile_{key}_{index}",
            help="Pattern for naming output files. Use {key} for column value and {index} for file index.",
        )
        self.parser.add_argument(
            "-v", "--verbose",
            action="store_true",
            help="Enable verbose output."
        )
        self.parser.add_argument(
            "-l", "--log_file",
            type=str,
            default="process.log",
            help="Path to the log file. Default is process.log.",
        )
        self.parser.add_argument(
            "-a", "--analyze",
            action="store_true",
            help="Perform data analysis before file splitting."
        )
        self.parser.add_argument(
            "-t", "--threading",
            action="store_true",
            help="Enable multi-threaded processing for faster execution."
        )

    def parse_arguments(self) -> Namespace:
        """
        Parses the command line arguments provided to the script.

        Returns:
            Namespace: An object containing all the parsed command line arguments.
        """
        return self.parser.parse_args()

    def validate_args(self, args: Namespace, column_names: list[str]) -> None:
        """
        Validates the command line arguments provided to the script.

        Args:
            args (Namespace): An object containing all the parsed command line arguments.
            column_names (list[str]): A list of column names to validate against.

        Raises:
            ValueError: If the column index is out of range or the chunk size is invalid.
        """
        if args.column_index < 1 or args.column_index > len(column_names):
            raise ValueError(
                f"Column index {args.column_index} is out of range. The file has {len(column_names)} columns.")

        if args.chunk_size < 1:
            raise ValueError(
                f"Chunk size {args.chunk_size} is invalid. Must be a positive integer.")
