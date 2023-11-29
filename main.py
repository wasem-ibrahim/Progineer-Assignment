import logging
import logging.config

from argument_parser import ArgumentParser
from csv_processor import CSVProcessor
from data_analyzer import DataAnalyzer
from file_splitter import FileSplitter
from logger_configurator import LoggerConfigurator


def confirm_file_usage(default_file: str) -> bool:
    """
    Asks the user to confirm the use of the default file if no file path is provided.

    Args:
        default_file (str): The default file path to be used if the user does not specify a file.

    Returns:
        bool: True if the user confirms the usage of the default file, False otherwise.
    """
    confirm = input(
        f"No file specified. The default file '{default_file}' will be used. Continue? (Y/n): ")
    return confirm.lower() in ['y', 'yes', '']


def main():
    # Instantiate and parse arguments
    arg_parser = ArgumentParser()
    args = arg_parser.parse_arguments()

    # Set up logging
    logger_configurator = LoggerConfigurator(
        "logging_config.json", args.log_file, args.verbose)
    logger_configurator.setup_logging()

    try:
        # Confirm file usage if the default file is specified
        if args.file == 'datafile.csv' and not confirm_file_usage(args.file):
            print("Operation cancelled.")
            return

        # Process CSV file
        csv_processor = CSVProcessor(args.file)
        column_names = csv_processor.read_and_validate_header()
        data_section = csv_processor.read_data()
        df = csv_processor.convert_to_dataframe(data_section, column_names)
        processed_df = csv_processor.process_dataframe(df)

        # Validate arguments
        arg_parser.validate_args(args, column_names)

    except ValueError as e:
        logging.error(f"Validation Error: {e}")
        return
    except FileNotFoundError as e:
        logging.error(f"File Not Found: {e}")
        return
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return

    # Perform data analysis (if specified)
    try:
        if args.analyze:
            data_analyzer = DataAnalyzer(processed_df)
            data_analyzer.display_basic_information()
            data_analyzer.display_value_counts_and_frequencies()
            data_analyzer.calculate_and_display_estimated_chunks(
                args.chunk_size)
            data_analyzer.display_missing_data_analysis()

            if input("Proceed with file splitting? (Y/n): ").lower() != 'y':
                print("Operation cancelled.")
                return
    except Exception as e:
        logging.error(f"Error during data analysis: {e}")
        return

    # Split and save files
    try:
        file_splitter = FileSplitter(
            processed_df, args.column_index - 1, args.output, args.chunk_size, args.name_pattern)

        # Split and save files using threading if specified
        if args.threading:
            file_splitter.split_and_save_threaded()

        # Split and save files without threading
        else:
            file_splitter.split_and_save()
    except Exception as e:
        logging.error(f"Error during file splitting: {e}")


if __name__ == "__main__":
    main()
