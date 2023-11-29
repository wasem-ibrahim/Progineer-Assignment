# SplitByColumn

## Overview

SplitByColumn is a Python application developed to efficiently split CSV files based on the unique values of a user-specified column. This tool is designed to cater to complex data processing needs, including data preprocessing, analysis, multi-threading for performance enhancement, and configurable output settings.

## Features

- **Column-based File Splitting:** Dynamically splits a CSV file into multiple files based on the distinct values of a specified column.
- **Data Preprocessing:** Enhances data quality by replacing missing values with “NA” and eliminating duplicate rows.
- **Data Analysis:** Offers an optional analysis feature providing insights such as value counts, frequencies, and missing data analysis.
- **Multi-threaded Processing:** Utilizes multi-threading to accelerate the file writing process, significantly improving performance for large datasets.
- **Customizable Output:** Users can set the output directory, define the maximum number of rows per file, and customize output file names.
- **Logging:** Comprehensive logging functionality with options for varying verbosity levels and log file specification.

## Requirements

- Python 3.x
- Pandas
- tqdm (for progress bar visualization)

## Installation

No specific installation steps are required other than setting up the Python environment and installing the required packages. Users can clone or download your code repository and install the required dependencies (Pandas and tqdm).

## Usage

Basic command structure:

```bash
python main.py [column_index] [options]
```

Options:

- `-f`, `--file`: Path to the CSV file (default: `datafile.csv`)
- `-o`, `--output`: Output folder (default: `split_output`)
- `-c`, `--chunk_size`: Number of rows per split file (default: 300)
- `-n`, `--name_pattern`: Pattern for naming output files (default: `datafile_{key}_{index}`)
- `-v`, `--verbose`: Enable verbose output
- `-l`, `--log_file`: Log file path (default: `process.log`)
- `-a`, `--analyze`: Perform data analysis before file splitting
- `-t`, `--threading`: Enable multi-threaded processing for faster execution

Example:

```bash
python main.py 3 -f mydata.csv -o output_folder -c 200 -n split_{key}_{index} -v -t
```

## Configuration

Logging can be configured via the `logging_config.json` file. Adjust the log level, output format, and destination as needed for your use case.

## Code Structure Overview

Detailed explanation of the code structure, including descriptions of the main modules: `main.py`, `configurator.py`, `splitter.py`, `analyzer.py`, `processor.py`, and `parser.py`.

### Module Details

- **main.py:** Orchestrates the flow of the program, including setup of logging, parsing arguments, data processing, and invoking the data analysis and file splitting functionalities.
- **configurator.py:** Utilizes JSON configuration for logging setup, allowing dynamic adjustments of logging settings.
- **splitter.py:** Contains the `FileSplitter` class for splitting and saving files, supporting both single-threaded and multi-threaded operations.
- **analyzer.py:** Houses the `DataAnalyzer` class for performing basic data analysis.
- **processor.py:** Implements the `CSVProcessor` class for reading and preprocessing the CSV file.
- **parser.py:** Defines the `ArgumentParser` class for parsing command-line arguments.

### Implementation Notes

- Comprehensive error handling to ensure stability and user-friendly error messages.
- Detailed logging throughout the application for troubleshooting and insights.
- Special attention to data integrity in preprocessing steps.

## Limitations & Known Issues

Currently, there are no known issues or limitations with this application. However, performance may vary depending on the size of the input dataset and the computing resources available.
