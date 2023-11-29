import logging
from io import StringIO

import pandas as pd


class CSVProcessor:
    """
    This class handles operations related to processing CSV files, including reading headers, 
    extracting data, and converting it to a pandas DataFrame.

    The CSVProcessor provides an interface for interacting with CSV files, making it easier to
    extract and manipulate data in a structured and efficient manner.

    """

    def __init__(self, file_path: str):
        """
        Initializes the CSVProcessor with the path to a CSV file.

        Args:
            file_path (str): The path to the CSV file to be processed.
        """
        self.file_path = file_path

    def read_and_validate_header(self) -> list[str]:
        """
        Reads the header of the CSV file, extracts column names, and validates the number of columns.

        Returns:
            list[str]: A list of column names extracted from the header section.

        Raises:
            ValueError: If the number of columns does not match the header declaration.
        """
        header_section = self.read_header()
        num_columns = self.get_num_columns(header_section)
        column_names = self.get_column_names(header_section)

        if len(column_names) != num_columns:
            raise ValueError(
                "Number of columns does not match the header declaration.")

        return column_names

    def read_header(self) -> list[str]:
        """
        Reads the header section of the CSV file.

        Returns:
            List[str]: A list of strings representing the lines in the header section.
        """
        try:
            with open(self.file_path, "r") as file:
                header_section = []
                for line in file:
                    if line.strip() == "</HEADER>":
                        break
                    header_section.append(line)
            return header_section
        except FileNotFoundError:
            logging.error(f"File not found: {self.file_path}")
            raise
        except Exception as e:
            logging.error(
                f"Error reading header from file {self.file_path}: {e}")
            raise

    def read_data(self) -> list[str]:
        """
        Reads the data section of the CSV file.

        Returns:
            List[str]: A list of strings representing the lines in the data section.
        """
        try:
            data_section = []
            with open(self.file_path, "r") as file:
                in_data_section = False
                for line in file:
                    if line.strip() == "<BOD>":
                        in_data_section = True
                        continue
                    elif in_data_section:
                        if not line.strip() or line.startswith("<"):
                            continue
                        data_section.append(line.strip())
            return data_section
        except FileNotFoundError:
            logging.error(f"File not found: {self.file_path}")
            raise
        except Exception as e:
            logging.error(
                f"Error reading data from file {self.file_path}: {e}")
            raise

    def convert_to_dataframe(self, data_section: list[str], column_names: list[str]) -> pd.DataFrame:
        """
        Converts a list of data lines into a pandas DataFrame.

        Args:
            data_section (List[str]): A list of strings representing the lines in the data section.
            column_names (List[str]): A list of column names for the DataFrame.

        Returns:
            pd.DataFrame: A DataFrame constructed from the data section.
        """
        data_str = "\n".join(data_section)
        return pd.read_csv(
            StringIO(data_str),
            header=None,
            delimiter=";",
            names=column_names,
            usecols=range(len(column_names)),
        )

    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Processes a DataFrame by replacing None with "NA" and dropping duplicates.

        Args:
            df (pd.DataFrame): The DataFrame to process.

        Returns:
            pd.DataFrame: The processed DataFrame.
        """
        df.replace({None: "NA"}, inplace=True)
        return df.drop_duplicates()

    @staticmethod
    def get_num_columns(header_section: list[str]) -> int:
        """
        Retrieves the number of columns from the header section of the CSV file.

        Args:
            header_section (List[str]): A list of strings representing the lines in the header section.

        Returns:
            int: The number of columns as specified in the header section.
        """
        if len(header_section) < 4:
            raise ValueError("Incomplete header format.")
        return int(header_section[2].split("=")[1].strip())

    @staticmethod
    def get_column_names(header_section: list[str]) -> list[str]:
        """
        Extracts the column names from the header section of the CSV file.

        Args:
            header_section (List[str]): A list of strings representing the lines in the header section.

        Returns:
            List[str]: A list of column names extracted from the header section.
        """
        column_names = header_section[3].split(
            "=")[1].strip().strip("[]").split(",")
        return column_names
