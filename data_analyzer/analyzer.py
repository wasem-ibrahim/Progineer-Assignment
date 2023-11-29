import logging

import pandas as pd


class DataAnalyzer:
    """
    Contains methods for analyzing data in a pandas DataFrame, including displaying basic 
    information, value counts, frequencies, estimated chunks for splitting, and missing data analysis.

    The DataAnalyzer provides a suite of analytical tools to gain insights from a given DataFrame. 
    It is designed to be flexible and useful for a variety of data analysis tasks.
    """

    def __init__(self, df: pd.DataFrame):
        """
        Initializes the DataAnalyzer with a pandas DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to be analyzed.
        """
        self.df = df

    def display_basic_information(self) -> None:
        """
        Displays basic information about the DataFrame, such as total rows, columns, and the first few rows.
        """
        print("Basic Information:")
        print("-------------------")
        print(f"Total Rows: {self.df.shape[0]}")
        print(f"Total Columns: {self.df.shape[1]}")
        print("\nFirst 5 Rows:")
        print(self.df.head())

    def display_value_counts_and_frequencies(self) -> None:
        """
        Displays the value counts and frequencies for each column in the DataFrame.
        """
        try:
            print("\nValue Counts and Frequencies for Each Column:")
            print("---------------------------------------------")
            for col in self.df.columns:
                value_counts = self.df[col].value_counts()
                print(f"\nColumn: {col}")
                print("Unique Values:", value_counts.count())
                print("Top 5 Values:\n", value_counts.head(5))
        except Exception as e:
            logging.error(f"Error analyzing data frequencies: {e}")
            raise

    def calculate_and_display_estimated_chunks(self, chunk_size: int) -> None:
        """
        Calculates and displays the estimated number of chunks for the top 5 values in each column, 
        based on a specified chunk size.

        Args:
            chunk_size (int): The number of rows per chunk.
        """
        print("\nEstimated Number of Chunks for Top 5 Values in Each Column:")
        print("------------------------------------------------------------")
        for col in self.df.columns:
            value_counts = self.df[col].value_counts()
            for value, count in value_counts.head(5).items():
                estimated_chunks = -(-count // chunk_size)  # Ceiling division
                print(
                    f"Column '{col}', Value '{value}': {estimated_chunks} chunk(s)")

    def display_missing_data_analysis(self) -> None:
        """
        Analyzes and displays the count of missing (null) values in each column of the DataFrame.
        """
        print("\nMissing Data Analysis:")
        print("----------------------")
        print(self.df.isnull().sum())
