
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import pandas as pd
from tqdm import tqdm


class FileSplitter:
    """
    Manages the splitting of a DataFrame into multiple files based on specified criteria, 
    and handles saving these files either in single-threaded or multi-threaded mode.

    The FileSplitter is useful for efficiently handling large datasets by splitting them into more 
    manageable chunks and saving them as separate files. It supports customization of the 
    splitting process based on column indices, output file naming patterns, and chunk sizes.
    """

    def __init__(self, df: pd.DataFrame, split_column_index: int, output_folder: str, chunk_size: int, filename_pattern: str):
        """
        Initializes the FileSplitter with the DataFrame and splitting criteria.

        Args:
            df (pd.DataFrame): The DataFrame to be split.
            split_column_index (int): The index of the column used for splitting the DataFrame.
            output_folder (str): The directory where split files will be saved.
            chunk_size (int): The number of rows per split file.
            filename_pattern (str): The pattern to be used for naming output files.
        """
        self.df = df
        self.split_column_index = split_column_index
        self.output_folder = output_folder
        self.chunk_size = chunk_size
        self.filename_pattern = filename_pattern

    def prepare_file_writing_tasks(self) -> list[tuple[pd.DataFrame, str]]:
        """
        Prepares tasks for file writing by splitting the DataFrame into subgroups.

        Returns:
            List[Tuple[pd.DataFrame, str]]: A list of tuples containing the subgroup DataFrame 
            and its corresponding output file name.
        """
        os.makedirs(self.output_folder, exist_ok=True)

        tasks = []
        for key, group in self.df.groupby(self.df.columns[self.split_column_index]):
            for group_index in range(0, len(group), self.chunk_size):
                sub_group = group.iloc[group_index:group_index +
                                       self.chunk_size]
                output_file_name = f"{self.output_folder}/{self.filename_pattern.format(key=key, index=group_index // self.chunk_size + 1)}.csv"
                tasks.append((sub_group, output_file_name))
        return tasks

    def split_and_save(self):
        """
        Splits the DataFrame and saves the files using a single-threaded approach.
        """
        tasks = self.prepare_file_writing_tasks()

        progress_bar = tqdm(
            total=len(tasks), desc="Splitting Process", unit="file")

        for sub_group, output_file_name in tasks:
            try:
                sub_group.to_csv(output_file_name, index=False)
                logging.info(f"Saved file: {output_file_name}")
            except IOError as e:
                logging.error(f"Failed to save file {output_file_name}: {e}")
            finally:
                progress_bar.update(1)

        progress_bar.close()

    def split_and_save_threaded(self, max_workers: int = 5):
        """
        Splits the DataFrame into multiple files and saves them using multi-threading.

        Args:
            max_workers (int, optional): The maximum number of threads to use. Default is 5.
        """
        tasks = self.prepare_file_writing_tasks()

        progress_lock = Lock()
        progress_bar = tqdm(
            total=len(tasks), desc="Splitting Process", unit="file")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(
                self.save_subgroup, task[0], task[1], progress_lock, progress_bar): task for task in tasks}
            for future in as_completed(futures):
                task = futures[future]
                try:
                    future.result()  # This will raise any exceptions caught in the save_subgroup method
                except IOError as e:
                    logging.error(f"Failed to save file {task[1]}: {e}")
                except Exception as e:
                    logging.error(
                        f"Unexpected error during saving file {task[1]}: {e}")

        progress_bar.close()

    @staticmethod
    def save_subgroup(sub_group: pd.DataFrame, output_file_name: str, progress_lock: Lock, progress_bar: tqdm):
        """
        Saves a subgroup of the DataFrame to a CSV file. This function is designed to be used with multi-threading.

        Args:
            sub_group (pd.DataFrame): The DataFrame subgroup to be saved.
            output_file_name (str): The name of the output file.
            progress_lock (Lock): A threading lock to synchronize progress bar updates.
            progress_bar (tqdm): A tqdm progress bar instance.
        """
        try:
            sub_group.to_csv(output_file_name, index=False)
            logging.info(f"Saved file: {output_file_name}")
        except IOError as e:
            logging.error(f"Failed to save file {output_file_name}: {e}")
            raise
        except Exception as e:
            logging.error(
                f"Unexpected error during saving file {output_file_name}: {e}")
            raise
        finally:
            with progress_lock:
                progress_bar.update(1)
