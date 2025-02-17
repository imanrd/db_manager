import os
import time
import sqlite3
import pandas as pd
from multiprocessing import Queue, Process
from data_models.model import DataModel
from database.db_creator import DBCreator
from database.db_connector import DBConnector
from data_models.model import load_data


def filter_dataframe(reference_df: pd.DataFrame, target_df: pd.DataFrame, time_window: int = 1) -> pd.DataFrame:
    """
    Filters rows in the target DataFrame that are within a specified time window of the reference DataFrame.
    Args:
        reference_df (pd.DataFrame): DataFrame containing reference time points.
        target_df (pd.DataFrame): DataFrame to be filtered.
        time_window (int): Time window (in minutes) around the reference time points.
    Returns:
        pd.DataFrame: Filtered DataFrame.
    """

    reference_df[DataModel.time_column] = pd.to_datetime(reference_df[DataModel.time_column])

    if DataModel.time_column in target_df.columns:
        candle_time = DataModel.time_column
        minutes = 1
        time_format = '%Y-%m-%d %H:%M:%S.%f'

    elif DataModel.time_column in target_df.columns:
        return target_df
    elif 'RELEASE_TIME' in target_df.columns:
        return target_df
    else:
        raise ValueError(
            "Neither 'DataModel.time_column' nor 'DataModel.time_column' and 'RELEASE_TIME' column found in the "
            "DataFrame")

    target_df[candle_time] = pd.to_datetime(target_df[candle_time], format=time_format, errors='coerce')
    target_df = target_df.dropna(subset=[candle_time]).reset_index(drop=True)

    reference_df['start_time'] = reference_df[DataModel.time_column] - pd.Timedelta(minutes=minutes)
    reference_df['end_time'] = reference_df[DataModel.time_column] + pd.Timedelta(minutes=minutes)

    merged_df = pd.merge_asof(
        target_df.sort_values(candle_time),
        reference_df[['start_time', 'end_time']].sort_values('start_time'),
        left_on=candle_time,
        right_on='start_time'
    )

    filtered_candles = merged_df[(merged_df[candle_time] >= merged_df['start_time']) &
                                 (merged_df[candle_time] <= merged_df['end_time'])]

    return filtered_candles.drop(columns=['start_time', 'end_time']).reset_index(drop=True)


class DataFrameChunkWriter:
    """
    Handles processing large dataframes in chunks and storing them into a database.
    """

    def __init__(self, name: str, reference_df: pd.DataFrame, queue: Queue):
        """
        Initialize the DataFrameChunkWriter.
        Args:
            name (str): Name of the database.
            reference_df (pd.DataFrame): Reference DataFrame for filtering.
            queue (Queue): Multiprocessing queue for communication.
        """
        self.db_name = name
        self.queue = queue
        self.news = reference_df

    def process_file(self, file_table: tuple) -> None:
        """
        Process CSV file in chunks, filter the data, and put the chunks into the queue.
        Args:
            file_table (tuple): Tuple containing file path and table name.
        """
        """ Process CSV file in chunks and filter around times. """
        chunk_size = 100000
        file_path, table = file_table
        try:
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                filtered_chunk = filter_dataframe(self.news, chunk)
                self.queue.put((filtered_chunk, table))
        except FileNotFoundError:
            print(f'File for {table} Table was not found.')

    def write_to_db(self) -> None:
        """
        Write chunks of data from the queue into the database.
        Retries on failure and closes the connection when finished.
        """
        cnx = sqlite3.connect(f"{self.db_name}.db")
        while True:
            chunk, table_name = self.queue.get()
            if chunk is None:
                break
            retry_count = 5
            while retry_count > 0:
                try:
                    self._prepare_chunk_for_db(chunk)
                    chunk.to_sql(table_name, cnx, if_exists='append', index=True)
                    break
                except sqlite3.OperationalError:
                    retry_count -= 1
                    time.sleep(1)
            del chunk
        cnx.close()

    @staticmethod
    def _prepare_chunk_for_db(chunk: pd.DataFrame) -> None:
        """
        Prepare chunk for database insertion by formatting time columns.
        Args:
            chunk (pd.DataFrame): Data chunk to be prepared.
        """
        """ Prepare chunk for database insertion by formatting time columns. """
        if DataModel.time_column in chunk.columns:
            DataFrameChunkWriter.format_time_column(chunk, DataModel.time_column, '%Y-%m-%d %H:%M:%S.%f')
            chunk.set_index(DataModel.time_column, inplace=True)
        elif DataModel.time_column in chunk.columns:
            DataFrameChunkWriter.format_time_column(chunk, DataModel.time_column, '%d.%m.%Y %H:%M:%S.%f')
            chunk.set_index(DataModel.time_column, inplace=True)
        elif 'RELEASE_TIME' in chunk.columns:
            DataFrameChunkWriter.format_time_column(chunk, 'RELEASE_TIME', '%Y.%m.%d %H:%M:%S')

    @staticmethod
    def format_time_column(df: pd.DataFrame, column_name: str, time_format: str) -> pd.DataFrame:
        """
        Formats a time column in the DataFrame to datetime objects.
        Args:
            df (pd.DataFrame): DataFrame containing the time column.
            column_name (str): Name of the time column to format.
            time_format (str): Time format to parse.
        Returns:
            pd.DataFrame: DataFrame with the formatted time column.
        """
        if column_name in df.columns:
            df[column_name] = pd.to_datetime(df[column_name], format=time_format, errors='coerce')
        return df


class DataExtractor:
    """
    Extracts and stores various types of data into the database.
    """

    def __init__(self,
                 name: str,
                 data_paths: dict,
                 directory: str,
                 reference_path: str = None):
        """
        Initialize the DataExtractor.
        Args:
            name (str): Name of the database or data type.
            data_paths (dict): Dictionary of file paths mapped to table names.
            directory (str): Directory containing additional CSV files.
            reference_path (str, optional): Path to the reference data file for filtering.
        """
        self.queue = Queue()
        self.data_paths = data_paths
        self.directory = directory
        self.name = name
        self.reference = load_data(reference_path) if reference_path else None
        self.csv_files = self.get_csv_files()

    def get_csv_files(self):
        """
        Get a dictionary of CSV file paths and corresponding table names.
        Returns:
            dict: Dictionary with file paths as keys and table names as values.
        """
        csv_files = self.data_paths.copy()

        files = [
            filename
            for filename in os.listdir(self.directory)
            if os.path.isfile(os.path.join(self.directory, filename))
               and filename.endswith(".csv")
        ]

        dynamic_tables = {os.path.join(self.directory, key): 'data_table' for key in files}
        csv_files.update(dynamic_tables)
        return csv_files

    def run(self):
        """
        Starts the data extraction and writing process.
        Spawns multiple processes for concurrent processing and database writing.
        """
        DBCreator(self.name).create_table()
        writer_process = Process(target=DataFrameChunkWriter(self.name, self.reference, self.queue).write_to_db)
        writer_process.start()

        processes = []
        for file_table in self.csv_files.items():
            p = Process(target=DataFrameChunkWriter(self.name, self.reference, self.queue).process_file,
                        args=(file_table,))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

        self.queue.put((None, None))
        writer_process.join()


def sort_table_on_time(db_name: str, table_name: str, time_column_name: str):
    """
    Sorts the specified table to facilitate easier access to specific times by:
        - Removing duplicate rows based on the specified time column.
        - Creating a new table with distinct rows ordered by the time column.
        - Replacing the old table with the new sorted version.
        - Creating a unique index on the time column to maintain order and uniqueness.
    Args:
        db_name (str): Name of the database.
        table_name (str): Name of the table to sort and clean.
        time_column_name (str): Name of the time column to base the sorting on.
    """
    queries = [
        ("DELETE FROM {} WHERE rowid NOT IN(SELECT MIN(rowid) FROM {} GROUP BY ?);", (table_name, table_name,
                                                                                      time_column_name)),
        ("CREATE TABLE {}_clean AS SELECT DISTINCT * FROM {} ORDER BY ?;", (table_name, table_name, time_column_name)),
        ("DROP TABLE {};", (table_name,)),
        ("ALTER TABLE {}_clean RENAME TO {};", (table_name, table_name)),
        ("CREATE UNIQUE INDEX idx_{}_{} ON {}(?);", (table_name, time_column_name.replace(' ', '_'),
                                                     table_name, time_column_name))
    ]
    with DBConnector(db_name) as db:
        for query, params in queries:
            db.execute_query(query.format(*params))
        print(f"Table {table_name} modified successfully in {db_name}")
