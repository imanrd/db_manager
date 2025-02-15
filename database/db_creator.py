import time
import re
from database.db_connector import DBConnector


class DBCreator:
    """
    Handles the creation of database tables for storing dataframe chunks.
    """
    VALID_COLUMN_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    def __init__(self, db_name: str, table_name: str, columns: dict):
        """
        Initialize the DBCreator with the database name, table name, and columns.

        Args:
            db_name (str): Name of the database.
            table_name (str): Name of the table to be created.
            columns (dict): Dictionary with column names as keys and data types as values.
        """
        self.db_name = db_name + '.db'
        self.table_name = self._validate_table_name(table_name)
        self.columns = self._validate_columns(columns)

    def _validate_table_name(self, table_name: str) -> str:
        """
        Validate the table name to prevent SQL injection.

        Args:
            table_name (str): The table name to validate.

        Returns:
            str: Sanitized table name.

        Raises:
            ValueError: If the table name is invalid.
        """
        if not self.VALID_COLUMN_NAME.match(table_name):
            raise ValueError(f"Invalid table name: {table_name}")
        return table_name

    def _validate_columns(self, columns: dict) -> dict:
        """
        Validate column names and types to prevent SQL injection.

        Args:
            columns (dict): Dictionary of column names and types.

        Returns:
            dict: Sanitized columns dictionary.

        Raises:
            ValueError: If any column name or type is invalid.
        """
        valid_types = {'TEXT', 'TIMESTAMP', 'REAL', 'INTEGER'}

        for col_name, col_type in columns.items():
            if not self.VALID_COLUMN_NAME.match(col_name):
                raise ValueError(f"Invalid column name: {col_name}")
            if col_type.upper() not in valid_types:
                raise ValueError(f"Invalid column type for {col_name}: {col_type}")

        return columns

    def create_table(self) -> None:
        """
        Creates a table in the database for storing dataframe chunks.
        """
        columns_def = ", ".join([f"{col_name} {col_type}" for col_name, col_type in self.columns.items()])

        create_table_query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns_def})"

        with DBConnector(self.db_name) as db:
            db.execute_query(create_table_query)
            print(f"Table '{self.table_name}' created successfully in {self.db_name}")
            time.sleep(5)
