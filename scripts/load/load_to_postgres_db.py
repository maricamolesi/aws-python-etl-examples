from psycopg2.extensions import connection as pg_connection
from pandas import DataFrame

from utils import database_utils as du

def load_to_postgres_db(conn: pg_connection, table: str, df: DataFrame, truncate: bool = False, delete_condition: str = None) -> None:
    """
    Loads a DataFrame into a database table.

    Args:
        conn (psycopg2.extensions.connection): Database connection object.
        table (str): Name of the target table.
        df (pd.DataFrame): DataFrame containing the data to be loaded.
        truncate (bool, optional): If True, truncates the table before loading the data.
        delete_condition (str, optional): Condition to delete specific rows before insertion.

    Returns:
        None

    Raises:
        RuntimeError: If there is an error during data loading.

    This function will:
        1. Optionally truncate or delete data from the table based on the provided conditions.
        2. Insert the data from the DataFrame into the specified table.
    """
    try:
        # Call delete_data to either truncate or delete based on the condition
        if truncate or delete_condition:
            du.delete_data(conn, table, truncate=truncate, delete_condition=delete_condition)

        # Call insert_records to load the data from the DataFrame
        du.insert_records(conn, df, table)

    except Exception as e:
        raise RuntimeError(f"Error loading data into table {table}: {e}")
