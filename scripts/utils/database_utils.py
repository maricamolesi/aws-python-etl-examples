import psycopg2
import pyodbc
from pymongo import MongoClient
from pandas import DataFrame
from io import StringIO
from typing import Any, Optional
from contextlib import contextmanager

from aws_utils import get_secret

#------------------MONGODB------------------#
def connect_mongo(secret_name: str) -> MongoClient:
    """
    Connects to MongoDB and returns the client that can be used to access multiple databases.
    
    Args:
        secret_name (str): The name of the secret in AWS Secrets Manager.
    
    Returns:
        MongoClient: A MongoDB client that can be used to access multiple databases.
    
    Raises:
        RuntimeError: If there is an error connecting to MongoDB.
    """
    try:
        credentials = get_secret(secret_name)
        host = credentials.get('host')
        port = credentials.get('port', 27017)

        if not host:
            raise ValueError("The 'host' must be provided in the secret.")

        client = MongoClient(host, port)

        print(f"Successfully connected to MongoDB at {host}:{port}.")

        return client

    except Exception as e:
        raise RuntimeError(f"Error while connecting to MongoDB: {e}")

#------------------POSTGRESQL/SQL SERVER------------------#
@contextmanager
def connect_db(secret_name: str, db_type: str):
    """
    A context manager for connecting to PostgreSQL or SQL Server databases.
    
    Args:
        secret_name (str): The name of the secret in AWS Secrets Manager.
        db_type (str): The type of the database. Options: 'postgres', 'sqlserver'.
    
    Yields:
        Any: A connection object for PostgreSQL or SQL Server.
    """
    credentials = get_secret(secret_name)
    host = credentials.get('host')
    database = credentials.get('database')
    user = credentials.get('username')
    password = credentials.get('password')
    port = credentials.get('port', 5432)

    connection = None
    try:
        if db_type.lower() == 'postgres':
            connection = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port
            )
            print("Successfully connected to PostgreSQL.")
            yield connection

        elif db_type.lower() == 'sqlserver':
            connection = pyodbc.connect(
                f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                f'SERVER={host},{port};'
                f'DATABASE={database};'
                f'UID={user};'
                f'PWD={password}'
            )
            print("Successfully connected to SQL Server.")
            yield connection

        else:
            raise ValueError("Unsupported database type. Use 'postgres' or 'sqlserver'.")

    except Exception as e:
        raise RuntimeError(f"Error while connecting to the database: {e}")
    
    finally:
        if connection:
            connection.close()
            print(f"Connection to {db_type} closed.")


def execute_query(conn: Any, query: str, parameters: Optional[Any] = None, fetch: bool = True, commit: bool = False) -> Optional[DataFrame]:
    """
    Executes a query on a PostgreSQL or SQL Server database.

    Args:
        conn (Any): Database connection object (psycopg2 or pyodbc connection).
        query (str): SQL query to execute.
        parameters (optional, Any): Query parameters, if any.
        fetch (bool, optional): Whether to fetch results. Default is True (for SELECT queries).
        commit (bool, optional): Whether to commit the query. Default is False (for SELECT queries).

    Returns:
        Optional[pd.DataFrame]: DataFrame if fetch=True and query is SELECT, None otherwise.

    Raises:
        ValueError: If both fetch and commit are set to True.
        RuntimeError: If there is any error executing the query.
    """
    if fetch and commit:
        raise ValueError("Cannot use both 'fetch' and 'commit' at the same time. Use 'fetch' for SELECT queries and 'commit' for non-SELECT queries.")
    
    try:
        cursor = conn.cursor()
        cursor.execute(query, parameters)

        if fetch:
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            return DataFrame(result, columns=columns)
        
        if commit:
            conn.commit()
            print("Commit successful.")
            return None
        else:
            raise ValueError("commit must be True for non-SELECT queries")

    except Exception as e:
        print(f"Error executing query: {e}")
        raise RuntimeError(f"Error executing query: {e}")

#------------------POSTGRESQL------------------#
def insert_records(conn: psycopg2.extensions.connection, df: DataFrame, table: str) -> None:
    """
    Insere registros em uma tabela PostgreSQL usando COPY a partir de um buffer de memória.

    Args:
        conn (psycopg2.extensions.connection): Conexão ativa do PostgreSQL.
        df (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
        table (str): Nome da tabela para onde os dados serão inseridos.

    Returns:
        None: Não retorna valor.
    
    Raises:
        RuntimeError: Se ocorrer algum erro durante a inserção.
    """
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0) 

        cursor = conn.cursor()
        cursor.copy_from(csv_buffer, table, sep=',')

        conn.commit()

        print(f"Successfully inserted records into {table}.")
    
    except Exception as e:
        print(f"Error inserting records into {table}: {e}")
        conn.rollback()
        raise RuntimeError(f"Error inserting records into {table}: {e}")

def delete_data(conn: psycopg2.extensions.connection, table: str, truncate: bool = False, delete_condition: str = None) -> None:
    """
    Deleta dados de uma tabela PostgreSQL com base em uma condição ou faz um truncamento completo.

    Args:
        conn (psycopg2.extensions.connection): Conexão ativa do PostgreSQL.
        table (str): Nome da tabela de onde os dados serão excluídos.
        truncate (bool, opcional): Se True, fará um TRUNCATE na tabela. Default é False.
        delete_condition (str, opcional): Condição para deletar linhas específicas (exemplo: 'column = value').

    Returns:
        None: Não retorna valor.

    Raises:
        RuntimeError: Se ocorrer algum erro durante a exclusão.
    """
    try:
        cursor = conn.cursor()

        if truncate:
            cursor.execute(f"TRUNCATE TABLE {table};")
            print(f"Successfully truncated the table {table}.")
        
        elif delete_condition:
            cursor.execute(f"DELETE FROM {table} WHERE {delete_condition};")
            print(f"Successfully deleted records from {table}.")
        
        else:
            raise ValueError("Must specify either 'truncate' or 'delete_condition'.")

        conn.commit()
    
    except Exception as e:
        print(f"Error deleting data from {table}: {e}")
        conn.rollback()

        raise RuntimeError(f"Error deleting data from {table}: {e}")

