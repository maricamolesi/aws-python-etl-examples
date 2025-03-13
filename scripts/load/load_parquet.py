import awswrangler as wr
from pandas import DataFrame
from typing import List

def load_parquet(path: str, df: DataFrame, partition_cols: list = None, mode: str = 'append', 
                 database: str = None, table: str = None) -> List[str]:
    """
    Saves a DataFrame as a Parquet file in an S3 bucket using AWS Wrangler.

    Args:
        path (str): The S3 bucket path where the Parquet file will be saved.
        df (pd.DataFrame): The DataFrame to be saved.
        partition_cols (list, optional): Columns used to partition the dataset.
        mode (str, optional): Write mode for the Parquet file. Can be 'append' (default), 'overwrite', or 'overwrite_partitions'.
            - 'append': Adds data to an existing file.
            - 'overwrite': Replaces the existing file.
            - 'overwrite_partitions': Replaces existing partitions but keeps the rest of the file.
        database (str, optional): The name of the AWS Glue Data Catalog database.
        table (str, optional): The name of the table in the AWS Glue Data Catalog.

    Returns:
        list: A list of the S3 paths where the Parquet files were saved.

    Raises:
        RuntimeError: If there is an error saving the DataFrame to Parquet.
    """
    try:
        response = wr.s3.to_parquet(
            df, 
            path=path, 
            dataset=True,
            partition_cols=partition_cols,
            mode=mode,
            database=database,
            table=table
        )

        print(f"Successfully saved {len(df)} rows to Parquet at {path}.")

        return response['paths']
    
    except Exception as e:
        raise RuntimeError(f"Error saving the DataFrame to Parquet: {e}")
