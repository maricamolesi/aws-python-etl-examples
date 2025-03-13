import awswrangler as wr
from pandas import DataFrame
from typing import List

def load_csv(df: DataFrame, path: str, sep: str = ';', index: bool = False, 
             partition_cols: list = None, mode: str = 'append') -> List[str]:
    """
    Saves a DataFrame as a CSV file to S3 using AWS Wrangler.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        path (str): The S3 path where the CSV file will be saved.
        sep (str, optional): Separator to use for CSV file. Default is ';'.
        index (bool, optional): Whether to write row names (index). Default is False.
        partition_cols (list, optional): Columns used to partition the dataset.
        mode (str, optional): Write mode for the CSV file. Can be 'append' (default), 'overwrite', or 'overwrite_partitions'.

    Returns:
        list: A list of the S3 paths where the CSV files were saved.

    Raises:
        RuntimeError: If there is an error saving the DataFrame to CSV.
    """
    try:
        response = wr.s3.to_csv(
            df, 
            path, 
            sep=sep, 
            index=index, 
            dataset=True, 
            partition_cols=partition_cols, 
            mode=mode
        )

        print(f"Successfully saved {len(df)} rows to CSV at {path}.")

        return response['paths']
    
    except Exception as e:
        raise RuntimeError(f"Error saving the DataFrame to CSV: {e}")
