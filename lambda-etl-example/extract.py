from pathlib import Path
import awswrangler as wr
from pandas import DataFrame

def extract_parquet(path: str = None, bucket: str = None, key: str = None) -> DataFrame:
    """
    Extracts data from a Parquet file stored in S3.

    Args:
        path (str, optional): Full S3 path (e.g., "s3://my-bucket/my-file.parquet").
        bucket (str, optional): The name of the S3 bucket.
        key (str, optional): The file path within the S3 bucket.

    Returns:
        pd.DataFrame: Data from the Parquet file as a pandas DataFrame.

    Raises:
        ValueError: If neither `path` nor both `bucket` and `key` are provided.
        RuntimeError: If the file extension is not `.parquet` or an error occurs during extraction.
    """
    
    if not path:
        if not bucket or not key:
            raise ValueError("Either `path` or both `bucket` and `key` must be provided.")
        path = f's3://{bucket}/{key}'

    file_extension = Path(path).suffix.lower()
    if file_extension != ".parquet":
        raise RuntimeError(f"Unsupported file extension: {file_extension}. Only Parquet files are supported.")

    try:
        return wr.s3.read_parquet(path)
    
    except Exception as e:
        raise RuntimeError(f"Error extracting data from {path}: {e}")
