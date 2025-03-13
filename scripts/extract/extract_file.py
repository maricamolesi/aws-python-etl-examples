from pathlib import Path
import awswrangler as wr
from pandas import DataFrame

def extract_file(path: str = None, bucket: str = None, key: str = None, sheet_name: str = None) -> DataFrame:
    """
    General function to extract data from different file types (CSV, JSON, Parquet, Excel) stored in S3.

    Args:
        path (str, optional): Full S3 path (e.g., "s3://my-bucket/my-file.csv").
        bucket (str, optional): The name of the S3 bucket.
        key (str, optional): The file path within the S3 bucket.
        sheet_name (str, optional): The name of the sheet to extract in case of Excel. If not provided, the first sheet will be used.

    Returns:
        pd.DataFrame: Data from the file as a pandas DataFrame.

    Raises:
        ValueError: If neither `path` nor both `bucket` and `key` are provided.
        RuntimeError: If the file extension is not supported or an error occurs during extraction.
    """
    
    if not path:
        if not bucket or not key:
            raise ValueError("Either `path` or both `bucket` and `key` must be provided.")
        path = f's3://{bucket}/{key}'
    
    file_extension = Path(path).suffix.lower()

    try:
        if file_extension == '.parquet':
            return wr.s3.read_parquet(path)
        
        elif file_extension == '.csv':
            return wr.s3.read_csv(path)
        
        elif file_extension == '.json':
            return wr.s3.read_json(path)
        
        elif file_extension in ['.xlsx', '.xls']:
            return wr.s3.read_excel(path, sheet_name=sheet_name)
        
        else:
            raise RuntimeError(f"Unsupported file extension: {file_extension}")
        
    except Exception as e:
        raise RuntimeError(f"Error extracting data from {path}: {e}")
