import boto3
from typing import List

def extract_file_from_s3(bucket: str, key: str) -> List[str]:
    """
    Extracts the lines of a .txt file stored in an S3 bucket.

    Args:
        bucket (str): S3 bucket name.
        key (str): The path to the file in the bucket.

    Returns:
        List[str]: A list of strings, where each string is a line from the file.
    
    Raises:
        RuntimeError: If there is an error accessing the S3 file or reading it.
    """
    s3 = boto3.client('s3')
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        lines = response['Body'].iter_lines()

        return [line.decode('latin-1') for line in lines]
    
    except Exception as e:
        raise RuntimeError(f"Error extracting file from S3: {e}")
