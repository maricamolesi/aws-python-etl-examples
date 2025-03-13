import boto3
from json import loads
from typing import List, Dict, Tuple

def get_secret(secret_name: str) -> Dict:
    """
    Retrieves the entire secret stored in AWS Secrets Manager.

    Args:
        secret_name (str): The name of the secret in AWS Secrets Manager.

    Returns:
        dict: The secret data as a dictionary.

    Raises:
        RuntimeError: If there is an error retrieving the secret or processing the response.
    """
    client = boto3.client('secretsmanager')
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret_string = response.get('SecretString')
        
        if secret_string is None:
            raise RuntimeError("The secret does not contain a 'SecretString' field.")
        
        secrets = loads(secret_string)
        return secrets

    except Exception as e:
        raise RuntimeError(f"Error retrieving or processing the secret: {e}")
    
def get_all_keys(bucket: str, prefix: str = None)-> List[str]:
    """
    Retrieves all keys from an S3 bucket that match the optional prefix.
    
    Args:
        bucket (str): The name of the S3 bucket.
        prefix (str, optional): The prefix to filter the S3 objects (default is None).
        
    Returns:
        List[str]: A list of S3 keys (file paths).
        
    Raises:
        Exception: If there is an error interacting with the S3 service.
    """
    client = boto3.client('s3')

    try:
        page_iterator = client.get_paginator('list_objects').paginate(Bucket=bucket, Prefix=prefix)
        return [page['Key'] for page in page_iterator.search("Contents[?Size > `0`][]")]
        
    except Exception as e:
        raise RuntimeError(f"Error retrieving keys from bucket '{bucket}': {e}")
    
def parse_s3_event(event: dict) -> Tuple[str, str]:
    """
    Extracts the bucket name and file path from an S3 event. 

    Args:
        event (dict): The S3 event received by the Lambda function.

    Returns:
        tuple: A tuple containing the bucket name and the S3 file path.
    """
    s3_part = event['Records'][0]['s3']

    bucket = s3_part['bucket']['name']
    key = s3_part['object']['key'].replace('+', ' ').replace('%3D', '=')

    print(f'Bucket: {bucket}, Path: {key}')
    
    return bucket, key