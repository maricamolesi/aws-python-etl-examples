from typing import Tuple

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