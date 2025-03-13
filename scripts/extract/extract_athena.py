import awswrangler as wr
from pandas import DataFrame

def extract_athena(query: str, database: str, s3_output: str = None, ctas_approach: bool = False) -> DataFrame:
    """
    Executes a SQL query on AWS Athena and returns the result as a DataFrame.

    Parameters:
    - query (str): The SQL query to execute.
    - database (str): The Athena database to use.
    - s3_output (str, optional): The S3 location where results will be stored (required if ctas_approach is True).
    - ctas_approach (bool, optional): If True, uses the Create Table As (CTAS) approach for executing the query.

    Returns:
    - pd.DataFrame: The result of the query.

    Raises:
    - ValueError: If `s3_output` is missing when `ctas_approach` is True.
    - RuntimeError: If an error occurs during extraction.
    """
    if ctas_approach and not s3_output:
        raise ValueError("The parameter 's3_output' must be provided when using ctas_approach.")

    try:
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            ctas_approach=ctas_approach,
            s3_output=s3_output if ctas_approach else None
        )
        return df
    
    except Exception as e:
        raise RuntimeError(f"Error extracting data: {e}")
