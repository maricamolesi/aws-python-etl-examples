from parse_s3_event import parse_s3_event
from extract import extract_parquet
from load import load_parquet

from config import PATH_TRUSTED, GLUE_DATABASE, GLUE_TABLE

def lambda_handler(event, context):
    """
    Executa o processo ETL (Extração, Transformação e Carregamento).

    Retorna:
    - 200 se o processo ETL for bem-sucedido.
    - 204 se não houver dados para processar.

    """
    # Etapa 1: Extração dos dados
    bucket, key = parse_s3_event(event)
    data = extract_parquet(bucket, key)

    if data:
        # Etapa 2: Transformação
        df = data.dropna(subset=['dt']).drop_duplicates(subset=['dt'])

        # Etapa 3: Carregamento
        response = load_parquet(PATH_TRUSTED, 
                                df, 
                                partition_cols=['dt'], 
                                mode='overwrite_partitions', 
                                database=GLUE_DATABASE, 
                                table=GLUE_TABLE)
        return {
                'statusCode': 200,
                'body': {'message': "ETL bem sucedido.",
                        'path': response }
            }

    return {
            'statusCode': 204,
            'body': "Sem dados."
            }


