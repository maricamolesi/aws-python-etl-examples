#BUCKETS DO DATA LAKE
BUCKET_RAW = 'raw'
BUCKET_STG = 'stg'
BUCKET_TRUSTED = 'trusted'

#VARIAVEIS DO DATA LAKE
AREA = ''
SOURCE = ''
TABLE = ''

#CAMINHOS DO DATA LAKE
KEY             = f'area={AREA}/fonte={SOURCE}/tabela={TABLE}'

PATH_RAW        = f's3://{BUCKET_RAW}/{KEY}'
PATH_STG        = f's3://{BUCKET_STG}/{KEY}'
PATH_TRUSTED    = f's3://{BUCKET_TRUSTED}/{KEY}'

