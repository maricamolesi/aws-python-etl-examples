#BUCKET
BUCKET_TRUSTED = GLUE_DATABASE = 'trusted'

#VARIABLES
AREA = 'example'
SOURCE = 'example_files'
TABLE = GLUE_TABLE = 'example_table'

#PATHS
KEY             = f'area={AREA}/source={SOURCE}/table={TABLE}'
PATH_TRUSTED    = f's3://{BUCKET_TRUSTED}/{KEY}'

