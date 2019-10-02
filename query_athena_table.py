from pyathena import connect
from pyathena.util import as_pandas
import argparse
import logging
import time
import sys


def run_query(database_name,table_name):

    Athena_output_bucket='YOUR_S3_BUCKET/path/to/'
    region_name='us-west-2'

    cursor = connect(s3_staging_dir='s3://'+Athena_output_bucket,
                 region_name=region_name).cursor()
    query = \
        """
        SELECT * FROM "{0}"."{1}" 
        """.format(database_name,table_name)
    
    cursor.execute(query)
    df = as_pandas(cursor)
    print(df.describe())

    
    
if __name__ == '__main__':
    
    # Initiate logging 
    logger = logging.getLogger('')

    # Parse arguments
    arg_parser = argparse.ArgumentParser(description = 'create athena table.') 
    arg_parser.add_argument('-d','--database_name', type = str, action = 'store',
        required = True, help = 'database name.') 
    arg_parser.add_argument('-t','--table_name', type = str, action = 'store',
    required = True, help = 'table name.') 
    args = arg_parser.parse_args()
    
    #call function

    run_query(args.database_name,args.table_name)
    # Cleanup
    logger.info('Finished')
