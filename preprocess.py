import pandas as pd
import pyarrow.parquet as pq
import os
import json
from datetime import datetime
import logging
import sys
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

def convert_parquet(logger):
    def save_parquet(df, filepath):
        df = df.astype('str')
        filepath_parquet = 'processed_data/' + filepath.split('/')[-1].split('.')[0] + '.parquet'
        df.to_parquet(filepath_parquet, compression='gzip')
    def log_error(filepath, type, exception, skipcount):
        skiplist.append({'file': filepath, 'error_type': type, 'error': str(exception)})	
        skipcount += 1
        return skipcount

    skiplist = []
    skipcount = 0
    for i, file in enumerate(os.listdir('raw_data')):
        logger.info(f'{i+1} - Processing file "{file}"')
        filepath = 'raw_data/' + file
        
        try:    
            df = pd.read_csv(filepath, sep=';', header=0, quotechar='"', doublequote=True, escapechar='\\')
        except Exception as exception:
            logger.error(f'{i+1} - SKIPPING - Error parsing: {file}')
            skipcount = log_error(filepath, 'parse_general', exception, skipcount)
            continue
        
        try:
            save_parquet(df, filepath)
        except Exception as exception:
            logger.error(f'{i+1} - SKIPPING - Error writing to .parquet file: {file}')
            skipcount = log_error(filepath, 'write_parquet', exception, skipcount)
            continue
        
        logger.info(f'{i+1} - Success saving file "{file}"')
        
    logger.info(f'Success writing {(i+1) - skipcount} files. Skipped count: {skipcount}')
    
    with(open('skiplist.json', 'w')) as f:
        json.dump(skiplist, f, indent=4)

def merge_parquet(logger):        
    files = ['processed_data/'+file for file in os.listdir('processed_data')]

    schema = pq.ParquetFile(files[0]).schema_arrow
    with pq.ParquetWriter("opendatasus_flu_syndrome.parquet", schema=schema) as writer:
        for i, file in enumerate(files):
            print(f'{i} - Writing file "{file}"')
            writer.write_table(pq.read_table(file, schema=schema))

def main():
    logger = logging.getLogger(__name__)
    start_time = datetime.now()
    logging.basicConfig(filename=f'process_{start_time.strftime("%Y-%m-%d_%H-%M-%S")}.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    
    logger.info(f'Starting script: {start_time.strftime('%Y-%m-%d %H:%M:%S')}')
    convert_parquet(logger)
    merge_parquet(logger)
    end_time = datetime.now()
    logger.info(f'Ending script: {end_time.strftime('%Y-%m-%d %H:%M:%S')}')
    logger.info(f'Elapsed time (secs): {round((end_time - start_time).total_seconds(),3)}')
    
if __name__ == "__main__":
    main()