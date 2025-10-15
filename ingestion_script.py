import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time



logging.basicConfig(
    filename="logs/ingestion_db.log", 
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s", 
    filemode="a"  
)

engine = create_engine('sqlite:///inventory.db')

    
def ingest_db(df, table_name, engine):
    '''this function will ingest the dataframe into database table'''
    df.to_sql(table_name, con = engine, if_exists = 'replace', index = False)
    

# def load_raw_data():
#     '''this function will load the CSVs as dataframe and ingest into db'''
#     start = time.time()
#     for file in os.listdir('data'):
#         if '.csv' in file:
#             df = pd.read_csv('data/'+file)
#             logging.info(f'Ingesting {file} in db')
#             ingest_db(df, file[:-4], engine)
#     end = time.time()
#     total_time = (end - start)/60
#     logging.info('--------------Ingestion Complete------------')
    
#     logging.info(f'\nTotal Time Taken: {total_time} minutes')          


def load_raw_data():
    start = time.time()
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            logging.info(f'Found file: {file}')
            try:
                df = pd.read_csv(f'data/{file}')
                logging.info(f'{file} loaded successfully')
            except Exception as e:
                logging.error(f'Failed to read {file}: {e}')
                continue  

            try:
                ingest_db(df, file[:-4], engine)
                logging.info(f'{file} ingested into database')
            except Exception as e:
                logging.error(f'Failed to ingest {file} into DB: {e}')
    end = time.time()
    total_time = (end - start) / 60
    logging.info(f'Ingestion complete in {total_time:.2f} minutes')
    
'''is only called when the script is run directly, not when it's imported as a module into another script '''
if __name__ == '__main__':
    load_raw_data()
