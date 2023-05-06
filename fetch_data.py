import pandas as pd
import requests
from Utils import utils, consts, vehicle_fetch_exception
import logging
logging.basicConfig(level=logging.INFO)

"""
Get vehicle messages from http server and save them as parquet partitioned by date and hour
"""
def get_vehicle_messages(url:str, raw_data_path: str)->pd.DataFrame:
    logging.info(f'Downloading vehicle messages from {url}...')
    try:
        response = requests.get(url)
        if response.status_code == 200:
            vehicle_messages_df = pd.DataFrame(response.json())
            vehicle_messages_df['ts'] = pd.to_datetime(vehicle_messages_df['timestamp'], unit='ms')
            vehicle_messages_df['date'] = vehicle_messages_df['ts'].dt.date
            vehicle_messages_df['hour'] = vehicle_messages_df['ts'].dt.hour
            utils.save_data_to_parquet(df=vehicle_messages_df, path=raw_data_path, partitions=consts.PARTITIONS)
            logging.info(f'The vehicle raw data was downloaded successfully and saved into {raw_data_path}')
            return vehicle_messages_df
        else:
            raise vehicle_fetch_exception.VehicleFetchException(response.status_code, 'Failed to get data from http server')

    except:
        logging.exception(f'Exception while downloading vehicle messages')
        return pd.DataFrame()

