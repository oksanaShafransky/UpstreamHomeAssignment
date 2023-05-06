import pandas as pd
from Utils import utils, consts

# Based on the BRONZE dataframe normalize the data:
# 1. Identify a manufacturer with trailing space and remove it
# 2. Filter rows with null vin
# 3. Convert 'gearPosition' to be integers only, in case of unconvertable value (like None or Neutral) - put 0

def normalize_vehicle_data(vehicle_df: pd.DataFrame, normalized_data_path: str)->pd.DataFrame:
    pd.set_option('mode.chained_assignment', None)
    vehicle_df['manufacturer'] = vehicle_df['manufacturer'].str.rstrip()
    vehicle_df = vehicle_df.dropna(subset=['vin'])
    vehicle_df['gearPosition'] = pd.to_numeric(vehicle_df['gearPosition'], errors ='coerce').fillna(0).astype('int')
    utils.save_data_to_parquet(df=vehicle_df, path=normalized_data_path, partitions=consts.PARTITIONS)
    return vehicle_df

