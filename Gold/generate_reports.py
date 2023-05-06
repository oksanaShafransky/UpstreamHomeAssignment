import pandas as pd

# This method generates vin_last_state report as following:
# vin - the vehicle id
# last_reported_timestamp - the most recent timestamp, for which we saw this vehicle in the data
# front_left_door_state - last reported, non-null value
# wipers_state - last reported, non-null value
def generate_vin_last_state_report(vehicle_df: pd.DataFrame)->pd.DataFrame:
    vin_last_state_df = vehicle_df.sort_values("timestamp")\
        .groupby("vin", as_index=False).last()
    vin_last_state_df = vin_last_state_df.dropna(subset=['frontLeftDoorState', 'wipersState'])
    vin_last_state_df = vin_last_state_df.rename(columns={'timestamp':'last_reported_timestamp',
                                                          'frontLeftDoorState':'front_left_door_state',
                                                          'wipersState':'wipers_state'})

    return vin_last_state_df[['vin','last_reported_timestamp','front_left_door_state','wipers_state']]



# This method generates report for top 10 "fastest" vehicles per hour, as following:
# Fastest vehicle will be determined by the highest velocity a vehicle has reported
# Report will contain vin, date-hour, and the top velocity of the vehicle ordered from highest to lowest
def generate_top_fastest_vehicles_report(vehicle_df: pd.DataFrame)->pd.DataFrame:
    vehicle_df_grouped = vehicle_df.groupby(['vin','date','hour'], as_index=False)['velocity'].max()
    return vehicle_df_grouped.nlargest(n=10, columns=['velocity'])