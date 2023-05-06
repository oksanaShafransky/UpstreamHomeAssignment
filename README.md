# UpstreamHomeAssignment
To run the program run it as following example:
vehicle_data_lake.py --base_url http://localhost:9900/upstream/vehicle_messages
                     --num_of_messages 10000
                     --data_path C:\Users\User\Documents\oksana\UpstreamHomeAssignment\data
                     --report vin_last_state

1. Fetch data from http server according to the provided params on url and num_of_messages
2. Save the data partitioned by date and hour under provided data_path + data/raw_data folder
3. Normalize the fetched data and save it under provided data_path + data/normalized_data
4. Th program provides report according the requested report name (can be one of vin_last_state, fastest_vehicles, violations)

