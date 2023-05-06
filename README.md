# UpstreamHomeAssignment
1. Fetch data from http server according to the provided params on url and num_of_messages
2. Normalize the fetched data
3. Provide report according the requested report name
To run the program run it as following example:
vehicle_data_lake.py --base_url http://localhost:9900/upstream/vehicle_messages
                     --num_of_messages 10000
                     --data_path C:\Users\User\Documents\oksana\UpstreamHomeAssignment\data
                     --report vin_last_state
