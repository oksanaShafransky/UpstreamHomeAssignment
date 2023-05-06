from Bronze import fetch_data
from Silver import normalize_data
from Gold import generate_reports, bonus
from os.path import abspath
import argparse

# This method runs the following:
# 1. Fetch data from http server according to the provided params on url and num_of_messages
# 2. Normalize the fetched data
# 3. Provide report according the requested report name
# To run the program run it as following example:
# vehicle_data_lake.py --base_url http://localhost:9900/upstream/vehicle_messages
#                      --num_of_messages 10000
#                      --data_path C:\Users\User\Documents\oksana\UpstreamHomeAssignment\data
#                      --report vin_last_state

def main(params):
    base_url = params.base_url
    num_of_messages = params.num_of_messages
    report = params.report
    raw_data_path = abspath(f'{params.data_path}\\raw_data')
    normalized_data_path = abspath(f'{params.data_path}\\normalized_data')

    url = f'{base_url}?amount={num_of_messages}'
    bronze_df = fetch_data.get_vehicle_messages(url, raw_data_path)
    silver_df = normalize_data.normalize_vehicle_data(bronze_df, normalized_data_path)

    if report == 'vin_last_state':
        return generate_reports.generate_vin_last_state_report(silver_df)
    elif report == 'fastest_vehicles':
        return generate_reports.generate_top_fastest_vehicles_report(silver_df)
    elif report == 'violations':
        return bonus.sqlInjectionReport(silver_df, ['vin'], ['''"('(''|[^'])*')|(;)|(\b(ALTER|CREATE|DELETE|DROP|EXEC(UTE){0,1}|INSERT( +INTO){0,1}|MERGE|SELECT|UPDATE|UNION( +ALL){0,1})\b)"'''])
    else:
        return 'No valid report name provided'


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--base_url', required=True, help='base url of http server')
    parser.add_argument('--num_of_messages', required=True, help='number of messages to be downloaded')
    parser.add_argument('--data_path', required=True, help='path where all data is located')
    parser.add_argument('--report', required=True, help='specific report name to get, can be: vin_last_state, fastest_vehicles, violations')
    args = parser.parse_args()

    df = main(args)
    print(df)