import json
import os
from io import StringIO
import re

test_file = '/Users/gkaberere/spark-warehouse/leanPlum/testABTestPull/working_with_json_data_test_767485044_20190701_20190820.json'
save_file = '/Users/gkaberere/spark-warehouse/leanPlum/testABTestPull/reconfigured_json_sample_file.json'
experiment_id = 767485044
variants_list = [790455048, 787185044]

with open(test_file, 'r') as read_file:
    leanplum_json_string = read_file.read()

    # Change string to lower case
    leanplum_json_string = leanplum_json_string.lower()

    # Remove spaces in keys to avoid column name errors when loading into bigquery
    leanplum_json_string = re.sub('(\s)', '_', leanplum_json_string)
    leanplum_json_string = re.sub(':_', ': ', leanplum_json_string)
    leanplum_json_string = re.sub(',_', ', ', leanplum_json_string)
    leanplum_json_string = re.sub('\.m', 'm', leanplum_json_string)

    # Convert string to dict
    leanplum_pull = json.loads(leanplum_json_string)


sample_day_data = leanplum_pull['response'][0]['data']['2019-08-12']
date_sample = '2019-08-12'
#print(sample_day_data)
#print(type(sample_day_data))

events_list = list(sample_day_data.keys())
#print(events_list)

report_data = sample_day_data
# TODO: Should spit out the result of the events transformation into a single dict separated by commas
dicts = {}
for event in events_list:

    variant_keys = list(report_data[f'{event}'].keys())

    metrics_list = ['daily_unique_users',
                    'unique_sessions',
                    'time_until_first_occurence_in_session',
                    'first_time_occurences',
                    'time_until_first_occurence_for_user',
                    'occurences']

    # Checks to see if a variant_id is included in the event - if not assigns to blank variant_id
    if any(i in variant_keys for i in metrics_list):
        variant_data = report_data[f'{event}']

        # Add experiment and event keys and values to metrics
        variant_data.update({f'event': f'{event}'})
        variant_data.update({f'variant_id': ''})
        variant_data.update({f'experiment_id': f'{experiment_id}'})
        print(dicts)
        print(f'variant data for blank variant with variant is : {variant_data}')

    else:
        for variants in variant_keys:
            variant_data = report_data[f'{event}'][f'{variants}']
            # Add experiment and event keys and values to metrics
            variant_data.update({f'event': f'{event}'})
            variant_data.update({f'variant_id': f'{variants}'})
            variant_data.update({f'experiment_id': f'{experiment_id}'})
            print(variant_data)
            print(type(variant_data))



sample_dict = {
        "date": "2019-08-12",
        "variants": [
            {"variant_id": "790455048"},
            {
                "experiment_id": "767485044",
                "variant_id": "787185044",
                "event": "basic_stats",
                "total_session_starts": "87",
                "total_session_length": "19961.819",
                "total_monthly_users": "246",
                "sessions_with_time": "76",
                "total_weekly_users": "243",
                "total_daily_users": "69",
                "total_sessions": "87"
             },
            {
                "experiment_id": "767485044",
                "variant_id": "787185044",
                "event": "e_opened_new_tab",
                "daily_unique_users": "11",
                "first_time_occurences": "1",
                "unique_sessions": "11",
                "time_until_first_occurence_for_user": "1322.716",
                "occurences": "11",
                "time_until_first_occurence_in_sessions": "647.282"
             },
            {
                "experiment_id": "767485044",
                "variant_id": "787185044",
                "event": "e_opened_app",
                "daily_unique_users": "57",
                "unique_sessions": "59",
                "occurences": "64",
                "time_until_first_occurence_in_sessions": "65.23"
            },
            {
                "experiment_id": "767485044",
                "variant_id": "787185044",
                "event": "uninstall",
                "daily_unique_users": "3",
                "first_time_occurences": "3",
                "occurences": "3",
                "time_until_first_occurence_for_user": "19712.437"
            },

        ]
}


#with open(save_file, 'w') as write_file:
#    json_string = json.dumps(sample_dict)
#    write_file.write(json_string)


