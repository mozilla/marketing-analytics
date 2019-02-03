from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType, DateType, StringType
from datetime import timedelta, datetime

# 1 Connect to Main Summary & New Profiles / Installs Table
print("connecting to tables")

spark = SparkSession.builder.appName('mainSummary').getOrCreate()

main_summary_table = spark.read.option('mergeSchema', 'true')\
    .parquet('s3://telemetry-parquet/main_summary/v4/')
new_profiles_table = spark.read.option('mergeSchema', 'true')\
    .parquet('s3://net-mozaws-prod-us-west-2-pipeline-data/telemetry-new-profile-parquet/v2/')

# 2 Set constants and variables

COLUMNS = ['submission_date_s3','profile_creation_date','subsession_start_date',
           'previous_subsession_id','install_year','client_id',
           'profile_subsession_counter','sample_id','app_version',
           'channel','normalized_channel','country',
           'geo_subdivision1','city','attribution.source','attribution.medium',
           'attribution.campaign','attribution.content','os',
           'os_version','app_name','windows_build_number',
           'scalar_parent_browser_engagement_total_uri_count']

start_period = datetime(year=2019, month=1, day=25)
end_period = datetime(year=2019, month=1, day=31)
period = end_period - start_period

start_period_string = start_period.strftime("%Y%m%d")
end_period_string = end_period.strftime("%Y%m%d")

# 3 Set functions
def reduce_main_summary_fields(start_period_string, end_period_string):
    reduced_main_summary = main_summary_table.filter(f"submission_date_s3 >= '{start_period_string}'"
                                                     f"AND submission_date_s3 <= '{end_period_string}'").select(COLUMNS)
    print("aggregating main summary total pageviews by client_id")
    agg_page_views = reduced_main_summary


def find_current_period_acquisitions():




def join_acquisitions_to_daily_usage():





def calculate_days_retained():





def aggregate_by_funnel_country():




def aggregate_final_export():




def write_daily_retention_file():






if period.days <= 7:
    print(f"starting loop: {start_period_string}-{end_period_string}")



#File structure


#Starting run for 20181008-20181014
#starting reduce_main_summary_fields
#aggregating main summary total pageviews by client_id
#reduce_main_summary_fields operation complete
#Starting find_current_period_acquisitions
#Finding unique install dates for acquisitions in desired period
#find_current_period_acquisitions operation complete
#Starting join_acquisitions_to_daily_usage
#join_acquisitions_to_daily_usage operation complete
#Start calculate_days_retained
#Calculating days retained
#calculate_days_retained operation complete
#Start aggregate_by_funnel_country
#aggregate_by_funnel_country operation complete
#Start aggregate_final_export
#aggregate_final_export complete
#Starting write_daily_retention_file
#20181008-20181014 completed and saved
#Setting new dates for next loop