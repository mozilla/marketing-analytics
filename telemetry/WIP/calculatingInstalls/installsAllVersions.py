# Background
# After noticing a discrepancy in the retention rates from telemetry tables churn_v2 and churn_v3, a bug was filed to
# further investigate
# https://bugzilla.mozilla.org/show_bug.cgi?id=1479810

# From discussions in the bug it was determined that the churn tables only look at installs for Fx Versions 55+ as they
# are pulling from the new profile table
# Approximately 30% of installs are from Fx versions older than Fx 55. These can only be pulled from Main summary
# As of Fx 57, a new table first shutdown ping was available

# Therefore this file looks at a way to meaningfully calculate the total installs on a given day to provide additional
# clarity to our reporting


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DateType, StringType
from datetime import timedelta, datetime

# 1 Connect to tables & Select fields of interest from tables
# 1a Connect to tables in Telemetry
spark = SparkSession.builder.appName('installs').getOrCreate()
mainSummaryTable = spark.read.option('mergeSchema', 'true').parquet('s3://telemetry-parquet/main_summary/v4/')
newProfilesTable = spark.read.option('mergeSchema', 'true').parquet('s3://net-mozaws-prod-us-west-2-pipeline-data/telemetry-new-profile-parquet/v2/')
firstRunTable = spark.read.option('mergeSchema', 'true').parquet('s3://telemetry-parquet/first_shutdown_summary/v4/')

#1b Select columns of interest
newProfilesColumns = ['submission',
                      'client_id'
                      ]
mainSummaryColumns = ['submission_date_s3',
                      'profile_creation_date',
                      'install_year',
                      'client_id',
                      'profile_subsession_counter',
                      'previous_subsession_id',
                      'app_version',
                      'sample_id',
                      'country',
                      'geo_subdivision1',
                      'city',
                      'attribution.source',
                      'attribution.medium',
                      'attribution.campaign',
                      'attribution.content',
                      'scalar_parent_browser_engagement_total_uri_count'
                       ]



#1c Pull
mainSummary = mainSummaryTable.select(mainSummaryColumns)

newProfilesColumns = ['submission',
                      'client_id',
                      '']







firstRunColumns =