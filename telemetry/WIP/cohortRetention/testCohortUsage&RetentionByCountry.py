from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType, DateType, StringType
from datetime import timedelta, datetime
import pandas as pd

# 1 Connect to Main Summary & New Profiles / Installs Table
print("connecting to tables")

spark = SparkSession.builder.appName('mainSummary').getOrCreate()
mainSummaryTable = spark.read.option('mergeSchema', 'true').parquet('s3://telemetry-parquet/main_summary/v4/')
newProfilesTable = spark.read.option('mergeSchema', 'true').parquet('s3://net-mozaws-prod-us-west-2-pipeline-data/telemetry-new-profile-parquet/v2/')

# 1a Reduce Main Summary to columns of interest and store in new dataframe
columns = ['submission_date_s3',
                       'profile_creation_date',
                       'subsession_start_date',
                       'previous_subsession_id',
                       'install_year',
                       'client_id',
                       'profile_subsession_counter',
                       'sample_id',
                       'app_version',
                       'channel',
                       'normalized_channel',
                       'country',
                       'geo_subdivision1',
                       'city',
                       'attribution.source',
                       'attribution.medium',
                       'attribution.campaign',
                       'attribution.content',
                       'os',
                       'os_version',
                       'app_name',
                       'windows_build_number',
                       'scalar_parent_browser_engagement_total_uri_count'
                       ]

#1b Create & filter mkgmainsummary and select desired fields
print('starting loop')
mkgMainSummary = mainSummaryTable.filter("submission_date_s3 >= '20180930' AND submission_date_s3 <= '20181104'").select(columns)

# 2 Aggregate total pageviews by client ID
aggPageviews = mkgMainSummary.groupBy('submission_date_s3', 'client_id', 'country', 'source', 'medium', 'campaign', 'content').agg(sum(mkgMainSummary.scalar_parent_browser_engagement_total_uri_count).alias('totalURI'))

# 3 Find all cohort acquisitions
currentYearAcquisitions = newProfilesTable.select('submission', 'client_id')
# Filter for desired time period
currentYearAcquisitions = currentYearAcquisitions.filter("submission >= '20180930' AND submission <= '20181006'")
currentYearAcquisitions = currentYearAcquisitions.withColumn('acqSegment', lit('selectCohort'))
currentYearAcquisitions = currentYearAcquisitions.withColumnRenamed('submission', 'installDate')
currentYearAcquisitions = currentYearAcquisitions.withColumnRenamed('client_id', 'np_clientID')

# 4 Determine current users who are new (acquired in current year) vs existing (acquired prior to current year)
# 4a Join currentYearAcquisitions to aggPageviews
aggPageviews = aggPageviews.alias('aggPageviews')
currentYearAcquisitions = currentYearAcquisitions.alias('currentYearAcquisitions')
joinedData = aggPageviews.join(currentYearAcquisitions, col('aggPageviews.client_id') == col('currentYearAcquisitions.np_clientId'), 'left')
# Filter for desired cohort only
joinedData = joinedData.filter("acqSegment = 'selectCohort'")

# 4b In joined data convert nulls to existing for column acqSegment
joinedData = joinedData.withColumn('acqSegment', when(joinedData.acqSegment.isNull(), 'existing').otherwise('selectCohort'))
# to enable installDate column to be converted to a date in section 5 below, assigning nulls 19700101. Code errored out when blank
joinedData = joinedData.withColumn('installDate', when(joinedData.installDate.isNull(), '19700101').otherwise(joinedData.installDate))

# 5 Calculate days retained
#  5a Convert submission_date_s3 and installDate to date format
func = udf(lambda x: datetime.strptime(str(x), '%Y%m%d'), DateType())
joinedData = joinedData.withColumn('submissionDate', func(col('submission_date_s3')))
joinedData = joinedData.withColumn('installDateConv', func(col('installDate')))

# 5b Calculate days retained
joinedData = joinedData.withColumn('daysRetained', when(year(joinedData.installDateConv) == '1970', '').otherwise(datediff(joinedData.submissionDate, joinedData.installDateConv)))

# 6 Aggregate including days retained and remove client_id as a dimension
metrics = joinedData.groupBy('submission_date_s3', 'installDate', 'daysRetained', 'acqSegment', 'country', 'source', 'medium', 'campaign', 'content').agg(countDistinct(joinedData.client_id).alias('DAU'),sum(when(joinedData.totalURI >= 5, 1).otherwise(0)).alias('activeDAU'), sum(joinedData.totalURI).alias('totalURI'))

#7 Write file
metrics.coalesce(1).write.option("header", "false").csv(
        's3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/gkabbz/retention/testCohortCountry-SpecificWeekv2.csv')
print("{}-{} completed and saved".format(startPeriodString, endPeriodString))




# TODO Graveyeard - Clean before pushing to cluster
#aws s3 sync s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/gkabbz/retention /home/hadoop/sparkAnalysis/retention/dailyRetention
#rsync -av gkabbz-001:/home/hadoop/sparkAnalysis/retention/dailyRetention /Users/gkaberere/spark-warehouse/retention
#gsutil cp /Users/gkaberere/spark-warehouse/retention/dailyRetention/testCohortCountry-SpecificWeekv2.csv/testCohortCountry-SpecificWeekv2.csv gs://gkabbz-upload

#rsync -av /Users/gkaberere/Google\ Drive/Github/marketing-analytics/telemetry gkabbz-001:/home/hadoop/sparkAnalysis/mAnalytics/telemetryQueries

#for pulling down the installs summary from 9 above
#aws s3 sync s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/retention/installs20180826.csv /home/hadoop/sparkAnalysis/retention/dailyRetention/v2
#rsync -av gkabbz-001:/home/hadoop/sparkAnalysis/retention/dailyRetention /Users/gkaberere/spark-warehouse/retention/v2