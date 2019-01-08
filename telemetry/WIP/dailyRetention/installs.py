from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType, DateType, StringType
from datetime import timedelta, datetime
import pandas as pd

#TODO: Calculate installs from main Summary by browser version. Output should have a date associated to each client ID
#TODO: Join the calculated installs to daily usage summary table. The install date from table above will be used to calculate days alive. Also add segment for existing vs new
#TODO: Calculate retention based on day. Should the retention be pre calculated or should it be done on the fly during reporting
#TODO: Validate fx 55+ by comparing to new profiles and first shutdown - to understand differences

# 1 Connect to Main Summary & New Profiles / Installs Table
print("connecting to tables")

spark = SparkSession.builder.appName('mainSummary').getOrCreate()
mainSummaryTable = spark.read.option('mergeSchema', 'true').parquet('s3://telemetry-parquet/main_summary/v4/')
newProfilesTable = spark.read.option('mergeSchema', 'true').parquet('s3://net-mozaws-prod-us-west-2-pipeline-data/telemetry-new-profile-parquet/v2/')
firstShutdownTable = spark.read.option('mergeSchema', 'true').parquet('s3://telemetry-parquet/first_shutdown_summary/v4/')

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


# Retrieve Data in Chunks
startPeriod = datetime(year=2018, month=8, day=1)
endPeriod = datetime(year=2018, month=8, day=19)
period = endPeriod - startPeriod

if period.days <= 7:
    startPeriodString = startPeriod.strftime("%Y%m%d")
    endPeriodString = endPeriod.strftime("%Y%m%d")

    #1b Create & filter mkgmainsummary and select desired fields
    print('starting loop')
    mkgMainSummary = mainSummaryTable.filter("submission_date_s3 >= '{}' AND submission_date_s3 <= '{}'".format(startPeriodString, endPeriodString)).select(columns)

    # 2 Aggregate total pageviews by client ID
    #  TODO: include appversion. Needs to be split to major version then find the max and assign to that user for that day. Also include channel - be weary of users who switch from release to developer for example
    aggPageviews = mkgMainSummary.groupBy('submission_date_s3', 'client_id').agg(sum(mkgMainSummary.scalar_parent_browser_engagement_total_uri_count).alias('totalURI'))

    # 3 Find all current year acquisitions
    currentYearAcquisitions = newProfilesTable.select('submission', 'client_id')
    # TODO: Due to this dimension build, need to figure out how to feed the year based off of actual year of acquisition. Currently simply pulling for 2018.
    currentYearAcquisitions = currentYearAcquisitions.filter("submission >= '20180101'")
    currentYearAcquisitions = currentYearAcquisitions.withColumn('acqSegment', lit('new2018'))
    currentYearAcquisitions = currentYearAcquisitions.withColumnRenamed('submission', 'installDate')
    currentYearAcquisitions = currentYearAcquisitions.withColumnRenamed('client_id', 'np_clientID')

    # 4 Determine current users who are new (acquired in current year) vs existing (acquired prior to current year)
    # #TODO: Due to new profiles only available for users with version 55+, this will lead to incorrect classification of new acquisitions
    # ## TODO: with older versions being classified as existing. This is fixable once we create an install table that is all encompassing of all versions

    # 4a Join currentYearAcquisitions to aggPageviews
    aggPageviews = aggPageviews.alias('aggPageviews')
    currentYearAcquisitions = currentYearAcquisitions.alias('currentYearAcquisitions')
    joinedData = aggPageviews.join(currentYearAcquisitions, col('aggPageviews.client_id') == col('currentYearAcquisitions.np_clientId'), 'left')

    # 4b In joined data convert nulls to existing for column acqSegment
    joinedData = joinedData.withColumn('acqSegment', when(joinedData.acqSegment.isNull(), 'existing').otherwise('new2018'))
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
    metrics = joinedData.groupBy('submission_date_s3', 'installDate', 'daysRetained', 'acqSegment').agg(countDistinct(joinedData.client_id).alias('DAU'),
                                                                                              sum(when(joinedData.totalURI >= 5, 1).otherwise(0)).alias('activeDAU'),
                                                                                              sum(joinedData.totalURI).alias('totalURI'))

    #7 Write file
    metrics.coalesce(1).write.option("header", "false").csv(
        's3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/gkabbz/retention/retention{}-{}.csv'.format(
            startPeriodString, endPeriodString))
    print("{}-{} completed and saved".format(startPeriodString, endPeriodString))

else:
    dayChunks = timedelta(days=7)
    currentPeriodStart = startPeriod
    currentPeriodEnd = startPeriod + dayChunks
    currentPeriodChunk = endPeriod - currentPeriodEnd

    while currentPeriodEnd <= endPeriod:
        startPeriodString = currentPeriodStart.strftime("%Y%m%d")
        endPeriodString = currentPeriodEnd.strftime("%Y%m%d")

        # 1b Create & filter mkgmainsummary and select desired fields
        print('starting loop')
        mkgMainSummary = mainSummaryTable.filter(
            "submission_date_s3 >= '{}' AND submission_date_s3 <= '{}'".format(startPeriodString,
                                                                               endPeriodString)).select(columns)

        # 2 Aggregate total pageviews by client ID
        #  TODO: include appversion. Needs to be split to major version then find the max and assign to that user for that day. Also include channel - be weary of users who switch from release to developer for example
        aggPageviews = mkgMainSummary.groupBy('submission_date_s3', 'client_id').agg(
            sum(mkgMainSummary.scalar_parent_browser_engagement_total_uri_count).alias('totalURI'))

        # 3 Find all current year acquisitions
        currentYearAcquisitions = newProfilesTable.select('submission', 'client_id')
        # TODO: Due to this dimension build, need to figure out how to feed the year based off of actual year of acquisition. Currently simply pulling for 2018.
        currentYearAcquisitions = currentYearAcquisitions.filter("submission >= '20180101'")
        currentYearAcquisitions = currentYearAcquisitions.withColumn('acqSegment', lit('new2018'))
        currentYearAcquisitions = currentYearAcquisitions.withColumnRenamed('submission', 'installDate')
        currentYearAcquisitions = currentYearAcquisitions.withColumnRenamed('client_id', 'np_clientID')

        # 4 Determine current users who are new (acquired in current year) vs existing (acquired prior to current year)
        # #TODO: Due to new profiles only available for users with version 55+, this will lead to incorrect classification of new acquisitions
        # ## TODO: with older versions being classified as existing. This is fixable once we create an install table that is all encompassing of all versions

        # 4a Join currentYearAcquisitions to aggPageviews
        aggPageviews = aggPageviews.alias('aggPageviews')
        currentYearAcquisitions = currentYearAcquisitions.alias('currentYearAcquisitions')
        joinedData = aggPageviews.join(currentYearAcquisitions,
                                       col('aggPageviews.client_id') == col('currentYearAcquisitions.np_clientId'),
                                       'left')

        # 4b In joined data convert nulls to existing for column acqSegment
        joinedData = joinedData.withColumn('acqSegment',
                                           when(joinedData.acqSegment.isNull(), 'existing').otherwise('new2018'))
        # to enable installDate column to be converted to a date in section 5 below, assigning nulls 19700101. Code errored out when blank
        joinedData = joinedData.withColumn('installDate', when(joinedData.installDate.isNull(), '19700101').otherwise(
            joinedData.installDate))

        # 5 Calculate days retained
        #  5a Convert submission_date_s3 and installDate to date format
        func = udf(lambda x: datetime.strptime(str(x), '%Y%m%d'), DateType())
        joinedData = joinedData.withColumn('submissionDate', func(col('submission_date_s3')))
        joinedData = joinedData.withColumn('installDateConv', func(col('installDate')))

        # 5b Calculate days retained
        joinedData = joinedData.withColumn('daysRetained',
                                           when(year(joinedData.installDateConv) == '1970', '').otherwise(
                                               datediff(joinedData.submissionDate, joinedData.installDateConv)))

        # 6 Aggregate including days retained and remove client_id as a dimension
        metrics = joinedData.groupBy('submission_date_s3', 'installDate', 'daysRetained', 'acqSegment').agg(
            countDistinct(joinedData.client_id).alias('DAU'),
            sum(when(joinedData.totalURI >= 5, 1).otherwise(0)).alias('activeDAU'),
            sum(joinedData.totalURI).alias('totalURI'))

        # 7 Write file
        metrics.coalesce(1).write.option("header", "false").csv(
            's3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/gkabbz/retention/retention{}-{}.csv'.format(
                startPeriodString, endPeriodString))

        print("{}-{} completed and saved".format(startPeriodString, endPeriodString))
        print('starting next loop')

        #8 Set new dates for next loop
        currentPeriodStart = currentPeriodEnd + timedelta(days=1)
        currentPeriodEnd = currentPeriodStart + dayChunks
        if currentPeriodStart > endPeriod:
            break
        else:
            if currentPeriodEnd > endPeriod:
                currentPeriodEnd = endPeriod

#9 Summarize installs per day from new profiles table and write to csv
installs = currentYearAcquisitions.groupBy('installDate').agg(countDistinct(currentYearAcquisitions.np_clientID).alias('installs'))
installs.coalesce(1).write.option("header", "false").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/retention/installs{}-{}.csv'.format(startPeriodString, endPeriodString))


# TODO Graveyeard - Clean before pushing to cluster
#aws s3 sync s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/gkabbz/retention /home/hadoop/sparkAnalysis/retention/dailyRetention
#rsync -av gkabbz-001:/home/hadoop/sparkAnalysis/retention/dailyRetention /Users/gkaberere/spark-warehouse/retention
#gsutil cp /Users/gkaberere/spark-warehouse/retention/dailyRetention/retention20190101-20190102.csv/retention20190101-20190102.csv gs://gkabbz-upload

#rsync -av /Users/gkaberere/Google\ Drive/Github/marketing-analytics/telemetry gkabbz-001:/home/hadoop/sparkAnalysis/mAnalytics/telemetryQueries

#for pulling down the installs summary from 9 above
#aws s3 sync s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/retention/installs20180826.csv /home/hadoop/sparkAnalysis/retention/dailyRetention/v2
#rsync -av gkabbz-001:/home/hadoop/sparkAnalysis/retention/dailyRetention /Users/gkaberere/spark-warehouse/retention/v2