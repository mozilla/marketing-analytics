#TODO: There are two tables currently being used. Churn v2 and churn v3. Find clarity on which is the correct table to be using
#TODO: Noticing a discrepancy in the weekly cohort number. This number should match up to installs but from high level looks, it looks like it does not match installs I'm currently reporting. Which one is right????
#TODO: File a bug to terminate the use of whichever table is not accurate to avoid confusion among teams

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DateType, StringType
from datetime import timedelta, datetime

# 1 Connect to new profiles and churn v3 tables
spark = SparkSession.builder.appName('retention').getOrCreate()
newProfilesTable = spark.read.option('mergeSchema', 'true').parquet('s3://net-mozaws-prod-us-west-2-pipeline-data/telemetry-new-profile-parquet/v2/')

# 2 Calculate installs per week from the new profiles table
installs = newProfilesTable.select('submission', 'metadata.geo_country', 'environment.settings.update.channel', 'client_id')
installs = installs.groupBy('submission', 'geo_country', 'channel').agg(countDistinct(newProfilesTable.client_id).alias('installs'))

#2b Convert date stored as string to date type, calculate week of year, assign minimum date to week
func = udf(lambda x: datetime.strptime(str(x), '%Y%m%d'), DateType())
installs = installs.withColumn('date', func(col('submission')))

#2c Ingest table with cohort information starting Sunday (weekofyear resulted in a monday start date which isn't consistent with rest of org)
cohortTable = spark.read.option('mergeSchema', 'true').csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/weeklyCohorts/weeklyCohortsv2.csv', header=True)

#2d Join cohort table to installs table to append cohorts
installs = installs.alias('installs')
cohortTable = cohortTable.alias('cohortTable')
installsJoin = installs.join(cohortTable, (installs.date == cohortTable.date), 'left')

#2e Aggregate installs by week
installsByWeek = installsJoin.groupBy('cohort', 'geo_country', 'channel').agg(sum(installsJoin.installs).alias('installs'))
installsByWeek = installsByWeek.groupBy('cohort', 'channel').agg(sum(installsByWeek.installs).alias('installs'))

#2f Filter for release
installsRelease = installsByWeek.filter(installsByWeek['channel'] == 'release').select('cohort', 'geo_country', 'channel', 'installs')
installsRelease = installsRelease.groupBy('cohort', 'channel').agg(sum(installsRelease.installs).alias('installs'))

#3 Write file to csv
installsRelease.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/releaseInstallsByWeek.csv')

installsByWeek.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/installsByWeek.csv')



# TODO Graveyeard - Clean before pushing to cluster
aws s3 sync s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/installsByWeek.csv /home/hadoop/sparkAnalysis/retention
rsync -av gkabbz-001:/home/hadoop/sparkAnalysis/retention /Users/gkaberere/spark-warehouse/retention

rsync -av /Users/gkaberere/projects/weeklyCohortsv2.csv gkabbz-001:/home/hadoop/sparkAnalysis/weeklyCohorts

aws s3 sync /home/hadoop/sparkAnalysis/weeklyCohorts s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/weeklyCohorts