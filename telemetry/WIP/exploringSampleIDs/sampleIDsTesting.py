from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('clientsDaily').getOrCreate()
clientsDaily = spark.read.option('mergeSchema', 'True').parquet('s3://telemetry-parquet/clients_daily/v6/')

# Add yearMonth column to allow quick filtering
#By profile creation date
clientsDaily2 = clientsDaily.withColumn('yearMonth', concat(clientsDaily.profile_creation_date.substr(1, 7)))
# By Submission date s3
clientsDaily2 = clientsDaily.withColumn('yearMonth', concat(clientsDaily.submission_date_s3.substr(1, 6)))

# Select Apr 2018 for acquisitions
apr2018 = clientsDaily2.filter((clientsDaily2['yearMonth']) == '2018-04')

# Select Apr 2018 for submission Date s3
apr2018 = clientsDaily2.filter((clientsDaily2['yearMonth']) == '201804')

# Select Acquisitions by Sample IDs based on profile_creation_date
aggApr2018Country = apr2018.groupBy('profile_creation_date', 'sample_id', 'country').agg(countDistinct('client_id'))

# Select DAU by submission Date S3 by sample IDs
apr2018DAU = apr2018.groupBy('submission_date_s3', 'sample_id').agg(countDistinct('client_id'))
apr2018DAU.write.csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/aprDAUBySampleID2.csv', header=False)

# Select DAU by submission Date S3 By sample ID and country
apr2018DAUCountry = apr2018.groupBy('submission_date_s3', 'sample_id', 'country').agg(countDistinct('client_id'))
apr2018DAUCountry.write.csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/aprDAUBySampleIDCountry2.csv', header=False)

# Select DAU by submission Date S3 By sample ID, country and os
apr2018DAUCountryOS = apr2018.groupBy('submission_date_s3', 'sample_id', 'country', 'os').agg(countDistinct('client_id'))
apr2018DAUCountryOS.write.csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/aprDAUBySampleIDCountryOS2.csv', header=False)

# Are sample IDs assigned to the same client ID each time
febMar2018 = clientsDaily2.filter((clientsDaily2['yearMonth'] == '201803') | (clientsDaily2['yearMonth'] == '201804'))



aggApr2018Country.write.csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/sampleIDByCountry.csv')
aggApr2018Country.write.csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/sampleIDByCountry2.csv')
aggApr2018CountryS3.write.csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/sampleIDByCountryS3Date.csv')