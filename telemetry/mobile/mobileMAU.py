from pyspark.sql.functions import avg, col, count, first, min, max, sum, when, countDistinct
from pyspark.sql.window import Window

# Connect to table
mobile_clients = spark.read.option('mergeSchema', 'true').parquet('s3://net-mozaws-prod-us-west-2-pipeline-data/telemetry-core-parquet/v3')

# Reduce data to fields and values of interest
mobile = mobile_clients.filter("submission_date_s3 >= '20170101' AND submission_date_s3 <= '20171231'").select('submission_date_s3', 'client_id', 'app_name', 'os', 'osversion', 'seq', 'metadata.normalized_channel')
mobile = mobile.filter(mobile.os.isin("Android", "iOS") & mobile.app_name.isin('Fennec', 'Focus', 'Zerda') & mobile.normalized_channel.isin('release'))
mobile = mobile.withColumn('period', mobile.submission_date_s3.substr(1,6))

testMAU = mobile.groupBy('period','app_name', 'os').agg(countDistinct('client_id'))

# Find 2018/2017 Installs / new clients
installs = mobile_clients.filter("submission_date_s3 >= '20170101' AND submission_date_s3 <= '20171231' AND seq == '1'").groupBy(col('client_id').alias('installsClientID'), col('app_name'), col('os')).agg(min('submission_date_s3').alias('minSubmissionDateS3'))

# Join installs to MAU to get in year acquisitions
mobile = mobile.alias('mobile')
installs = installs.alias('installs')
mobileJoin = mobile.join(installs, (mobile.client_id == installs.installsClientID) & (mobile.app_name == installs.app_name) & (mobile.os == installs.os), 'left')
mobileJoin = mobileJoin.withColumn('acqSegment', when(mobileJoin.minSubmissionDateS3.isNull(), 'existing').otherwise('new2017'))

MAU = mobileJoin.groupBy('period','mobile.app_name', 'mobile.os', 'acqSegment').agg(countDistinct('client_id').alias('MAU'))
MAU.coalesce(1).write.option("header", "true").csv(
        's3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/adHoc/testMAUMobile2017.csv')

#aws s3 sync s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/adHoc /home/hadoop/sparkAnalysis/adHoc
#rsync -av gkabbz-001:/home/hadoop/sparkAnalysis/adHoc /Users/gkaberere/spark-warehouse/adHoc


# Calculate MAU by Client_id
window_28_days = Window\
    .partitionBy(col('client_id'))\
    .orderBy(col('submission_date_s3'))\
    .rowsBetween(Window.currentRow - 2, Window.currentRow) #adjust to 27 when done testing

mobile_mau_by_platform = mobile\
    .select('submission_date_s3', 'client_id', 'app_name', 'os', 'osversion')\
    .groupBy(['submission_date_s3', 'client_id'])\
    .agg(first(col('app_name')).alias('app_name'),
        first(col('os')).alias('os'),
        first(col('osversion')).alias('osversion'))\
    .withColumn('dau_28', count(col('submission_date_s3')).over(window_28_days))\
    .withColumn('is_mau', when(sum(col('dau_28')).over(window_28_days) >= 1, 1).otherwise(0))\
    .groupBy(['submission_date_s3', 'os', 'app_name'])\
    .agg(sum(col('is_mau')).alias('mau'))





# Find 2018 Installs / new clients
installs = mobile.filter("submission_date_s3 >= '20180101' AND seq == '1'").groupBy(col('client_id').alias('installsClientID')).agg(min('submission_date_s3').alias('minSubmissionDateS3'))
installs.persist()

# Join installs to MAU to get in year acquisitions
mobile = mobile.alias('mobile')
installs = installs.alias('installs')
mobileJoin = mobile.join(installs, (mobile.client_id == installs.installsClientID), 'left')
mobileJoin.persist()

# Aggregate MAU by day

testMAU = mobileJoin.filter("submission_date_s3 >= '20181127' AND app_name = 'Fennec' AND os = 'Android'").select('submission_date_s3', 'client_id', 'app_name', 'os', 'minSubmissionDateS3')
testMAU = testMAU.withColumn('acqSegment', when(testMAU.minSubmissionDateS3.isNull(), 'existing').otherwise('new2018'))
dailyDAU = testMAU.groupBy('submission_date_s3', 'acqSegment', 'app_name', 'os').agg(countDistinct('client_id').alias('dau'))



window_28_days = Window\
    .partitionBy(col('client_id'))\
    .orderBy(col('submission_date_s3'))\
    .rowsBetween(Window.currentRow - 27, Window.currentRow)

mobile_mau_by_platform = mobile_clients\
    .filter(col('submission_date_s3') >= '20141201' & col('app_name').isin('Fennec', 'Focus') & col('os').isin("Android", "iOS"))\
    .select('submission_date_s3', 'client_id', 'app_name', 'os', 'osversion')\
    .groupBy(['submission_date_s3', 'client_id'])\
    .agg(first(col('app_name')).alias('app_name'),
        first(col('os')).alias('os'),
        first(col('osversion')).alias('osversion'),
        first(col('device')).alias('device'))\
    .withColumn('dau_28', count(col('submission_date_s3')).over(window_28_days))\
    .withColumn('is_mau', when(sum(col('dau_28')).over(window_28_days) >= 1, 1).otherwise(0))\
    .groupBy(['submission_date_s3', 'os', 'app_name'])\
    .agg(sum(col('is_mau')).alias('mau'))

currentYearAcquisitions = currentYearAcquisitions.withColumn('acqSegment', lit('new2018'))

47,038,032,367
147,798,545
###############################


window_28_days = Window\
    .partitionBy(col('client_id'))\
    .orderBy(col('submission_date_s3'))\
    .rowsBetween(Window.currentRow - 27, Window.currentRow)

mobile_mau_by_platform = mobile_clients\
    .filter(col('submission_date_s3') >= '20141201')\
    .select('submission_date_s3', 'client_id', 'app_name', 'os', 'osversion', 'device')\
    .groupBy(['submission_date_s3', 'client_id'])\
    .agg(first(col('app_name')).alias('app_name'),
        first(col('os')).alias('os'),
        first(col('osversion')).alias('osversion'),
        first(col('device')).alias('device'))\
    .withColumn('dau_28', count(col('submission_date_s3')).over(window_28_days))\
    .withColumn('is_mau', when(sum(col('dau_28')).over(window_28_days) >= 1, 1).otherwise(0))\
    .groupBy(['submission_date_s3', 'os', 'app_name'])\
    .agg(sum(col('is_mau')).alias('mau'))



s3_path = 's3://<insert output path>'
mobile_mau_by_platform\
    .write\
    .mode('overwrite')\
.parquet(s3_path)
