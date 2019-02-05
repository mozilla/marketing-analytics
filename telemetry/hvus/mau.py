from pyspark.sql.functions import col, count, countDistinct, sum, when

## Read main_summary
main_summary = spark\
    .read\
    .option('mergeSchema', 'true')\
    .parquet('s3://telemetry-parquet/main_summary/v4/')

## Static window over last 28 days of October
main_summary_oct = main_summary\
    .filter(col('submission_date_s3') >= '20181004')\
    .filter(col('submission_date_s3') <= '20181031')\
    .select('submission_date_s3', 'client_id', 'country', 'scalar_parent_browser_engagement_total_uri_count')\
    .groupBy('submission_date_s3', 'client_id', 'country')\
    .agg(sum(col('scalar_parent_browser_engagement_total_uri_count')).alias('sum_uri_count'))\
    .withColumn('is_adau', when(col('sum_uri_count') >= 5, 1).otherwise(0))

## MAU as of 10/31 segmented into US, CA, DE
mau_clients = main_summary_oct\
    .groupBy('submission_date_s3', 'client_id')\
    .agg(count(col('submission_date_s3')).alias('dau'))\
    .groupBy('client_id')\
    .agg(sum(col('dau')).alias('sum_dau'))\
    .filter(col('sum_dau') >= 1)

main_summary_oct\
    .join(mau_clients, main_summary_oct.client_id == mau_clients.client_id)\
    .groupBy('country')\
    .agg(countDistinct(main_summary_oct.client_id))\
    .filter(col('country').isin(['US', 'DE', 'CA']))\
    .show()
