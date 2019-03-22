from pyspark.sql.functions import avg, col, count, first, min, max, sum, when, countDistinct
from pyspark.sql.window import Window

# Connect to table
mobile_clients = spark.read.option('mergeSchema', 'true').parquet('s3://net-mozaws-prod-us-west-2-pipeline-data/telemetry-core-parquet/v3')

mobile = mobile_clients.filter("submission_date_s3 >= '20190101' AND submission_date_s3 <= '20190101'").select('submission_date_s3', 'client_id', 'metadata.geo_country', 'os', 'app_name')
