from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DateType, StringType
from datetime import timedelta, datetime

# Connect to Main Summary Table
spark = SparkSession.builder.appName('mainSummary').getOrCreate()
mainSummaryTable = spark.read.option('mergeSchema', 'true').parquet('s3://telemetry-parquet/main_summary/v4/')

mauDF = mainSummaryTable.filter("app_name = 'Firefox' AND normalized_channel = 'release' AND submission_date_s3 >= '20180501' AND submission_date_s3 <= '20180531'").select('submission_date_s3', 'client_id')

mau = mauDF.agg(countDistinct(mauDF.client_id).alias('MAU')