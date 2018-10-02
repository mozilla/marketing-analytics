from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('mainSummary').getOrCreate()
sampleData = spark.read.csv('/Users/gkaberere/spark-warehouse/weeklyReporting/dimensionedFinal/dimensioned/20180703-20180708/20180703-20180708.csv', inferSchema=True)

sampleData.show(20)