
import data_ingest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .master("local[*]") \
    .getOrCreate()

def clean_data(env,spark):
   df=data_ingest.read_orders(env,spark)
   df2= df.withColumn("OrderDate",to_date(col("OrderDate")))
   return df2

