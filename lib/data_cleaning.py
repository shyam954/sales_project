


from pyspark.sql.functions import *
from lib import data_ingest


def clean_data(env,spark):
   df=data_ingest.read_orders(env,spark)
   df2= df.withColumn("OrderDate",to_date(col("OrderDate")))
   return df2


