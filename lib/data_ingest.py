import configReader
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType


from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .master("local[*]") \
    .getOrCreate()




def order_schema():
    schema = StructType([
        StructField("OrderID", IntegerType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Product", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Price", FloatType(), True),
        StructField("OrderDate", StringType(), True)
    ])
    return schema

def read_orders (env,spark):
    filed=configReader.get_app_config(env)
    filepath=filed["data.file.path"]
    return spark.read.format("csv").option("header",True).schema(order_schema()).load(filepath)



