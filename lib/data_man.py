
from pyspark.sql.functions import * 
from pyspark.sql.window import Window

from lib import data_ingest, data_cleaning






def get_peak_months(env,spark):
    df=data_cleaning.clean_data(env,spark)
    df2= df.withColumn("month",date_format(col("OrderDate"), "MMMM")).withColumn("year",year("OrderDate"))
    df3=df2.withColumn("totalqp",col("quantity")*col("price"))
    gdf=df3.groupBy("month","year","Category").agg(sum("totalqp").alias("totalct"))
    w = Window.partitionBy("Category").orderBy(col("totalct").desc())
    wdf=gdf.withColumn("rank",dense_rank().over(w)).where(col("rank")==1).drop("rank")
    return wdf



def preferred_category_per_customer(env,spark):
     df=data_cleaning.clean_data(env,spark)
     df2=df.withColumn("totalqp",col("quantity")*col("price"))
     df3=df2.groupBy("CustomerID","Category").agg(sum("totalqp").alias("total"))
     w = Window.partitionBy("CustomerID")
     df4=df3.withColumn("maxspent",max("total").over(w)).where(col("total")==col("maxspent")).drop("total")
   
     return df4



#story 2

def repeat_customers(env,spark):
     df=data_cleaning.clean_data(env,spark)
     df1=df.withColumn("revenue",col("Quantity")*col("price"))
     df2=df1.groupBy("CustomerID").agg(sum("revenue").alias("total_revenue"),count("CustomerID").alias("repeated_times"))
     w=Window.orderBy(col("total_revenue").desc())
     df3=df2.withColumn("rank",dense_rank().over(w)).where(col("rank")<=4)
     return df3




def preferred_product(env,spark):
     df=data_cleaning.clean_data(env,spark)
     df1=df.groupBy("CustomerID","Category").agg(count("Category").alias("total_times_buyed"))
     return df1



def Product_Performance(env,spark):
    df=data_cleaning.clean_data(env,spark)
    df1=df.withColumn("revenue",col("quantity")*col("price"))
    
    df2=df1.groupBy("product").agg(sum("revenue").alias("total_rev"),count("orderid").alias("order_count"))
   
    w=Window.orderBy(col("total_rev").desc())
    df3=df2.withColumn("rank",dense_rank().over(w))
    df4 = df3.withColumn(
        "performance",
        when((col("total_rev") >= 5000) & (col("order_count") >= 5), "High Performer")
        .when((col("total_rev") >= 2000) & (col("order_count") >= 3), "Moderate Performer")
        .otherwise("Low Performer")
    )
    df5 = df4.withColumn(
        "recommendation",
        when(col("performance") == "High Performer", "Stock More")
        .when(col("performance") == "Moderate Performer", "Monitor Closely")
        .otherwise("Consider Discontinue")
    )
    return df5

     
def revenue_by_month(env,spark):
     df=data_cleaning.clean_data(env,spark)
     df1= df.withColumn("month",month(col("OrderDate"))).withColumn("year",year("OrderDate"))
     df2=df1.withColumn("totalqp",col("quantity")*col("price"))
     df3=df2.groupBy("month","year").agg(sum("totalqp").alias("trpm")).orderBy(col("month"))
     w=Window.partitionBy("year").orderBy("month")
     wdf=df3.withColumn("lag",lag("trpm",1,0).over(w))
     wdf1=wdf.withColumn("perchange",((col("trpm")-col("lag"))/col("trpm")))
     return wdf1


