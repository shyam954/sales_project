# main.py
import logging
from pyspark.sql import SparkSession
from lib.data_man import revenue_by_month, Product_Performance, preferred_category_per_customer
from lib.data_visual import plot_revenue_trend, plot_product_performance, plot_customer_heatmap

# Setup logging
logging.basicConfig(level=logging.INFO)

def main():
    spark = SparkSession.builder \
        .appName("SalesAnalysis") \
        .master("local[*]") \
        .getOrCreate()
    
    env = "Local"
    
    # Revenue trend
    rev_df = revenue_by_month(env, spark).toPandas()
    plot_revenue_trend(rev_df)
    logging.info("Revenue chart generated")   # log after plotting
    
    # Product performance
    prod_df = Product_Performance(env, spark).toPandas()
    plot_product_performance(prod_df)
    logging.info("Product performance chart generated")   # log after plotting
    
    # Customer preferences
    cust_df = preferred_category_per_customer(env, spark).toPandas()
    plot_customer_heatmap(cust_df)
    logging.info("Customer heatmap generated")   # log after plotting

if __name__ == "__main__":
    main()
