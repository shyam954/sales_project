import matplotlib.pyplot as plt
import seaborn as sns
from lib import data_man
from lib import data_ingest




import matplotlib.pyplot as plt
import seaborn as sns

def plot_revenue_trend(df):
    plt.figure(figsize=(10,6))
    plt.plot(df['month'], df['trpm'], marker='o', linestyle='-')
    plt.title("Monthly Revenue Trend")
    plt.xlabel("Month")
    plt.ylabel("Total Revenue")
    plt.grid(True)
    plt.show()

def plot_product_performance(df):
    plt.figure(figsize=(12,6))
    plt.bar(df['product'], df['total_rev'], color='skyblue')
    plt.xticks(rotation=45)
    plt.title("Product Revenue Performance")
    plt.xlabel("Product")
    plt.ylabel("Total Revenue")
    plt.show()

def plot_customer_heatmap(df):
    heatmap_data = df.pivot_table(index='CustomerID', columns='Category', values='maxspent', fill_value=0)
    plt.figure(figsize=(10,8))
    sns.heatmap(heatmap_data, annot=True, cmap="YlGnBu")
    plt.title("Customer Spending Heatmap by Category")
    plt.show()
