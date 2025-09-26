import matplotlib.pyplot as plt
import seaborn as sns
from data_man import revenue_by_month, Product_Performance, preferred_category_per_customer, spark
import data_ingest
# ----- Revenue Trend -----
rev_month = revenue_by_month("Local", spark).toPandas()
plt.figure(figsize=(10,6))
plt.plot(rev_month['month'], rev_month['trpm'], marker='o', linestyle='-')
plt.title("Monthly Revenue Trend")
plt.xlabel("Month")
plt.ylabel("Total Revenue")
plt.grid(True)
plt.show()

# ----- Product Performance -----
prod_perf = Product_Performance("Local", spark).toPandas()
plt.figure(figsize=(12,6))
plt.bar(prod_perf['product'], prod_perf['total_rev'], color='skyblue')
plt.xticks(rotation=45)
plt.title("Product Revenue Performance")
plt.xlabel("Product")
plt.ylabel("Total Revenue")
plt.show()

# ----- Customer Preferences Heatmap -----
cust_pref = preferred_category_per_customer("Local", spark).toPandas()
heatmap_data = cust_pref.pivot_table(index='CustomerID', columns='Category', values='maxspent', fill_value=0)

plt.figure(figsize=(10,8))
sns.heatmap(heatmap_data, annot=True, cmap="YlGnBu")
plt.title("Customer Spending Heatmap by Category")
plt.show()