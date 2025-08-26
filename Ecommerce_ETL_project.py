# Databricks notebook source
# Simulated raw e-commerce transactions
data = [
    ("2025-08-01", "C001", "Electronics", 1200.50),
    ("2025-08-01", "C002", "Books", 350.00),
    ("2025-08-02", "C001", "Clothing", 800.75),
    ("2025-08-02", "C003", "Electronics", 2200.00),
    ("2025-08-03", "C002", "Clothing", 560.00),
    ("2025-08-03", "C004", "Books", 125.00),
    ("2025-08-04", "C001", "Electronics", 499.99),
    ("2025-08-04", "C005", "Clothing", 1499.00)
]

columns = ["order_date", "customer_id", "category", "amount"]

raw_df = spark.createDataFrame(data, columns)
print("Raw Data")
raw_df.show()


# COMMAND ----------

from pyspark.sql.functions import col, to_date

# Cast date column properly
clean_df = raw_df.withColumn("order_date", to_date(col("order_date")))

# Ensure amount is float type
clean_df = clean_df.withColumn("amount", col("amount").cast("double"))

print("Cleaned Data")
clean_df.show()

# COMMAND ----------

#1. Total Sales by Category



sales_by_category = clean_df.groupBy("category").sum("amount")
sales_by_category.show()

#2. Daily Revenue Trend



daily_sales = clean_df.groupBy("order_date").sum("amount").orderBy("order_date")
daily_sales.show()

#3. Top Customers



top_customers = clean_df.groupBy("customer_id").sum("amount").orderBy(col("sum(amount)").desc())
top_customers.show()

# COMMAND ----------

# Save results as managed tables inside Databricks CE
from pyspark.sql.functions import col

sales_by_category_clean = sales_by_category.withColumnRenamed(
    "sum(amount)", "total_amount"
)
daily_sales_clean = daily_sales.withColumnRenamed(
    "sum(amount)", "total_amount"
)
top_customers_clean = top_customers.withColumnRenamed(
    "sum(amount)", "total_amount"
)

sales_by_category_clean.write.mode("overwrite").saveAsTable("sales_by_category")
daily_sales_clean.write.mode("overwrite").saveAsTable("daily_sales")
top_customers_clean.write.mode("overwrite").saveAsTable("top_customers")
