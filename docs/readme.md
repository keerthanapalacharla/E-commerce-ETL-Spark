from pyspark.sql.functions import col, to_date

# Cast date column properly
clean_df = raw_df.withColumn("order_date", to_date(col("order_date")))

# Ensure amount is float type
clean_df = clean_df.withColumn("amount", col("amount").cast("double"))

print("Cleaned Data")
clean_df.show()
======================================
Cleaned Data
+----------+-----------+-----------+------+
|order_date|customer_id|   category|amount|
+----------+-----------+-----------+------+
|2025-08-01|       C001|Electronics|1200.5|
|2025-08-01|       C002|      Books| 350.0|
|2025-08-02|       C001|   Clothing|800.75|
|2025-08-02|       C003|Electronics|2200.0|
|2025-08-03|       C002|   Clothing| 560.0|
|2025-08-03|       C004|      Books| 125.0|
|2025-08-04|       C001|Electronics|499.99|
|2025-08-04|       C005|   Clothing|1499.0|
+----------+-----------+-----------+------+
