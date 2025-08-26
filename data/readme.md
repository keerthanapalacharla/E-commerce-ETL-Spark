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
=============================================
Raw Data
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
