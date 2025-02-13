from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("LAGFunctionExample").getOrCreate()

data = [("HDFC", "2023-07-01", 150.00), ("HDFC", "2023-07-02", 155.00), ("HDFC", "2023-07-03", 160.00)]
columns = ["stock_symbol", "date", "closing_price"]
df = spark.createDataFrame(data, columns)

window_spec = Window.partitionBy("stock_symbol").orderBy("date")
df_with_lag = df.withColumn("previous_day_price", F.lag("closing_price").over(window_spec))
df_with_lag = df_with_lag.withColumn("price_change", F.col("closing_price") - F.col("previous_day_price"))
df_with_lag.show(truncate=False)