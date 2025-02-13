# Calculate the standard deviation of stock prices within each sector from a large financial dataset containing information about various stocks
# using PySpark's Aggregate and STDDEV functions

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window as W

spark = SparkSession.builder.appName("FinAnalysis").getOrCreate()

data = [("AAPL", "Tech", 150.00),
    ("AAPL", "Tech", 155.00),
    ("MSFT", "Tech", 200.00),
    ("MSFT", "Tech", 205.00),
    ("GOOG", "Tech", 250.00)]

columns = ["stock_symbol", "sector", "closing_price"]
df = spark.createDataFrame(data, columns)

win_spec = W.partitionBy("sector")
df_stddev = df.withColumn("Price_StdDev", F.stddev("closing_price").over(win_spec))
df_stddev.show(truncate= False)


# The window specification is based on sectors. The Aggregate and STDDEV functions within PySpark enable the calculation of the standard deviation of stock prices
# within each sector. This analysis can provide insights into the level of risk associated with different industries.



