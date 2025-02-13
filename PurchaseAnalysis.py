from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("PurchaseAnalysis").getOrCreate()

data = [(1, 100, "2023-01-15"),
    (2, 150, "2023-02-20"),
    (1, 200, "2023-03-10"),
    (3, 50, "2023-04-05"),
    (2, 120, "2023-05-15"),
    (1, 300, "2023-06-25")]
schema = StructType([ \
    StructField("Cust_id",IntegerType(),True), \
    StructField("Purch_amt",IntegerType(),True), \
    StructField("Purch_dt",StringType(),True)
  ])
df = spark.createDataFrame(data=data, schema=schema)

tot_pur_per_cust = df.groupBy("Cust_id").agg(F.sum("Purch_amt").alias("Tot_pur_amt"))
tot_pur_per_cust.show() 