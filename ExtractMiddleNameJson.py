# Find out middle names buried within these structures from a dataset filled with user profiles

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("ExtractMiddleName").getOrCreate()

data = [
    {
        "user_id": 1,
        "name": {
            "first": "John",
            "middle": "William",
            "last": "Doe"
        }
    },
    {
        "user_id": 2,
        "name": {
            "first": "Jane",
            "last": "Smith"
        }
    }
]

df = spark.createDataFrame(data)
df_Mname = df.withColumn("MiddleName", F.col("name.middle"))
df_Mname.show()