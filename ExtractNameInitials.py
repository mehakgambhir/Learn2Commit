# From a dataset brimming with user profiles, each containing full name strings, extract initials of full names

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("NameInitialsExtraction").getOrCreate()
data = [{"user_id": 1, "full_name": "John Doe"}, {"user_id": 2, "full_name": "Jane Smith"},
        {"user_id": 3, "full_name": "Michael Johnson"}]
df = spark.createDataFrame(data)

df_with_initials = df.withColumn("first_name_initial", F.expr("substring(full_name, 1, 1)"))
df_with_initials = df_with_initials.withColumn("last_name_initial",
                                               F.expr("substring(full_name, instr(full_name, ' ') + 1, 1)"))
df_with_initials.select(F.col("full_name"), F.col("user_id"),
                        F.concat(F.col("first_name_initial"), F.col("last_name_initial")).alias("name_initial")).show()
