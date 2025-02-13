from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Joins").getOrCreate()

df1 = spark.read.options(header = "true", sep = ",").csv("file:///home/hadoop/spark/practdata/dataset_1.csv")
df2 = spark.read.options(header = "true", sep = ",").csv("file:///home/hadoop/spark/practdata/dataset_2.csv")

#df1.show()
#df2.show()

df_innerjoin = (df1.join(df2, on= ["id"], how= "inner")).select(df1["*"], df2["*"])
df_innerjoin.show()

df_leftjoin = (df1.join(df2, on= ["id"], how= "left_outer")).select(df1["*"], df2["*"])
df_leftjoin.show()

df_rightjoin = (df1.join(df2, on= ["id"], how= "right_outer")).select(df1["*"], df2["*"])
df_rightjoin.show()