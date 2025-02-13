from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadIntoDF").getOrCreate()

tab_data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data = tab_data, schema = columns)
df.createOrReplaceTempView("Person_Data")
df_1 = spark.sql("select * from Person_Data")
group_df = spark.sql("select gender, count(*) as count from Person_Data group by gender")
df_1.show()
group_df.show()


