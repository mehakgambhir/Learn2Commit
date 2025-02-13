from pyspark.sql import SparkSession

# create a SparkSession which internally creates a SparkContext for you
spark = SparkSession.builder.appName("Parallelize").getOrCreate()
sc = spark.sparkContext

# create rdd from list of collection
rdd = sc.parallelize([1,2,3,4,5])
collect_rdd = rdd.collect()
empty_rdd = sc.parallelize([])
part_rdd = sc.parallelize(range(0,25),6)
#part_rdd.saveAsTextFile("file:///home/hadoop/spark/practdata/partition")
repart_rdd = part_rdd.repartition(4)
#repart_rdd.saveAsTextFile("file:///home/hadoop/spark/practdata/re-partition")
coal_rdd = part_rdd.coalesce(4)
coal_rdd.saveAsTextFile("file:///home/hadoop/spark/practdata/coalesce")


print("No. of Partitions: "+str(rdd.getNumPartitions()))
print("First Element: "+str(rdd.first()))
print("RDD is Empty: "+str(empty_rdd.isEmpty()))
print("No. of Partitions in Range: "+str(part_rdd.getNumPartitions()))
print("No. of Partitions in Re-partition: "+str(repart_rdd.getNumPartitions()))
print("No. of Partitions in Coalesce: "+str(coal_rdd.getNumPartitions()))
