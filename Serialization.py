from pyspark import SparkContext
from pyspark.serializers import MarshalSerializer

sc = SparkContext("local", "Serialized App", serializer= MarshalSerializer())
pr_data = sc.parallelize(list(range(100)))
sr_data = pr_data.map(lambda x : 2 * x)
top_10 = sr_data.take(10)

print("The Serialized output is : "+str(top_10))
sc.stop()


