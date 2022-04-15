from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.getOrCreate()
df = spark.read.option("delimiter", "\t").csv('hdfs://hw2-m/user/root/p2t2_whole')
df_count = df.groupBy("_c0").count()
newdf = df_count.withColumn("rank", lit(1))
newdf = newdf.rdd.map(lambda x: (x[0], x[1], x[2], x[2]/x[1])).toDF(['article', 'count', 'rank', 'contribution'])
for _ in range(10):
    newdf = df.join(newdf, df._c1 == newdf.article, 'inner')
    newdf = newdf.groupBy(['_c0', 'count']).sum('contribution')
    newdf = newdf.rdd.map(lambda x: (x[0], x[1], 0.15 + 0.85 * x[2],
                                     (0.15 + 0.85 * x[2])/x[1])).toDF(['article', 'count', 'rank', 'contribution'])
newdf.sort(newdf.rank.asc()).select('article', 'rank').repartition(10).write.option("delimiter", "\t").csv('p2t3_whole')
