from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.getOrCreate()
df = spark.read.option("delimiter", "\t").csv('hdfs://hw2-m/user/root/p1t2_whole')
df_count = df.groupBy("_c0").count()
newdf = df_count.withColumn("rank", lit(1))
newdf = newdf.rdd.map(lambda x: (x[0], x[1], x[2], x[2]/x[1])).toDF(['article', 'count', 'rank', 'contribution'])
for _ in range(10):
    newdf = df.join(newdf, df._c0 == newdf.article, 'inner')
    newdf = newdf.groupBy(['_c1']).sum('contribution').rdd.map(lambda x: (x[0], 0.15 + 0.85 * x[1])).toDF(['article', 'rank'])
    newdf = df_count.join(newdf, df._c0 == newdf.article, 'left')
    newdf = newdf.na.fill(0.15)
    newdf = newdf.rdd.map(lambda x:(x[0], x[1], x[3], x[3]/x[1])).toDF(['article', 'count', 'rank', 'contribution'])
newdf = newdf.select('article', 'rank').orderBy('rank')
newdf.repartition(10).write.option("delimiter", "\t").csv('p1t3_whole')
