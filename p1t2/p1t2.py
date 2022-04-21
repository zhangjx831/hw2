import regex
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, lower
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs://hw2-m/enwiki_test.xml')
def extractLink(text):
    try:
        results = regex.findall(r'\[\[((?:[^[\]]+|(?R))*+)\]\]', text)
    except:
        results = []
    output = []
    for res in results:
        for link in res.split('|'):
            if ':' in link and 'Category:' not in link:
                continue
            elif '#' in link:
                continue
            else:
                output.append(link.lower())
                break
    return output

link_udf = udf(lambda text: extractLink(text), ArrayType(StringType()))
newdf = df.withColumn("article", explode(link_udf(col("revision.text._VALUE"))))
<<<<<<< HEAD:hw2t2.py
newdf.select('title', 'article').repartition(10).write.option("delimiter", "\t").csv('p2t2_test')
=======
newdf = newdf.select(lower(col('title')).alias('title'), 'article').orderBy('title', 'article')
newdf.repartition(10).write.option("delimiter", "\t").csv('p1t2_whole')

>>>>>>> 71c0b8da0f84eebcc00e598c5e161d99957567cc:p1t2/p1t2.py
