import regex
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, lower
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs://hw2-m/enwiki_whole.xml')
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
newdf = newdf.select(lower(col('title')).alias('title'), 'article').orderBy('title', 'article')
newdf.repartition(10).write.option("delimiter", "\t").csv('p1t2_whole')

