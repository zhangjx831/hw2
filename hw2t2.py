import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs://hw2-m/enwiki_whole.xml')
def extractLink(text):
    try:
        results = re.findall(r'(?<=\[\[).*?(?=\]\])', text)
    except:
        results = []
    output = []
    for res in results:
        if ':' in res and 'Category:' not in res:
            continue
        else:
            temp = res.split('|') #all links
            valid_links = [ x.lower() for x in temp if "#" not in x]
            if len(valid_links)==0:
                continue
            else:
                output.append(valid_links[0])
    return output

link_udf = udf(lambda text: extractLink(text), ArrayType(StringType()))
newdf = df.withColumn("article", explode(link_udf(col("revision.text._VALUE"))))
newdf.select('title', 'article').repartition(10).write.option("delimiter", "\t").csv('p2t2_whole')

