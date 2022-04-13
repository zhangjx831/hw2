#!/usr/bin/env python
# coding: utf-8


import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_whole.xml')



import re
from pyspark.sql.functions import udf, col, explode, array

def extractLink(text):
    results = re.findall(r'(?<=\[\[).*?(?=\]\])', text)
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




from pyspark.sql.types import StringType, ArrayType

link_udf = udf(lambda text: extractLink(text), ArrayType(StringType()))
newdf = df.withColumn("article", explode(link_udf(col("revision.text._VALUE"))))



newdf.select('title', 'article').repartition(1).write.option("delimiter", "\t").csv('whole')

