#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'


# In[2]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_test.xml')


# In[3]:


df.show()


# In[4]:


df.printSchema()


# In[5]:


df.select('revision.text._VALUE').show()


# In[6]:


df.select('title').show()


# In[19]:


import re
from pyspark.sql.functions import udf, col, explode, array

def extractLink(text):
    results = re.findall(r'(?<=\[\[).*?(?=\]\])', text)
    output = []
    for res in results:
        if ':' in res and 'Category:' not in res:
            continue
        elif '#' in res:
            continue
        else:
            output.append(res.split('|')[0].lower())
    return output


# In[24]:


from pyspark.sql.types import StringType, ArrayType

link_udf = udf(lambda text: extractLink(text), ArrayType(StringType()))
newdf = df.withColumn("article", explode(link_udf(col("revision.text._VALUE"))))


# In[25]:


newdf.printSchema()


# In[33]:


newdf.select('title', 'article').repartition(1).write.option("delimiter", "\t").csv('test')

