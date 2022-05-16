#!/usr/bin/env python
# coding: utf-8

# In[ ]:

!pip install pyspark
!pip install pandas
import csv
import json
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)
spark
from pyspark.sql.types import DateType, IntegerType, MapType, StringType, FloatType

keyfood_nyc_stores = json.load(open('keyfood_nyc_stores.json','r'))
Sample_items = dict(map(lambda x: (x[0].split('-')[-1], x[1]), pd.read_csv('keyfood_sample_items.csv').to_numpy()))
udfGetName = F.udf(lambda x: Sample_items.get(x.split('-')[-1],None), T.StringType())
udfGetPrice = F.udf(lambda x: float(x.split()[0].lstrip('$')), T.FloatType())
udfGetScore = F.udf(lambda x: keyfood_nyc_stores[x]['foodInsecurity']*100, T.FloatType())
from re import escape
outputTask1 = spark.read.csv('keyfood_products.csv',
                         header = True, escape = '"')\
                         .select(udfGetName('upc').alias('name'), udfGetPrice('price'), udfGetScore('store'))\
                         .dropna(subset = ['name'])

outputTask1.to_csv('input',sep='\t',index=False)

if __name__=='__main__':
  
  sc = SparkContext()
  sc.textFile('input', use_unicode=True)         .saveAsTextFile('output')

