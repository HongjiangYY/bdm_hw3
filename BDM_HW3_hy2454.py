#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import csv
import json
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import IPython
get_ipython().run_line_magic('matplotlib', 'inline')
IPython.display.set_matplotlib_formats('svg')
pd.plotting.register_matplotlib_converters()


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)
spark
from pyspark.sql.types import DateType, IntegerType, MapType, StringType, FloatType

#get keyfood_sample_items dataframe with valid upc code (only number after - )
keyfood_sample_items = spark.read.load('keyfood_sample_items.csv', format='csv', header=True, inferSchema=True)
keyfood_sample_items = keyfood_sample_items.withColumn('UPC code', (F.split(keyfood_sample_items['UPC code'], "-"))[1])


# get keyfood_products datafrome with vliad output for upc and price columns
keyfood_products = spark.read.load('keyfood_products.csv', format='csv', header=True, inferSchema=True)
keyfood_products = keyfood_products.withColumn('upc', (F.split(keyfood_products['upc'], "-"))[1])

def get_price(price):
  i = 4
  while i < len(price) and (price[i] in ['1','2','3','4','5','6','6','8', '9','0','.']):
    i += 1
  return float(price[1:i])

udfExpand = F.udf(get_price, FloatType())
keyfood_products_new = keyfood_products.select('store', 'department', 'upc', 'product',
                 udfExpand(keyfood_products['price']).alias('Price($)'))
#keyfood_products_new.show()

# get keyfood_nyc_stores dataframe wih valid output for Food Insecurity column
keyfood_nyc_stores = pd.read_json('keyfood_nyc_stores.json')
keyfood_nyc_stores2 = keyfood_nyc_stores.iloc[[1,29,30]]
keyfood_nyc_stores3 = keyfood_nyc_stores2.T
keyfood_nyc_stores3.reset_index(drop=True, inplace=True)
keyfood_nyc_stores_new = spark.createDataFrame(keyfood_nyc_stores3)


def get_Insecurity(insecurity):
  return insecurity*100
udfExpand = F.udf(get_Insecurity, FloatType())
keyfood_nyc_stores_new = keyfood_nyc_stores_new.select('name', 'communityDistrict',
                 udfExpand(keyfood_nyc_stores_new['foodInsecurity']).alias('Food Insecurity'))

#keyfood_nyc_stores_new.show()

#join tables and return dataframe with valid coumns (Item Name, Price($) and Food Insecurity)
dfTask1 = keyfood_sample_items.join(keyfood_products_new, keyfood_sample_items['UPC code'] == keyfood_products_new['upc'], how='inner')
dfTask2 = dfTask1.join(keyfood_nyc_stores_new, dfTask1.store == keyfood_nyc_stores_new.name, how = 'left' )
outputTask1 = dfTask2.select('Item Name', 'Price($)', 'Food Insecurity')


outputTask1.to_csv('input',sep='\t',index=False)

if __name__=='__main__':
  
  sc = SparkContext()
  sc.textFile('input', use_unicode=True)         .saveAsTextFile('output')

