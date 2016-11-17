
'''
I will try to improve this part later...
This is just a copy from pyspark/shell.py
'''
import os
import platform
import py4j
import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.storagelevel import StorageLevel
if os.environ.get("SPARK_EXECUTOR_URI"):
    SparkContext.setSystemProperty("spark.executor.uri", os.environ["SPARK_EXECUTOR_URI"])

SparkContext._ensure_initialized()

try:
    # Try to access HiveConf, it will raise exception if Hive is not added
    SparkContext._jvm.org.apache.hadoop.hive.conf.HiveConf()
    spark = SparkSession.builder\
        .enableHiveSupport()\
        .getOrCreate()
except py4j.protocol.Py4JError:
    spark = SparkSession.builder.getOrCreate()
except TypeError:
    spark = SparkSession.builder.getOrCreate()
'''
pyspark end
'''


## work in pyspark
import json
import os
import pyspark
from datetime import date, timedelta as td
from pyspark.sql.types import StringType
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.functions import *

## get days
start_date = date(2015, 02, 17)
end_date = date(2015, 03, 26)
region = 'us'


delta = end_date - start_date
## all days
days = [ (start_date + td(days=x)).strftime("%Y%m%d")  for x in range(delta.days + 1)]


# day = '20150216'
for day in days:
    try:
        file_path =  "s3n://log.sharethis.com/amankesarwani/" + region + "/" + str(day) + "/part-00000000000[0]*"
        #file_path =  "s3n://log.sharethis.com/amankesarwani/us/20160324/part-000000000000*"
        to_path = "s3n://log.sharethis.com/results/" + region + "/" + str(day)

        sharethis_json = spark.read.json(file_path) ## spark 2.0
        # sharethis_json.printSchema()  # take a look of the sql structure
        # sharethis_json.cache()  # cache it
        temp_json_raw = sharethis_json.select('*',explode(col('companies')).alias('tempCol')).drop('companies')
        json_cleaned = (temp_json_raw.select( '*' ,temp_json_raw.tempCol.getItem("count").alias('company_count'),
            temp_json_raw.tempCol.getItem("sentiment_score").alias('company_sentiment_score'),
            temp_json_raw.tempCol.getItem("ric").alias('company_ric'))
            .drop('tempCol').drop('stid').drop('url').drop('userAgent')
            .filter(col('company_ric').isNotNull())
            .filter(col('company_sentiment_score').isNotNull())
            )
            #pyspark.sql.DataFrameWriter(json_cleaned).save(to_path)
        json_cleaned.write.csv(to_path, compression = 'bzip2', mode = 'overwrite')
        print(day+' done')

    except:
        pass


# device_categories = json_cleaned.select("deviceType").distinct().rdd.flatMap(lambda x: x).collect()
device_categories = [u'Personal computer', u'Tablet', u'Smartphone']


refDomain_categories = json_cleaned.groupBy("refDomain").count().orderBy(desc("count"))\
                        .select("refDomain").rdd.flatMap(lambda x: x).collect()


refDomain_categories_filter = []
for x in refDomain_categories:
    try:
        temp = x.split('.')[-2]
        if temp not in refDomain_categories_filter:
            refDomain_categories_filter.append(temp)
    except:
        pass



exprs_device = [F.when(F.col("deviceType") == category, 1).otherwise(0).alias("is_device_"+category)
         for category in device_categories]
exprs_domain = [F.when(F.col("refDomain") == category, 1).otherwise(0).alias("is_domain_"+category)
         for category in refDomain_categories_filter[0:100]]

labeled_json_cleaned = json_cleaned.select("*", *exprs_device ) \
        .select("*", *exprs_domain).drop("deviceType").drop("refDomain")
