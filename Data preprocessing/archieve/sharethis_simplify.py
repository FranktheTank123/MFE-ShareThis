
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
from pyspark.sql.functions import explode, udf, col
from pyspark.sql.types import StringType
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.storagelevel import StorageLevel
'''
4 general functions
'''
def get_company_count_(s):
    try:
        return s[0]
    except:
        pass

def get_company_ric_(s):
    try:
        return s[1]
    except:
        pass

def get_company_sentiment_score_(s):
    try:
        return s[3]
    except:
        pass

def toCSVLine(data):
    return ','.join(str(d) for d in data)

## cast functions to udf
get_company_count = udf(get_company_count_, StringType())
get_company_sentiment_score = udf(get_company_sentiment_score_, StringType())
get_company_ric = udf(get_company_ric_, StringType())

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
        file_path = "s3n://log.sharethis.com/amankesarwani/" + region + "/" + str(day) + "/*"
        # file_path =  "s3n://log.sharethis.com/amankesarwani/us/20160324/part-000000000000*"
            to_path = "s3n://log.sharethis.com/results_simplified/" + region + "/" + str(day)
        sharethis_json = spark.read.json(file_path) # spark 2.0
        # sharethis_json.printSchema()  # take a look of the sql structure
        # sharethis_json.cache()  # cache it
        temp_json_raw = sharethis_json.select('*',explode(col('companies')).alias('tempCol')).drop('companies')
        json_cleaned = (temp_json_raw.select( 'standardTimestamp',
            get_company_sentiment_score(col('tempCol')).alias('company_sentiment_score'),
            get_company_ric(col('tempCol')).alias('company_ric'))
            .filter(col('company_ric').isNotNull())
            .filter(col('company_sentiment_score').isNotNull())
            )
#json_cleaned.rdd.coalesce(1).map(toCSVLine).saveAsTextFile(to_path)
        json_cleaned.write.csv(to_path, compression = 'bzip2', mode = 'overwrite')
    except:
        pass
