# work in pyspark
import os
from datetime import date , datetime
from pyspark.sql.types import StringType, TimestampType, IntegerType
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import *


day = '20150217'
num_of_domain = 100
region = "us"

# set path
file_path = "s3n://log.sharethis.com/amankesarwani/" + region + \
    "/" + str(day) + "/part-000000000001*"
stock_return_path = \
    "s3n://log.sharethis.com/Stock_Proceesed_return_Frank_lag.csv"


"""
let's clean the stock data
"""
stock_return_raw = spark.read.csv(stock_return_path, sep=",", header=True)
## clean the time
stock_return_cleaned = stock_return_raw.withColumn("TimeStamp", from_utc_timestamp(stock_return_raw.Lagged_Time, "UTC")) \
                        .drop("Lagged_Time").withColumnRenamed("variable", "Ticker")

stock_return_cleaned.cache()

SP500_tickers = [x.Ticker for x in stock_return_cleaned.select("Ticker").distinct().collect()] # we might need to broadcast this later



"""
Then we process the sentiment data
"""
## personalized UDF
ricToTicker = udf(lambda x: x.split('.')[0])

# load data
sharethis_json = spark.read.json(file_path)  # spark 2.0

# data process
temp_json_raw = sharethis_json.select('*', explode(col('companies')).alias('tempCol')).drop('companies')
json_cleaned = (temp_json_raw.select( '*', temp_json_raw.tempCol.getItem("count").alias('company_count'),
    temp_json_raw.tempCol.getItem("sentiment_score").alias('company_sentiment_score'),
    temp_json_raw.tempCol.getItem("ric").alias('company_ric'))
    .drop('tempCol').drop('stid').drop('url').drop('userAgent')
    .filter(col('company_ric').isNotNull())
    .filter(col('company_sentiment_score').isNotNull())
    .withColumn("Ticker", ricToTicker(col("company_ric"))).drop("company_ric")
    )

## cache the data
json_cleaned.cache()

## we only need SP500 tickers
json_cleaned = json_cleaned.filter(col("Ticker").isin(SP500_tickers))


device_categories = [u'Personal computer', u'Tablet', u'Smartphone']

refDomain_categories = (json_cleaned.groupBy("refDomain")
                        .count().orderBy(desc("count")).select("refDomain")
                        .rdd.flatMap(lambda x: x).top(num_of_domain))

refDomain_categories_filter = []
for x in refDomain_categories:
    try:
        temp = x.split('.')[-2]
        if temp not in refDomain_categories_filter:
            refDomain_categories_filter.append(temp)
    except:
        pass

exprs_device = [when(col("deviceType") == category, 1).otherwise(0).alias("is_device_"+category)
        for category in device_categories]
exprs_domain = [when(col("refDomain") == category, 1).otherwise(0).alias("is_domain_"+category)
        for category in refDomain_categories_filter]

labeled_json_cleaned = json_cleaned.select("*", *exprs_device) \
        .select("*", *exprs_domain).drop("deviceType").drop("refDomain")

# finally we parse the time
parse_sharethis_time = udf(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ"), TimestampType())
getHours = udf(lambda x: x.hour ,IntegerType())

labeled_json_final = labeled_json_cleaned.withColumn("TimeStamp", parse_sharethis_time(col("standardTimestamp"))).drop("standardTimestamp")

#labeled_json_final = json_cleaned.withColumn("TimeStamp", parse_sharethis_time(col("standardTimestamp"))).drop("standardTimestamp")

#stock_return_cleaned.select(getHours(col("TimeStamp")).alias("Hour")).groupBy("Hour").count().orderBy("Hour").show()
#labeled_json_final.select(getHours(col("TimeStamp")).alias("Hour")).groupBy("Hour").count().orderBy("Hour").show(25)

##
joined_dataframe = labeled_json_final.join(stock_return_cleaned, ["TimeStamp", "Ticker"], how = "left_outer")

joined_dataframe_filtered = joined_dataframe.filter(col('Return').isNotNull())
