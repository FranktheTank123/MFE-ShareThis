"""
This file will parse the data and save as parquet
"""

import os
from datetime import date, datetime, timedelta
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import *
import pyspark

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

day = '20150217'
num_of_domain = 100
region = "us"
seed = 1014
save_parquet = True

file_path = "s3n://log.sharethis.com/amankesarwani/" + region + "/" + str(day)  + "/part-000000000005*"
stock_return_path = "s3n://log.sharethis.com/Stock_Proceesed_Minute_60_Second_Return_lag.csv"



"""Functions here."""

## personalized UDF: AAPL.x -> AAPL
def ricToTicker_(ric):
    try:
        return ric.split('.')[0]
    except:
        pass

ricToTicker = udf(ricToTicker_)


def explodeAndDropCol(pySparkDataFrame, col_names, alias="tempCol"):
    """Explode a column and drop the un-exploded one."""
    return pySparkDataFrame.select('*', explode(col(col_names)) \
        .alias(alias)).drop(col_names)


def selectCompanyInfo(pySparkDataFrame, col_name="tempCol"):
    """Further filter the nested column."""
    _temp = (
             pySparkDataFrame.withColumn("company_count", col(col_name).getItem("count").cast("Integer"))
             .withColumn("company_sentiment_score", col(col_name).getItem("sentiment_score").cast("Double"))
             .withColumn("Ticker", ricToTicker(col(col_name).getItem("ric")))
             .drop(col_name)
             .filter(col('Ticker').isNotNull())
             .filter(col('company_sentiment_score').isNotNull())
            )
    return _temp


def getTopDomain(refDomain_categories, num_of_domain):
    """Get top domain form the list."""
    refDomain_categories_filter = []
    for x in refDomain_categories:
        if len(refDomain_categories_filter) < num_of_domain:
            try:
                temp = (x[0]).split('.')[-2]
                if temp not in refDomain_categories_filter:
                    refDomain_categories_filter.append(temp)
            except:
                pass
    return refDomain_categories_filter


def parse_sharethis_time_(time):
    """set second = 0"""
    return datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ").replace(second=0)


parse_sharethis_time = udf(parse_sharethis_time_, TimestampType())

getHours = udf(lambda x: x.hour, IntegerType())



def main():
    """Let's clean the stock data"""
    stock_return_raw = spark.read.csv(stock_return_path, sep=",", header=True)

    ## clean the time
    stock_return_cleaned = \
        stock_return_raw.withColumn("TimeStamp", from_utc_timestamp(stock_return_raw.Lagged_Time, "UTC")) \
        .drop("Lagged_Time").withColumnRenamed("variable", "Ticker") \
        .withColumn("Return", col("Return").cast("Double"))

    stock_return_cleaned.cache()

    # we might need to broadcast this later
    SP500_tickers = \
        [x.Ticker for x in stock_return_cleaned.select("Ticker").distinct().collect()]
    SP500_tickers_bc = spark.sparkContext.broadcast(SP500_tickers)



    """Then we process the sentiment data"""
    # load data
    sharethis_json = spark.read.json(file_path)  # spark 2.0

    # data process
    temp_json_raw_1 = explodeAndDropCol(sharethis_json, "companies")  # explode the companies to multiples cols
    temp_json_raw_2 = selectCompanyInfo(temp_json_raw_1)  # unnest the cols and clean the tickers
    sharethis_json_cleaned = temp_json_raw_2.drop('stid').drop('url').drop('userAgent')  # drop unnecessary cols

    # cache for further usage
    sharethis_json_cleaned.cache()

    ## we only need SP500 tickers
    sharethis_json_cleaned = sharethis_json_cleaned.filter(col("Ticker").isin(SP500_tickers_bc.value))


    """Add dummies"""

    device_categories = [u'Personal computer', u'Tablet', u'Smartphone']
    mappedEvent_categories = [u'pview', u'click', u'search', u'share']

    refDomain_categories = (sharethis_json_cleaned.groupBy("refDomain")
                            .count().orderBy(desc("count")).select("refDomain")
                            .collect())

    refDomain_categories_filter = getTopDomain(refDomain_categories, num_of_domain)

    exprs_device = [when(col("deviceType") == category, 1).otherwise(0).alias("is_device_"+category.replace (" ", "_"))
            for category in device_categories]
    exprs_domain = [when(col("refDomain") == category, 1).otherwise(0).alias("is_domain_"+category)
            for category in refDomain_categories_filter]
    exprs_mappedEvent = [when(col("mappedEvent") == category, 1).otherwise(0).alias("is_event_"+category)
            for category in mappedEvent_categories]

    labeled_sharethis_json_cleaned = sharethis_json_cleaned.select("*", *exprs_device) \
            .select("*", *exprs_domain).select("*", *exprs_mappedEvent) \
            .drop("deviceType").drop("refDomain").drop("mappedEvent").drop("os").drop("browserFamily")

    labeled_sharethis_json_final = (labeled_sharethis_json_cleaned
                          .withColumn("TimeStamp", parse_sharethis_time(col("standardTimestamp")))
                          .drop("standardTimestamp")
                         )

    ## join and filter
    joined_dataframe = labeled_sharethis_json_final.join(stock_return_cleaned, ["TimeStamp", "Ticker"], how = "left_outer")

    joined_dataframe_filtered = joined_dataframe.filter(col('Return').isNotNull())
    #First we creat T/F labels for return
    joined_dataframe_filtered = \
        joined_dataframe_filtered.withColumn("Label",(joined_dataframe_filtered["Return"]>0).cast("Double"))

    ## finally save
    if save_parquet:
        joined_dataframe_filtered.write.parquet("sentiment_processed_temp.parquet")
    # joined_dataframe_filtered.cache()

if __name__ == '__main__':
    main()
