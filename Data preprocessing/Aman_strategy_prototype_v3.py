# -*- coding: utf-8 -*-
"""
@author: Frank Xia
"""
from __future__ import division  # python 2
from datetime import date, datetime, timedelta
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import *
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()



"""Global variable here."""
time_interval = 15 # 15 min bin of time
look_back_period = 10  # in hours
count_threshold = 5 # each period between [t-lookback, t]
					# will filter out the tickers with apparence count < threshold
region = "us"
SP_500_list_path = "s3n://log.sharethis.com/SP500.csv"
s3_save_path = "s3n://log.sharethis.com/SentimentAverage_v3/"+ region + "1/"


"""Functions here."""

def getSP500List(SP_500_list_path):
	"""return a list of SP500 tickers"""
	SP500_tickers_df = spark.read.csv(SP_500_list_path)
	return [x._c0 for x in SP500_tickers_df.collect()] # eventually we need a list


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

def splitTimeInterval_(row_time):
	return row_time.replace(minute=row_time.minute//time_interval * time_interval) + timedelta(minutes=time_interval)

splitTimeInterval = udf(splitTimeInterval_, TimestampType())

getHours = udf(lambda x: x.hour, IntegerType())


def cleanData_v2(file_path, SP500_tickers):
	"""
	Then we process the sentiment data into:

	root
	 |-- company_count: integer (nullable = true)
	 |-- company_sentiment_score: double (nullable = true)
	 |-- Ticker: string (nullable = true)
	 |-- is_event_pview: integer (nullable = false)
	 |-- is_event_click: integer (nullable = false)
	 |-- is_event_search: integer (nullable = false)
	 |-- is_event_share: integer (nullable = false)
	 |-- TimeStamp: timestamp (nullable = true)
	 |-- TimeInterval: timestamp (nullable = true)
	"""
	SP500_tickers_bc = spark.sparkContext.broadcast(SP500_tickers)  # broadcast
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
	mappedEvent_categories = [u'pview', u'click', u'search', u'share']
	exprs_mappedEvent = [when(col("mappedEvent") == category, 1).otherwise(0).alias("is_event_"+category)
	        for category in mappedEvent_categories]
	labeled_sharethis_json_cleaned = sharethis_json_cleaned.select("*", *exprs_mappedEvent) \
	        .drop("deviceType").drop("refDomain").drop("mappedEvent").drop("os").drop("browserFamily") \
	        .drop("channel").drop("shortIp")
	labeled_sharethis_json_final = (labeled_sharethis_json_cleaned
	                      .withColumn("TimeStamp", parse_sharethis_time(col("standardTimestamp")))
	                      .drop("standardTimestamp")
	                      .withColumn("TimeInterval", splitTimeInterval(col("TimeStamp")))
	                     )
	return labeled_sharethis_json_final


def getSentimentSummary_bin_3(labeled_sharethis_json_final):
	"""
	here we set look back to x min and do a rolling mean sentiment without considering the
	trading period
	"""
	# only save the current timeframe's data	    
	total_results = (labeled_sharethis_json_final.groupby("TimeInterval", "Ticker")
		.agg(mean("company_sentiment_score").alias("sentiment_mean"), 
					stddev("company_sentiment_score").alias("sentiment_std"),
					count("company_sentiment_score").alias("sentiment_count"),
					sum("is_event_pview").alias("event_pview_count"), 
					sum("is_event_click").alias("event_click_count"), 
					sum("is_event_search").alias("event_search_count"),
					sum("is_event_share").alias("event_share_count"))
		.filter(col("sentiment_count") >= count_threshold) )
	return total_results




def main():
	daystart = date(2016, 1, 1)
	#dayend = daystart
	dayend = date(2016, 3, 26)
	curr_date = daystart - timedelta(days=1) # will mod this later
	SP500_tickers = getSP500List(SP_500_list_path)  # get the list
	while curr_date < dayend:
		## increment the dat first
		curr_date += timedelta(days=1)

		## check weekend
		if curr_date.weekday() in [5,6]:  
			continue

		print("Now processing day: {}".format(curr_date))

		try:
			file_path = "s3n://log.sharethis.com/amankesarwani/" + region + "/" + \
				curr_date.strftime("%Y%m%d") #  + "/part-000000000005*"

			labeled_sharethis_json_final = cleanData_v2(file_path, SP500_tickers)  # get cleaned data

			total_results = getSentimentSummary_bin_3(labeled_sharethis_json_final)
			temp_path = s3_save_path +curr_date.strftime("%Y%m%d")
			print("save to", temp_path)
			total_results.coalesce(1).write.csv(temp_path, mode="overwrite", header=True)

		except: 
			print(curr_date.strftime("%Y%m%d"), "is skipped.")
			continue



if __name__ == '__main__':
	main()
	print("Job finished.")