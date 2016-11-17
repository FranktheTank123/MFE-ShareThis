# -*- coding: utf-8 -*-
"""
Aman's strategy prototype

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
import boto3
from pyspark.sql import SparkSession
# import pandas as pd
spark = SparkSession.builder.getOrCreate()

# from Sentiment_average_v1 import *



"""Global variable here."""
look_back_period = 10  # in hours
count_threshold = 5 # each period between [t-lookback, t]
					# will filter out the tickers with apparence count < threshold
region = "us"
SP_500_list_path = "s3n://log.sharethis.com/SP500.csv"
emr_local_save_path = "/home/hadoop/"
# s3_save_path = "SentimentAverage_v1/"+ region + "/"
s3_save_path = "s3n://log.sharethis.com/SentimentAverage_v1/"+ region + "/"
aws_access_key_id="AKIAJ47TT2JRJ33WRSZQ"
aws_secret_access_key="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"
s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)


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

getHours = udf(lambda x: x.hour, IntegerType())


def cleanData_v1(file_path, SP500_tickers):
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
	                     )
	return labeled_sharethis_json_final


def getSentimentSummary(labeled_sharethis_json_final, curr_date, total_results, save_to_path):
	labeled_sharethis_json_final.cache() # cache first
 	# first we create the time of each day's start/end trading period
	start_time = datetime(curr_date.year, curr_date.month, curr_date.day, 14, 30, 0)
	end_time= datetime(curr_date.year, curr_date.month, curr_date.day, 21, 0, 0)
	# then we create a trading time list
	timelist = [ (start_time + timedelta(minutes=x*60)) for x in range(int((7)*60/60))]
	for each_time in timelist:
	    print("Now Processing:", each_time)
	    look_back_time = (each_time - timedelta(hours = look_back_period)) 
	    # create a function to filter in the period: 
	    # [ t-look_back, t ]
	    getTime = udf(lambda x: x.time() <= each_time.time() and x.time() >= look_back_time.time(), BooleanType())
	    _temp = (labeled_sharethis_json_final.filter(getTime("TimeStamp"))
	    			.groupby("Ticker")
		            .agg(mean("company_sentiment_score").alias("sentiment_mean"),
		                  count("company_sentiment_score").alias("sentiment_count"),
		                  sum("is_event_pview").alias("event_pview_count"),
		                  sum("is_event_click").alias("event_click_count"),
		                  sum("is_event_search").alias("event_search_count"),
		                  sum("is_event_share").alias("event_share_count")
		                  ).filter(col("sentiment_count") >= count_threshold)
		            .withColumn("TimeStamp", lit(each_time.strftime("%Y%m%d-%H:%M:%S")))
		            #.orderBy(col("sentiment_mean").desc())
		            #.withColumn("TimeStamp", udf(lambda x:each_time, TimestampType())(col("Ticker")))
	     		)
	    # _temp.write.csv(save_to_path, mode="append", header=False)
	    total_results = total_results.union(_temp)
	return total_results


def getSentimentSummary_bin(labeled_sharethis_json_final, curr_date, total_results):
	"""
	here we set look back to x min and do a rolling mean sentiment without considering the
	trading period
	"""
	labeled_sharethis_json_final.cache() # cache first
 	# first we create the time of each day's start/end trading period
	start_time = datetime(curr_date.year, curr_date.month, curr_date.day, 0, 0, 0)
	# then we create a trading time list
	timelist = [ (start_time + timedelta(minutes=x*30)) for x in range(48)]
	for i, each_time in enumerate(timelist):
	    # print("Now Processing:", each_time)
	    each_time_end = (each_time + timedelta(minutes = 30)) 
	    # create a function to filter in the period: 
	    # [ t-look_back, t ]
	    if i == 47:
	    	getTime = udf(lambda x: x.time() >= each_time.time(), BooleanType()) ## edge case
	    else:
	    	getTime = udf(lambda x: x.time() < each_time_end.time() and x.time() >= each_time.time(), BooleanType())
	    # only save the current timeframe's data	    
	    _temp = (labeled_sharethis_json_final.filter(getTime("TimeStamp"))
	    			.groupby("Ticker")
		            .agg(mean("company_sentiment_score").alias("sentiment_mean"),
		                  count("company_sentiment_score").alias("sentiment_count"),
		                  sum("is_event_pview").alias("event_pview_count"),
		                  sum("is_event_click").alias("event_click_count"),
		                  sum("is_event_search").alias("event_search_count"),
		                  sum("is_event_share").alias("event_share_count")
		                  ).filter(col("sentiment_count") >= count_threshold)
		            .withColumn("TimeStamp", lit(each_time.strftime("%Y%m%d-%H:%M:%S")))
		            #.orderBy(col("sentiment_mean").desc())
		            #.withColumn("TimeStamp", udf(lambda x:each_time, TimestampType())(col("Ticker")))
	     		)
	    total_results = total_results.union(_temp)
	return total_results


def getSentimentSummary_legacy(labeled_sharethis_json_final, curr_date):
	"""
	CURRENTLY DEPRECATED
	"""
	labeled_sharethis_json_final.cache() # cache first
 	# first we create the time of each day's start/end trading period
	start_time = datetime(curr_date.year, curr_date.month, curr_date.day, 14, 30, 0)
	end_time= datetime(curr_date.year, curr_date.month, curr_date.day, 21, 0, 0)
	# then we create a trading time list
	timelist = [ (start_time + timedelta(minutes=x*60)) for x in range(int((7)*60/60))]
	sentiment_summary = []  # place holder
	for each_time in timelist:
	    # print("Now Processing:", each_time)
	    look_back_time = (each_time - timedelta(hours = look_back_period)) 
	    # create a function to filter in the period: 
	    # [ t-look_back, t ]
	    getTime = udf(lambda x: x.time() <= each_time.time() and x.time() >= look_back_time.time(), BooleanType())
	    # only save the current timeframe's data
	    df_current = labeled_sharethis_json_final.filter(getTime("TimeStamp"))
	    # get the counts of each ticker's apparence 
	    counts = df_current.groupby("Ticker").count().filter(col("count") >= count_threshold).select("Ticker").collect()
	    # take off the "rows" in the name
	    counts = [x[0] for x in counts]
	    counts_bc = spark.sparkContext.broadcast(counts)  # broadcast
	    # only save the ticker with count >= the prespecified threshold
	    df_current = df_current.filter(df_current.Ticker.isin(counts_bc.value))
	    sentiment_summary.append(df_current.groupby("Ticker")
	                             .agg(mean("company_sentiment_score").alias("sentiment_mean"),
	                                  count("company_sentiment_score").alias("sentiment_count"),
	                                  sum("is_event_pview").alias("event_pview_count"),
	                                  sum("is_event_click").alias("event_click_count"),
	                                  sum("is_event_search").alias("event_search_count"),
	                                  sum("is_event_share").alias("event_share_count")
	                                  )
	                             .orderBy(col("sentiment_mean").desc()).collect()
	                            )
	return timelist, sentiment_summary

def resultsToDf_legacy(total_timelist, total_sentiment_summary, col_name, save_to_path):
	"""
	CURRENTLY DEPRECATED

	finally convert to df and save
	"""
	sentiment_results = pd.DataFrame(columns=col_name)
	for each_time, sentiment_list in zip(total_timelist, total_sentiment_summary):
	    for each_sentiment in sentiment_list:
	        sentiment_results.loc[sentiment_results.shape[0]] = [each_time]+list(each_sentiment)
	sentiment_results.to_csv(save_to_path, index=False, compression="gzip")
	print("Successfully write to file {}".format(save_to_path))

"""
def pysparkToS3(total_results, curr_date):
	total_results_pd = total_results.toPandas() # transfer to pandas
	save_file_temp = emr_local_save_path+curr_date.strftime("%Y%m%d")+".csv"
	total_results_pd.to_csv(save_file_temp, index=False) # save pandas locally
	s3_file_temp = s3_save_path + curr_date.strftime("%Y%m%d")+".csv"
	s3_client.upload_file(save_file_temp, 'log.sharethis.com', s3_file_temp) # upload to s3
"""


def main():
	daystart = date(2015, 2, 17)
	#dayend = daystart
	dayend = date(2015,2, 17)
	curr_date = daystart - timedelta(days=1) # will mod this later
	SP500_tickers = getSP500List(SP_500_list_path)  # get the list
	while curr_date < dayend:
		curr_date += timedelta(days=1)
		if curr_date.weekday() in [5,6]:  ## check weekend
			continue
		print("Now processing day: {}".format(curr_date))
		try:
			# create a new spark Dataframe and schema
			schema = StructType([
			    StructField("Ticker", StringType(), False),
			    StructField("sentiment_mean", DoubleType(), False),
			    StructField("sentiment_count", LongType(), False),
			    StructField("event_pview_count", LongType(), False),
			    StructField("event_click_count", LongType(), False),
			    StructField("event_search_count", LongType(), False),
			    StructField("event_share_count", LongType(), False),
			    StructField("TimeStamp", StringType(), False)
			])
			total_results = spark.createDataFrame([], schema)
			file_path = "s3n://log.sharethis.com/amankesarwani/" + region + "/" + \
				curr_date.strftime("%Y%m%d") + "/part-000000000005*"	
			# save_to_path = "s3n://log.sharethis.com/SentimentAverage_v1/" + region + "/" + curr_date.strftime("%Y%m%d")
			labeled_sharethis_json_final = cleanData_v1(file_path, SP500_tickers)  # get cleaned data
			total_results = getSentimentSummary_bin(labeled_sharethis_json_final, curr_date, total_results)

			print("save to", s3_save_path)
			total_results.coalesce(1).write.csv(s3_save_path +curr_date.strftime("%Y%m%d"), mode="overwrite", header=True)
			# total_results.coalesce(1).rdd.saveAsTextFile(s3_save_path +curr_date.strftime("%Y%m%d"))
		except: 
			print(curr_date.strftime("%Y%m%d"), "is skipped.")
			continue



if __name__ == '__main__':
	main()
	print("Job finished.")