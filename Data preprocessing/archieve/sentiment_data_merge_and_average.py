"""
This script will read all of the processed sentiment files (from Pyspark),
and:
1. merge them into 1 file
2. take the weighted 10hr sentiment average/sd
3. take the sum of the past 10hr counts
4. save into a file
"""


import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta

def wavg(group):
    m = group['sentiment_mean']
    w = group['sentiment_count']

    return (m * w).sum()/ w.sum()

def wstd(group):
    w = group['sentiment_count']
    sd = group['sentiment_std']
    return (np.power(sd, 2)* np.power(w, 2)).sum()/np.power(w, 2).sum()

def main():
	# import the data
	sentiment_15min_df = pd.read_csv("sentiment_15min.csv",header=0)
	stock_df = pd.read_csv("stock_df.csv",header=0)

	# parse the time
	sentiment_15min_df["Time"] = sentiment_15min_df["Time"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
	stock_df["Time"] = stock_df["Time"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))

	# now we extract the distinct time intervals
	time_list = sentiment_15min_df["Time"].unique()
	time_list.sort()

	# below is magic, but very slow...
	length_hours = 10
	list_=[]

	time = time_list[150]
	df = sentiment_15min_df

	total_len= len(time_list[40:])
	count=0
	for time in time_list[40:]:
	    count +=1
	    if(count % 100 == 0):
	        print("{0:.4f} done".format(count/total_len))
	    
	    end_time = time
	    start_time = (end_time - np.timedelta64(length_hours, "h"))
	    
	    mask = (df['Time'] > start_time) & (df['Time'] <= end_time)
	    df_current = df.loc[mask]

	    # groupby operators
	    grouped = df_current.groupby('Ticker') 
	    grouped_df = grouped.agg("sum")
	    grouped_df["sentiment_mean"] = grouped.apply(wavg)
	    grouped_df["sentiment_std"] = grouped.apply(wstd)
	    grouped_df["Time"] = end_time
	    grouped_df.reset_index()

	    list_.append(grouped_df)

	groupped_sentiment_15min_df = pd.concat(list_) # concact

	groupped_sentiment_15min_df = groupped_sentiment_15min_df.reset_index()
	groupped_sentiment_15min_df.to_csv("/Users/tianyixia/Downloads/groupped_sentiment_15min_df.csv",index=False)


if __name__ == '__main__':
	main()