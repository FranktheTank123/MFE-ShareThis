library(jsonlite)
library(parallel) #mac version of parallel lapply
library(snow)     #windows version of parallel lapply
library(doParallel)#parallel on both
library(plyr)
library(dplyr)
library(urltools)
library(data.table)

##############################
#  Configuration
##############################

# please edit this path
json_file <- "\\\\hcs-data2\\tianyi_xia$\\Documents\\ShareThis\\part-000000000346.log" 
pc_cores <- detectCores()


##############################
# mac version
#json_data<- mclapply(readLines(json_file), fromJSON, flatten = TRUE) #parse each line of the JSON and combine into a list

#windows version
cl<-makeCluster( 16, type="SOCK") #  choose how many cores to use
json_data <- parLapply(cl, readLines(json_file), fromJSON, flatten = TRUE)



##############################
#  Speed test
#  use whichever is faster, mclapply or clusterApply, or parLapply
##############################

#system.time(mclapply(readLines(json_file), fromJSON, flatten = TRUE) )
#system.time(clusterApply(cl, readLines(json_file), fromJSON, flatten = TRUE))
#system.time(parLapply(cl, readLines(json_file), fromJSON, flatten = TRUE))


##############################
# we are only looking for public companies' related share, 
# so we get rid of elements without any ticker informations
cond <- sapply(json_data, function(x) dim(x$companies)[2]>0)

sum(cond) / length(json_data) # only 25% of the data are useful in this case

##############################
# decided to do via data.table
##############################

# parse the list into a data.table
# for mac, can replace parLapply(cl, ...) by mclappy(...)
#raw_json_dt <- as.data.table(do.call(rbind.fill, parLapply(cl, json_data[cond], as.data.frame)))  # this step is gonna be very slow...

raw_json_dt <- do.call(rbindlist,list( l = parLapply(cl, json_data[cond], as.data.frame), fill = T))


#get a temp working copy, remove counts without companies tickesrs
cleaning_json_dt <- raw_json_dt[!is.na(companies.ric),]

cleaning_json_dt <- cleaning_json_dt[, c("companies.sentiment_score", 
                                         "companies.count", 
                                         "companies.sentiment", 
                                         "standardTimestamp", 
                                         "companies.ric", 
                                         "domain") :=
                                       list(as.numeric(companies.sentiment_score), 
                                            as.numeric(companies.count),
                                            as.factor(companies.sentiment),
                                            # we are assuming the time is PDT
                                            as.POSIXct(standardTimestamp, "%Y-%m-%dT%H:%M:%S", tz = "America/Los_Angeles"),
                                            as.factor(companies.ric), 
                                            domain(url))
                                     ]
# user  system elapsed 
# 0.10    0.00    0.11 


summary_json_domain <- cleaning_json_dt[, list(  ave_sentiment_score = mean( companies.sentiment_score, na.rm = T),
                                                 count = length(companies.sentiment_score) )
                                        , by=domain] [ order(-count)]

# user  system elapsed 
# 0.02    0.00    0.01 

summary_json_ticker <- cleaning_json_dt[, list(  ave_sentiment_score = mean( companies.sentiment_score, na.rm = T),
                                                 count = length(companies.sentiment_score) )
                                        , by=companies.ric] [ order(-count)]


head(summary_json_domain, n = 15 )
head(summary_json_ticker, n = 15 )


##############################
# decided to do via dplyr way
##############################

# parse the list into a tbl file
raw_json_tbl <- tbl_df( do.call(rbind.fill, parLapply(cl, json_data[cond], as.data.frame)) )

#get a temp working copy, remove counts without companies tickesrs
cleaning_json_tbl <- filter( raw_json_tbl, !is.na(companies.ric) )

cleaned_json_tbl <- mutate( cleaning_json_tbl, 
                        companies.sentiment_score = as.numeric(companies.sentiment_score),
                        companies.count = as.numeric(companies.count),
                        companies.sentiment = as.factor(companies.sentiment),
                        # we are assuming the time is PDT
                        standardTimestamp = as.POSIXct(standardTimestamp, "%Y-%m-%dT%H:%M:%S", tz = "America/Los_Angeles"),
                        companies.ric = as.factor(companies.ric),
                        domain = domain(url)
                        )
##    user  system elapsed   this is 7 times slower
##    0.77    0.00    0.77 


summary_json_domain <- (
  group_by(cleaned_json_tbl,  domain) %>%
    summarise( ave_sentiment_score = mean( companies.sentiment_score, na.rm = T),
               count = length( companies.sentiment )
    ) %>%
    arrange( desc(count))
)

# user  system elapsed  this is 18 times slower
# 0.19    0.00    0.18


summary_json_ticker <- (
  group_by(cleaned_json_tbl,  companies.ric) %>%
    summarise( ave_sentiment_score = mean( companies.sentiment_score, na.rm = T),
               count = length( companies.sentiment )
    ) %>%
    arrange( desc(count))
)


head(summary_json_domain, n = 15 )
head(summary_json_ticker, n = 15 )