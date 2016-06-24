library(jsonlite)
library(parallel) #mac version of parallel lapply
library(snow)     #windows version of parallel lapply
library(doParallel)#parallel on both
library(plyr)
library(dplyr)
library(urltools)
library(data.table)


preprocessing <- function( input, output, is_mac = FALSE, cores = 16, cores_auto = FALSE){
    ## if the mac case
    if(is_mac){
        json_data<- mclapply(readLines(input), fromJSON, flatten = TRUE)
        cond <- sapply(json_data, function(x) dim(x$companies)[2]>0)
        raw_json_dt <- do.call(rbindlist,list( l = mclappy( json_data[cond], as.data.frame), fill = T))
    }
    ## if windows
    else{
        if (cores_auto) 
            cores <- detectCores()
        
        cl<-makeCluster( cores, type="SOCK") #  choose how many cores to use
        json_data <- parLapply(cl, readLines(input), fromJSON, flatten = TRUE)
        cond <- sapply(json_data, function(x) dim(x$companies)[2]>0)
        raw_json_dt <- do.call(rbindlist,list( l = parLapply(cl, json_data[cond], as.data.frame), fill = T))
    }
    
    
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
    ## save the file as ao object, so that we can load them very quickly using:
    ## desired_name <- get(load(target_file_name))
    save(cleaning_json_dt, file = output)
    
}


## feel free to change the feature here...
wd <- ("H:\\Documents\\ShareThis\\ShareThisSample")
file_names <- readLines( "\\\\hcs-data2\\tianyi_xia$\\Documents\\ShareThis\\sample_input_list.txt" )
output_names <- readLines( "\\\\hcs-data2\\tianyi_xia$\\Documents\\ShareThis\\sample_out_list.txt" )



for ( i in 1:5){
    input <-  paste(wd,"\\",file_names[i],sep='')
    output <- paste(wd,"\\",output_names[i],sep='')
    print(i)
    preprocessing(input, output, is_mac = FALSE, cores = 16, cores_auto = FALSE)
}
