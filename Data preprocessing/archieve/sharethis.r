install.packages("magrittr")
library(magrittr)

df <- read.df("s3n://log.sharethis.com/amankesarwani/us/20150301/part-00000000000[0]*", "json")
to_path = "s3n://log.sharethis.com/results_r/1"


df$tempcol <- explode(df$companies)

temp_df <- df %>% drop("companies") %>% drop("stid") %>% drop("url") %>% drop("userAgent")

json_cleaned <- select(temp_df, c("*", alias(getItem(temp_df$tempcol, "count"), "company_count"),
                        alias(getItem(temp_df$tempcol, "ric"), "company_ric"),
                        alias(getItem(temp_df$tempcol, "sentiment_score"), "company_sentiment_score"))) %>%
                        drop("tempCol")
json_cleaned <- json_cleaned %>% filter(isNotNull(json_cleaned$company_ric)) %>% filter(isNotNull(json_cleaned$company_sentiment_score))

saveDF(json_cleaned, to_path)
