Sys.setenv(SPARK_HOME="~/spark")
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
library(SparkR)
packageVersion("SparkR")
find.package("SparkR")



