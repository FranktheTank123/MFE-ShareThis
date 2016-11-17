# AWS EMR server walk-through guide
##### Author: Frank Xia
##### Date last Modified: 9/10/2016

## `SSH` to AWS-ec2
After you create a cluster on using AWS EMR (or EC2), you can use the `ssh` command to connect to the server using the following syntax:

```
ssh -i YOUR_PEM.pem ec2-user@EC2-ip-address.amazonaws.com
```
For example:

```
ssh -i ~/.ssh/Franksmac.pem ec2-user@ec2-54-197-2-2.compute-1.amazonaws.com
```


## Trouble shooting
When you are running `pyspark` on EC2, and the `ssh` connection happened to stop, the `pyspark` program on EC2 will not be terminated. Therefore, before you initiate another Spark instance, it is recommended to manually kill the previous one using:

```
ps -ef | grep spark-shell
kill -9 job_id_number
```

OR

```
yarn application -kill application_1473793005702_0001
```

Otherwise, when you trying to access/modify the Hive database, an error will occur. Another way to walk around this is to delete all the `*.lck` files in the `metastore_db` folder, so that multiple users can access the same Hive database. It can be achieved by:

```
rm *.lck  # in metastore_db folder
```

## Uploading files to EC2
Sometime it's efficient to upload the whole scripts to EC2 and run on `nohup`. This can be achieved by:  
**Note: under your local directory, not `ssh`:**

```
scp -i ~/.ssh/Franksmac.pem -prq '/Users/tianyixia/Google Drive/ShareThis/Frank/Data preprocessing/Aman_strategy_prototype_v3.py' hadoop@ec2-54-204-85-224.compute-1.amazonaws.com:~/
```


## `nohup`
It is recommend to run `.py` files using `nohup` command using `sudo` in this case. Before that, we need to config the followings:

```
export AWS_ACCESS_KEY_ID=AKIAJ47TT2JRJ33WRSZQ
export AWS_SECRET_ACCESS_KEY=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
export SPARK_HOME=/usr/lib/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.1-src.zip:$PYTHONPATH
```

* **If you want to run plain `nohup`:**

```
nohup spark-submit --verbose --deploy-mode cluster --master yarn  Aman_strategy_prototype_v3.py  > sharethis.log &
```

* **If you want to run `nohup` under `sudo`:**

```
nohup sudo -HE env PATH=$PATH PYTHONPATH=$PYTHONPATH spark-submit --verbose --deploy-mode cluster --master yarn  Aman_strategy_prototype_v3.py  > sharethis.log &
```

* **If you want to just run `spark-submit` (NOT RECOMMEND):**

```
spark-submit --verbose --deploy-mode cluster --master yarn  Aman_strategy_prototype.py
```

* **If you want to run `spark-submit` under `sudo` (NOT RECOMMEND):**

```
sudo -HE env PATH=$PATH PYTHONPATH=$PYTHONPATH spark-submit --verbose --deploy-mode cluster --master yarn  SOME_FILE.py
```

## How to kill `nohup`

```
jobs -l
sudo kill xxxxx
```

## `yarn` debug for `spark-submit`
Sometimes jobs at `spark-submit` just broke without telling you why.   
This can be tracked at:

```
sudo yarn logs -applicationId application_1475526098927_0002
```

## How to install 
Please refer to [this](https://blog.datapolitan.com/2015/08/25/installing-matplotlib-and-pandas-on-amazon-ec2/).

```
#!/bin/bash
sudo yum install update
sudo yum groupinstall "Development Tools"
sudo yum install python-devel libpng-devel freetype-devel 
#the last two are necessary for pip to run without failing with error 'Command "python setup.py egg_info" failed with error code 1'
sudo pip install matplotlib pandas #Finally it works!
```


