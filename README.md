# spark-emr-ddb-writetoddb

This example helps you to write Dataset/query output to DynamoDB table.

Steps taken:
* Create external tables using Hive
* Run queries on s3 data and save result to [genreRatingsCount](https://github.com/chappidmAWS/spark-emr-ddb-writetoddb/blob/master/src/main/scala/com/chappidm/spark_emr_ddb/writetoddb/UserRatingCountDDB.scala#L28) DataSet
* Convert the DataSet to RDD and run map function on it to create ITEMs
* Using saveAsHadoopDataset and DDBConf([emr-ddb-hadoop](https://github.com/awslabs/emr-dynamodb-connector)), write the ITEMs to DynamoDB table.

Command to run
```
spark-submit --jars /usr/share/aws/emr/ddb/lib/emr-ddb-hadoop.jar --class com.chappidm.spark_emr_ddb.writetoddb.UserRatingCountDDB spark_emr_ddb.writetoddb-0.0.1-SNAPSHOT.jar
```
