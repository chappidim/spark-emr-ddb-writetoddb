# spark-emr-ddb-writetoddb

This example helps you to write Dataset/query output to DynamoDB table.

Steps taken:
* Create external tables using Hive
* Run queries on s3 data and save result to 'genreRatingsCount' DataSet
* Convert the DataSet to RDD and run map function on it to create ITEMs
* Using saveAsHadoopDataset and DDBConf(emr-ddb-hadoop), write the ITEMs to DynamoDB table.
