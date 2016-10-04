package com.chappidm.spark_emr_ddb.writetoddb

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd
import org.apache.spark.sql.Row
import org.apache.hadoop.io.Text;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.LongWritable
import java.util.HashMap

object UserRatingCountDDB{
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Rating Count")
      .enableHiveSupport()
      .getOrCreate()
    val ddbConf = new JobConf(spark.sparkContext.hadoopConfiguration)
    ddbConf.set("dynamodb.output.tableName", "GenreRatingCounts")
    ddbConf.set("dynamodb.throughput.write.percent", "0.5")
    ddbConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
    ddbConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    
    val genreRatingsCount = spark.sql("select genre, rating, count(*)  ratingCount from UserMovieRatings r join (select movieid, title, explode(genres) as genre from moviedetails) m on (r.movieid = m.movieid) group by genre, rating order by genre, rating")
    
    val out = genreRatingsCount.rdd.map(a => {
        var ddbMap = new HashMap[String, AttributeValue]()
        var catValue = new AttributeValue()
        catValue.setS(a.get(0).toString)
        ddbMap.put("genre", catValue)
        var ratingValue = new AttributeValue()
        ratingValue.setN(a.get(1).toString)
        ddbMap.put("rating", ratingValue)
        var countValue = new AttributeValue()
        countValue.setN(a.get(2).toString)
        ddbMap.put("count", countValue)
        var item = new DynamoDBItemWritable()
        item.setItem(ddbMap)
        (new Text(""), item)
     })
     out.saveAsHadoopDataset(ddbConf)
     
  }
}