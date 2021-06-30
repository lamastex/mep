// Databricks notebook source
// MAGIC %md 
// MAGIC # Functions to easily identify and work with Retweet cascades with proper timestamps in ms

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// COMMAND ----------

//Returns all Retweet cascades in a TTTDF twitter Data set
def IdentifyRetweetCascades(tweetsInputDF: DataFrame): DataFrame = {
  tweetsInputDF.groupBy($"OriginalTwIDinRT")
    .agg(first($"CreationDateOfOrgTwInRT") ,first($"OPostUserNameinRT"), first($"CurrentTweet"), count($"OriginalTwIDinRT"))
    .toDF("OriginalTwIDinRT","CreationDateOfOrgTwInRT", "OPostUserNameinRT", "CurrentTweet", "NumberOfRetweets")
}

//Given ID of one orignal tweet in TTTDF data set, returns the Retweet cascade (original tweet and all retweets)
def getRetweetCascade(ID: Long, tweetsInputDF: DataFrame): DataFrame = {
  tweetsInputDF.filter($"OriginalTwIDinRT" === ID || $"CurrentTwID" === ID)  //suffices to write like this? 
}

// COMMAND ----------

//Gets timestamp in ms from Tweet-ID
def getSnowflakeTimestamp(ID: Long): Long = {
  val twepoch = 1288834974657L
  val base = 2L
  val sequence_id_bits = scala.math.pow(base,12).toLong
  val worker_id_bits = scala.math.pow(base,5).toLong 
  val datacenter_id_bits = scala.math.pow(base,5).toLong
  val timestamp = ID / sequence_id_bits / worker_id_bits / datacenter_id_bits  
  return timestamp + twepoch 
}

val snowflakeUDF = spark.udf.register("snowflakeUDF",getSnowflakeTimestamp(_))

// COMMAND ----------

//returns a retweet cascade with proper timestamps in ms. Needed for librares such as evently in Birdspotter
def getRetweetCascadeWithTimestamps(RTCascade: DataFrame): DataFrame ={
  val RTCascadeTimestampsAbsolute  = RTCascade.withColumn("absolute_time", snowflakeUDF($"CurrentTwID")).sort("absolute_time")
  val time_zero = RTCascadeTimestampsAbsolute.select($"absolute_time").first.getLong(0)
  val RTCascadeTimestamps = RTCascadeTimestampsAbsolute.withColumn("time", ($"absolute_time" - time_zero)/1000.0).select($"time", $"followersCount".alias("magnitude"), $"CPostUserId".alias("user_id"))
  return RTCascadeTimestamps
}
