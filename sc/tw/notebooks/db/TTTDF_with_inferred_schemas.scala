// Databricks notebook source
// MAGIC %md
// MAGIC ## Here we transfer collected Tweets as .jsons into a Spark context
// MAGIC - Includes different TTTDF versions for tweets collected by twarc and twitter4j
// MAGIC - Inferred Schemas has been added for efficiency 

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType};
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import java.text.SimpleDateFormat
import java.util.Date
import java.sql.Timestamp


// COMMAND ----------

val pathToTwitter4j_schema = "/put/path/here"
val pathToTwarc_schema = "/put/path/here"

//to load the saved schema for tweets collected with Twarc
val savedSchema = spark.read.text(pathToTwitter4j_schema)
val savedSchema_twarc = spark.read.text(pathToTwarc_schema)

//savedSchema will be loaded as a dataframe, and we just want everything as a string
val savedSchemaString = savedSchema.first.getString(0)
val savedSchemaString_twarc = savedSchema_twarc.first.getString(0)

//from the string we can get the proper type for schemas ie StructType
val inferred_schema = DataType.fromJson(savedSchemaString).asInstanceOf[StructType]
val inferred_schema_twarc = DataType.fromJson(savedSchemaString_twarc).asInstanceOf[StructType]


// COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def fromParquetFile2DF(InputDFAsParquetFilePatternString: String): DataFrame = {
      sqlContext.
        read.parquet(InputDFAsParquetFilePatternString)
}

def tweetsJsonStringDF2TweetsDF(tweetsAsJsonStringInputDF: DataFrame): DataFrame = {
      sqlContext
        .read
        .json(tweetsAsJsonStringInputDF.map({case Row(val1: String) => val1}))
      }

def tweetsIDLong_JsonStringPairDF2TweetsDF(tweetsAsIDLong_JsonStringInputDF: DataFrame): DataFrame = {
      sqlContext
        .read
        .json(tweetsAsIDLong_JsonStringInputDF.map({case Row(val0:Long, val1: String) => val1}))
      }

def tweetsIDLong_JsonString_tweetDate_DF2TweetsDF(tweetsAsIDLong_JsonStringInput_tweetDateDF: DataFrame) : DataFrame = {
      sqlContext
        .read
        .json(tweetsAsIDLong_JsonStringInput_tweetDateDF.map({case Row(val0:Long, val1: String, val2: Timestamp) => val1}))
      }

//The following code is for reading Jsons with an already defined schema:

def fromJsonFile2DF(InputDFAsParquetFilePatternString: String, schema: StructType): DataFrame = {
      spark
        .read
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .json(InputDFAsParquetFilePatternString)
}

def fromParquetFile2DF(InputDFAsParquetFilePatternString: String, schema: StructType): DataFrame = {
      spark
        .read
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .parquet(InputDFAsParquetFilePatternString)
}

def tweetsJsonStringDF2TweetsDFWithSchema(tweetsAsJsonStringInputDF: DataFrame, schema: StructType): DataFrame = {
      spark
        .read
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .json(tweetsAsJsonStringInputDF.map({case Row(val1: String) => val1}))
      }

def tweetsIDLong_JsonStringPairDF2TweetsDFWithSchema(tweetsAsIDLong_JsonStringInputDF: DataFrame, schema: StructType): DataFrame = {
      spark
        .read
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .json(tweetsAsIDLong_JsonStringInputDF.map({case Row(val0:Long, val1: String) => val1}))
      }

def tweetsIDLong_JsonString_tweetDate_DF2TweetsDFWithSchema(tweetsAsIDLong_JsonStringInput_tweetDateDF: DataFrame, schema: StructType) : DataFrame = {
      spark
        .read
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .json(tweetsAsIDLong_JsonStringInput_tweetDateDF.map({case Row(val0:Long, val1: String, val2: Timestamp) => val1}))
      }

// COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

def tweetsDF2TTTDF(tweetsInputDF: DataFrame): DataFrame = {
 tweetsInputDF.select(
  unix_timestamp($"createdAt", """MMM dd, yyyy hh:mm:ss a""").cast(TimestampType).as("CurrentTweetDate"),
  $"id".as("CurrentTwID"),
  $"lang".as("lang"),
  $"geoLocation.latitude".as("lat"),
  $"geoLocation.longitude".as("lon"),
  unix_timestamp($"retweetedStatus.createdAt", """MMM dd, yyyy hh:mm:ss a""").cast(TimestampType).as("CreationDateOfOrgTwInRT"), 
  $"retweetedStatus.id".as("OriginalTwIDinRT"),  
  unix_timestamp($"quotedStatus.createdAt", """MMM dd, yyyy hh:mm:ss a""").cast(TimestampType).as("CreationDateOfOrgTwInQT"), 
  $"quotedStatus.id".as("OriginalTwIDinQT"), 
  $"inReplyToStatusId".as("OriginalTwIDinReply"), 
  $"user.id".as("CPostUserId"),
  unix_timestamp($"user.createdAt", """MMM dd, yyyy hh:mm:ss a""").cast(TimestampType).as("userCreatedAtDate"),
  $"retweetedStatus.user.id".as("OPostUserIdinRT"), 
  $"quotedStatus.user.id".as("OPostUserIdinQT"),
  $"inReplyToUserId".as("OPostUserIdinReply"),
  $"user.name".as("CPostUserName"), 
  $"retweetedStatus.user.name".as("OPostUserNameinRT"), 
  $"quotedStatus.user.name".as("OPostUserNameinQT"), 
  $"user.screenName".as("CPostUserSN"), 
  $"retweetedStatus.user.screenName".as("OPostUserSNinRT"), 
  $"quotedStatus.user.screenName".as("OPostUserSNinQT"),
  $"inReplyToScreenName".as("OPostUserSNinReply"),
  $"user.favouritesCount",
  $"user.followersCount",
  $"user.friendsCount",
  $"user.isVerified",
  $"user.isGeoEnabled",
  $"text".as("CurrentTweet"), 
  $"retweetedStatus.userMentionEntities.id".as("UMentionRTiD"), 
  $"retweetedStatus.userMentionEntities.screenName".as("UMentionRTsN"), 
  $"quotedStatus.userMentionEntities.id".as("UMentionQTiD"), 
  $"quotedStatus.userMentionEntities.screenName".as("UMentionQTsN"), 
  $"userMentionEntities.id".as("UMentionASiD"), 
  $"userMentionEntities.screenName".as("UMentionASsN")
 ).withColumn("TweetType",
    when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply" === -1,
      "Original Tweet")
    .when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply" > -1,
      "Reply Tweet")
    .when($"OriginalTwIDinRT".isNotNull &&$"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply" === -1,
      "ReTweet")
    .when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply" === -1,
      "Quoted Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply" === -1,
      "Retweet of Quoted Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply" > -1,
      "Retweet of Reply Tweet")
    .when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply" > -1,
      "Reply of Quoted Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply" > -1,
      "Retweet of Quoted Rely Tweet")
      .otherwise("Unclassified"))
.withColumn("MentionType", 
    when($"UMentionRTid".isNotNull && $"UMentionQTid".isNotNull, "RetweetAndQuotedMention")
    .when($"UMentionRTid".isNotNull && $"UMentionQTid".isNull, "RetweetMention")
    .when($"UMentionRTid".isNull && $"UMentionQTid".isNotNull, "QuotedMention")
    .when($"UMentionRTid".isNull && $"UMentionQTid".isNull, "AuthoredMention")
    .otherwise("NoMention"))
.withColumn("Weight", lit(1L))
}

def tweetsDF2TTTDFWithURLsAndHashtags(tweetsInputDF: DataFrame): DataFrame = {
 tweetsInputDF.select(
  unix_timestamp($"createdAt", """MMM dd, yyyy hh:mm:ss a""").cast(TimestampType).as("CurrentTweetDate"),
  $"id".as("CurrentTwID"),
  $"lang".as("lang"),
  $"geoLocation.latitude".as("lat"),
  $"geoLocation.longitude".as("lon"),
  unix_timestamp($"retweetedStatus.createdAt", """MMM dd, yyyy hh:mm:ss a""").cast(TimestampType).as("CreationDateOfOrgTwInRT"), 
  $"retweetedStatus.id".as("OriginalTwIDinRT"),  
  unix_timestamp($"quotedStatus.createdAt", """MMM dd, yyyy hh:mm:ss a""").cast(TimestampType).as("CreationDateOfOrgTwInQT"), 
  $"quotedStatus.id".as("OriginalTwIDinQT"), 
  $"inReplyToStatusId".as("OriginalTwIDinReply"), 
  $"user.id".as("CPostUserId"),
  unix_timestamp($"user.createdAt", """MMM dd, yyyy hh:mm:ss a""").cast(TimestampType).as("userCreatedAtDate"),
  $"retweetedStatus.user.id".as("OPostUserIdinRT"), 
  $"quotedStatus.user.id".as("OPostUserIdinQT"),
  $"inReplyToUserId".as("OPostUserIdinReply"),
  $"user.name".as("CPostUserName"), 
  $"retweetedStatus.user.name".as("OPostUserNameinRT"), 
  $"quotedStatus.user.name".as("OPostUserNameinQT"), 
  $"user.screenName".as("CPostUserSN"), 
  $"retweetedStatus.user.screenName".as("OPostUserSNinRT"), 
  $"quotedStatus.user.screenName".as("OPostUserSNinQT"),
  $"inReplyToScreenName".as("OPostUserSNinReply"),
  $"user.favouritesCount",
  $"user.followersCount",
  $"user.friendsCount",
  $"user.isVerified",
  $"user.isGeoEnabled",
  $"text".as("CurrentTweet"), 
  $"retweetedStatus.userMentionEntities.id".as("UMentionRTiD"), 
  $"retweetedStatus.userMentionEntities.screenName".as("UMentionRTsN"), 
  $"quotedStatus.userMentionEntities.id".as("UMentionQTiD"), 
  $"quotedStatus.userMentionEntities.screenName".as("UMentionQTsN"), 
  $"userMentionEntities.id".as("UMentionASiD"), 
  $"userMentionEntities.screenName".as("UMentionASsN"),
  $"urlEntities.expandedURL".as("URLs"),
  $"hashtagEntities.text".as("hashTags")
 ).withColumn("TweetType",
    when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply" === -1,
      "Original Tweet")
    .when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply" > -1,
      "Reply Tweet")
    .when($"OriginalTwIDinRT".isNotNull &&$"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply" === -1,
      "ReTweet")
    .when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply" === -1,
      "Quoted Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply" === -1,
      "Retweet of Quoted Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply" > -1,
      "Retweet of Reply Tweet")
    .when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply" > -1,
      "Reply of Quoted Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply" > -1,
      "Retweet of Quoted Rely Tweet")
      .otherwise("Unclassified"))
.withColumn("MentionType", 
    when($"UMentionRTid".isNotNull && $"UMentionQTid".isNotNull, "RetweetAndQuotedMention")
    .when($"UMentionRTid".isNotNull && $"UMentionQTid".isNull, "RetweetMention")
    .when($"UMentionRTid".isNull && $"UMentionQTid".isNotNull, "QuotedMention")
    .when($"UMentionRTid".isNull && $"UMentionQTid".isNull, "AuthoredMention")
    .otherwise("NoMention"))
.withColumn("Weight", lit(1L))
}

def twarcTTTDF(tweetsInputDF: DataFrame): DataFrame = {
   tweetsInputDF.select(
   unix_timestamp($"created_at", """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("CurrentTweetDate"),
   $"id".as("CurrentTwID"),  
   $"lang".as("lang"),
   element_at($"coordinates.coordinates", 2).as("lat"), 
   element_at($"coordinates.coordinates", 1).as("lon"),
   unix_timestamp($"retweeted_status.created_at", """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("CreationDateOfOrgTwInRT"), 
   $"retweeted_status.id".as("OriginalTwIDinRT"),  
   unix_timestamp($"quoted_status.created_at", """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("CreationDateOfOrgTwInQT"), 
   $"quoted_status_id".as("OriginalTwIDinQT"),
   $"in_reply_to_status_id".as("OriginalTwIDinReply"),
   $"user.id".as("CPostUserId"),
   unix_timestamp($"user.created_at", """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("userCreatedAtDate"),
   $"retweeted_status.user.id".as("OPostUserIdinRT"),  
   $"quoted_status.user.id".as("OPostUserIdinQT"),
   $"in_reply_to_user_id".as("OPostUserIdinReply"),
   $"user.name".as("CPostUserName"), 
   $"retweeted_status.user.name".as("OPostUserNameinRT"), 
   $"quoted_status.user.name".as("OPostUserNameinQT"), 
   $"user.screen_name".as("CPostUserSN"), 
   $"retweeted_status.user.screen_name".as("OPostUserSNinRT"), 
   $"quoted_status.user.screen_name".as("OPostUserSNinQT"),   
   $"in_reply_to_screen_name".as("OPostUserSNinReply"),
   $"user.favourites_count".as("favouritesCount"),  
   $"user.followers_count".as("followersCount"),
   $"user.friends_count".as("friendsCount"),
   $"user.verified".as("isVerified"),
   $"user.geo_enabled".as("isGeoEnabled"),
   $"full_text".as("CurrentTweet"),
   $"retweeted_status.entities.user_mentions.id".as("UMentionRTiD"), 
   $"retweeted_status.entities.user_mentions.screen_name".as("UMentionRTsN"),   
   $"quoted_status.entities.user_mentions.id".as("UMentionQTiD"),
   $"quoted_status.entities.user_mentions.screen_name".as("UMentionQTsN"),
   $"entities.user_mentions.id".as("UMentionASiD"),  
   $"entities.user_mentions.screen_name".as("UMentionASsN"),  
//   $"entities.urls.expanded_url".as("URLs"), 
//   $"entities.hashtags.text".as("hashTags"),
//   $"extended_entities.media.type".as("mediaType") 
 ).withColumn("TweetType",  //note that column OriginalTwIDinReply, can be null here instead of -1
    when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply".isNull,
      "Original Tweet")
    .when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply".isNotNull,
      "Reply Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply".isNull,
      "ReTweet")
    .when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply".isNull,
      "Quoted Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply".isNull,
      "Retweet of Quoted Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply".isNotNull,
      "Retweet of Reply Tweet")
    .when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply".isNotNull,
      "Reply of Quoted Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply".isNotNull,
      "Retweet of Quoted Reply Tweet")
      .otherwise("Unclassified"))
.withColumn("MentionType", 
    when($"UMentionRTid".isNotNull && $"UMentionQTid".isNotNull, "RetweetAndQuotedMention")
    .when($"UMentionRTid".isNotNull && $"UMentionQTid".isNull, "RetweetMention")
    .when($"UMentionRTid".isNull && $"UMentionQTid".isNotNull, "QuotedMention")
    .when($"UMentionRTid".isNull && $"UMentionQTid".isNull, "AuthoredMention")
    .otherwise("NoMention"))
.withColumn("Weight", lit(1L))
}

def twarcTTTDFWithURLsAndHashtags(tweetsInputDF: DataFrame): DataFrame = {
   tweetsInputDF.select(
   unix_timestamp($"created_at", """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("CurrentTweetDate"),
   $"id".as("CurrentTwID"),  
   $"lang".as("lang"),
   element_at($"coordinates.coordinates", 2).as("lat"), 
   element_at($"coordinates.coordinates", 1).as("lon"),
   unix_timestamp($"retweeted_status.created_at", """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("CreationDateOfOrgTwInRT"), 
   $"retweeted_status.id".as("OriginalTwIDinRT"),  
   unix_timestamp($"quoted_status.created_at", """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("CreationDateOfOrgTwInQT"), 
   $"quoted_status_id".as("OriginalTwIDinQT"),
   $"in_reply_to_status_id".as("OriginalTwIDinReply"),
   $"user.id".as("CPostUserId"),
   unix_timestamp($"user.created_at", """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("userCreatedAtDate"),
   $"retweeted_status.user.id".as("OPostUserIdinRT"),  
   $"quoted_status.user.id".as("OPostUserIdinQT"),
   $"in_reply_to_user_id".as("OPostUserIdinReply"),
   $"user.name".as("CPostUserName"), 
   $"retweeted_status.user.name".as("OPostUserNameinRT"), 
   $"quoted_status.user.name".as("OPostUserNameinQT"), 
   $"user.screen_name".as("CPostUserSN"), 
   $"retweeted_status.user.screen_name".as("OPostUserSNinRT"), 
   $"quoted_status.user.screen_name".as("OPostUserSNinQT"),   
   $"in_reply_to_screen_name".as("OPostUserSNinReply"),
   $"user.favourites_count".as("favouritesCount"),  
   $"user.followers_count".as("followersCount"),
   $"user.friends_count".as("friendsCount"),
   $"user.verified".as("isVerified"),
   $"user.geo_enabled".as("isGeoEnabled"),
   $"full_text".as("CurrentTweet"),
   $"retweeted_status.entities.user_mentions.id".as("UMentionRTiD"), 
   $"retweeted_status.entities.user_mentions.screen_name".as("UMentionRTsN"),   
   $"quoted_status.entities.user_mentions.id".as("UMentionQTiD"),
   $"quoted_status.entities.user_mentions.screen_name".as("UMentionQTsN"),
   $"entities.user_mentions.id".as("UMentionASiD"),  
   $"entities.user_mentions.screen_name".as("UMentionASsN"),  
   $"entities.urls.expanded_url".as("URLs"), 
   $"entities.hashtags.text".as("hashTags"),
//   $"extended_entities.media.type".as("mediaType") 
 ).withColumn("TweetType",  //note that column OriginalTwIDinReply, can be null here instead of -1
    when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply".isNull,
      "Original Tweet")
    .when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply".isNotNull,
      "Reply Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply".isNull,
      "ReTweet")
    .when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply".isNull,
      "Quoted Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply".isNull,
      "Retweet of Quoted Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply".isNotNull,
      "Retweet of Reply Tweet")
    .when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply".isNotNull,
      "Reply of Quoted Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply".isNotNull,
      "Retweet of Quoted Reply Tweet")
      .otherwise("Unclassified"))
.withColumn("MentionType", 
    when($"UMentionRTid".isNotNull && $"UMentionQTid".isNotNull, "RetweetAndQuotedMention")
    .when($"UMentionRTid".isNotNull && $"UMentionQTid".isNull, "RetweetMention")
    .when($"UMentionRTid".isNull && $"UMentionQTid".isNotNull, "QuotedMention")
    .when($"UMentionRTid".isNull && $"UMentionQTid".isNull, "AuthoredMention")
    .otherwise("NoMention"))
.withColumn("Weight", lit(1L))
}

//The following function twarcTTTDFWithRetweetsLikesAndMedia() includes the number of retweets and likes a Tweet has. This only makes sense to use if the tweets are collected retroactively. It also includes media content. For Tweets collected via streaming, use twarcTTTDF()

def twarcTTTDFWithRetweetsLikesAndMedia(tweetsInputDF: DataFrame): DataFrame = {
   tweetsInputDF.select(
   unix_timestamp($"created_at", """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("CurrentTweetDate"),
   $"id".as("CurrentTwID"),  
   $"lang".as("lang"),
   element_at($"coordinates.coordinates", 2).as("lat"), 
   element_at($"coordinates.coordinates", 1).as("lon"),
   unix_timestamp($"retweeted_status.created_at", """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("CreationDateOfOrgTwInRT"), 
   $"retweeted_status.id".as("OriginalTwIDinRT"),  
   unix_timestamp($"quoted_status.created_at", """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("CreationDateOfOrgTwInQT"), 
   $"quoted_status_id".as("OriginalTwIDinQT"),
   $"in_reply_to_status_id".as("OriginalTwIDinReply"),
   $"favorite_count".as("CTweetFavourites"),
   $"retweet_count".as("CTweetRetweets"), 
   $"user.id".as("CPostUserId"),
   unix_timestamp($"user.created_at", """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("userCreatedAtDate"),
   $"retweeted_status.user.id".as("OPostUserIdinRT"),  
   $"quoted_status.user.id".as("OPostUserIdinQT"),
   $"in_reply_to_user_id".as("OPostUserIdinReply"),
   $"user.name".as("CPostUserName"), 
   $"retweeted_status.user.name".as("OPostUserNameinRT"), 
   $"quoted_status.user.name".as("OPostUserNameinQT"), 
   $"user.screen_name".as("CPostUserSN"), 
   $"retweeted_status.user.screen_name".as("OPostUserSNinRT"), 
   $"quoted_status.user.screen_name".as("OPostUserSNinQT"),   
   $"in_reply_to_screen_name".as("OPostUserSNinReply"),
   $"user.favourites_count".as("favouritesCount"),  
   $"user.followers_count".as("followersCount"),
   $"user.friends_count".as("friendsCount"),
   $"user.verified".as("isVerified"),
   $"user.geo_enabled".as("isGeoEnabled"),
   $"full_text".as("CurrentTweet"),
   $"retweeted_status.entities.user_mentions.id".as("UMentionRTiD"), 
   $"retweeted_status.entities.user_mentions.screen_name".as("UMentionRTsN"),   
   $"quoted_status.entities.user_mentions.id".as("UMentionQTiD"),
   $"quoted_status.entities.user_mentions.screen_name".as("UMentionQTsN"),
   $"entities.user_mentions.id".as("UMentionASiD"),  
   $"entities.user_mentions.screen_name".as("UMentionASsN"),  
   $"entities.urls.expanded_url".as("URLs"), 
   $"entities.hashtags.text".as("hashTags"),
   $"extended_entities.media.type".as("mediaType") 
 ).withColumn("TweetType",  
    when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply".isNull,
      "Original Tweet")
    .when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply".isNotNull,
      "Reply Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply".isNull,
      "ReTweet")
    .when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply".isNull,
      "Quoted Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply".isNull,
      "Retweet of Quoted Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNull && $"OriginalTwIDinReply".isNotNull,
      "Retweet of Reply Tweet")
    .when($"OriginalTwIDinRT".isNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply".isNotNull,
      "Reply of Quoted Tweet")
    .when($"OriginalTwIDinRT".isNotNull && $"OriginalTwIDinQT".isNotNull && $"OriginalTwIDinReply".isNotNull,
      "Retweet of Quoted Reply Tweet")
      .otherwise("Unclassified"))
.withColumn("MentionType", 
    when($"UMentionRTid".isNotNull && $"UMentionQTid".isNotNull, "RetweetAndQuotedMention")
    .when($"UMentionRTid".isNotNull && $"UMentionQTid".isNull, "RetweetMention")
    .when($"UMentionRTid".isNull && $"UMentionQTid".isNotNull, "QuotedMention")
    .when($"UMentionRTid".isNull && $"UMentionQTid".isNull, "AuthoredMention")
    .otherwise("NoMention"))
.withColumn("Weight", lit(1L))
}
