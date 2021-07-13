package org.lamastex.mep.tw.ttt
import org.apache.spark.sql.types.{StructType, StructField, StringType};
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.Row
import java.text.SimpleDateFormat
import java.util.Date
import java.sql.Timestamp
object TTTDF_with_inferred_schemas{   
  def twarcTTTDF(tweetsInputDF: DataFrame): DataFrame = {
    val list_ =udf(() => List.empty[Long])
    tweetsInputDF.select(
      unix_timestamp(col("created_at"), """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("CurrentTweetDate"),
      col("id").as("CurrentTwID"),  
      col("lang").as("lang"),
      element_at(col("coordinates.coordinates"), 2).as("lat"), 
      element_at(col("coordinates.coordinates"), 1).as("lon"),
      unix_timestamp(col("retweeted_status.created_at"), """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("CreationDateOfOrgTwInRT"), 
      col("retweeted_status.id").as("OriginalTwIDinRT"),  
      unix_timestamp(col("quoted_status.created_at"), """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("CreationDateOfOrgTwInQT"), 
      col("quoted_status_id").as("OriginalTwIDinQT"),
      col("in_reply_to_status_id").as("OriginalTwIDinReply"),
      col("user.id").as("CPostUserId"),
      unix_timestamp(col("user.created_at"), """EEE MMM dd HH:mm:ss ZZZZ yyyy""").cast(TimestampType).as("userCreatedAtDate"),
      col("retweeted_status.user.id").as("OPostUserIdinRT"),  
      col("quoted_status.user.id").as("OPostUserIdinQT"),
      col("in_reply_to_user_id").as("OPostUserIdinReply"),
      col("user.name").as("CPostUserName"), 
      col("retweeted_status.user.name").as("OPostUserNameinRT"), 
      col("quoted_status.user.name").as("OPostUserNameinQT"), 
      col("user.screen_name").as("CPostUserSN"), 
      col("retweeted_status.user.screen_name").as("OPostUserSNinRT"), 
      col("quoted_status.user.screen_name").as("OPostUserSNinQT"),   
      col("in_reply_to_screen_name").as("OPostUserSNinReply"),
      col("user.favourites_count").as("favouritesCount"),  
      col("user.followers_count").as("followersCount"),
      col("user.friends_count").as("friendsCount"),
      col("user.verified").as("isVerified"),
      col("user.geo_enabled").as("isGeoEnabled"),
      col("full_text").as("CurrentTweet"),
      col("retweeted_status.entities.user_mentions.id").as("UMentionRTiD"), 
      col("retweeted_status.entities.user_mentions.screen_name").as("UMentionRTsN"),   
      col("quoted_status.entities.user_mentions.id").as("UMentionQTiD"),
      col("quoted_status.entities.user_mentions.screen_name").as("UMentionQTsN"),
      col("entities.user_mentions.id").as("UMentionASiD"),  
      col("entities.user_mentions.screen_name").as("UMentionASsN"))
      .withColumn("TweetType",  
        when(col("OriginalTwIDinRT").isNull && col("OriginalTwIDinQT").isNull && col("OriginalTwIDinReply").isNull,
          "Original Tweet")
        .when(col("OriginalTwIDinRT").isNull && col("OriginalTwIDinQT").isNull && col("OriginalTwIDinReply").isNotNull,
          "Reply Tweet")
        .when(col("OriginalTwIDinRT").isNotNull && col("OriginalTwIDinQT").isNull && col("OriginalTwIDinReply").isNull,
          "ReTweet")
        .when(col("OriginalTwIDinRT").isNull && col("OriginalTwIDinQT").isNotNull && col("OriginalTwIDinReply").isNull,
          "Quoted Tweet")
        .when(col("OriginalTwIDinRT").isNotNull && col("OriginalTwIDinQT").isNotNull && col("OriginalTwIDinReply").isNull,
          "Retweet of Quoted Tweet")
        .when(col("OriginalTwIDinRT").isNotNull && col("OriginalTwIDinQT").isNull && col("OriginalTwIDinReply").isNotNull,
          "Retweet of Reply Tweet")
        .when(col("OriginalTwIDinRT").isNull && col("OriginalTwIDinQT").isNotNull && col("OriginalTwIDinReply").isNotNull,
          "Reply of Quoted Tweet")
        .when(col("OriginalTwIDinRT").isNotNull && col("OriginalTwIDinQT").isNotNull && col("OriginalTwIDinReply").isNotNull,
          "Retweet of Quoted Reply Tweet")
      .otherwise("Unclassified"))
      .withColumn("MentionType", 
        when(col("UMentionRTid").isNotNull && col("UMentionQTid").isNotNull, "RetweetAndQuotedMention")
        .when(col("UMentionRTid").isNotNull && col("UMentionQTid").isNull, "RetweetMention")
        .when(col("UMentionRTid").isNull && col("UMentionQTid").isNotNull, "QuotedMention")
        .when(col("UMentionRTid").isNull && col("UMentionQTid").isNull, "AuthoredMention")
      .otherwise("NoMention")) //As far as I can tell NoMention cant happen, check with Raaz
      .withColumn("Weight", lit(1L))   
  }
   

  /*
   .withColumn("MentionType", 
        when((size(col("UMentionRTid")) =!= 0) && (size(col("UMentionQTid")) =!= 0), "RetweetAndQuotedMention")
        .when((size(col("UMentionRTid")) =!= 0) && (size(col("UMentionQTid")) === 0), "RetweetMention")
        .when((size(col("UMentionRTid")) === 0) && (size(col("UMentionQTid")) =!=0), "QuotedMention")
        .when((size(col("UMentionRTid")) === 0) && (size(col("UMentionQTid")) === 0), "AuthoredMention") // weird as far as I can tell no mention cant happen, check with Raaz
      .otherwise("NoMention"))

      .withColumn("UMentionRTiD", when(col("UMentionRTiD").isNull, list_()).otherwise(col("UMentionRTiD")))
      .withColumn("UMentionRTsN", when(col("UMentionRTsN").isNull, list_()).otherwise(col("UMentionRTsN")))
      .withColumn("UMentionQTiD", when(col("UMentionQTiD").isNull, list_()).otherwise(col("UMentionQTiD")))
      .withColumn("UMentionQTsN", when(col("UMentionQTsN").isNull, list_()).otherwise(col("UMentionQTsN")))
      .withColumn("UMentionASiD", when(col("UMentionASiD").isNull, list_()).otherwise(col("UMentionASiD")))
      .withColumn("UMentionASsN", when(col("UMentionASsN").isNull, list_()).otherwise(col("UMentionASsN")))
      */
}
  
