package org.lamastex.mep.tw.ttt
import java.sql.Timestamp
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.types.{StructType,DataType};
import org.apache.spark.sql.Dataset
import org.lamastex.mep.tw.ttt.TTTConverters._
object TTTFormats{
    /**
    * @param CurrentTweetDate
    * @param CurrentTwID
    * @param lang
    * @param lat
    * @param lat
    * @param CreationDateOfOrgTwInRT
    * @param OriginalTwIDinRT
    * @param CreationDateOfOrgTwInQT
    * @param OriginalTwIDinQT
    * @param OriginalTwIDinReply
    * @param CPostUserId
    * @param userCreatedAtDate
    * @param OPostUserIdinRT
    * @param OPostUserIdinQT
    * @param OPostUserIdinReply
    * @param CPostUserName
    * @param OPostUserNameinRT
    * @param OPostUserNameinQT
    * @param CPostUserSN
    * @param OPostUserSNinRT
    * @param OPostUserSNinQT
    * @param OPostUserSNinReply
    * @param favouritesCount
    * @param followersCount
    * @param friendsCount
    * @param isVerified
    * @param isGeoEnabled
    * @param CurrentTweet
    * @param UMentionRTiD
    * @param UMentionQTiD
    * @param UMentionQTsN
    * @param UMentionASiD
    * @param UMentionASsN
    * @param TweetType
    * @param MentionType
    * @param Weight
    * @param error
    */
    case class TTT(
                    CurrentTweetDate: Timestamp, 
                    CurrentTwID: Option[Long], 
                    lang: Option[String],
                    lat: Option[Double],
                    lon: Option[Double],
                    CreationDateOfOrgTwInRT: Option[Timestamp],
                    OriginalTwIDinRT: Option[Long],
                    CreationDateOfOrgTwInQT: Option[Timestamp],
                    OriginalTwIDinQT: Option[Long],
                    OriginalTwIDinReply: Option[Long],
                    CPostUserId: Option[Long],
                    userCreatedAtDate: Option[Timestamp],
                    OPostUserIdinRT: Option[Long],
                    OPostUserIdinQT: Option[Long],
                    OPostUserIdinReply: Option[Long],
                    CPostUserName: Option[String],
                    OPostUserNameinRT: Option[String],
                    OPostUserNameinQT: Option[String],
                    CPostUserSN: Option[String],
                    OPostUserSNinRT: Option[String],
                    OPostUserSNinQT: Option[String],
                    OPostUserSNinReply: Option[String],
                    favouritesCount: Option[Long],
                    followersCount: Option[Long],
                    friendsCount: Option[Long],
                    isVerified: Option[Boolean],
                    isGeoEnabled: Option[Boolean],
                    CurrentTweet: Option[String],
                    UMentionRTiD: List[Long] = List.empty[Long],
                    UMentionQTiD: List[Long] = List.empty[Long],
                    UMentionQTsN: List[String] = List.empty[String],
                    UMentionASiD: List[Long] = List.empty[Long],
                    UMentionASsN: List[String] = List.empty[String],
                    TweetType: Option[String],
                    MentionType: Option[String],
                    Weight: Option[Long],
                    error: Option[String]
                )
    /**
    *
    * @param CurrentTweetDate
    * @param CurrentTwID
    * @param lang
    * @param lat
    * @param lat
    * @param CreationDateOfOrgTwInRT
    * @param OriginalTwIDinRT
    * @param CreationDateOfOrgTwInQT
    * @param OriginalTwIDinQT
    * @param OriginalTwIDinReply
    * @param CTweetFavourites
    * @param CTweetRetweets
    * @param CPostUserId
    * @param userCreatedAtDate
    * @param OPostUserIdinRT
    * @param OPostUserIdinQT
    * @param OPostUserIdinReply
    * @param CPostUserName
    * @param OPostUserNameinRT
    * @param OPostUserNameinQT
    * @param CPostUserSN
    * @param OPostUserSNinRT
    * @param OPostUserSNinQT
    * @param OPostUserSNinReply
    * @param favouritesCount
    * @param followersCount
    * @param friendsCount
    * @param isVerified
    * @param isGeoEnabled
    * @param CurrentTweet
    * @param UMentionRTiD
    * @param UMentionQTiD
    * @param UMentionQTsN
    * @param UMentionASiD
    * @param UMentionASsN
    * @param URLs
    * @param hashTags
    * @param TweetType
    * @param MentionType
    * @param Weight
    * @param error
    */
    case class TTTURLsAndHashtags(
                                    CurrentTweetDate: Timestamp, 
                                    CurrentTwID: Option[Long], 
                                    lang: Option[String],
                                    lat: Option[Double],
                                    lon: Option[Double],
                                    CreationDateOfOrgTwInRT: Option[Timestamp],
                                    OriginalTwIDinRT: Option[Long],
                                    CreationDateOfOrgTwInQT: Option[Timestamp],
                                    OriginalTwIDinQT: Option[Long],
                                    OriginalTwIDinReply: Option[Long],
                                    CPostUserId: Option[Long],
                                    userCreatedAtDate: Option[Timestamp],
                                    OPostUserIdinRT: Option[Long],
                                    OPostUserIdinQT: Option[Long],
                                    OPostUserIdinReply: Option[Long],
                                    CPostUserName: Option[String],
                                    OPostUserNameinRT: Option[String],
                                    OPostUserNameinQT: Option[String],
                                    CPostUserSN: Option[String],
                                    OPostUserSNinRT: Option[String],
                                    OPostUserSNinQT: Option[String],
                                    OPostUserSNinReply: Option[String],
                                    favouritesCount: Option[Long],
                                    followersCount: Option[Long],
                                    friendsCount: Option[Long],
                                    isVerified: Option[Boolean],
                                    isGeoEnabled: Option[Boolean],
                                    CurrentTweet: Option[String],
                                    UMentionRTiD: List[Long] = List.empty[Long],
                                    UMentionQTiD: List[Long] = List.empty[Long],
                                    UMentionQTsN: List[String] = List.empty[String],
                                    UMentionASiD: List[Long] = List.empty[Long],
                                    UMentionASsN: List[String] = List.empty[String],
                                    URLs:List[String] = List.empty[String],
                                    hashTags: List[String] =List.empty[String],
                                    TweetType: Option[String],
                                    MentionType: Option[String],
                                    Weight: Option[Long],
                                    error: Option[String]
                                )
    /**
    *
    * @param CurrentTweetDate
    * @param CurrentTwID
    * @param lang
    * @param lat
    * @param lat
    * @param CreationDateOfOrgTwInRT
    * @param OriginalTwIDinRT
    * @param CreationDateOfOrgTwInQT
    * @param OriginalTwIDinQT
    * @param OriginalTwIDinReply
    * @param CPostUserId
    * @param userCreatedAtDate
    * @param OPostUserIdinRT
    * @param OPostUserIdinQT
    * @param OPostUserIdinReply
    * @param CPostUserName
    * @param OPostUserNameinRT
    * @param OPostUserNameinQT
    * @param CPostUserSN
    * @param OPostUserSNinRT
    * @param OPostUserSNinQT
    * @param OPostUserSNinReply
    * @param favouritesCount
    * @param followersCount
    * @param friendsCount
    * @param isVerified
    * @param isGeoEnabled
    * @param CurrentTweet
    * @param UMentionRTiD
    * @param UMentionQTiD
    * @param UMentionQTsN
    * @param UMentionASiD
    * @param UMentionASsN
    * @param URLs
    * @param hashTags
    * @param TweetType
    * @param MentionType
    * @param Weight
    * @param error
    */
    case class TTTRTLikesAndMedia(
                                    CurrentTweetDate: Timestamp, 
                                    CurrentTwID: Option[Long], 
                                    lang: Option[String],
                                    lat: Option[Double],
                                    lon: Option[Double],
                                    CreationDateOfOrgTwInRT: Option[Timestamp],
                                    OriginalTwIDinRT: Option[Long],
                                    CreationDateOfOrgTwInQT: Option[Timestamp],
                                    OriginalTwIDinQT: Option[Long],
                                    OriginalTwIDinReply: Option[Long],
                                    CTweetFavourites: Option[Long],
                                    CTweetRetweets: Option[Long],
                                    CPostUserId: Option[Long],
                                    userCreatedAtDate: Option[Timestamp],
                                    OPostUserIdinRT: Option[Long],
                                    OPostUserIdinQT: Option[Long],
                                    OPostUserIdinReply: Option[Long],
                                    CPostUserName: Option[String],
                                    OPostUserNameinRT: Option[String],
                                    OPostUserNameinQT: Option[String],
                                    CPostUserSN: Option[String],
                                    OPostUserSNinRT: Option[String],
                                    OPostUserSNinQT: Option[String],
                                    OPostUserSNinReply: Option[String],
                                    favouritesCount: Option[Long],
                                    followersCount: Option[Long],
                                    friendsCount: Option[Long],
                                    isVerified: Option[Boolean],
                                    isGeoEnabled: Option[Boolean],
                                    CurrentTweet: Option[String],
                                    UMentionRTiD: List[Long] = List.empty[Long],
                                    UMentionQTiD: List[Long] = List.empty[Long],
                                    UMentionQTsN: List[String] = List.empty[String],
                                    UMentionASiD: List[Long] = List.empty[Long],
                                    UMentionASsN: List[String] = List.empty[String],
                                    URLs:List[String] = List.empty[String],
                                    mediaType: List[String] = List.empty[String],
                                    hashTags: List[String] =List.empty[String],
                                    TweetType: Option[String],
                                    MentionType: Option[String],
                                    Weight: Option[Long],
                                    error: Option[String]
                                )
        
    implicit class TTTSpark(dfReader: DataFrameReader) {

        def twarcToTTT(schema_path: String,inputPaths: String*): Dataset[TTT] = {
            val schema_twarc = DataType.fromJson(dfReader.text(schema_path).first.getString(0)).asInstanceOf[StructType]
            val ds = twarcTTTDF(dfReader.option("mode", "DROPMALFORMED").schema(schema_twarc).json(inputPaths:_*))
            import ds.sparkSession.implicits._
            ds.as[TTT]
        }

        def twarcToTTT(schema_path: String,inputPath: String): Dataset[TTT] = {
            twarcToTTT(schema_path,Seq(inputPath): _*)
        }
        
        def twarcToTTTURLsAndHashtags(schema_path: String,inputPaths: String*): Dataset[TTTURLsAndHashtags] = {
            val schema_twarc = DataType.fromJson(dfReader.text(schema_path).first.getString(0)).asInstanceOf[StructType]
            val ds = twarcTTTDFWithURLsAndHashtags(dfReader.option("mode", "DROPMALFORMED").schema(schema_twarc).json(inputPaths:_*))
            import ds.sparkSession.implicits._
            ds.as[TTTURLsAndHashtags]
        }

        def twarcToTTTURLsAndHashtags(schema_path: String,inputPath: String): Dataset[TTTURLsAndHashtags] = {
            twarcToTTTURLsAndHashtags(schema_path,Seq(inputPath): _*)
        }

        def twarcToTTTRTLikesAndMedia(schema_path: String,inputPaths: String*): Dataset[TTTRTLikesAndMedia] = {
            val schema_twarc = DataType.fromJson(dfReader.text(schema_path).first.getString(0)).asInstanceOf[StructType]
            val ds = twarcTTTDFWithRetweetsLikesAndMedia(dfReader.option("mode", "DROPMALFORMED").schema(schema_twarc).json(inputPaths:_*))
            import ds.sparkSession.implicits._
            ds.as[TTTRTLikesAndMedia]
        }

        def twarcToTTTRTLikesAndMedia(schema_path: String,inputPath: String): Dataset[TTTRTLikesAndMedia] = {
            twarcToTTTRTLikesAndMedia(schema_path,Seq(inputPath): _*)
        } 

        def twitter4jToTTT(schema_path: String,inputPaths: String*): Dataset[TTT] = {
            val schema_twitter4j = DataType.fromJson(dfReader.text(schema_path).first.getString(0)).asInstanceOf[StructType]
            val ds = tweetsDF2TTTDF(dfReader.option("mode", "DROPMALFORMED").schema(schema_twitter4j).json(inputPaths:_*))
            import ds.sparkSession.implicits._
            ds.as[TTT]
        }

        def twitter4jToTTT(schema_path: String,inputPath: String): Dataset[TTT] = {
            twitter4jToTTT(schema_path,Seq(inputPath): _*)
        } 

        def twitter4jToTTTURlsAndHashtags(schema_path: String,inputPaths: String*): Dataset[TTTURLsAndHashtags] = {
            val schema_twitter4j = DataType.fromJson(dfReader.text(schema_path).first.getString(0)).asInstanceOf[StructType]
            val ds = tweetsDF2TTTDFWithURLsAndHashtags(dfReader.option("mode", "DROPMALFORMED").schema(schema_twitter4j).json(inputPaths:_*))
            import ds.sparkSession.implicits._
            ds.as[TTTURLsAndHashtags]
        }

        def twitter4jToTTTURlsAndHashtags(schema_path: String,inputPath: String): Dataset[TTTURLsAndHashtags] = {
            twitter4jToTTTURlsAndHashtags(schema_path,Seq(inputPath): _*)
        } 
    }
}