package org.lamastex.mep.tw.ttt
import java.sql.Timestamp
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
    }