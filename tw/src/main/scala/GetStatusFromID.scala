package org.lamastex.mep.tw
import twitter4j.Twitter
import twitter4j.Status
import scala.collection.mutable.ArrayBuffer
import scala.util.{Try,Success,Failure}

object getStatusFromID extends TwitterBasic {
  
  def main(args : Array[String]): Unit = {

    // read the config file and create a Twitter instance
    populateFromConfigFile()
    val twitter = getTwitterInstance
    // use the twitter object to get a user's timeline
    val Tweet_ID: Long = 689614253028839424L //
    val Tweet_IDs: List[Long] = List(705938638635343872L,714708139082428416L)
    val statuses = ArrayBuffer[Status]()
    // get Status fro Tweet ID and accrue it to statuses
    getStatusFromStatusID(twitter, Tweet_ID) match {
      case Success(status) => statuses += status
      case Failure(s) => println(s"Failed. Reason: $s")
    }
    printTweets(statuses)
  }
}
