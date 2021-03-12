package org.lamastex.mep.tw
import twitter4j.Status
import scala.collection.mutable.ArrayBuffer
import scala.util.{Try,Success,Failure}
import java.io._
import scala.io.{Source, BufferedSource}


object getStatusFromID extends TwitterBasic {
  
  def main(args : Array[String]): Unit = {

    // read the config file and create a Twitter instance
    populateFromConfigFile()
    val twitter = getTwitterInstance
    // use the twitter object to get a user's timeline
    var Tweet_ID: Long = 689614253028839424L //

    val wd = os.pwd / "work"
    val fileRoot = "tweetIDs"
    val inputTweetIDs = fileRoot+".txt"
    val outputTweets = fileRoot+".JSON"
    // Streaming the lines to the console
    val statuses = ArrayBuffer[Status]()
    var statusGSON: String = new String()
    val input = os.read.lines.stream(wd / inputTweetIDs)
    if (os.exists(wd / outputTweets)) {os.remove(wd / outputTweets)}
    for(line <- input){
      println(line)
      tryStringToLong(line) match {
        case Success(id) => { 
          Tweet_ID = id;
          tryStatusFromStatusID(twitter, Tweet_ID) match {
            case Success(s) => {
              statusGSON = statusToGson(s)
              os.write.append(wd / outputTweets,statusGSON+"\n")
            }
            case Failure(s) => println(s"Failed. Reason: $s")
          }
        }  
        case Failure(e) => println(s"Failed. Reason: $e")
        }
      }// end for loop
      // get Status fro Tweet ID and accrue it to statuses
      //--- if you just want successful status
      //val status = tryStatusFromStatusID(twitter, Tweet_ID)
      //if (status.isSuccess) {statuses += status.get}
      /*tryStatusFromStatusID(twitter, Tweet_ID) match {
        case Success(status) => statuses += status
        case Failure(s) => println(s"Failed. Reason: $s")
      }
    }
    printTweets(statuses)*/
  }
}
