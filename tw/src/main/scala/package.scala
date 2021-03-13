package org.lamastex.mep
import twitter4j.conf.ConfigurationBuilder
import twitter4j.Twitter
import twitter4j.TwitterFactory
import twitter4j.TwitterStream
import twitter4j.TwitterStreamFactory
import twitter4j.TwitterException
import twitter4j.Status
import twitter4j.StatusListener
import twitter4j.StatusDeletionNotice
import twitter4j.StallWarning
//import twitter4j.
import scala.collection.mutable.ArrayBuffer
import com.google.gson.Gson 
import scala.util.{Try,Success,Failure}

package object tw {

  def T[A](r: () => A): Option[A] = {
    try {
      val x: A = r.apply()
      x match {
        case _ if(x == null) => None
        case p: String => if (p.trim.length == 0) None else Some(p.trim.asInstanceOf[A])
        case _ => Some(x)
      }
    } catch {
      case _: Throwable => None
    }
  }

/**
  * A class to handle most basic Twitter config settings.
  * copy resources/application.conf.template resources/application.conf 
  * and put you Twitter developer credentials there.
  */
class TwitterBasic {

  // twitter
  var APIKey = ""
  var APISecret = ""
  var accessToken = ""
  var accessTokenSecret = ""
  val startTime = System.nanoTime()

  def populateFromConfigFile(): Unit = {
    val twconf = com.typesafe.config.ConfigFactory.load()
    APIKey = twconf.getString("TwitterConf.Oauth.APIKey")
    APISecret = twconf.getString("TwitterConf.Oauth.APISecret")
    accessToken = twconf.getString("TwitterConf.Oauth.AccessToken")
    accessTokenSecret = twconf.getString("TwitterConf.Oauth.AccessTokenSecret")
  }

  def getTwitterInstance: Twitter = {
    // read the config file and create a Twitter instance
    populateFromConfigFile()
    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(APIKey)
      .setOAuthConsumerSecret(APISecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
    return new TwitterFactory(cb.build()).getInstance
  }

  def getTwitterStreamInstance: TwitterStream = {
    // read the config file and create a Twitter instance
    populateFromConfigFile()
    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(APIKey)
      .setOAuthConsumerSecret(APISecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
    return new TwitterStreamFactory(cb.build()).getInstance
  }

  def stopTwitterStreamInstance(twitterStream: TwitterStream, stopAfterMs: Long): Unit = {
    if(stopAfterMs > 0) {
      System.err.println("Stopping TwitterStreamInstance...")
      Thread.sleep(stopAfterMs)
      twitterStream.cleanUp
      twitterStream.shutdown
    }
  }

  def statusToGson(status: Status): String = {
    val gson = new Gson();
    val statusJson = gson.toJson(status)
    return statusJson
  }

  def simpleStatusListener = new StatusListener() {
    //def onStatus(status: Status): Unit = { println(status.getText) }
    def onStatus(status: Status): Unit = { println(statusToGson(status)) }
    def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}
    def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}
    def onException(ex: Exception): Unit = { ex.printStackTrace }
    def onScrubGeo(arg0: Long, arg1: Long) : Unit = {}
    def onStallWarning(warning: StallWarning) : Unit = {}
  }

  def sleep(ms: Long): Unit = {
    try { Thread.sleep(ms); }
    catch{
      case intExcpt: InterruptedException => { Thread.currentThread().interrupt(); }
      case _: Throwable => System.err.println("Got some other kind of Throwable exception")
    }
  }

  def tryStringToLong(Tweet_ID_String: String): Try[Long] = Try {
      Tweet_ID_String.toLong
  } 

  def tryStatusFromStatusID(twitter: Twitter, Tweet_ID: Long): Try[Status] = Try {
      twitter.showStatus(Tweet_ID)
  } 

  def printTweets(statuses: ArrayBuffer[Status]): Unit = {
    val num_statuses = statuses.size
    println("Showing user timeline with number of status updates = " + num_statuses.toString)
    val it = statuses.iterator
    while (it.hasNext) {
      val status = it.next()
      println(status.getUser.getName + ":" + status.getId + ":"+ status.getText);
      println(statusToGson(status))
    }
    println("Just Showed user timeline with number of status updates = " + num_statuses.toString)
  }

}

}
