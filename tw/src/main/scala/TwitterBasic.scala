import twitter4j.conf.ConfigurationBuilder
import twitter4j.Twitter
import twitter4j.TwitterFactory
import twitter4j.TwitterException
import twitter4j.Status
import scala.collection.mutable.ArrayBuffer
import com.google.gson.Gson 
import scala.util.{Try,Success,Failure}

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

  def getTwitterInstance: Twitter = {
      val cb = new ConfigurationBuilder()
      cb.setDebugEnabled(true)
          .setOAuthConsumerKey(APIKey)
          .setOAuthConsumerSecret(APISecret)
          .setOAuthAccessToken(accessToken)
          .setOAuthAccessTokenSecret(accessTokenSecret)
      return new TwitterFactory(cb.build()).getInstance
  }

  def populateFromConfigFile(): Unit = {
      val twconf = com.typesafe.config.ConfigFactory.load()
      APIKey = twconf.getString("TwitterConf.Oauth.APIKey")
      APISecret = twconf.getString("TwitterConf.Oauth.APISecret")
      accessToken = twconf.getString("TwitterConf.Oauth.AccessToken")
      accessTokenSecret = twconf.getString("TwitterConf.Oauth.AccessTokenSecret")
  }

  def sleep(ms: Long): Unit = {
    try { Thread.sleep(ms); }
    catch{
      case intExcpt: InterruptedException => { Thread.currentThread().interrupt(); }
      case _: Throwable => println("Got some other kind of Throwable exception")
    }
  }

  def getStatusFromStatusID(twitter: Twitter, Tweet_ID: Long): Try[Status] = Try {
      twitter.showStatus(Tweet_ID)
  } 

  def statusToGson(status: Status): String = {
    val gson = new Gson();
    val statusJson = gson.toJson(status)
    return statusJson
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
