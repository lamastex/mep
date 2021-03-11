import twitter4j.TwitterFactory
import twitter4j.TwitterException
import twitter4j.Twitter
import twitter4j.Paging
import twitter4j.Status
import twitter4j.conf.ConfigurationBuilder
import scala.collection.mutable.ArrayBuffer
//import scala.collection.JavaConverters._
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks._

object getUserTimeline extends TwitterBasic {
  
  def main(args : Array[String]): Unit = {

    // read the config file and create a Twitter instance
    populateFromConfigFile()
    val twitter = getTwitterInstance
    // use the twitter object to get a user's timeline
    val screenName = "raazozone"
    val statuses = ArrayBuffer[Status]()
    var pageno = 1;
    breakable {
      while(true) {
        try {
          println("getting tweets");
          val size: Int = statuses.size; // actual tweets count we got
          val page: Paging = new Paging(pageno, 200);
          val statusesPerPage = twitter.getUserTimeline(screenName, page)
          statuses.appendAll(statusesPerPage.asScala);
          System.out.println("total got : " + statuses.size);
          if (statuses.size == size) break() // we did not get new tweets so we have done the job
          pageno = pageno+1;
          sleep(1000); // 900 rqt / 15 mn <=> 1 rqt/s
        }
        catch  {
          case e: TwitterException => println(e.getErrorMessage());
        }
      } // end while
    }// end breakable
    printTweets(statuses)
  }
}
