import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j.Paging
import twitter4j.conf.ConfigurationBuilder

object getUserTimeline extends TwitterBasic {

    def main(args : Array[String]) {

        // read the config file and create a Twitter instance
        populateFromConfigFile()
        val twitter = getTwitterInstance

        // use the twitter object to get a user's timeline
        val paging = new Paging(1, 200)
        val statuses = twitter.getUserTimeline("elonmusk", paging) 
        val num_statuses = statuses.size
        println("Showing user timeline with number of status updates = " + num_statuses.toString)
        val it = statuses.iterator
        while (it.hasNext()) {
            val status = it.next
            println(status.getUser.getName + ":" + status.getText);
        }
    }

}
