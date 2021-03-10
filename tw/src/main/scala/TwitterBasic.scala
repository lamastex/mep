import twitter4j.conf.ConfigurationBuilder
import twitter4j.Twitter
import twitter4j.TwitterFactory

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

  def populateFromConfigFile() {
      val twconf = com.typesafe.config.ConfigFactory.load()
      APIKey = twconf.getString("TwitterConf.Oauth.APIKey")
      APISecret = twconf.getString("TwitterConf.Oauth.APISecret")
      accessToken = twconf.getString("TwitterConf.Oauth.AccessToken")
      accessTokenSecret = twconf.getString("TwitterConf.Oauth.AccessTokenSecret")
  }
}
