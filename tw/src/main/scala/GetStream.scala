package org.lamastex.mep.tw


object StatusStreamer extends TwitterBasic {
  def main(args : Array[String]): Unit = {

    // read the config file and create a Twitter instance
    populateFromConfigFile()
    val twitterStream = getTwitterStreamInstance

    twitterStream.addListener(simpleStatusListener)
    twitterStream.sample
    Thread.sleep(10000)
    twitterStream.cleanUp
    twitterStream.shutdown
  }
}
