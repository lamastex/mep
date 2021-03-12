package org.lamastex.mep.tw
import twitter4j.FilterQuery

object StatusStreamer extends TwitterBasic {
  def main(args : Array[String]): Unit = {
    val twitterStream = getTwitterStreamInstance
    twitterStream.addListener(simpleStatusListener)
    twitterStream.sample
    Thread.sleep(10000)
    twitterStream.cleanUp
    twitterStream.shutdown
  }
}

object SearchStreamer extends TwitterBasic {
  def main(args: Array[String]): Unit = {
    val twitterStream = getTwitterStreamInstance
    twitterStream.addListener(simpleStatusListener)
    twitterStream.filter(new FilterQuery().track(args:_*))
    Thread.sleep(10000)
    twitterStream.cleanUp
    twitterStream.shutdown
  }
}
