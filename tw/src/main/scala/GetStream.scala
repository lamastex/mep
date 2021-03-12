package org.lamastex.mep.tw


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
