package org.lamastex.mep.tw
import twitter4j.FilterQuery

object StatusStreamer extends TwitterBasic {
  def main(args : Array[String]): Unit = {
    val twitterStream = getTwitterStreamInstance
    twitterStream.addListener(simpleStatusListener)
    twitterStream.sample
    val stopStreamInMs = T(()=>args(0).toLong).getOrElse(10000L)
    stopTwitterStreamInstance(twitterStream, stopStreamInMs)
  }
}

object SearchStreamer extends TwitterBasic {
  def main(args: Array[String]): Unit = {
    val twitterStream = getTwitterStreamInstance
    twitterStream.addListener(simpleStatusListener)
    val stopStreamInMs = T(()=>args(0).toLong).getOrElse(10000L)
    twitterStream.filter(new FilterQuery().track(args.drop(1):_*))
    stopTwitterStreamInstance(twitterStream, stopStreamInMs)
  }
}
