package org.lamastex.mep.tw
import twitter4j.FilterQuery

// This is adapted from
// https://bcomposes.wordpress.com/2013/02/09/using-twitter4j-with-scala-to-access-streaming-tweets/

object StatusStreamer extends TwitterBasic {
  def main(args : Array[String]): Unit = {
    val twitterStream = getTwitterStreamInstance
    //sys.addShutdownHook(stopTwitterStreamInstance(twitterStream, 1000L))
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

object FollowIdsStreamer extends TwitterBasic {
  def main(args: Array[String]): Unit = {
    val twitterStream = getTwitterStreamInstance
    twitterStream.addListener(simpleStatusListener)
    val stopStreamInMs = T(()=>args(0).toLong).getOrElse(10000L)
    var FollowIds = args.drop(1).map( i => T(()=>i.toLong)  )
                                  .collect({ case Some(i) => i })
    //Use default as the IDs for Wired Magazine (@wired), 
    //The Economist (@theeconomist), the New York Times (@nytimes), 
    //and the Wall Street Journal (@wsj). 
    if(FollowIds.isEmpty) FollowIds = Array(1344951L,5988062L,807095L,3108351L)
    //FollowIds.map(println)
    twitterStream.filter(new FilterQuery(FollowIds:_*))
    stopTwitterStreamInstance(twitterStream, stopStreamInMs)
  }
}

object LocationStreamer extends TwitterBasic {
  def main(args: Array[String]): Unit = {
    val twitterStream = getTwitterStreamInstance
    twitterStream.addListener(simpleStatusListener)
    val stopStreamInMs = T(()=>args(0).toLong).getOrElse(10000L)
    // see https://gist.github.com/graydon/11198540
    val swedenBox = Array(Array(11.0273686052, 55.3617373725),Array(23.9033785336, 69.1062472602))
    val finlandBox = Array(Array(20.6455928891, 59.846373196),Array(31.5160921567, 70.1641930203))
    var boundingBoxes = args.drop(1).map( i => T(()=>i.toDouble)  )
                                  .collect({ case Some(i) => i })
                                  .grouped(2).toArray
    if(boundingBoxes.isEmpty || boundingBoxes.size<1) boundingBoxes = swedenBox++finlandBox
    boundingBoxes.map(x => x.map(println))
    twitterStream.filter(new FilterQuery().locations(boundingBoxes:_*))
    stopTwitterStreamInstance(twitterStream, stopStreamInMs)
  }
}
