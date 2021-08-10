package org.lamastex.mep.tw

import java.lang.Thread
import java.util.concurrent.{
  Executors,
  ScheduledExecutorService,
  ScheduledFuture,
  TimeUnit
}
import java.io.{
  File,
  FileWriter
}
import twitter4j.{
  TwitterStream,
  TwitterStreamFactory,
  StatusListener,
  Status,
  FilterQuery,
  StatusDeletionNotice,
  StallWarning
}
import scala.collection.JavaConverters._


/**
  * A class to read a Twitter stream into a buffer.
  *
  * @param streamConfig Configuration for the stream.
  */
class BufferedTwitterStream(val streamConfig: StreamConfig) extends TwitterBasic with Runnable {
  
  var currentConfig: StreamConfig = streamConfig
  var idsToTrack: Seq[Long] = Seq.empty
  var buffer: Iterator[TweetSchema] = Iterator.empty
  var twitterStream: TwitterStream = null

  def getBuffer(): Iterator[TweetSchema] = buffer
  
  /**
    * Looks up the Twitter Users belonging to a Seq of handles.
    *
    * @param twitterHandles
    * @return A list of users corresponding to the handles.
    */
  def lookupUserSNs(twitterHandles: Seq[String]) = {
    val grouped = twitterHandles.grouped(100).toList 
    val twitter = getTwitterInstance
    for {group <- grouped  
      users = twitter.lookupUsers(group:_*)
      user <- users.asScala 
    } yield user     
  }

  /**
    * Given a list of Twitter handles, returns the Twitter Ids corresponding to the handles.
    *
    * @param handles
    * @return A Seq of the valid Ids.
    */
  def getValidTrackedUserIds(handles: Seq[String]) = lookupUserSNs(handles)
    .map(u => u.getId())
    .toSet
    .toSeq
    .filter(_.isValidLong)  

  def getFollowIdsFromFile(handleFilename: String): (Seq[Long], Boolean) = {
    val handlesToTrack = IOHelper.readHandles(handleFilename)
    if (handlesToTrack.nonEmpty)
      (getValidTrackedUserIds(handlesToTrack), false)
    else
      (Seq.empty, true)
  }

  protected def setFollowIdsFromFile(handleFilename: String): Unit = {
    val (validIds, emptyHandles) = getFollowIdsFromFile(handleFilename)
    if (!emptyHandles) {
      println(s"${validIds.length} valid Ids tracked.")
      idsToTrack = validIds
    } else {
      println("No valid Ids tracked. Streaming random sample.")
      idsToTrack = Seq.empty
    }
  }

  protected def getQuery: FilterQuery = {
    val query = new FilterQuery()
    query.follow(idsToTrack: _*)
    return query
  }

  /**
    * When the stream is updated with a new filter, there should be some
    * overlap between the old stream and the new in order to not miss any tweets
    * while the stream is restarting.
    *
    * @param streamOverlap Number of milliseconds to overlap by, default 15000 (15s).
    */
  protected def patchStream(streamOverlap: Long = 15000L): Unit = {
    println("Building new stream.")
    val newStream = new TwitterStreamFactory(twitterStream.getConfiguration).getInstance
    newStream.addListener(simpleStatusListener)
    println("Starting new stream.")
    if (idsToTrack.nonEmpty)
      newStream.filter(getQuery)
    else
      newStream.sample
    Thread.sleep(10000L)
    println("Cleaning up old stream.")
    twitterStream.cleanUp
    twitterStream = newStream
  }

  def updateStream(newConfig: StreamConfig): Unit = {
    setFollowIdsFromFile(newConfig.handlesFilePath)
    currentConfig = newConfig
    getQuery
    patchStream()
  }
  
  /**
    * Handles an incoming status update by adding it to the buffer.
    *
    * @param status
    */
  def handleStatus(status: Status): Unit = {
    val tweet = TweetSchema(
      status.getId, 
      statusToGson(status), 
      status.getCreatedAt.getTime,
      status.getUser.getId,
      "status"
    )    
    buffer = buffer ++ Iterator(tweet)
  }

  def handleDeletion(status: StatusDeletionNotice): Unit = {
    val notice = TweetSchema(
      status.getStatusId,
      "{}",
      0L,
      status.getUserId,
      "deletion"
    )
    buffer = buffer ++ Iterator(notice)
  }
  
  override def simpleStatusListener = new StatusListener() {
    def onStatus(status: Status): Unit = { 
      handleStatus(status) 
    }
    
    def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {
      handleDeletion(statusDeletionNotice)
      //System.err.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
    }
    def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {
      System.err.println("Got track limitation notice:" + numberOfLimitedStatuses);
    }
    def onException(ex: Exception): Unit = { 
      ex.printStackTrace();
    }
    def onScrubGeo(userId: Long, upToStatusId: Long) : Unit = {
      val scrubGeo = TweetSchema(
        upToStatusId,
        "{}",
        0L,
        userId,
        "scrubGeo"
      )
      buffer = buffer ++ Iterator(scrubGeo)
      //System.err.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
    }
    def onStallWarning(warning: StallWarning) : Unit = {
      System.err.println("Got stall warning:" + warning);
    }
  }
  
  def stopTwitterStreamInstance(stopAfterMs: Long): Unit = {
    if(stopAfterMs > 0) {
      Thread.sleep(stopAfterMs)
      System.err.println("Stopping TwitterStreamInstance...")
      twitterStream.cleanUp
      twitterStream.shutdown
      var remTweets = 0
      val filename = "tmp/remainingTweets" + java.time.Instant.now.getEpochSecond.toString + ".jsonl"
      val filewriter = new FileWriter(new File(filename))
      while (buffer.hasNext) {
        filewriter.write(buffer.next() + "\n")
        remTweets = remTweets + 1
      }
      filewriter.close()
      printf("%d tweets written to %s\n", remTweets, filename)
      printf("%d tweets remaining in buffer\n", buffer.size)
    }
  }
  
  override def run(): Unit = {
    twitterStream = getTwitterStreamInstance
    setFollowIdsFromFile(currentConfig.handlesFilePath)
    twitterStream.addListener(simpleStatusListener)
    if (idsToTrack.isEmpty) {
      twitterStream.sample
    } else {
      val query = getQuery
      twitterStream.filter(query)
    }
    stopTwitterStreamInstance(currentConfig.streamDuration)
  }
}
