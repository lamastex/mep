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
  StatusListener,
  Status,
  FilterQuery,
  StatusDeletionNotice,
  StallWarning
}
import scala.collection.JavaConverters._


/**
  * A class that reads a Twitter stream into a buffer
  *
  * @param buffer The buffer to save tweets into.
  * @param stopStreamInMs If a positive value is given, the stream will stop after this number of ms, otherwise it will stream indefinitely. Note that there currently are no checks in place to make sure that the buffer is within memory limits. If the stream is to continue indefinitely, the buffer MUST be emptied in some other way, for example by writing it to disk.
  */  
class BufferedTwitterStream(val streamConfig: StreamConfig) extends TwitterBasic with Runnable {
  
  var streamDuration: Long = streamConfig.streamDuration
  var idsToTrack: Seq[Long] = Seq.empty
  var buffer: Iterator[TweetSchema] = Iterator.empty

  def setIdsFromFile(handleFilename: String): Unit = {
    val handlesToTrack = IOHelper.readHandles(handleFilename)
    if (handlesToTrack.nonEmpty) {
      val validIds = getValidTrackedUserIds(handlesToTrack)
      println(s"${validIds.length} valid Ids tracked.")
      idsToTrack = validIds
    }
  }
  
  def handleStatus(status: Status): Unit = {
    val tweet = TweetSchema(status.getId, statusToGson(status), status.getCreatedAt.getTime)
    buffer = buffer ++ Iterator(tweet)
  }
  
  override def simpleStatusListener = new StatusListener() {
    def onStatus(status: Status): Unit = { 
      handleStatus(status) 
    }
    
    def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {
      //System.err.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
    }
    def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {
      System.err.println("Got track limitation notice:" + numberOfLimitedStatuses);
    }
    def onException(ex: Exception): Unit = { 
      ex.printStackTrace();
    }
    def onScrubGeo(userId: Long, upToStatusId: Long) : Unit = {
      System.err.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
    }
    def onStallWarning(warning: StallWarning) : Unit = {
      System.err.println("Got stall warning:" + warning);
    }
  }
  
  override def stopTwitterStreamInstance(twitterStream: TwitterStream, stopAfterMs: Long): Unit = {
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
  
  def getBuffer(): Iterator[TweetSchema] = buffer
  
  def lookupUserSNs(retweeterIds:Seq[String]) = {
    val grouped = retweeterIds.grouped(100).toList 
    val twitter = getTwitterInstance
    for {group <- grouped  
      users = twitter.lookupUsers(group:_*)
      user <- users.asScala 
    } yield user     
  }

  def getValidTrackedUserIds(handles: Seq[String]) = lookupUserSNs(handles)
    .map(u => u.getId())
    .toSet
    .toSeq
    .filter(_.isValidLong)  
  
  override def run(): Unit = {
    val twitterStream = getTwitterStreamInstance
    twitterStream.addListener(simpleStatusListener)
    setIdsFromFile(streamConfig.handlesFilePath)
    if (idsToTrack.isEmpty) {
      twitterStream.sample
    } else {
      val query = new FilterQuery()
      query.follow(idsToTrack: _*)
      twitterStream.filter(query)
    }
    stopTwitterStreamInstance(twitterStream, streamDuration)
  }
}