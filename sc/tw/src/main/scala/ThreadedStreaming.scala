package org.lamastex.mep.tw

import java.util.concurrent.Executors
import java.lang.Thread
import java.util.concurrent.TimeUnit
import twitter4j.TwitterStream
import java.io.{File,FileWriter}
import twitter4j.StatusListener
import twitter4j.StatusDeletionNotice
import twitter4j.Status
import twitter4j.StallWarning
import scala.collection.JavaConverters._
import twitter4j.FilterQuery

class BufferedTwitterStream(var buffer: Iterator[String], val stopStreamInMs: Long = 60000L) extends TwitterBasic with Runnable {
  
  var idsToTrack: Seq[Long] = Seq.empty

  def setIdsToTrack(ids: Seq[Long]): Unit = {
    idsToTrack = ids
  }
  
  def handleStatus(status: Status): Unit = {
    buffer = buffer ++ Iterator(statusToGson(status))
  }
  
  override def simpleStatusListener = new StatusListener() {
    //def onStatus(status: Status): Unit = { println(status.getText) }
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
      val filename = "tmp/remaingTweets" + java.time.Instant.now.getEpochSecond.toString + ".csv"
      val filewriter = new FileWriter(new File(filename))
      while (buffer.hasNext) {
        filewriter.write(buffer.next() + "\n")
        remTweets = remTweets + 1
      }
      filewriter.close()
      printf("%d tweets written to %s\n", remTweets, filename)
      printf("%d tweets remaining in buffer", buffer.size)
    }
  }
  
  def getBuffer(): Iterator[String] = buffer
  
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
    if (idsToTrack.isEmpty) {
      twitterStream.sample
    } else {
      val query = new FilterQuery()
      query.follow(idsToTrack: _*)
      twitterStream.filter(query)
    }
    stopTwitterStreamInstance(twitterStream, stopStreamInMs)
  }
  
}

//class AsyncWrite(buffer: Iterator[String], filename: String) extends Runnable {
class AsyncWrite(streamer: BufferedTwitterStream, filename: String) extends Runnable {
  override def run(): Unit = {
    val filenameWithTime = filename + java.time.Instant.now.getEpochSecond.toString + ".csv"
    val buffer = streamer.getBuffer()
    val filewriter = new FileWriter(new File(filenameWithTime))
    var tweetsWritten = 0
    while(buffer.hasNext) {
      filewriter.write(buffer.next + "\n")
      tweetsWritten = tweetsWritten + 1
    }
    filewriter.close()
    printf("%d tweets written to %s\n", tweetsWritten, filenameWithTime)
  }
}
  
object RunThreadedStreamWithWrite {
  def main(args : Array[String]): Unit = {
    val pool = Executors.newFixedThreadPool(2)
    
    var buffer: Iterator[String] = Iterator.empty
    
    val streamer = new BufferedTwitterStream(buffer)
    
    pool.submit(streamer)
    
    Thread.sleep(20000L)
    //pool.submit(new AsyncWrite(streamer.getBuffer, "tmp/test1.csv"))
    pool.submit(new AsyncWrite(streamer, "tmp/test1.csv"))
    
    Thread.sleep(20000L)
    //pool.submit(new AsyncWrite(streamer.getBuffer, "tmp/test2.csv"))
    pool.submit(new AsyncWrite(streamer, "tmp/test1.csv"))
    
    pool.shutdown()
    pool.awaitTermination(20, TimeUnit.SECONDS)
  }
}