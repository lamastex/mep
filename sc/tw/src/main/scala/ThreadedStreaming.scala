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
import os.read

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
      val filename = "tmp/remainingTweets" + java.time.Instant.now.getEpochSecond.toString + ".csv"
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

class AsyncWrite(streamer: BufferedTwitterStream, filename: String, maxFileSizeBytes: Long = 3 * 1024 * 1024L) extends Runnable {
  
  // Estimate of byte size of one tweet.
  // Average size estimated as ~4000 B,
  // get 4096 with a little headroom
  final val TWEETSIZE = 4096

  def getLastFile(path: String): Option[File] = {
    val files = Option(new File(path).listFiles)
    if (files.isDefined && files.get.nonEmpty) {
      Some(files.get.sortBy(file => file.getName).last)
    } else None
  }

  def writeBufferToFile(
    file: String, 
    buffer: Iterator[String], 
    allowedBytes: Long, 
    append: Boolean = false
  ): Iterator[String] = {
    println("Will try to write to " + file)
    val writer = new FileWriter(new File(file), append)
    var bytesWritten = 0L
    var nextLine = ""
    while(buffer.hasNext && bytesWritten + nextLine.getBytes.length < allowedBytes - TWEETSIZE) {
      nextLine = buffer.next + "\n"
      writer.write(nextLine)
      bytesWritten += nextLine.getBytes.length
    }
    writer.close
    printf("%d bytes written to %s\n", bytesWritten, file)
    return buffer
  }
  
  override def run(): Unit = {
    val filePath = {
      val pathArray = filename.split('/').dropRight(1)
      pathArray.tail.foldLeft(pathArray.head)(_ + "/" + _)
    }
    println("Using filepath " + filePath)
    val lastFile = getLastFile(filePath)
    println("lastFile: " + lastFile.getOrElse(None).toString)

    var filenameWithTime = ""
    var fileSize = 0L
    var append = false
    if (lastFile.isDefined && lastFile.get.length < maxFileSizeBytes - TWEETSIZE) {
      filenameWithTime = lastFile.get.getPath
      fileSize = lastFile.get.length
      append = true
    } else {
      filenameWithTime = filename + java.time.Instant.now.getEpochSecond.toString + ".csv"
    }
    var buffer = streamer.getBuffer()
    /* val filewriter = new FileWriter(new File(filenameWithTime), append)
    var tweetsWritten = 0
    var nextLine = ""
    var lineSize = 0 */
    writeBufferToFile(
      filenameWithTime, 
      buffer, 
      maxFileSizeBytes - fileSize, 
      append
    )

    while(buffer.hasNext) {
      /* nextLine = buffer.next + "\n"
      lineSize = nextLine.getBytes.length */
      /*if (fileSize + lineSize > maxFileSizeBytes) {
        // Write into a new file instead
        filewriter.close()
        printf("%d tweets written to %s (size %d B)\n", tweetsWritten, filenameWithTime, fileSize)
        filenameWithTime = filename + java.time.Instant.now.getEpochSecond.toString + ".csv"
        filewriter = new FileWriter(new File(filenameWithTime))
        fileSize = 0L
        tweetsWritten = 0
      }*/

      writeBufferToFile(
        filename + java.time.Instant.now.getEpochSecond.toString + ".csv",
        buffer,
        maxFileSizeBytes
        )
      
      /* filewriter.write(nextLine)
      tweetsWritten = tweetsWritten + 1
      fileSize += nextLine.getBytes.length */
    }
    //filewriter.close()
    //printf("%d tweets written to %s (size %d B)\n", tweetsWritten, filenameWithTime, fileSize)
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