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
import java.io.FileNotFoundException

/*  A class that reads a Twitter stream into a buffer
 *  buffer: The buffer to save tweets into.
 *  stopStreamInMs: If a positive value is given, the stream
 *                  will stop after this number of ms, otherwise
 *                  it will stream indefinitely. Note that there
 *                  currently are no checks in place to make sure
 *                  that the buffer is within memory limits.
 *                  If the stream is to continue indefinitely,
 *                  the buffer MUST be emptied in some other way,
 *                  for example by writing it to disk.
 */
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

object IOHelper {

  // Estimate of byte size of one tweet.
  // Average size estimated as ~4000 B,
  // 4096 with a little headroom
  final val TWEETSIZE = 4096

  // Returns the file with the largest name w.r.t. string comparison
  def getLastFile(path: String): Option[File] = {
    val files = Option(new File(path).listFiles)
    if (files.isDefined && files.get.nonEmpty) {
      Some(files.get.sortBy(file => file.getName).last)
    } else None
  }

  def writeBufferToFile(
    file: String, 
    buffer: Iterator[String], 
    allowedBytes: Long, // max Bytes written to the file
    append: Boolean = false
  ): Iterator[String] = {
    println("Writing to " + file)
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
}

/*  A class to write the buffered stream asynchronously.
 *  streamer: The object responsible for running the twitter stream
 *            and recording tweets into its buffer
 *  filename: The path and root file name of the files into which tweets are written.
 *            Files are named by (filename + timestamp + ".jsonl") where timestamp is
 *            Unix Epoch (seconds since 00:00:00 1/1/1970) * 
 *  maxFileSizeBytes: The maximum file size in Bytes. It is very unlikely that any file 
 *                    is larger than this, but it is not completely guaranteed.
 */ 
class AsyncWrite(streamer: BufferedTwitterStream, filename: String, maxFileSizeBytes: Long = 3 * 1024 * 1024L) extends Runnable {
  override def run(): Unit = {
    val filePath = {
      val pathArray = filename.split('/').dropRight(1)
      pathArray.tail.foldLeft(pathArray.head)(_ + "/" + _)
    }
    val lastFile = IOHelper.getLastFile(filePath)
    println("last file: " + lastFile.getOrElse(None).toString)

    var filenameWithTime = ""
    var fileSize = 0L
    var append = false
    // Only write to the last file if it exists and is not full
    if (lastFile.isDefined && lastFile.get.length < maxFileSizeBytes - IOHelper.TWEETSIZE) {
      filenameWithTime = lastFile.get.getPath
      fileSize = lastFile.get.length
      append = true
    } else {
      filenameWithTime = filename + java.time.Instant.now.getEpochSecond.toString + ".jsonl"
    }

    val buffer = streamer.getBuffer()

    // Writing into the last file
    IOHelper.writeBufferToFile(
      filenameWithTime, 
      buffer, 
      maxFileSizeBytes - fileSize, 
      append
    )

    // When the last file is full, write the rest into new files
    while(buffer.hasNext) {
      IOHelper.writeBufferToFile(
        filename + java.time.Instant.now.getEpochSecond.toString + ".jsonl",
        buffer,
        maxFileSizeBytes
      )
    }
  }
}

object ThreadedTwitterStreamWithWrite {
  def main(args: Array[String]): Unit = {
    // Clean tmp directory
    for {
      files <- Option(new File("tmp/").listFiles)
      file <- files if file.getName.endsWith(".jsonl")
    } file.delete()

    // get Handles to track
    val handleFilename = "src/test/resources/trackedHandles.txt"
    var handlesToTrack: Seq[String] = Seq.empty

    try {
      val handleReader = scala.io.Source.fromFile(handleFilename)
      for {line <- handleReader.getLines} {
        handlesToTrack = handlesToTrack :+ line
      }
      handleReader.close
      printf("%d handles to track\n", handlesToTrack.size)
    } catch {
      case e: FileNotFoundException => println(handleFilename + " not found!")
    }

    val pool = Executors.newScheduledThreadPool(2)
    val stopStreamInS = 70L
    val writeDelayInS = 20L // Delay before starting write job
    val writeRateInS = 20L  // Delay between write jobs 

    var buffer: Iterator[String] = Iterator.empty

    val streamer = new BufferedTwitterStream(buffer, stopStreamInS * 1000L)
    val idsToTrack: Seq[Long] = if (handlesToTrack.size > 0) {
      println("getting ids to track...")
      val idsToTrack = streamer.getValidTrackedUserIds(handlesToTrack)
      printf("%d ids tracked\n", idsToTrack.size)
      idsToTrack
    } else Seq.empty
    
    streamer.setIdsToTrack(idsToTrack)

    // Start the Twitter stream
    pool.submit(streamer)

    // Create and start write jobs
    val writeJob = new AsyncWrite(streamer, "tmp/async")
    pool.scheduleAtFixedRate(writeJob, writeDelayInS, writeRateInS, TimeUnit.SECONDS)

    // Wait until stream has finished
    Thread.sleep(stopStreamInS * 1000)

    pool.shutdown()
    pool.awaitTermination(stopStreamInS*2, TimeUnit.SECONDS)
    val tweetsWrittenAsync = Option(new File("tmp/").listFiles().toSeq.filter(_.getName().contains("async")))
      .getOrElse(Seq.empty)
      .map(file => io.Source.fromFile(file).getLines.size)
      .sum
    printf("Tweets written Async: %d\n", tweetsWrittenAsync)
  }
}
  
// Obsolete
/* object RunThreadedStreamWithWrite {
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
} */