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
import java.nio.file.{Files, Path, StandardCopyOption}
import com.typesafe.config._
import os.write

case class StreamConfig (
  handlesFilePath: String,
  streamDuration: Long
)

case class WriteConfig (
  outputFilenames: String,
  fullFilesDirectory: String,
  maxFileSize: Long,
  writeRate: Long
)

/**
  * A class that reads a Twitter stream into a buffer
  *
  * @param buffer The buffer to save tweets into.
  * @param stopStreamInMs If a positive value is given, the stream will stop after this number of ms, otherwise it will stream indefinitely. Note that there currently are no checks in place to make sure that the buffer is within memory limits. If the stream is to continue indefinitely, the buffer MUST be emptied in some other way, for example by writing it to disk.
  */  
class BufferedTwitterStream(val streamConfig: StreamConfig) extends TwitterBasic with Runnable {
  
  var streamDuration: Long = streamConfig.streamDuration
  var idsToTrack: Seq[Long] = Seq.empty
  var buffer: Iterator[String] = Iterator.empty

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
    stopTwitterStreamInstance(twitterStream, streamDuration)
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
    if (files.isDefined && files.get.filter(file => file.isFile).nonEmpty) {
      Some(files.get.filter(file => file.isFile).sortBy(file => file.getName).last)
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

  /**
    * Moves a file
    *
    * @param file path of file to be moved
    * @param destination destination to move file to, must include filename.
    */  
  def moveFile(
    file: String,
    destination: String
  ): Unit = {
    val filePath = new File(file).toPath
    val destPath = new File(destination).toPath
    Files.move(filePath, destPath, StandardCopyOption.REPLACE_EXISTING)
  }

  def getConfig(configFile: String): Config = {
    ConfigFactory.parseFile(new File(configFile))
  }

  def getStreamConfig(config: Config): StreamConfig = {
    val streamConfig = config.getConfig("streamConfig")
    StreamConfig(
      handlesFilePath = streamConfig.getString("handles-to-track"),
      streamDuration = streamConfig.getLong("stream-duration")
    )
  }

  def getWriteConfig(config: Config): WriteConfig = {
    val writeConfig = config.getConfig("writeConfig")
    WriteConfig(
      outputFilenames = writeConfig.getString("output-filenames"),
      fullFilesDirectory = writeConfig.getString("completed-file-directory"),
      maxFileSize = writeConfig.getLong("max-file-size"),
      writeRate = writeConfig.getLong("write-rate")
    )
  }
}

/**
  * A class to write the buffered stream asynchronously.
  *
  * @param streamer The object responsible for running the twitter stream and recording tweets into its buffer.
  * @param filename The path and root file name of the files into which tweets are written.
  * Files are named by (filename + timestamp + ".jsonl") where timestamp is
  * Unix Epoch (seconds since 00:00:00 1/1/1970) when the file was created. 
  * @param maxFileSizeBytes The maximum file size in Bytes. It is very unlikely that any file 
  * is larger than this, but it is not completely guaranteed. Default 10 MB.
  * @param storageDirectory If given a non-empty String, full files are moved to this path. 
  * Directory name must contain trailing "/". Default empty string.
  */  
class AsyncWrite(
  streamer: BufferedTwitterStream, 
  filename: String, 
  maxFileSizeBytes: Long = 10 * 1024 * 1024L,
  storageDirectory: String = ""
) extends Runnable {
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

    // When the last file is full, write the rest into new files.
    // If everything is already written, the buffer will be empty.
    while(buffer.hasNext) {
      // If the program gets here, the last file is full and can be moved.
      if (storageDirectory.nonEmpty)
        IOHelper.moveFile(filenameWithTime, storageDirectory + filenameWithTime.split("/").last)

      filenameWithTime = filename + java.time.Instant.now.getEpochSecond.toString + ".jsonl"
      IOHelper.writeBufferToFile(
        filenameWithTime,
        buffer,
        maxFileSizeBytes
      )
    }
  }
}

object ThreadedTwitterStreamWithWrite {
  /**
    * Sets up a Twitter stream and writes tweets to files on disk.
    *
    * @param args Full path to .conf file with configuration for the stream. 
    * Default streamConfig.conf
    */
  def main(args: Array[String]): Unit = {

    // Parsing arguments

    val configFile: String = T(() => args(0)).getOrElse("streamConfig.conf")
    val mainConfig = IOHelper.getConfig(configFile)
    val streamConfig = IOHelper.getStreamConfig(mainConfig)
    val writeConfig = IOHelper.getWriteConfig(mainConfig)

    val stopStreamInMs: Long = streamConfig.streamDuration
    val handleFilename: String = streamConfig.handlesFilePath
    val writeRateInMs: Long = writeConfig.writeRate
    val maxFileSizeBytes: Long = writeConfig.maxFileSize
    val outputFilenames: String = writeConfig.outputFilenames
    val fullFilesDirectory: String = writeConfig.fullFilesDirectory

    /*
    val stopStreamInMs: Long = streamConfig.getLong("stream-duration")
    val writeRateInMs: Long = streamConfig.getLong("write-rate")
    val maxFileSizeBytes: Long = streamConfig.getLong("max-file-size")
    val outputFilenames: String = streamConfig.getString("output-filenames")
    val handleFilename: String = streamConfig.getString("handles-to-track")
    val fullFilesDirectory: String = streamConfig.getString("completed-file-directory")
    */

    // get Handles to track
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
    val writeDelayInMs = writeRateInMs // Delay before starting write job

    var buffer: Iterator[String] = Iterator.empty

    val streamer = new BufferedTwitterStream(streamConfig)
    val idsToTrack: Seq[Long] = if (handlesToTrack.size > 0) {
      println("getting ids to track...")
      val idsToTrack = streamer.getValidTrackedUserIds(handlesToTrack)
      printf("%d valid ids tracked\n", idsToTrack.size)
      idsToTrack
    } else Seq.empty
    
    streamer.setIdsToTrack(idsToTrack)

    // Start the Twitter stream
    pool.submit(streamer)

    // Create and start write jobs
    val writeJob = new AsyncWrite(streamer, outputFilenames, maxFileSizeBytes, fullFilesDirectory)
    pool.scheduleAtFixedRate(writeJob, writeDelayInMs, writeRateInMs, TimeUnit.MILLISECONDS)
    
    // Wait until stream has finished
    if (stopStreamInMs > 0) {
      Thread.sleep(stopStreamInMs)
      pool.shutdown()
      pool.awaitTermination(stopStreamInMs*2, TimeUnit.MILLISECONDS)
    }
  }
}