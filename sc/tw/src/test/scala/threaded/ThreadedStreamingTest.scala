package org.lamastex.mep.tw

import twitter4j.{
  Status,
  TwitterStream
}
import java.io.{
  File,
  FileWriter,
  FileNotFoundException
}
import java.util.concurrent.{
  Callable,
  Executors,
  TimeUnit
}
import java.util.Date
import scala.io.Source

/**
  * An extension of BufferedTwitterStream with some additional debug functionality.
  *
  * @param streamConfig
  */
class BufferedTwitterStreamTest(streamConfig: StreamConfig) extends BufferedTwitterStream(streamConfig) {

  var tweetsRead = 0

  override def handleStatus(status: Status): Unit = {
    val tweet = TweetSchema(
      status.getId, 
      statusToGson(status), 
      status.getCreatedAt.getTime,
      status.getUser.getId,
      "status"
    )
    buffer = buffer ++ Iterator(tweet)
    tweetsRead = tweetsRead + 1
  }

  override def stopTwitterStreamInstance(stopAfterMs: Long): Unit = {
    if(stopAfterMs > 0) {
      Thread.sleep(stopAfterMs)
      System.err.println("Stopping TwitterStreamInstance...")
      twitterStream.cleanUp
      twitterStream.shutdown
      var remTweets = 0
      val filename = "tmp/remainingTweets.jsonl"
      val filewriter = new FileWriter(new File(filename))
      while (buffer.hasNext) {
        filewriter.write(buffer.next() + "\n")
        remTweets = remTweets + 1
      }
      filewriter.close()
      printf("%d tweets written to %s\n", remTweets, filename)
      printf("Total number of received tweets: %d\n", tweetsRead)
      printf("Tweets to be written Async: %d\n", tweetsRead - remTweets)
    }
  }
}
class ThreadedStreamingTest extends org.scalatest.funsuite.AnyFunSuite {
  test("Threaded Streaming") {

    // Load config 
    val configFilePath = "src/test/resources/streamConfig.conf"
    val mainConfig = IOHelper.getConfig(configFilePath)
    val streamConfig = IOHelper.getStreamConfig(mainConfig)
    val writeConfig = IOHelper.getWriteConfig(mainConfig)
    val updateConfig = IOHelper.getUpdateConfig(mainConfig)

    // Needed for determining if the right number of tweets are written
    val outputFilenames = writeConfig.outputFilenames
    val fullFilesDirectory = writeConfig.fullFilesDirectory
    val writeDir = outputFilenames.split("/").dropRight(1).mkString("/") + "/"

    // Clean tmp directory
    for {
      files <- Option(new File(writeDir).listFiles)
      file <- files if file.getName.endsWith(".jsonl")
    } file.delete()
    for {
      files <- Option(new File(fullFilesDirectory).listFiles)
      file <- files if file.getName.endsWith(".jsonl")
    } file.delete()

    val pool = Executors.newScheduledThreadPool(2)
    val stopStreamInMs = streamConfig.streamDuration

    val streamer = new BufferedTwitterStreamTest(streamConfig)

    // Start the Twitter stream
    pool.submit(streamer)

    // Create and start write jobs
    val writer = new AsyncWrite(streamer, writeConfig)
    writer.startJob(pool)

    // Create and start configuration updater
    val updater = new AsyncUpdateConfig(
      configFilePath,
      streamer,
      writer,
      pool
    )
    updater.updateSelf(updateConfig)

    // Wait until half the test is done
    Thread.sleep(stopStreamInMs/2)

    // Wait until stream has finished
    Thread.sleep(stopStreamInMs/2)

    pool.shutdown()
    pool.awaitTermination(stopStreamInMs*2, TimeUnit.MILLISECONDS)
    val tweetsInFullFiles = Option(new File(fullFilesDirectory).listFiles().toSeq.filter(_.getName().contains("tweetTest")))
      .getOrElse(Seq.empty)
      .map(file => Source.fromFile(file).getLines.size)
      .sum
    val tweetsInNonFullFiles = Option(new File(writeDir).listFiles().toSeq.filter(_.getName().contains("tweetTest")))
      .getOrElse(Seq.empty)
      .map(file => Source.fromFile(file).getLines.size)
      .sum
    printf("Tweets written Async: %d\n", tweetsInFullFiles + tweetsInNonFullFiles)
  }
}
