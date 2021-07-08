package org.lamastex.mep.tw

import twitter4j.Status
import twitter4j.TwitterStream
import java.io.{File,FileWriter}
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.Date

class BufferedTwitterStreamTest(volatileBuffer: Iterator[String], stopStreamInMs: Long) extends BufferedTwitterStream(volatileBuffer, stopStreamInMs) {

  var tweetsRead = 0

  override def handleStatus(status: Status): Unit = {
    buffer = buffer ++ Iterator(statusToGson(status))
    tweetsRead = tweetsRead + 1
  }

  override def stopTwitterStreamInstance(twitterStream: TwitterStream, stopAfterMs: Long): Unit = {
    if(stopAfterMs > 0) {
      Thread.sleep(stopAfterMs)
      System.err.println("Stopping TwitterStreamInstance...")
      twitterStream.cleanUp
      twitterStream.shutdown
      var remTweets = 0
      val filename = "tmp/remainingTweets.csv"
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

    // Clean tmp directory
    for {
      files <- Option(new File("tmp/").listFiles)
      file <- files if file.getName.endsWith(".csv")
    } file.delete()

    // get Handles to track
    val handleFilename = "src/test/resources/trackedHandles.txt"
    val handleReader = scala.io.Source.fromFile(handleFilename)
    val handlesToTrack: Seq[String] = handleReader.getLines.toSeq
    handleReader.close
    printf("%d handles to track\n", handlesToTrack.size)

    val pool = Executors.newScheduledThreadPool(2)
    val stopStreamInS = 40

    var buffer: Iterator[String] = Iterator.empty

    val streamer = new BufferedTwitterStreamTest(buffer, stopStreamInS * 1000L)
    println("getting ids to track...")
    val idsToTrack = streamer.getValidTrackedUserIds(handlesToTrack)
    printf("%d ids tracked\n", idsToTrack.size)

    streamer.setIdsToTrack(idsToTrack)

    pool.submit(streamer)

    val writeJob = new AsyncWrite(streamer, "tmp/async")

    pool.scheduleAtFixedRate(writeJob, 20L, 10L, TimeUnit.SECONDS)

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