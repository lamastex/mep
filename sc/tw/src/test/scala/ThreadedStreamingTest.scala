package org.lamastex.mep.tw

import twitter4j.Status
import twitter4j.TwitterStream
import java.io.{File,FileWriter}
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.Date

class BufferedTwitterStreamTest(volatileBuffer: Iterator[String], val stopStreamInMs: Long) extends BufferedTwitterStream(volatileBuffer) {

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
      val filename = "tmp/remaingTweets.csv"
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

  override def run(): Unit = {
    val twitterStream = getTwitterStreamInstance
    twitterStream.addListener(simpleStatusListener)
    twitterStream.sample
    stopTwitterStreamInstance(twitterStream, stopStreamInMs)
  }

}

class AsyncWriteReturnWritten(buffer: Iterator[String], filename: String) extends Callable[Int] {
    override def call(): Int = {
        val filewriter = new FileWriter(new File(filename))
        var tweetsWritten = 0
        while(buffer.hasNext) {
            filewriter.write(buffer.next)
            tweetsWritten = tweetsWritten + 1
        }
        filewriter.close()
        printf("%d tweets written to %s\n", tweetsWritten, filename)
        tweetsWritten
    }
}

class ThreadedStreamingTest extends org.scalatest.funsuite.AnyFunSuite {
  test("Threaded Streaming") {

    // Clean tmp directory
    for {
      files <- Option(new File("tmp/").listFiles)
      file <- files if file.getName.endsWith(".csv")
    } file.delete()

    val pool = Executors.newScheduledThreadPool(2)
    val stopStreamInS = 40

    var buffer: Iterator[String] = Iterator.empty

    val streamer = new BufferedTwitterStreamTest(buffer, stopStreamInS * 1000L)

    pool.submit(streamer)

    val writeJob = new AsyncWrite(streamer, "tmp/async")

    pool.scheduleAtFixedRate(writeJob, 10L, 10L, TimeUnit.SECONDS)

/*     Thread.sleep(10000L)
    tweetsWrittenAsync = tweetsWrittenAsync + pool.submit[Int](
      new AsyncWriteReturnWritten(streamer.getBuffer, "tmp/test1.csv"),
    ).get()

    Thread.sleep(5000L)
    tweetsWrittenAsync = tweetsWrittenAsync + pool.submit[Int](
      new AsyncWriteReturnWritten(streamer.getBuffer, "tmp/test2.csv"),
    ).get() */

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