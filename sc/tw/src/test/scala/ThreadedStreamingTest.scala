package org.lamastex.mep.tw

import twitter4j.Status
import twitter4j.TwitterStream
import java.io.{File,FileWriter}
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class BufferedTwitterStreamTest(volatileBuffer: Iterator[String], val stopStreamInMs: Long) extends BufferedTwitterStream(volatileBuffer) {

  var seqBuffer: Seq[String] = Seq.empty

  override def handleStatus(status: Status): Unit = {
    buffer = buffer ++ Iterator(statusToGson(status))
    seqBuffer = seqBuffer :+ statusToGson(status)
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
        filewriter.write(buffer.next())
        remTweets = remTweets + 1
      }
      filewriter.close()
      printf("%d tweets written to %s\n", remTweets, filename)
      printf("Total number of received tweets: %d\n", seqBuffer.size)
      printf("Tweets to be written Async: %d\n", seqBuffer.size - remTweets)
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
    val pool = Executors.newFixedThreadPool(2)
    val stopStreamInMs = 20 * 1000L
    var tweetsWrittenAsync = 0

    var buffer: Iterator[String] = Iterator.empty

    val streamer = new BufferedTwitterStreamTest(buffer, stopStreamInMs)

    pool.submit(streamer)

    Thread.sleep(8000L)
    tweetsWrittenAsync = tweetsWrittenAsync + pool.submit(
      new AsyncWriteReturnWritten(streamer.getBuffer, "tmp/test1.csv")
    ).get()

    Thread.sleep(5000L)
    tweetsWrittenAsync = tweetsWrittenAsync + pool.submit(
      new AsyncWriteReturnWritten(streamer.getBuffer, "tmp/test2.csv")
    ).get()

    pool.shutdown()
    pool.awaitTermination(20, TimeUnit.SECONDS)
    printf("Tweets written Async: %d\n", tweetsWrittenAsync)
  }
}