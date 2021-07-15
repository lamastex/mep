package org.lamastex.mep.tw

import java.util.concurrent.{
  Executors,
  TimeUnit
}

object ThreadedTwitterStreamWithWrite {
  /**
    * Sets up a Twitter stream and writes tweets to files on disk.
    *
    * @param args Full path to .conf file with configuration for the stream. 
    * Default twitterConfig.conf
    */
  def main(args: Array[String]): Unit = {

    // Parsing arguments

    val configFile: String = T(() => args(0)).getOrElse("twitterConfig.conf")
    val mainConfig = IOHelper.getConfig(configFile)
    val streamConfig = IOHelper.getStreamConfig(mainConfig)
    val writeConfig = IOHelper.getWriteConfig(mainConfig)
    val updateConfig = IOHelper.getUpdateConfig(mainConfig)

    val stopStreamInMs: Long = streamConfig.streamDuration

    val pool = Executors.newScheduledThreadPool(2)

    val streamer = new BufferedTwitterStream(streamConfig)

    // Start the Twitter stream
    pool.submit(streamer)

    // Create and start write jobs
    val writer = new AsyncWrite(streamer, writeConfig)
    writer.startJob(pool)

    val updateJob = new AsyncUpdateConfig(
      configFile,
      streamer,
      writer,
      pool
    )
    updateJob.updateSelf(updateConfig)
    
    // Wait until stream has finished
    if (stopStreamInMs > 0) {
      Thread.sleep(stopStreamInMs)
      pool.shutdown()
      pool.awaitTermination(stopStreamInMs*2, TimeUnit.MILLISECONDS)
    }
  }
}