package org.lamastex.mep.tw

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.ScheduledFuture

/**
  * A class which manages the configuration of a Twitter stream and 
  * the writer associated with that stream.
  *
  * @param configFilePath Filename, including path, to the configuration file.
  * @param streamer
  * @param writer
  * @param executorPool An executor pool to run the writer and this on.
  */
class AsyncUpdateConfig(
  configFilePath: String, 
  streamer: BufferedTwitterStream, 
  writer: AsyncWrite,
  executorPool: ScheduledExecutorService
) extends Runnable {

  var currentConfig: UpdateConfig = null
  var runningJob: ScheduledFuture[_] = null

  /**
    * Starts the job for this instance. Note that currentConfig must be set before running this.
    */
  def startJob: Unit = {
    runningJob = executorPool.scheduleAtFixedRate(
      this,
      currentConfig.updateRate,
      currentConfig.updateRate,
      TimeUnit.MILLISECONDS
    )
  }

  def stopJob(mayInterruptIfRunning: Boolean): Boolean =
    runningJob.cancel(mayInterruptIfRunning)

  def isRunning: Boolean = !(runningJob == null || runningJob.isCancelled() || runningJob.isDone())

  def restartJob: Unit = {
    if (isRunning) stopJob(false)
    while(isRunning) Thread.sleep(200L)
    startJob
  }
  
  def updateStreamer(newConfig: StreamConfig): Unit = {
    val oldIds = streamer.idsToTrack
    val (newIds,_) = streamer.getFollowIdsFromFile(newConfig.handlesFilePath)
    if (oldIds != newIds)
      streamer.updateStream(newConfig)
  }

  def updateWriter(newConfig: WriteConfig): Unit = {
    val oldConfig = writer.getConfig
    if (oldConfig != newConfig) {
      writer.setConfig(newConfig)
      if (oldConfig.writeRate != newConfig.writeRate)
        writer.updateJob(executorPool)
    }
  }

  def updateSelf(newConfig: UpdateConfig): Unit = {
    if (currentConfig != newConfig) {
      currentConfig = newConfig
      restartJob
    }
  }
  
  override def run(): Unit = {
    println("Reading config from " + configFilePath)
    val mainConfig = IOHelper.getConfig(configFilePath)
    updateSelf(IOHelper.getUpdateConfig(mainConfig))
    updateStreamer(IOHelper.getStreamConfig(mainConfig))
    updateWriter(IOHelper.getWriteConfig(mainConfig))
  }
}
