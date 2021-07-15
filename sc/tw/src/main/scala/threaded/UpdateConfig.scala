package org.lamastex.mep.tw

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.ScheduledFuture

class AsyncUpdateConfig(
  configFilePath: String, 
  streamer: BufferedTwitterStream, 
  writer: AsyncWrite,
  executorPool: ScheduledExecutorService
) extends Runnable {

  var currentConfig: UpdateConfig = null
  var runningJob: ScheduledFuture[_] = null

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

  def updateJob(pool: ScheduledExecutorService): Unit = {
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
      updateJob(executorPool)
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