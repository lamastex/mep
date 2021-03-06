package org.lamastex.mep.tw

import java.util.concurrent.{
  ScheduledFuture,
  ScheduledExecutorService,
  TimeUnit
}

/**
  * A class to asynchronously write the buffer in a BufferedTwitterStream to file.
  *
  * @param streamer Streamer with the buffer to write.
  * @param writeConfig Configuration for the writer.
  */
class AsyncWrite(
  streamer: BufferedTwitterStream, 
  writeConfig: WriteConfig
) extends Runnable {
  private var currentConfig = writeConfig
  private var runningJob: ScheduledFuture[_] = null

  def setConfig(newConfig: WriteConfig): Unit = {
    currentConfig = newConfig
  }

  def getConfig: WriteConfig = currentConfig

  def startJob(pool: ScheduledExecutorService): Unit = {
    runningJob = pool.scheduleAtFixedRate(
      this, 
      currentConfig.writeRate,
      currentConfig.writeRate,
      TimeUnit.MILLISECONDS)
  }

  def stopJob(mayInterruptIfRunning: Boolean): Boolean = 
    runningJob.cancel(mayInterruptIfRunning)

  def isRunning: Boolean = !(runningJob == null || runningJob.isCancelled() || runningJob.isDone())

  /**
    * Restarts the job, necessary to update write rate.
    *
    * @param pool The executor pool to run the job on.
    */
  def updateJob(pool: ScheduledExecutorService): Unit = {
    stopJob(false)
    while(isRunning) Thread.sleep(200) // Wait for job to finish
    startJob(pool)
  }

  override def run(): Unit = {
    val filename = currentConfig.outputFilenames
    val maxFileSizeBytes = currentConfig.maxFileSize
    val storageDirectory = currentConfig.fullFilesDirectory

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

    val buffer = streamer.getBuffer.toList.toIterator

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
