package org.lamastex.mep.tw

import java.util.concurrent.{
  ScheduledFuture,
  ScheduledExecutorService,
  TimeUnit
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
  writeConfig: WriteConfig
) extends Runnable {
/* class AsyncWrite(
  streamer: BufferedTwitterStream, 
  filename: String, 
  maxFileSizeBytes: Long = 10 * 1024 * 1024L,
  storageDirectory: String = ""
) extends Runnable { */
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