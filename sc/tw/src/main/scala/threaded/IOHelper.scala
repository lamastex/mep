package org.lamastex.mep.tw

import java.io.{
  File,
  FileWriter,
  FileNotFoundException
}
import java.nio.file.{
  Files,
  StandardCopyOption
}
import com.typesafe.config.{
  Config,
  ConfigFactory
}

/**
  * Helper object with various IO functions needed for the Twitter stream.
  */
object IOHelper {

  // Estimate of byte size of one tweet.
  // Average size estimated as ~4000 B,
  // 4096 with a little headroom
  final val TWEETSIZE = 4096

  /**
    * Returns the file with the largest name w.r.t. string comparison.
    *
    * @param path Path to search for files.
    * @return Largest filename w.r.t. string comparison, None if it cannot find a file.
    */
  def getLastFile(path: String): Option[File] = {
    val files = Option(new File(path).listFiles)
    if (files.isDefined && files.get.filter(file => file.isFile).nonEmpty) {
      Some(files.get.filter(file => file.isFile).sortBy(file => file.getName).last)
    } else None
  }

  /**
    * Writes a buffer of tweets to file.
    *
    * @param file Full path to the file that should be written.
    * @param buffer The buffer with tweets to be written.
    * @param allowedBytes Number of bytes that are allowed to be written to the file.
    * @param append If true, will append to an existing file.
    * @return
    */
  def writeBufferToFile(
    file: String, 
    buffer: Iterator[TweetSchema], 
    allowedBytes: Long,
    append: Boolean = false
  ): Iterator[TweetSchema] = {
    println("Writing to " + file)
    val writer = new FileWriter(new File(file), append)
    var bytesWritten = 0L
    var nextTweet = TweetSchema(0L,"",0L,0L,"")
    var nextLine = ""
    while(buffer.hasNext && bytesWritten + nextLine.getBytes.length < allowedBytes - TWEETSIZE) {
      nextTweet = buffer.next
      nextLine = s"""{"tweetId":${nextTweet.id},"json":${nextTweet.json},"time":${nextTweet.time},"userId":${nextTweet.userID},"statusType":"${nextTweet.statusType}"}\n"""
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

  /**
    * Reads a .conf file from disk.
    *
    * @param configFile Filename including path to config file
    * @return The read config.
    */
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

  def getUpdateConfig(config: Config): UpdateConfig = {
    val updateConfig = config.getConfig("updateConfig")
    UpdateConfig(
      updateRate = updateConfig.getLong("update-rate")
    )
  }

  /**
    * Reads a file of Twitter handles from disk. 
    * Each line of the file should be a single Twitter handle without @.
    *
    * @param handlesFilePath Filename, including path.
    * @return Seq of read handles.
    */
  def readHandles(handlesFilePath: String): Seq[String] = {
    var handlesToTrack: Seq[String] = Seq.empty

    try {
      val handleReader = scala.io.Source.fromFile(handlesFilePath)
      for {line <- handleReader.getLines} {
        handlesToTrack = handlesToTrack :+ line
      }
      handleReader.close
      printf("%d handles to track\n", handlesToTrack.size)
    } catch {
      case e: FileNotFoundException => println(handlesFilePath + " not found!")
    }
    return handlesToTrack
  }
}
