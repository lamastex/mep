package org.lamastex.mep.tw

import scala.io.Source
import java.io.File

class IOHelperTest extends org.scalatest.funsuite.AnyFunSuite {
  val rootPath = "src/test/resources/iohelper/"

  test("Get last file") {
    val lastFile = IOHelper.getLastFile(rootPath)
    assert(lastFile.isDefined)
    assert(lastFile.get.getName == "zzz.test")
  }

  test("Write buffer") {
    /**
      * Reads a JSON line written by an AsyncWrite.
      */
    def lineToTweet(line: String) = {
      val arr = line.split(":")
      val id = arr(1).split(",").head.toLong
      val json = arr.drop(2).mkString(":").split(",").dropRight(1).mkString(",")
      val time = arr.last.split("}").head.toLong
      TweetSchema(id, json, time)
    }

    val source = Source.fromFile(rootPath + "testTweets.jsonl")
    val testTweetsSeq: Seq[TweetSchema] = source
      .getLines
      .toSeq
      .map(lineToTweet)

    val testTweetsIter = testTweetsSeq.toIterator

    val testDir = new File(rootPath + "writeTest")
    testDir.mkdir()

    val testFile = rootPath + "writeTest/test.jsonl"
    IOHelper.writeBufferToFile(
      testFile, 
      testTweetsIter, 
      10*1024*1024L
    )

    val compareSource = Source.fromFile(testFile)
    assert(compareSource.getLines.toSeq.map(lineToTweet) == testTweetsSeq)
    
    new File(testFile).delete
    testDir.delete
    source.close
    compareSource.close
  }

  test("File moving test") {
    val testDir = new File(rootPath + "moveTestDir")
    testDir.mkdir()

    val filename = "moveTest"

    val sourceDir = rootPath
    val destDir = rootPath + "moveTestDir/"
    val sourceFile = rootPath + filename
    val destFile = destDir + filename

    IOHelper.moveFile(sourceFile, destFile)

    assert(testDir.listFiles.map(_.getName).contains("moveTest"))

    IOHelper.moveFile(destFile, sourceFile)

    testDir.delete
  }

  test("Configuration loader test") {
    val configTestFile = rootPath + "testConfig.conf"

    val configTest = IOHelper.getConfig(configTestFile)

    assert(configTest.getString("string") == "test")
    assert(configTest.getLong("long") == 1L)

    val completeConfigTestFile = rootPath + "streamConfig.conf"
    val completeConfigTest = IOHelper.getConfig(completeConfigTestFile)

    val streamConfig = IOHelper.getStreamConfig(completeConfigTest)
    assert(streamConfig.streamDuration == 21000L)

    val writeConfig = IOHelper.getWriteConfig(completeConfigTest)
    assert(writeConfig.writeRate == 10000L)
    assert(writeConfig.fullFilesDirectory == "tmp/full/")

    val updateConfig = IOHelper.getUpdateConfig(completeConfigTest)
    assert(updateConfig.updateRate == 30000L)
  }
}
