package org.lamastex.mep.tw.ttt
import org.apache.spark.sql.types.{StructType,DataType};
import org.apache.spark.sql.SparkSession
import org.lamastex.mep.tw.ttt.TTTFormats._
import org.lamastex.mep.tw.ttt.TTTConverters.{tweetsDF2TTTDF,tweetsDF2TTTDFWithURLsAndHashtags}
import java.sql.Timestamp
import org.apache.spark.sql.Row
import sys.process._
import ujson.read
import scala.io.Source

class ParseTwitter4jTest extends org.scalatest.funsuite.AnyFunSuite{
  //generating test files
  val bashScript: String = getClass.getResource("/getTwitter4jFiles.sh").getPath
  val rootPath = bashScript.split("/").dropRight(4).mkString("/")
  s"bash $bashScript $rootPath" !!

  //spark configurations 
  val spark = SparkSession.builder.appName("test").config("spark.master", "local").getOrCreate()
  import spark.implicits._
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  // paths
  val schemaPath = rootPath+"/src/test/resources/schemas/twitter4j_schema.json" 
  val testFilePath = rootPath+"/src/test/resources/test_twitter4j.jsonl"
  val testFileReaderPath = rootPath+"/src/test/resources/test_reader_twitter4j" 
  
  // needed schema
  val schema = DataType.fromJson(spark.read.text(schemaPath).first.getString(0)).asInstanceOf[StructType]
  
  // needed dataFrame
  val twitter4jJsonSeq = 
    spark
      .read
      .option("mode", "DROPMALFORMED")
      .text(testFilePath)
      .collect()
      .map(x=>x.getString(0))
      .filter(x => ujson.read(x)("statusType").str == "status")
      .map(x=>ujson.read(x)("json").toString)
                                                                 
  val twitter4jDF = 
    spark
      .read
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .json(twitter4jJsonSeq.toSeq.toDS)
      
  twitter4jDF.write.mode("overwrite").json(testFileReaderPath)
  
  //tests
  test("test converting twitter4j data to TTT"){ 
      tweetsDF2TTTDF(twitter4jDF).as[TTT].show()
  }

  test("test converting twitter4j data to TTTURLsAndHashtags") {
      tweetsDF2TTTDFWithURLsAndHashtags(twitter4jDF).as[TTTURLsAndHashtags].show()
  }
  test("test the twitter4jToTTT reader") {
      spark.read.twitter4jToTTT(schemaPath,testFileReaderPath).show()
  }

  test("test the twitter4jToTTTURlsAndHashtags reader") {
      spark.read.twitter4jToTTTURlsAndHashtags(schemaPath,testFileReaderPath).show()
  }
    
}
