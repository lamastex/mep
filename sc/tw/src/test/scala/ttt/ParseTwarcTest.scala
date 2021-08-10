package org.lamastex.mep.tw.ttt
import org.apache.spark.sql.types.{StructType,DataType};
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import scala.util.Try
import org.lamastex.mep.tw.ttt.TTTFormats._
import sys.process._
import scala.io.Source

class ParseTwarcTest extends org.scalatest.funsuite.AnyFunSuite{
  val bashScript: String = getClass.getResource("/getTwarcFiles.sh").getPath
  val rootPath = bashScript.split("/").dropRight(4).mkString("/")
  s"bash $bashScript $rootPath" !!
  //generates the testfile
  //"bash /root/GIT/py/twarc/getTwarcFiles.sh" !!
  //spark configurations 
  val spark = SparkSession.builder.appName("test").config("spark.master", "local").getOrCreate()
  import spark.implicits._
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  //paths
  val schemaPath = rootPath+"/src/test/resources/schemas/twarc_v1.1_schema.json"
  val wrongSchemaPath = rootPath+"/src/test/resources/schemas/twarc_wrong_schema.json"
  val testFilePath = rootPath+"/src/test/resources/test_twarc.jsonl"

  //neeeded schemas
  val schema = DataType.fromJson(spark.read.text(schemaPath).first.getString(0)).asInstanceOf[StructType]
  val wrongSchema = DataType.fromJson(spark.read.text(wrongSchemaPath).first.getString(0)).asInstanceOf[StructType]
  //needed dataframes
  val twarcDF = spark.read.option("mode", "DROPMALFORMED").schema(schema).json(testFilePath)
  val wrongSchematwarcDF =spark.read.option("mode", "DROPMALFORMED").schema(wrongSchema).json(testFilePath)

  test("testing to read data from twarc as TTT"){
    TTTConverters.twarcTTTDF(twarcDF).as[TTTFormats.TTT].show()
  }

  test("testing to read data from twarc as TTTURLsAndHashtags"){
    TTTConverters.twarcTTTDFWithURLsAndHashtags(twarcDF).as[TTTFormats.TTTURLsAndHashtags].show()
  }

  test("testing to read data from twarc as TTTRTLikesAndMedia") {
    TTTConverters.twarcTTTDFWithRetweetsLikesAndMedia(twarcDF).as[TTTFormats.TTTRTLikesAndMedia].show()
  }
  test("reading twarc data with schema without created_at") {
    try {
        TTTConverters.twarcTTTDF(wrongSchematwarcDF)
        fail()
    } catch {
        case _: org.apache.spark.sql.AnalysisException => println("SCHEMA DOES NOT CONTAIN created_at SO AnalysisException WAS EXPECTED")
    }
  }
   test("test the twarcToTTT reader") {
    spark.read.twarcToTTT(schemaPath,testFilePath).show()
  }

  test("test the twarcToTTTURLsAndHashtags reader") {
    spark.read.twarcToTTTURLsAndHashtags(schemaPath,testFilePath).show()
  }

  test("test the twarcToTTTRTLikesAndMedia reader") {
    spark.read.twarcToTTTRTLikesAndMedia(schemaPath,testFilePath).show()
  }
}
