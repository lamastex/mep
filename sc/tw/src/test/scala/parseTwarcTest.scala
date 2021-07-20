package org.lamastex.mep.tw.ttt
import org.apache.spark.sql.types.{StructType,DataType};
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import scala.util.Try
import org.lamastex.mep.tw.ttt.TTTFormats._

class parseTwarcTest extends org.scalatest.funsuite.AnyFunSuite{
  val spark = SparkSession.builder.appName("test").config("spark.master", "local").getOrCreate()
  import spark.implicits._
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  val schema_twarc = DataType.fromJson(spark.read.text("schemas/twarc_v1.1_schema.json").first.getString(0)).asInstanceOf[StructType]
  val twarcDF = spark.read.option("mode", "DROPMALFORMED").schema(schema_twarc).json("src/test/scala/resources/test_twarc.jsonl")

  test("testing to read data from twarc as TTT"){
    TTTConverters.twarcTTTDF(twarcDF).as[TTTFormats.TTT].show()
  }

  test("testing to read data from twarc as TTTURLsAndHashtags"){
    TTTConverters.twarcTTTDFWithURLsAndHashtags(twarcDF).as[TTTFormats.TTTURLsAndHashtags].show()
  }

  test("testing to read data from twarc as TTTRTLikesAndMedia") {
    TTTConverters.twarcTTTDFWithRetweetsLikesAndMedia(twarcDF).as[TTTFormats.TTTRTLikesAndMedia].show()
  }

  test("reading twarc data without created_at") {
    val schema_twarc = DataType.fromJson(spark.read.text("schemas/twarc_v1.1_schema.json").first.getString(0)).asInstanceOf[StructType]
    val testDF = spark.read.option("mode", "DROPMALFORMED").schema(schema_twarc).json("src/test/scala/resources/test_twarc_without_created_at.jsonl")
    val testDS = TTTConverters.twarcTTTDF(testDF).as[TTTFormats.TTT]
    testDS.select(col("CurrentTweetDate")).show()
  }

  test("reading twarc data with schema without created_at") {
    val schema_twarc_wrong = DataType.fromJson(spark.read.text("schemas/twarc_wrong_schema.json").first.getString(0)).asInstanceOf[StructType]
    val testDF2 = spark.read.option("mode", "DROPMALFORMED").schema(schema_twarc_wrong).json("src/test/scala/resources/test_twarc.jsonl")
    try {
        TTTConverters.twarcTTTDF(testDF2)
        fail()
    } catch {
        case _: org.apache.spark.sql.AnalysisException => println("SCHEMA DOES NOT CONTAIN created_at SO AnalysisException WAS EXPECTED")
    }
  }
   test("test the twarcToTTT reader") {
    spark.read.twarcToTTT("schemas/twarc_v1.1_schema.json","src/test/scala/resources/test_twarc.jsonl").show()
  }

  test("test the twarcToTTTURLsAndHashtags reader") {
    spark.read.twarcToTTTURLsAndHashtags("schemas/twarc_v1.1_schema.json","src/test/scala/resources/test_twarc.jsonl").show()
  }

  test("test the twarcToTTTRTLikesAndMedia reader") {
    spark.read.twarcToTTTRTLikesAndMedia("schemas/twarc_v1.1_schema.json","src/test/scala/resources/test_twarc.jsonl").show()
  }
}