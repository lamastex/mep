package org.lamastex.mep.tw.ttt
import org.apache.spark.sql.types.{StructType,DataType};
import org.apache.spark.sql.SparkSession
import org.lamastex.mep.tw.ttt.TTTFormats._


class parseTwitter4jTest extends org.scalatest.funsuite.AnyFunSuite{
    val spark = SparkSession.builder.appName("test").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    val schema_twitter4j = DataType.fromJson(spark.read.text("schemas/twitter4j_schema.json").first.getString(0)).asInstanceOf[StructType]
    val twitter4jDF = spark.read.option("mode", "DROPMALFORMED").schema(schema_twitter4j).json("src/test/scala/resources/test_twitter4j.jsonl")
    
    test("test converting twitter4j data to TTT") {
        TTTConverters.tweetsDF2TTTDF(twitter4jDF).as[TTTFormats.TTT].show()
    }

    test("test converting twitter4j data to TTTURLsAndHashtags") {
        TTTConverters.tweetsDF2TTTDFWithURLsAndHashtags(twitter4jDF).as[TTTFormats.TTTURLsAndHashtags].show()
    }

     test("test the twitter4jToTTT reader") {
        spark.read.twitter4jToTTT("schemas/twitter4j_schema.json","src/test/scala/resources/test_twitter4j.jsonl").show()
    }
    test("test the twitter4jToTTTURlsAndHashtags reader") {
        spark.read.twitter4jToTTTURlsAndHashtags("schemas/twitter4j_schema.json","src/test/scala/resources/test_twitter4j.jsonl").show()
    }
}
