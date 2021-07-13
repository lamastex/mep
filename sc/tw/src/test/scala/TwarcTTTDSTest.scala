package org.lamastex.mep.tw.ttt
import org.apache.spark.sql.types.{StructType, StructField, StringType};
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset

class TwarcTTTDFTest extends org.scalatest.funsuite.AnyFunSuite{
    val spark = SparkSession.builder.appName("test").config("spark.master", "local").getOrCreate()
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    import spark.implicits._
    val schema_twarc = DataType.fromJson(spark.read.text("schemas/twarc_v1.1_schema.json").first.getString(0)).asInstanceOf[StructType]
    val testDF = spark.read.option("mode", "DROPMALFORMED").schema(schema_twarc).json("src/test/scala/resources/test_twarc.jsonl")
   val tttds =TTTDF_with_inferred_schemas.twarcTTTDF(testDF).as[TTT]
   tttds.show()
   tttds.printSchema
}
  
