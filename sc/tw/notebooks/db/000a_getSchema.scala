// Databricks notebook source
// MAGIC %md 
// MAGIC #Inferring and then storing a schema, to make loading tweets quicker in the future.

// COMMAND ----------

val pathToRawTwitterData = "put/path/here"

//load everything available to infer schema 
val raw_tweets = spark.read.option("mode", "DROPMALFORMED").json(pathToRawTwitterData)
val schemaJson = raw_tweets.schema.json

// COMMAND ----------

val pathToSavedSchema = "your/path/" + "schema.json"
//save the infered schema
dbutils.fs.put(pathToSavedSchema, schemaJson)

// COMMAND ----------

// MAGIC %md 
// MAGIC # To load saved schema:

// COMMAND ----------

val savedSchema = spark.read.text(pathToSavedSchema)

//this will be loaded as a dataframe, and we just want everything as a string
val savedSchemaString = savedSchema.first.getString(0)

//from the string we can get the proper type for schemas ie StructType
val twarc_schema = DataType.fromJson(savedSchema.first.getString(0)).asInstanceOf[StructType]
