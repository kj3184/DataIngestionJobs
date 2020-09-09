package com.taiger.bumblebee.data.ingestion.jobs

import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, SparkSession}

class Csv2Json {

  def startDataIngestion(): Unit = {

    val session = connectToSpark()
    val df = readingCSVfile(session)
    processCSVFile(df)
    transformCSVJson(df)
  }

  def connectToSpark(): SparkSession = {
    val session = SparkSession.builder().appName("CSV to json conversion")
      .master("local").getOrCreate()
    return session
  }

  def readingCSVfile(session: SparkSession): sql.DataFrame = {
    val df= session.read.options(Map("inferSchema"->"false","delimiter"->",","header"->"true","multiline"->"true"))
      .schema("accession_no_csv string, Image string, object_work_type string, title_text string, preference string, " +
        "title_language string, creator_2 string, creator_1 string, creator_role string, creation_date string, " +
        "creation_place_original_location string, styles_periods_indexing_terms string, inscriptions string, " +
        "inscription_language string, scale_type string, shape string, materials_name string, techniques_name string, " +
        "object_colour string, edition_description string, physical_appearance string, subject_terms_1 string, " +
        "subject_terms_2 string, subject_terms_3 string, subject_terms_4 string, context_1 string, context_2 string, context_3 string, " +
        "context_4 string, context_5 string, context_6 string, context_7 string, context_8 string, context_9 string, context_10 string, " +
        "context_11 string, context_12 string, context_13 string, context_14 string, context_15 string, context_16 string, " +
        "context_17 string, context_18 string, context_19 string, context_20 string, context_21 string, context_22 string, " +
        "context_23 string, context_24 string, sgcool_label_text string")
      .csv("inputfile/Consolidated_R2_20190327.csv")
     return df
  }

  def processCSVFile(df: sql.DataFrame): Unit = {
    println("processCSVfile")
    println("Excerpt of the dataframe content:")
    df.show(10)
    df.printSchema()
    println("Dataframe's schema:")
  }

  def transformCSVJson(df: sql.DataFrame): Unit = {
    println("transformCSVJson")
    df.write
      .json("inputfile/json/Consolidated_R2_20190327.json")
  }

}


object DataIngestionObj extends App {

  val csv2jsonobj = new Csv2Json
  csv2jsonobj.startDataIngestion()


}