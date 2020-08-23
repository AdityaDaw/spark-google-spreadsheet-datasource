package com.github.liam8.spark.datasource

import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.FunSuite

class WriterTest extends FunSuite {

  private val spark: SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("datasource test")
      .getOrCreate()

  import spark.implicits._

  private val formatName = "google-spreadsheet"

  private val credentialFile = "service_account_credentials.json"

  private val spreadsheetId = "1pJIU-cFzemvuxuDJ7zssV1J2j80QvUiXMZiGy9Ujoa8"

  private val sheetName = "Sheet2"

  test("overwrite mode") {
    val df = Seq(
      ("1", "word", "3.14"),
      ("2a", "word2", "3.145")
    ).toDF("a", "b", "c")
      .coalesce(1)
      .select('a.cast(IntegerType), 'b, 'c.cast(DoubleType))
    df.printSchema()
    df.show(truncate = false)
    df.write.format(formatName)
      .option("credentialsPath", credentialFile)
      .option("spreadsheetId", spreadsheetId)
      .option("sheetName", sheetName)
      .mode(SaveMode.Overwrite)
      .save()
  }



}
