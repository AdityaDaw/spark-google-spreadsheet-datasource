package com.github.liam8.spark.datasource

import com.github.liam8.spark.datasource.googlesheet.GoogleSpreadsheetDataSourceException
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.FunSuite
import org.apache.spark.SparkException

class WriterTest extends FunSuite {

  private val spark: SparkSession =
    SparkSession.builder()
      .master("local[*]")
      .appName("datasource test")
      .getOrCreate()

  import spark.implicits._

  private val formatName = "google-spreadsheet"

  private val credentialFile = "service_account_credentials.json"

  private val spreadsheetId = "1pJIU-cFzemvuxuDJ7zssV1J2j80QvUiXMZiGy9Ujoa8"

  private val sheetName = "Sheet2"

  test("overwrite mode with 1 partition") {
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

    val data = spark.read.format(formatName)
      .option("credentialsPath", credentialFile)
      .option("spreadsheetId", spreadsheetId)
      .option("sheetName", sheetName)
      .load()
    assert(data.schema.names sameElements Array("a", "b", "c"))
    assert(data.count() == 2)
  }

  test("append mode with 1 partition") {
    val df = Seq(
      ("1", "word", "3.14"),
      ("2", "word2", "3.145")
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

    df.write.format(formatName)
      .option("credentialsPath", credentialFile)
      .option("spreadsheetId", spreadsheetId)
      .option("sheetName", sheetName)
      .option("firstRowAsHeader", value = false)
      .mode(SaveMode.Append)
      .save()

    val data = spark.read.format(formatName)
      .option("credentialsPath", credentialFile)
      .option("spreadsheetId", spreadsheetId)
      .option("sheetName", sheetName)
      .load()
    assert(data.schema.names sameElements Array("a", "b", "c"))
    assert(data.count() == 4)
  }

  test("not support multiple partitions") {
    val df = Seq(
      ("1", "word", "3.14"),
      ("2", "word2", "3.145"),
      ("3", "word3", "3.145"),
      ("4", "word4", "3.145")
    ).toDF("a", "b", "c")
      .repartition(2)
      .select('a.cast(IntegerType), 'b, 'c.cast(DoubleType))
    df.printSchema()
    df.show(truncate = false)
    assertThrows[SparkException](
      df.write.format(formatName)
        .option("credentialsPath", credentialFile)
        .option("spreadsheetId", spreadsheetId)
        .option("sheetName", sheetName)
        .mode(SaveMode.Overwrite)
        .save()
    )
  }

  test("not support ErrorIfExists save mode") {
    val df = Seq(
      ("1", "word", "3.14")
    ).toDF("a", "b", "c")
      .coalesce(1)
    val e = intercept[SparkException](
      df.write.format(formatName)
        .option("credentialsPath", credentialFile)
        .option("spreadsheetId", spreadsheetId)
        .option("sheetName", sheetName)
        .mode(SaveMode.ErrorIfExists)
        .save()
    )
    assert(e.getCause.getCause.isInstanceOf[GoogleSpreadsheetDataSourceException])
  }

  test("not support Ignore save mode") {
    val df = Seq(
      ("1", "word", "3.14")
    ).toDF("a", "b", "c")
      .coalesce(1)
    val e = intercept[SparkException](
      df.write.format(formatName)
        .option("credentialsPath", credentialFile)
        .option("spreadsheetId", spreadsheetId)
        .option("sheetName", sheetName)
        .mode(SaveMode.Ignore)
        .save()
    )
    assert(e.getCause.getCause.isInstanceOf[GoogleSpreadsheetDataSourceException])
  }

}
