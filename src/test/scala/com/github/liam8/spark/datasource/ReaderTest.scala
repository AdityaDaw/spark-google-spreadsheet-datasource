package com.github.liam8.spark.datasource

import com.github.liam8.spark.datasource.googlesheet.GoogleSpreadsheetDataSourceException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.FunSuite

class ReaderTest extends FunSuite {

  private val spark: SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("datasource test")
      .getOrCreate()

  private val formatName = "google-spreadsheet"

  private val credentialFile = "service_account_credentials.json"

  private val spreadsheetId = "1pJIU-cFzemvuxuDJ7zssV1J2j80QvUiXMZiGy9Ujoa8"

  private val sheetName1 = "Sheet1"

  private val sheetName2 = "Sheet2"

  test("infer schema") {
    val data = spark.read.format(formatName)
      .option("credentialsPath", credentialFile)
      .option("spreadsheetId", spreadsheetId)
      .option("sheetName", sheetName1)
      .load()
    data.printSchema()
    assert(data.schema.contains(StructField("b", DataTypes.StringType)))
    assert(data.count() > 0)
  }

  test("specify schema") {
    val schema = StructType(
      StructField("a", DataTypes.StringType) ::
        StructField("b", DataTypes.IntegerType) ::
        StructField("c", DataTypes.DoubleType) ::
        StructField("day", DataTypes.DateType) ::
        StructField("ts", DataTypes.TimestampType) :: Nil
    )

    val data = spark.read.format(formatName)
      .option("credentialsPath", credentialFile)
      .option("spreadsheetId", spreadsheetId)
      .option("sheetName", sheetName1)
      .schema(schema)
      .load()
    data.printSchema()
    assert(data.schema.contains(StructField("day", DataTypes.DateType)))
    assert(data.schema.contains(StructField("ts", DataTypes.TimestampType)))
    assert(data.count() > 0)
  }

  test("specify schema without header") {
    val schema = StructType(
      StructField("a", DataTypes.StringType) :: Nil
    )

    val data = spark.read.format(formatName)
      .option("credentialsPath", credentialFile)
      .option("spreadsheetId", spreadsheetId)
      .option("sheetName", sheetName1)
      .option("firstRowAsHeader", value = false)
      .schema(schema)
      .load()
    data.printSchema()
    assert(data.schema.contains(StructField("a", DataTypes.StringType)))
    assert(data.count() == 31)
  }

  test("infer schema without header") {
    assertThrows[GoogleSpreadsheetDataSourceException](
      spark.read.format(formatName)
        .option("credentialsPath", credentialFile)
        .option("spreadsheetId", spreadsheetId)
        .option("sheetName", sheetName1)
        .option("firstRowAsHeader", value = false)
        .load()
    )
  }

  test("prune schema") {
    val data = spark.read.format(formatName)
      .option("credentialsPath", credentialFile)
      .option("spreadsheetId", spreadsheetId)
      .option("sheetName", sheetName1)
      .load()
      .select("a", "b", "day")
    data.printSchema()
    data.show(truncate = false)
    assert(data.schema.contains(StructField("b", DataTypes.StringType)))
  }

  test("row count on sheet1") {
    val data = spark.read.format(formatName)
      .option("credentialsPath", credentialFile)
      .option("spreadsheetId", spreadsheetId)
      .option("sheetName", sheetName1)
      .load()
      .select("b")
    data.show(50, truncate = false)
    assert(data.count() == 30)
  }

  test("row count on sheet2") {
    val data = spark.read.format(formatName)
      .option("credentialsPath", credentialFile)
      .option("spreadsheetId", spreadsheetId)
      .option("sheetName", sheetName2)
      .load()
      .select("a")
    data.show(50, truncate = false)
    assert(data.count() == 60)
  }

  test("parallelism") {
    val parallelism = 2
    val data = spark.read.format(formatName)
      .option("credentialsPath", credentialFile)
      .option("spreadsheetId", spreadsheetId)
      .option("sheetName", sheetName1)
      .option("parallelism", parallelism)
      .load()
    assert(data.rdd.partitions.length == parallelism)
  }
}
