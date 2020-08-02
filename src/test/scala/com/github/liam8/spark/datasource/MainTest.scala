package com.github.liam8.spark.datasource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import org.scalatest.FunSuite

class MainTest extends FunSuite {

  private val spark: SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("datasource test")
      .getOrCreate()

  test("infer schema") {
    val data = spark.read.format("google-spreadsheet")
      .option("credentialsPath", "service_account_credentials.json")
      .option("spreadsheetId", "1pJIU-cFzemvuxuDJ7zssV1J2j80QvUiXMZiGy9Ujoa8")
      .option("sheetName", "Sheet1")
      .load()
    data.printSchema()
    data.show(truncate = false)
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

    val data = spark.read.format("google-spreadsheet")
      .option("credentialsPath", "service_account_credentials.json")
      .option("spreadsheetId", "1pJIU-cFzemvuxuDJ7zssV1J2j80QvUiXMZiGy9Ujoa8")
      .option("sheetName", "Sheet1")
      .schema(schema)
      .load()
    data.printSchema()
    data.show(truncate = false)
    assert(data.schema.contains(StructField("day", DataTypes.DateType)))
    assert(data.schema.contains(StructField("ts", DataTypes.TimestampType)))
    assert(data.count() > 0)
  }
}
