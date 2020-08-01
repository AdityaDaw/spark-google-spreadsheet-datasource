package com.github.liam8.spark.datasource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.FunSuite

class MainTest extends FunSuite {

  private val spark: SparkSession =
    SparkSession.builder()
      .master("local[2]")
      .appName("datasource test")
      .getOrCreate()

  test("load data from gs") {
    val schema = StructType(
      StructField("a", DataTypes.StringType) ::
        StructField("b", DataTypes.StringType) ::
        StructField("c", DataTypes.DoubleType) :: Nil
    )

    val data = spark.read.format("google-spreadsheet")
      .option("credentialsPath", "service_account_credentials.json")
      .option("spreadsheetId", "1pJIU-cFzemvuxuDJ7zssV1J2j80QvUiXMZiGy9Ujoa8")
      .option("sheetName", "Sheet1")
      .schema(schema)
      .load()
    data.printSchema()
    data.show()
    assert(data.count() > 0)
  }
}
