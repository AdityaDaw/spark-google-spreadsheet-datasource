package com.github.liam8.spark.datasource

import org.apache.spark.sql.SparkSession

object ExampleMain {

  private val spark: SparkSession =
    SparkSession.builder()
      .appName("datasource test")
      .getOrCreate()

  def main(args: Array[String]): Unit = {
    val data = spark.read.format("com.github.liam8.spark.datasource.googlesheet.GoogleSpreadsheetDatasource")
      .option("credentialsPath","service_account_credentials.json")
      .option("spreadsheetId","1pJIU-cFzemvuxuDJ7zssV1J2j80QvUiXMZiGy9Ujoa8")
      .option("sheetName","Sheet1")
      .load()

    data.show()
  }
}
