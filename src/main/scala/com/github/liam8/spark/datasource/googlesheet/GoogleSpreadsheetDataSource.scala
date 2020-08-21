package com.github.liam8.spark.datasource.googlesheet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupport}
import org.apache.spark.sql.types.StructType

/**
 * @todo support column pruning
 */
class GoogleSpreadsheetDataSource extends ReadSupport with DataSourceRegister {

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    createReader(null, options)
  }

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {
    new GoogleSpreadsheetDataSourceReader(
      options.get("spreadsheetId").get(),
      options.get("sheetName").get(),
      options.get("credentialsPath").get(),
      options.getInt("bufferSizeOfEachPartition", 100),
      Option(schema),
      options.getBoolean("firstRowAsHeader", true),
      options.getInt("parallelism",
        SparkSession.getActiveSession.get.sparkContext.defaultParallelism)
    )
  }

  override def shortName(): String = "google-spreadsheet"
}

