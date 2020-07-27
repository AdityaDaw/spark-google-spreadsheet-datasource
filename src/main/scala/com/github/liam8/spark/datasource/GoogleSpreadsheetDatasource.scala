package com.github.liam8.spark.datasource

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

/**
 * @todo support specify schema
 *       support gs buffer
 *       specify header
 */
class GoogleSpreadsheetDatasource extends DataSourceV2 with ReadSupport {

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new GoogleSpreadsheetDataSourceReader(
      options.get("spreadsheetId").get(),
      options.get("sheetName").get(),
      options.get("credentialsPath").get(),
      options.getInt("bufferSizeOfEachPartition",10)
    )
  }

}
