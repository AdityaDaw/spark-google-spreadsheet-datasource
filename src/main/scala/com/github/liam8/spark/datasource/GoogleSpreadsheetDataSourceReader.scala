package com.github.liam8.spark.datasource

import java.util

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.sheets.v4.{Sheets, SheetsScopes}
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._

class GoogleSpreadsheetDataSourceReader(
                                         spreadsheetId: String,
                                         sheetName: String,
                                         credentialsPath: String,
                                         bufferSizeOfEachPartition: Int) extends DataSourceReader {

  private val numPartitions = SparkSession.getActiveSession.get.sparkContext.defaultParallelism

  private val sheets: Sheets = new Sheets.Builder(
    GoogleNetHttpTransport.newTrustedTransport,
    JacksonFactory.getDefaultInstance,
    new HttpCredentialsAdapter(GoogleCredentials.fromStream(
      this.getClass.getClassLoader.getResourceAsStream(credentialsPath)
    ).createScoped(SheetsScopes.SPREADSHEETS))
  ).build()

  override def readSchema(): StructType = {
    val head = sheets.spreadsheets().values()
      .get(spreadsheetId, s"$sheetName!1:1").execute().getValues.asScala
    if (head.isEmpty) {
      throw GoogleSpreadsheetDatasourceException("Can not refer schema from empty sheet.")
    }
    StructType(head.head.asScala.map(v => StructField(v.toString, StringType, nullable = true)))
  }

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val rowCount = sheets.spreadsheets().get(spreadsheetId).setFields("sheets.properties").execute()
      .getSheets.get(0).getProperties.getGridProperties.getRowCount
    val step = Math.ceil(rowCount / numPartitions).toInt
    Range.inclusive(2, rowCount, step).map { i =>
      new GoogleSpreadsheetInputPartition(
        credentialsPath, spreadsheetId, sheetName, i, Math.min(i + step - 1, rowCount), bufferSizeOfEachPartition
      ).asInstanceOf[InputPartition[InternalRow]]
    }.toList.asJava
  }

}
