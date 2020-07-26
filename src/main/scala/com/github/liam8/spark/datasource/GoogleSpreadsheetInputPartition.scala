package com.github.liam8.spark.datasource

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.sheets.v4.{Sheets, SheetsScopes}
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

class GoogleSpreadsheetInputPartition(
                                       credentialsPath: String,
                                       spreadsheetId: String,
                                       sheetName: String,
                                       startOffset: Int,
                                       endOffset: Int) extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = new InputPartitionReader[InternalRow] {

    private val sheets: Sheets = new Sheets.Builder(
      GoogleNetHttpTransport.newTrustedTransport,
      JacksonFactory.getDefaultInstance,
      new HttpCredentialsAdapter(GoogleCredentials.fromStream(
        this.getClass.getClassLoader.getResourceAsStream(credentialsPath)
      ).createScoped(SheetsScopes.SPREADSHEETS))
    ).build()

    private var currentOffset = startOffset

    private var currentValue: Option[InternalRow] = None

    //todo buffer
    //    private var buffer: List[AnyVal] = List.empty

    override def next(): Boolean = {
      if (currentOffset > endOffset) {
        return false
      }
      val rows = sheets.spreadsheets().values()
        .get(spreadsheetId, s"$sheetName!$currentOffset:$currentOffset").execute().getValues
      if (rows == null || rows.isEmpty) {
        return false
      }
      val curRow = rows.get(0).asScala.map(f => UTF8String.fromString(String.valueOf(f)))
      currentValue = Some(InternalRow.fromSeq(curRow))
      currentOffset += 1
      true
    }

    override def get(): InternalRow = currentValue.get

    override def close(): Unit = {}
  }

}
