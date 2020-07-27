package com.github.liam8.spark.datasource

import java.{util => ju}

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.sheets.v4.{Sheets, SheetsScopes}
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

class GoogleSpreadsheetInputPartition(
                                       credentialsPath: String,
                                       spreadsheetId: String,
                                       sheetName: String,
                                       startOffset: Int,
                                       endOffset: Int,
                                       bufferSize: Int) extends InputPartition[InternalRow] with Logging {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = new InputPartitionReader[InternalRow] {

    private var currentOffset = startOffset

    private var buffer: ju.List[ju.List[Object]] = _

    private var bufferIter: ju.Iterator[ju.List[Object]] = _

    private val sheets: Sheets = new Sheets.Builder(
      GoogleNetHttpTransport.newTrustedTransport,
      JacksonFactory.getDefaultInstance,
      new HttpCredentialsAdapter(GoogleCredentials.fromStream(
        this.getClass.getClassLoader.getResourceAsStream(credentialsPath)
      ).createScoped(SheetsScopes.SPREADSHEETS))
    ).build()

    override def next(): Boolean = {
      if (bufferIter != null && bufferIter.hasNext) {
        return true
      }
      if (currentOffset > endOffset) {
        return false
      }
      val end = (currentOffset + bufferSize) min endOffset
      val range = s"$sheetName!$currentOffset:$end"
      val rows = sheets.spreadsheets().values()
        .get(spreadsheetId, range).execute().getValues
      if (rows == null || rows.isEmpty) {
        return false
      }
      logInfo(s"read ${rows.size} rows from $range")
      buffer = rows
      bufferIter = rows.iterator()
      currentOffset = end + 1
      true
    }

    override def get(): InternalRow = {
      val curRow = bufferIter.next.asScala.map(f => UTF8String.fromString(String.valueOf(f)))
      InternalRow(curRow: _*)
    }

    override def close(): Unit = {}
  }

}
