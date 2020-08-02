package com.github.liam8.spark.datasource.googlesheet

import java.{util => ju}

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.sheets.v4.{Sheets, SheetsScopes}
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

class GoogleSpreadsheetInputPartition(
  credentialsPath: String,
  spreadsheetId: String,
  sheetName: String,
  startOffset: Int,
  endOffset: Int,
  bufferSize: Int,
  schema: StructType
) extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new GoogleSpreadsheetInputPartitionReader(credentialsPath, spreadsheetId, sheetName,
      startOffset, endOffset, bufferSize, schema)

}

class GoogleSpreadsheetInputPartitionReader(
  credentialsPath: String,
  spreadsheetId: String,
  sheetName: String,
  startOffset: Int,
  endOffset: Int,
  bufferSize: Int,
  schema: StructType
) extends InputPartitionReader[InternalRow] {

  private var currentOffset = startOffset

  private var buffer: ju.List[ju.List[Object]] = _

  private var bufferIter: ju.Iterator[ju.List[Object]] = _

  private lazy val sheets: Sheets = new Sheets.Builder(
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
    buffer = rows
    bufferIter = rows.iterator()
    currentOffset = end + 1
    true
  }

  override def get(): InternalRow = {
    val curRow = bufferIter.next.asScala.zipWithIndex
      .filter(_._2 < schema.size)
      .map { case (f, i) =>
        val v = f.asInstanceOf[String]
        schema(i).dataType match {
          case StringType => UTF8String.fromString(v)
          case IntegerType => v.toInt
          case LongType => v.toLong
          case DoubleType => v.toDouble
          case FloatType => v.toFloat
          case BooleanType => v.toBoolean
          case ShortType => v.toShort
          case DateType =>
            DateTimeUtils.stringToDate(UTF8String.fromString(v)).getOrElse(null)
          case TimestampType =>
            DateTimeUtils.stringToTimestamp(UTF8String.fromString(v)).getOrElse(null)
          case t => throw GoogleSpreadsheetDataSourceException(s"Not support the $t type right now")
        }
      }
    InternalRow(curRow: _*)
  }

  override def close(): Unit = {}

}
