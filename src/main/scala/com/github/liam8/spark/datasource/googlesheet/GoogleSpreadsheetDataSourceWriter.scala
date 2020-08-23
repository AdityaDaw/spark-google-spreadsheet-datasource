package com.github.liam8.spark.datasource.googlesheet

import java.util

import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.model.{ClearValuesRequest, ValueRange}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable

class GoogleSpreadsheetDataSourceWriter(
  mode: SaveMode,
  spreadsheetId: String,
  sheetName: String,
  credentialsPath: String,
  bufferSizeOfEachPartition: Int,
  var schema: StructType,
  firstRowAsHeader: Boolean
) extends DataSourceWriter {
  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new GoogleSpreadsheetDataWriterFactory(
      mode, schema, credentialsPath, spreadsheetId, sheetName,
      bufferSizeOfEachPartition, firstRowAsHeader)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

class GoogleSpreadsheetDataWriterFactory(
  mode: SaveMode,
  schema: StructType,
  credentialsPath: String,
  spreadsheetId: String,
  sheetName: String,
  bufferSize: Int,
  firstRowAsHeader: Boolean
) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(
    partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    if (partitionId > 0) {
      throw GoogleSpreadsheetDataSourceException(
        "GoogleSpreadsheetDataSource ONLY support data with 1 partition")
    }
    new GoogleSpreadsheetDataWriter(
      mode, schema, credentialsPath, spreadsheetId, sheetName, bufferSize, firstRowAsHeader)
  }

}

class GoogleSpreadsheetDataWriter(
  saveMode: SaveMode,
  schema: StructType,
  credentialsPath: String,
  spreadsheetId: String,
  sheetName: String,
  bufferSize: Int,
  firstRowAsHeader: Boolean
) extends DataWriter[InternalRow] {

  private lazy val sheets: Sheets = GoogleSpreadsheetDataSource.buildSheet(credentialsPath)

  private val buffer = mutable.Buffer[util.List[AnyRef]]()

  private var haveWrittenHeader = false

  private val VALUE_INPUT_OPTION = "RAW"

  override def write(record: InternalRow): Unit = {
    if (!haveWrittenHeader && firstRowAsHeader) {
      buffer.append(schema.names.toSeq.map(_.asInstanceOf[AnyRef]).asJava)
      haveWrittenHeader = true
    }
    if (buffer.size >= bufferSize) {
      writeToSheet()
    }
    buffer.append(
      record.toSeq(schema).map { v =>
        if (v != null) v.toString.asInstanceOf[AnyRef] else ""
      }.asJava
    )
  }

  override def commit(): WriterCommitMessage = {
    writeToSheet()
    GoogleSpreadsheetCommitMessage
  }

  override def abort(): Unit = {
    buffer.clear()
  }

  private def writeToSheet() {
    val body = new ValueRange().setValues(buffer.asJava)
    saveMode match {
      case SaveMode.Overwrite => sheets.spreadsheets.values
        .clear(spreadsheetId, sheetName, new ClearValuesRequest())
        .execute
        sheets.spreadsheets.values
          .update(spreadsheetId, sheetName, body)
          .setValueInputOption(VALUE_INPUT_OPTION)
          .execute
      case SaveMode.Append => sheets.spreadsheets.values
        .append(spreadsheetId, sheetName, body)
        .setValueInputOption(VALUE_INPUT_OPTION)
        .execute
      case x => throw GoogleSpreadsheetDataSourceException(s"do NOT support the `$x` save mode up to now")
    }
    buffer.clear()
  }
}

/**
 * An empty [[WriterCommitMessage]]
 * [[GoogleSpreadsheetDataSourceWriter]] implementations have no global coordination.
 */
case object GoogleSpreadsheetCommitMessage extends WriterCommitMessage
