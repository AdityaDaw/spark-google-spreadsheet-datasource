package com.github.liam8.spark.datasource.googlesheet

import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.model.ValueRange
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
      mode, schema, credentialsPath, spreadsheetId, sheetName, bufferSizeOfEachPartition)
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
  bufferSize: Int
) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(
    partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new GoogleSpreadsheetDataWriter(
      mode, schema, credentialsPath, spreadsheetId, sheetName, bufferSize, partitionId)
  }

}

class GoogleSpreadsheetDataWriter(
  mode: SaveMode,
  schema: StructType,
  credentialsPath: String,
  spreadsheetId: String,
  sheetName: String,
  bufferSize: Int,
  partitionId: Int
) extends DataWriter[InternalRow] {

  private lazy val sheets: Sheets = GoogleSpreadsheetDataSource.buildSheet(credentialsPath)

  private val buffer = mutable.Buffer[InternalRow]()

  override def write(record: InternalRow): Unit = {
    buffer.append(record.copy())
    if (buffer.size >= bufferSize) {
      writeToSheet()
    }
  }

  override def commit(): WriterCommitMessage = {
    GoogleSpreadsheetCommitMessage
  }

  override def abort(): Unit = {
    buffer.clear()
  }

  private def writeToSheet() {
    // todo support append mode
    val range = if (partitionId > 0) s"$sheetName-$partitionId!1:1" else s"$sheetName!1:1"
    val body = new ValueRange().setValues(
      buffer.map(
        _.toSeq(schema).map(_.asInstanceOf[AnyRef]).asJava
      ).asJava
    )
    sheets.spreadsheets.values
      .update(spreadsheetId, range, body)
      .setValueInputOption("RAW")
      .execute
  }
}

/**
 * An empty [[WriterCommitMessage]]
 * [[GoogleSpreadsheetDataSourceWriter]] implementations have no global coordination.
 */
case object GoogleSpreadsheetCommitMessage extends WriterCommitMessage
