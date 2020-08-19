package com.github.liam8.spark.datasource.googlesheet

import java.util

import com.google.api.services.sheets.v4.Sheets
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._

class GoogleSpreadsheetDataSourceReader(
  spreadsheetId: String,
  sheetName: String,
  credentialsPath: String,
  bufferSizeOfEachPartition: Int,
  var schema: Option[StructType] = None,
  firstRowAsHeader: Boolean
) extends DataSourceReader with SupportsPushDownRequiredColumns {

  private val numPartitions = SparkSession.getActiveSession.get.sparkContext.defaultParallelism

  private lazy val sheets: Sheets = GoogleSpreadsheetDataSource.buildSheet(credentialsPath)

  private var prunedSchema: Option[StructType] = None

  override def readSchema(): StructType =
    if (prunedSchema.nonEmpty) {
      prunedSchema.get
    } else {
      originalSchema
    }

  private def originalSchema = {
    if (schema.isEmpty) {
      schema = Option(inferSchema)
    }
    schema.get
  }

  private def inferSchema = {
    if (!firstRowAsHeader) {
      throw GoogleSpreadsheetDataSourceException(
        "can not infer schema without header, please specify schema manually")
    }
    val head = sheets.spreadsheets().values()
      .get(spreadsheetId, s"$sheetName!1:1").execute().getValues.asScala
    if (head.isEmpty) {
      throw GoogleSpreadsheetDataSourceException("Can not refer schema from empty sheet.")
    }
    StructType(head.head.asScala.map(v => StructField(v.toString, StringType, nullable = true)))
  }

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val rowCount = sheets.spreadsheets().get(spreadsheetId).setFields("sheets.properties").execute()
      .getSheets.get(0).getProperties.getGridProperties.getRowCount
    val step = Math.ceil(rowCount / numPartitions).toInt
    val start = if (firstRowAsHeader) 2 else 1
    Range.inclusive(start, rowCount, step).map { i =>
      new GoogleSpreadsheetInputPartition(
        credentialsPath,
        spreadsheetId,
        sheetName,
        i,
        Math.min(i + step - 1, rowCount),
        bufferSizeOfEachPartition,
        originalSchema,
        prunedSchema
      ).asInstanceOf[InputPartition[InternalRow]]
    }.toList.asJava
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    prunedSchema = Option(requiredSchema)
  }

}
