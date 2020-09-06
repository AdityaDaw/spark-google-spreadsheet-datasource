package com.github.liam8.spark.datasource.googlesheet

import java.util

import com.google.api.services.sheets.v4.Sheets
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

class GoogleSpreadsheetDataSourceReader(
  spreadsheetId: String,
  sheetName: String,
  credentialsPath: String,
  bufferSizeOfEachPartition: Int,
  var schema: Option[StructType] = None,
  firstRowAsHeader: Boolean,
  parallelism: Int
) extends DataSourceReader with SupportsPushDownRequiredColumns {

  private lazy val sheetService: Sheets = GoogleSpreadsheetDataSource.buildSheet(credentialsPath)

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
    val head = sheetService.spreadsheets().values()
      .get(spreadsheetId, s"$sheetName!1:1").execute().getValues.asScala
    if (head.isEmpty) {
      throw GoogleSpreadsheetDataSourceException("Can not refer schema from empty sheet.")
    }
    StructType(head.head.asScala.map(v => StructField(v.toString, StringType, nullable = true)))
  }

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val sheets = sheetService.spreadsheets().get(spreadsheetId)
      .setFields("sheets.properties").execute().getSheets
    val sheet = sheets.asScala.find(_.getProperties.getTitle == sheetName).getOrElse(
      throw GoogleSpreadsheetDataSourceException(s"Can't find the sheet named $sheetName")
    )
    val rowCount = sheet.getProperties.getGridProperties.getRowCount
    val step = Math.ceil(rowCount / parallelism).toInt
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

class GoogleSpreadsheetInputPartition(
  credentialsPath: String,
  spreadsheetId: String,
  sheetName: String,
  startOffset: Int,
  endOffset: Int,
  bufferSize: Int,
  schema: StructType,
  prunedSchema: Option[StructType]
) extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new GoogleSpreadsheetInputPartitionReader(credentialsPath, spreadsheetId, sheetName,
      startOffset, endOffset, bufferSize, schema, prunedSchema)

}

class GoogleSpreadsheetInputPartitionReader(
  credentialsPath: String,
  spreadsheetId: String,
  sheetName: String,
  startOffset: Int,
  endOffset: Int,
  bufferSize: Int,
  schema: StructType,
  prunedSchema: Option[StructType]
) extends InputPartitionReader[InternalRow] with Logging {

  private var currentOffset = startOffset

  private var buffer: List[List[Any]] = _

  private var bufferIter: Iterator[List[Any]] = _

  private lazy val sheets: Sheets = GoogleSpreadsheetDataSource.buildSheet(credentialsPath)

  override def next(): Boolean = {
    if (bufferIter != null && bufferIter.hasNext) {
      return true
    }
    if (currentOffset > endOffset) {
      return false
    }
    val end = (currentOffset + bufferSize - 1) min endOffset
    val ranges = getColumnsBySchema.map(c => s"$sheetName!$c$currentOffset:$c$end").toList
    logDebug("fetching from sheet with range:" + ranges.toString())
    val jValueRanges = sheets.spreadsheets().values()
      .batchGet(spreadsheetId)
      .setRanges(ranges.asJava)
      .setMajorDimension("COLUMNS")
      .execute().getValueRanges
    if (jValueRanges == null || jValueRanges.isEmpty) {
      return false
    }
    val valueRanges = jValueRanges.asScala
    val maxRowNum = valueRanges.map(vr =>
      if (vr.getValues != null) vr.getValues.get(0).size else 0
    ).max
    buffer = (0 until maxRowNum).map { r =>
      valueRanges.indices.map { c =>
        val col = valueRanges(c).getValues.get(0)
        if (col != null && col.size() > r) {
          col.get(r)
        } else {
          null
        }
      }.toList
    }.toList
    bufferIter = buffer.iterator
    currentOffset = end + 1
    bufferIter.hasNext
  }

  override def get(): InternalRow = {
    val curRow = bufferIter.next.zipWithIndex
      .filter(_._2 < schema.size)
      .map { case (f, i) =>
        val v = f.asInstanceOf[String]
        if (v == null) {
          null
        } else if (v.isEmpty) {
          if (schema(i).dataType == StringType) {
            UTF8String.fromString(v)
          } else {
            null
          }
        } else {
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
            case t =>
              throw GoogleSpreadsheetDataSourceException(s"Not support the $t type right now")
          }
        }
      }
    InternalRow(curRow: _*)
  }

  override def close(): Unit = {}

  private def getColumnsBySchema = {
    val colIdx = if (prunedSchema.isEmpty || prunedSchema.get.names.isEmpty) {
      schema.indices.toArray
    } else {
      val remained = prunedSchema.get.names
      schema.names.zipWithIndex.filter { case (name, _) =>
        remained.contains(name)
      }.map(_._2)
    }
    colIdx.map(i => getColumnByOrdinal(i + 1))
  }

  private def getColumnByOrdinal(idx: Int) = {
    var i = idx
    var col = ""
    while (i > 0) {
      val q = i / 26
      val mod = i % 26
      col += ('A' + mod - 1).toChar
      i = q
    }
    col.reverse
  }

}