package com.kouzoh.data.loader.source.mysql

import com.google.cloud.sql.CredentialFactory
import com.google.cloud.sql.mysql.SocketFactory
import com.kouzoh.data.loader.configs.mysql.MysqlSourceConfig
import com.kouzoh.data.loader.utils.ServiceAccountCredentialFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{
  DateType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  TimestampType
}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession}

import java.util.Properties

object MysqlDataLoader {

  val MAX_COL: String = "max_value"
  val MIN_COL: String = "min_value"

  def loadSnapshot(spark: SparkSession, tableName: String, conf: MysqlSourceConfig): DataFrame = {
    import conf._

    val (reader: DataFrameReader, connectionProp: Properties) = buildDataReader(spark, conf)

    // This is only use for local
    maybeCredentialFile.map { credentialFile =>
      // Force use self managed credential factory
      System.setProperty(
        CredentialFactory.CREDENTIAL_FACTORY_PROPERTY,
        ServiceAccountCredentialFactory.getClass.getName.dropRight(1)
      )
      System.setProperty(ServiceAccountCredentialFactory.CREDENTIAL_FILE_PATH, credentialFile)
    }


    val connectionUrl: String = s"jdbc:mysql:///$dbName"

    val df: DataFrame = (maybeSplitColumn, maybeSplitCount) match {
      case (Some(splitColumn), Some(splitCount)) =>
        val result: DataFrame = reader.jdbc(
          connectionUrl,
          s"(SELECT MAX($splitColumn) as $MAX_COL, MIN($splitColumn) as $MIN_COL FROM $tableName) TMP",
          connectionProp
        )

        val predicates: Seq[String] =
          determineTypeAndCreatePredicates(result, splitCount, splitColumn)
        reader.jdbc(connectionUrl, tableName, predicates.toArray, connectionProp)

      case _ =>
        reader.jdbc(connectionUrl, tableName, connectionProp)
    }

    val dfWithColumnDropped = if(excludeColumns.contains(tableName)) {
      println(s"will exclude ${excludeColumns(tableName)}")
      df.drop(excludeColumns(tableName).toSeq: _*)
    } else {
      df
    }


    dfWithColumnDropped.withColumn(
        "binlog_ts_ms",
        to_timestamp(lit("1970-01-01T00:00:00Z"), "yyyy-MM-dd'T'HH:mm:ssXXX")
      )
      .withColumn("binlog_pos", lit(0))
      .withColumn("binlog_row", lit(0))
  }

  protected[mysql] def determineTypeAndCreatePredicates(
    df: DataFrame,
    splitCount: Long,
    splitColumn: String
  ): Seq[String] = {
    df.schema.fields(0).dataType match {
      case DateType | TimestampType =>
        val newResult: DataFrame =
          df.withColumn(MAX_COL, unix_timestamp(col(MAX_COL)))
            .withColumn(MIN_COL, unix_timestamp(col(MIN_COL)))

        val resultRow: Array[Row] = newResult.collect
        val max: Long = resultRow(0).getLong(0)
        val min: Long = resultRow(0).getLong(1)

        buildPredicatesForDateType(min, max, splitCount, splitColumn)

      case LongType | IntegerType =>
        val newResult: DataFrame =
          df.withColumn(MAX_COL, col(MAX_COL).cast(LongType))
            .withColumn(MIN_COL, col(MIN_COL).cast(LongType))

        val resultRow: Array[Row] = newResult.collect
        val max: Long = resultRow(0).getLong(0)
        val min: Long = resultRow(0).getLong(1)

        buildPredicatesForNumberType(min, max, splitCount, splitColumn)

      case DoubleType | FloatType =>
        val newResult: DataFrame =
          df.withColumn(MAX_COL, col(MAX_COL).cast(DoubleType))
            .withColumn(MIN_COL, col(MIN_COL).cast(DoubleType))

        val resultRow: Array[Row] = newResult.collect()
        val max: Double = resultRow(0).getDouble(0)
        val min: Double = resultRow(0).getDouble(1)

        buildPredicatesForNumberType(min, max, splitCount, splitColumn)

      case _ =>
        throw new RuntimeException(s"Unsupported type: ${df.schema.fields(0).dataType}")
    }
  }

  protected[mysql] def buildDataReader(
    spark: SparkSession,
    conf: MysqlSourceConfig
  ): (DataFrameReader, Properties) = {
    val options = Map[String, String](
      "connectTimeout" -> "120000",
      "socketTimeout" -> "120000",
      "fetchSize" -> "500"
    ) ++ conf.jdbcOptions

    val connectionProperty: Properties = new Properties()

    options.foreach { case (k, v) => connectionProperty.put(k, v) }
    connectionProperty.put("driver", "com.mysql.cj.jdbc.Driver")
    connectionProperty.put("socketFactory", classOf[SocketFactory].getName.split("$").head)

    import conf._
    connectionProperty.put("cloudSqlInstance", cloudSqlInstance)
    connectionProperty.put("user", username)
    connectionProperty.put("password", password)
    (spark.read.options(options), connectionProperty)
  }

  // using jdbc, spark will separate task by sql predicates, by adjust executor count and split count
  // we are able to load all data from huge table as well
  protected[mysql] def buildPredicatesForNumberType(
    min: Long,
    max: Long,
    splitCount: Long,
    splitColumn: String
  ): Seq[String] = {
    val realMax: Long = max + 1
    val diff: Long = realMax - min
    val chunkSize: Long = diff / splitCount

    if (chunkSize <= 1000) {
      Seq(s"$splitColumn >= $min AND $splitColumn < $realMax")
    } else {
      var result: Seq[String] = Seq()
      for (term <- 0L until splitCount) {
        val rangeStart: Long = min + term * chunkSize
        var rangeEnd: Long = rangeStart + chunkSize
        if (term == splitCount - 1L) {
          if (rangeEnd != realMax) rangeEnd = realMax
        }
        result :+= s"$splitColumn >= $rangeStart AND $splitColumn < $rangeEnd"
      }
      result
    }
  }

  protected[mysql] def buildPredicatesForNumberType(
    min: Double,
    max: Double,
    splitCount: Long,
    splitColumn: String
  ): Seq[String] = {
    val realMax: Double = max + 1
    val diff: Double = realMax - min
    val chunkSize: Double = diff / splitCount

    if (chunkSize <= 1000) {
      Seq(s"$splitColumn >= ${"%.6f".format(min)} AND $splitColumn < ${"%.6f".format(realMax)}")
    } else {
      var result: Seq[String] = Seq()
      for (term <- 0L until splitCount) {
        val rangeStart: Double = min + term * chunkSize
        var rangeEnd: Double = rangeStart + chunkSize
        if (term == splitCount - 1L) {
          if (rangeEnd != realMax) rangeEnd = realMax
        }
        result :+= s"$splitColumn >= ${"%.6f".format(rangeStart)} AND $splitColumn < ${"%.6f".format(rangeEnd)}"
      }
      result
    }
  }

  protected[mysql] def buildPredicatesForDateType(
    from: Long,
    to: Long,
    splitCount: Long,
    splitColumn: String
  ): Seq[String] = {
    val realTo: Long = to + 1000L
    val durationMillis: Long = realTo - from
    val chunkSize: Long = durationMillis / splitCount

    if (chunkSize <= 1000) {
      Seq(s"$splitColumn >= FROM_UNIXTIME($from) AND $splitColumn < FROM_UNIXTIME($realTo)")
    } else {
      var result: Seq[String] = Seq()
      for (term <- 0L until splitCount) {
        val rangeStart: Long = from + term * chunkSize
        var rangeEnd: Long = rangeStart + chunkSize
        if (term == splitCount - 1L) {
          if (rangeEnd != realTo) rangeEnd = realTo
        }
        result :+= s"$splitColumn >= FROM_UNIXTIME($rangeStart) AND $splitColumn < FROM_UNIXTIME($rangeEnd)"
      }
      result
    }
  }
}
