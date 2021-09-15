package com.kouzoh.data.loader.source.mysql

import com.kouzoh.data.loader.configs.mysql.MysqlSourceConfig
import org.apache.spark.sql.functions.{lit, to_timestamp}
import org.apache.spark.sql.types.{DateType, DoubleType, FloatType, IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession}

import java.util.Properties

object MysqlDataLoader {

  def loadSnapshot(spark: SparkSession, tableName: String, conf: MysqlSourceConfig): DataFrame = {
    import conf._

    val (reader: DataFrameReader, connectionProp: Properties) = buildDataReader(spark, conf)

    val connectionUrl: String = s"jdbc:mysql://$url:$port/$dbName"

    val df = (maybeSplitColumn, maybeSplitCount) match {
      case (Some(splitColumn), Some(splitCount)) =>
        val result: DataFrame = reader.jdbc(
          connectionUrl,
          s"(SELECT MAX($splitColumn), MIN($splitColumn) FROM $tableName) TMP",
          connectionProp
        )

        result.schema.fields(0).dataType match {
          case DateType =>
            val newResult: DataFrame = reader.jdbc(
              connectionUrl,
              s"(SELECT UNIX_TIMESTAMP(MAX($splitColumn)), UNIX_TIMESTAMP(MIN($splitColumn)) FROM $tableName) TMP",
              new Properties()
            )
            val resultRow: Array[Row] = newResult.collect()
            val max: Long = resultRow(0).getLong(0)
            val min: Long = resultRow(0).getLong(1)

            val predicates = buildPredicatesForDateType(min, max, splitCount, splitColumn)
            reader.jdbc(connectionUrl, tableName, predicates.toArray, connectionProp)

          case LongType | IntegerType =>
            val resultRow: Array[Row] = result.collect()
            val max: Long = resultRow(0).getLong(0)
            val min: Long = resultRow(0).getLong(1)

            val predicates = buildPredicatesForNumberType(min, max, splitCount, splitColumn)
            reader.jdbc(connectionUrl, tableName, predicates.toArray, connectionProp)

          case DoubleType | FloatType =>
            val newResult: DataFrame = reader.jdbc(
              connectionUrl,
              s"(SELECT CAST(MAX($splitColumn) AS DOUBLE), CAST(MIN($splitColumn) AS DOUBLE) FROM $tableName) TMP",
              new Properties()
            )
            val resultRow: Array[Row] = newResult.collect()
            val max: Double = resultRow(0).getDouble(0)
            val min: Double = resultRow(0).getDouble(1)

            val predicates = buildPredicatesForNumberType(min, max, splitCount, splitColumn)
            reader.jdbc(connectionUrl, tableName, predicates.toArray, connectionProp)
        }


      case _ =>
        reader.jdbc(connectionUrl, tableName, connectionProp)
    }

    df
      .withColumn("binlog_ts_ms", to_timestamp(lit("1970-01-01T00:00:00Z"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
      .withColumn("binlog_pos", lit(0))
      .withColumn("binlog_row", lit(0))
  }


  def buildDataReader(spark: SparkSession, conf: MysqlSourceConfig): (DataFrameReader, Properties) = {
    val options = Map[String, String] (
      "connectTimeout" -> "120000",
      "socketTimeout" -> "120000",
      "fetchSize" -> "500"
    ) ++ conf.jdbcOptions

    val connectionProperty: Properties = new Properties()
    import conf._

    options.foreach { case(k,v) => connectionProperty.put(k, v) }
    connectionProperty.put("driver", "com.mysql.cj.jdbc.Driver")
    connectionProperty.put("user", username)
    connectionProperty.put("password", password)
    (spark.read.options(options), connectionProperty)
  }


  // using jdbc, spark will separate task by sql predicates, by adjust executor count and split count
  // we are able to load all data from huge table as well
  def buildPredicatesForNumberType(min: Long, max: Long, splitCount: Long, splitColumn: String): Seq[String] = {
    val realMax: Long = max + 1
    val diff: Long = realMax - min
    val chunkSize: Long = diff / splitCount

    if (chunkSize <= 500) {
      Seq(s"$splitColumn >= $min AND $splitColumn < $max")
    } else {
      var result: Seq[String] = Seq()
      for (term <- 0L until splitCount) {
        val rangeStart: Long = min + term * chunkSize
        var rangeEnd: Long = rangeStart + chunkSize
        if(term == splitCount - 1L) {
          if(rangeEnd != realMax) rangeEnd = realMax
        }
        result :+= s"$splitColumn >= $rangeStart AND $splitColumn < $rangeEnd"
      }
      result
    }
  }

  def buildPredicatesForNumberType(min: Double, max: Double, splitCount: Long, splitColumn: String): Seq[String] = {
    val realMax: Double = max + 1
    val diff: Double = realMax - min
    val chunkSize: Double = diff / splitCount

    if (chunkSize <= 500) {
      Seq(s"$splitColumn >= $min AND $splitColumn < $max")
    } else {
      var result: Seq[String] = Seq()
      for (term <- 0L until splitCount) {
        val rangeStart: Double = min + term * chunkSize
        var rangeEnd: Double = rangeStart + chunkSize
        if(term == splitCount - 1L) {
          if(rangeEnd != realMax) rangeEnd = realMax
        }
        result :+= s"$splitColumn >= $rangeStart AND $splitColumn < $rangeEnd"
      }
      result
    }
  }

  def buildPredicatesForDateType(from: Long, to: Long, splitCount: Long, splitColumn: String): Seq[String] = {
    val realTo: Long = to + 1000L
    val durationMillis: Long = realTo - from
    val chunkSize: Long = durationMillis / splitCount

    if (chunkSize <= 500) {
      Seq(s"$splitColumn >= FROM_UNIXTIME($from) AND $splitColumn < FROM_UNIXTIME($realTo)")
    } else {
      var result: Seq[String] = Seq()
      for (term <- 0L until splitCount) {
        val rangeStart: Long = from + term * chunkSize
        var rangeEnd: Long = rangeStart + chunkSize
        if(term == splitCount - 1L) {
          if(rangeEnd != realTo) rangeEnd = realTo
        }
        result :+= s"$splitColumn >= FROM_UNIXTIME($rangeStart) AND $splitColumn < FROM_UNIXTIME($rangeEnd)"
      }
      result
    }
  }
}
