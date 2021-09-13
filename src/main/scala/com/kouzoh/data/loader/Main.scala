package com.kouzoh.data.loader

import com.kouzoh.data.loader.configs.bq.BigQueryDestConfig
import com.kouzoh.data.loader.configs.mysql.MysqlSourceConfig
import com.kouzoh.data.loader.dest.bq.BigQueryDestination
import com.kouzoh.data.loader.source.mysql.MysqlDataLoader
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Simple Application")
      .getOrCreate()
    loadSnapshot(spark)
  }

  def loadSnapshot(spark: SparkSession): Unit = {

    val tables: Seq[String] = Seq()

    val mysqlConf: MysqlSourceConfig = MysqlSourceConfig(
      url = "127.0.0.1",
      port = 3308,
      username = "",
      password = "",
      dbName = "contact",
      tableNames = tables,
      excludeColumns = Map[String, Set[String]](
      )
    )

    val bqConf: BigQueryDestConfig = BigQueryDestConfig(
      projectId = "",
      datasetName = "",
      temporaryGcsBucket = "",
      gcpAccessToken = None,
      credentialFile = Some(""),
      partitionKey = None,
      suffix = Some("_snapshot")
    )

    tables.foreach{ table =>
      val df = MysqlDataLoader.load(spark, table, mysqlConf)
      val dfCache = df.persist
      dfCache.count()
      BigQueryDestination.write(dfCache, table, bqConf)
    }


  }
}
