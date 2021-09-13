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


    val mysqlConf: MysqlSourceConfig = MysqlSourceConfig(
      url = "127.0.0.1",
      port = 3308,
      username = "",
      password = "",
      dbName = "contact",
      tableName = "admin_tag_layers"
    )

    val df = MysqlDataLoader.load(spark, mysqlConf)
    val dfCache = df.persist
    dfCache.count()

    val bqConf: BigQueryDestConfig = BigQueryDestConfig(
      projectId = "",
      datasetName = "",
      tableName = mysqlConf.tableName,
      temporaryGcsBucket = "",
      gcpAccessToken = None,
      credentialFile = Some(""),
      partitionKey = None,
      suffix = Some("_snapshot")
    )
    
    BigQueryDestination.write(dfCache, bqConf)
  }
}
