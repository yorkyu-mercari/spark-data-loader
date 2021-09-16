package com.kouzoh.data.loader

import com.kouzoh.data.loader.configs.bq.BigQueryDestConfig
import com.kouzoh.data.loader.configs.mysql.MysqlSourceConfig
import com.kouzoh.data.loader.dest.bq.BigQueryDestination
import com.kouzoh.data.loader.source.mysql.MysqlDataLoader
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Main {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Simple Application")
      .getOrCreate()

    val mysqlConf: MysqlSourceConfig = MysqlSourceConfig.parse(args)
    val bqConf: BigQueryDestConfig = BigQueryDestConfig.parse(args)


    mysqlConf.tableNames.par.foreach { table =>
      val df = MysqlDataLoader.loadSnapshot(spark, table, mysqlConf)
      // not cache here
      BigQueryDestination.write(df, table, bqConf)
    }
  }
}
