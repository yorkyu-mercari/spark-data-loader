package com.kouzoh.data.loader.source.mysql

import com.kouzoh.data.loader.configs.mysql.MysqlSourceConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object MysqlDataLoader {

  def load(spark: SparkSession, conf: MysqlSourceConfig): DataFrame = {
    import conf._
    spark.read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", s"jdbc:mysql://$url:$port/$dbName")
      .option("dbtable", tableName)
      .option("user", username)
      .option("password", password)
      .load()
  }
}
