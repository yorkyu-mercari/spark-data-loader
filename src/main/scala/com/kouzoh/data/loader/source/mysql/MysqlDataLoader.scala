package com.kouzoh.data.loader.source.mysql

import com.kouzoh.data.loader.configs.mysql.MysqlSourceConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object MysqlDataLoader {

  def load(spark: SparkSession, tableName: String, conf: MysqlSourceConfig): DataFrame = {
    import conf._
    val sourceDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", s"jdbc:mysql://$url:$port/$dbName")
      .option("dbtable", tableName)
      .option("user", username)
      .option("password", password)
      .load()

    if (conf.excludeColumns.contains(tableName)) {
      sourceDF.drop(conf.excludeColumns.getOrElse(tableName, Set.empty).toSeq: _*)
    } else {
      sourceDF
    }
  }
}
