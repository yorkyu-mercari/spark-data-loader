package com.kouzoh.data.loader.tools

import com.kouzoh.data.loader.configs.tools.BigQueryToolConfig
import com.kouzoh.data.loader.dest.bq.BigQueryDestination
import org.apache.spark.sql.SparkSession

object EmptyCDCTableCreator {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Simple Application")
      .getOrCreate()

    val bqConf: BigQueryToolConfig = BigQueryToolConfig.parse(args)

    BigQueryDestination.createEmptyCDCTableIfNotExist(bqConf, spark)
  }

}
