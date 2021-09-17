package com.kouzoh.data.loader.dest.bq

import com.kouzoh.data.loader.configs.bq.BigQueryDestConfig
import com.kouzoh.data.loader.configs.tools.BigQueryToolConfig
import com.kouzoh.data.loader.utils.CommonLogger
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode, SparkSession}

object BigQueryDestination extends CommonLogger {
  def write(df: DataFrame, tableName: String, conf: BigQueryDestConfig): Unit = {
    import conf._

    maybeCredentialFile.foreach { credentialFile =>
      // Workaround of local execution problem
      // https://github.com/broadinstitute/gatk/issues/4369#issuecomment-385198863
      val hadoopConf: Configuration = df.sparkSession.sparkContext.hadoopConfiguration
      hadoopConf.set("fs.gs.project.id", projectId)
      hadoopConf.set("google.cloud.auth.service.account.json.keyfile", credentialFile)
    }

    val base: DataFrameWriter[Row] = df.write
      .format("bigquery")
      .option("project", projectId)
      .option("parentProject", projectId)
      .option("temporaryGcsBucket", s"$temporaryGcsBucket/test/$tableName")
      .option("httpConnectTimeout", "15000")
      .option("httpReadTimeout", "15000")

    val authed: DataFrameWriter[Row] = (maybeCredentialFile, maybeGcpAccessToken) match {
      case (Some(file), _) =>
        base.option("credentialsFile", file)
      case (None, Some(token)) =>
        base
      case _ =>
        base
    }

    val partitioned: DataFrameWriter[Row] = if (df.schema.fields.exists(f => f.name == "created")) {
      authed.option("partitionField", "created")
    } else {
      authed
    }

    log.info(
      s"""will write to $projectId.$datasetName.$tableName${maybeSuffix.getOrElse("")}"""
    )
    partitioned
      .mode(SaveMode.Overwrite)
      .save(s"""$projectId.$datasetName.$tableName${maybeSuffix.getOrElse("")}""")
  }

  def createEmptyCDCTableIfNotExist(config: BigQueryToolConfig, spark: SparkSession): Unit = {
    import config._

    maybeCredentialFile.foreach { credentialFile =>
      // Workaround of local execution problem
      // https://github.com/broadinstitute/gatk/issues/4369#issuecomment-385198863
      val hadoopConf: Configuration = spark.sparkContext.hadoopConfiguration
      hadoopConf.set("fs.gs.project.id", projectId)
      hadoopConf.set("google.cloud.auth.service.account.json.keyfile", credentialFile)
    }

    val schema: StructType = new StructType(
      Array(
        StructField("key", StringType),
        StructField("before", StringType),
        StructField("after", StringType),
        StructField("op", StringType),
        StructField("ts_ms", TimestampType),
        StructField("transaction", StringType),
        StructField("database_name", StringType),
        StructField("ddl", StringType),
        StructField(
          "source",
          new StructType(
            Array(
              StructField("version", StringType),
              StructField("connector", StringType),
              StructField("name", StringType),
              StructField("ts_ms", TimestampType),
              StructField("snapshot", StringType),
              StructField("db", StringType),
              StructField("table", StringType),
              StructField("server_id", LongType),
              StructField("gtid", StringType),
              StructField("file", StringType),
              StructField("pos", LongType),
              StructField("row", LongType),
              StructField("thread", LongType),
              StructField("query", StringType)
            )
          )
        )
      )
    )

    val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    tableNames.par.foreach { tableName =>
      emptyDF.write
        .format("bigquery")
        .option("project", projectId)
        .option("parentProject", projectId)
        .option("temporaryGcsBucket", s"$temporaryGcsBucket/test2/$tableName")
        .option("httpConnectTimeout", "15000")
        .option("httpReadTimeout", "15000")
        .option("partitionType", "DAY")
        .mode(SaveMode.Ignore)
        .save(s"""$projectId.$datasetName.$tableName""")
    }
  }
}
