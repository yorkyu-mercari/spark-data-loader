package com.kouzoh.data.loader.dest.bq

import com.kouzoh.data.loader.configs.bq.BigQueryDestConfig
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}

object BigQueryDestination {
  def write(df: DataFrame, conf: BigQueryDestConfig): Unit = {
    import conf._

    // Workaround of local execution problem
    // https://github.com/broadinstitute/gatk/issues/4369#issuecomment-385198863
    val hadoopConf: Configuration = df.sparkSession.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.gs.project.id", projectId)
    hadoopConf.set("google.cloud.auth.service.account.json.keyfile", credentialFile.getOrElse(""))
    hadoopConf.forEach(f => {
      System.out.println(s"${f.getKey}, ${f.getValue}")
    })

    val base: DataFrameWriter[Row] = df.write
      .format("bigquery")
      .option("project", projectId)
      .option("parentProject", projectId)
      .option("temporaryGcsBucket", temporaryGcsBucket)
      .option("httpConnectTimeout", "15000")
      .option("httpReadTimeout", "15000")

    val authed: DataFrameWriter[Row] = (credentialFile, gcpAccessToken) match {
      case (Some(file), _) =>
        base.option("credentialsFile", file)
      case (None, Some(token)) =>
        base
    }

    val partitioned: DataFrameWriter[Row] = if(df.schema.fields.exists(f => f.name == "created")) {
      authed.option("partitionField", "created")
    } else {
      authed
    }

    System.out.println(s"""will write to $projectId.$datasetName.$tableName${suffix.getOrElse("")}""")
    partitioned
      .mode(SaveMode.Overwrite)
      .save(s"""$projectId.$datasetName.$tableName${suffix.getOrElse("")}""")


  }
}
