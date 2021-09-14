package com.kouzoh.data.loader.configs.bq

import scopt.OptionParser

case class BigQueryDestConfig(
  projectId: String,
  datasetName: String,
  temporaryGcsBucket: String,
  gcpAccessToken: Option[String],
  credentialFile: Option[String],
  partitionKey: Option[String],
  suffix: Option[String]
)

object BigQueryDestConfig {

  private val defaultConfig = BigQueryDestConfig(
    projectId = null,
    datasetName = null,
    temporaryGcsBucket = null,
    gcpAccessToken = None,
    credentialFile = None,
    partitionKey = null,
    suffix = Some("")
  )

  private val parser: OptionParser[BigQueryDestConfig] =
    new OptionParser[BigQueryDestConfig]("BigQueryDestConfig") {
      override def errorOnUnknownArgument = false

      opt[String]("projectId")
        .action((value, conf) => conf.copy(projectId = value))
        .text("projectId of bigquery")
        .required()

      opt[String]("datasetName")
        .action((value, conf) => conf.copy(datasetName = value))
        .text("datasetName of bigquery")
        .required()

      opt[String]("temporaryGcsBucket")
        .action((value, conf) => conf.copy(temporaryGcsBucket = value))
        .text("temporaryGcsBucket to put spark temp data")
        .required()

      opt[String]("gcpAccessToken")
        .action((value, conf) => conf.copy(gcpAccessToken = Some(value)))
        .text("gcpAccessToken")

      opt[String]("credentialFile")
        .action((value, conf) => conf.copy(credentialFile = Some(value)))
        .text("credentialFile file path, e.g. /path/to/file.json")

      opt[String]("partitionKey")
        .action((value, conf) => conf.copy(partitionKey = Some(value)))
        .text("partitionKey, optional")

      opt[String]("suffix")
        .action((value, conf) => conf.copy(suffix = Some(value)))
        .text("suffix which will add to the end of table name")

    }

  def parse(args: Array[String]): BigQueryDestConfig = {
    parser
      .parse(args, defaultConfig)
      .getOrElse(throw new RuntimeException("Unable to parse config"))
  }
}
