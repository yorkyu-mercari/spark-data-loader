package com.kouzoh.data.loader.configs.bq

import scopt.OptionParser

case class BigQueryDestConfig(
  projectId: String,
  datasetName: String,
  temporaryGcsBucket: String,
  maybeGcpAccessToken: Option[String],
  maybeCredentialFile: Option[String],
  maybePartitionKey: Option[String],
  maybeSuffix: Option[String]
)

object BigQueryDestConfig {

  private val defaultConfig = BigQueryDestConfig(
    projectId = null,
    datasetName = null,
    temporaryGcsBucket = null,
    maybeGcpAccessToken = None,
    maybeCredentialFile = None,
    maybePartitionKey = null,
    maybeSuffix = Some("")
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
        .action((value, conf) => conf.copy(maybeGcpAccessToken = Some(value)))
        .text("gcpAccessToken")

      opt[String]("credentialFile")
        .action((value, conf) => conf.copy(maybeCredentialFile = Some(value)))
        .text("credentialFile file path, e.g. /path/to/file.json")

      opt[String]("partitionKey")
        .action((value, conf) => conf.copy(maybePartitionKey = Some(value)))
        .text("partitionKey, optional")

      opt[String]("suffix")
        .action((value, conf) => conf.copy(maybeSuffix = Some(value)))
        .text("suffix which will add to the end of table name")

    }

  def parse(args: Array[String]): BigQueryDestConfig = {
    parser
      .parse(args, defaultConfig)
      .getOrElse(throw new RuntimeException("Unable to parse config"))
  }
}
