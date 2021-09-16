package com.kouzoh.data.loader.configs.tools

import scopt.OptionParser

case class BigQueryToolConfig(
  projectId: String,
  datasetName: String,
  temporaryGcsBucket: String,
  maybeGcpAccessToken: Option[String],
  maybeCredentialFile: Option[String],
  tableNames: Seq[String]
)

object BigQueryToolConfig {

  private val defaultConfig = BigQueryToolConfig(
    projectId = null,
    datasetName = null,
    temporaryGcsBucket = null,
    maybeGcpAccessToken = None,
    maybeCredentialFile = None,
    tableNames = null
  )

  private val parser: OptionParser[BigQueryToolConfig] =
    new OptionParser[BigQueryToolConfig]("BigQueryToolConfig") {
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

      opt[String]("tableNames")
        .action((value, conf) => conf.copy(tableNames = value.split(",").map(_.trim).toSeq))
        .text("target table names, e.g. t1,t2...")
        .required()

    }

  def parse(args: Array[String]): BigQueryToolConfig = {
    parser
      .parse(args, defaultConfig)
      .getOrElse(throw new RuntimeException("Unable to parse config"))
  }
}
