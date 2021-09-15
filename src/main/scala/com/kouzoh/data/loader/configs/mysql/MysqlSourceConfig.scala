package com.kouzoh.data.loader.configs.mysql

import scopt.OptionParser

case class MysqlSourceConfig(
  cloudSqlInstance: String,
  credentialFile: String,
  username: String,
  password: String,
  dbName: String,
  tableNames: Seq[String],
  excludeColumns: Map[String, Set[String]],
  jdbcOptions: Map[String, String],
  maybeSplitColumn: Option[String],
  maybeSplitCount: Option[Long]
)

object MysqlSourceConfig {

  private val defaultConfig = MysqlSourceConfig(
    cloudSqlInstance = null,
    credentialFile = null,
    username = null,
    password = null,
    dbName = null,
    tableNames = null,
    excludeColumns = null,
    jdbcOptions = Map.empty,
    maybeSplitColumn = None,
    maybeSplitCount = None
  )

  private val parser: OptionParser[MysqlSourceConfig] =
    new OptionParser[MysqlSourceConfig]("MysqlSourceConfig") {
      override def errorOnUnknownArgument = false

      opt[String]("cloudSqlInstance")
        .action((value, conf) => conf.copy(cloudSqlInstance = value))
        .text("cloudSqlInstance of db")
        .required()

      opt[String]("credentialFile")
        .action((value, conf) => conf.copy(credentialFile = value))
        .text("credentialFile path")
        .required()

      opt[String]("username")
        .action((value, conf) => conf.copy(username = value))
        .text("username of db")
        .required()

      opt[String]("password")
        .action((value, conf) => conf.copy(password = value))
        .text("password of db")
        .required()

      opt[String]("dbName")
        .action((value, conf) => conf.copy(dbName = value))
        .text("db name")
        .required()

      opt[String]("tableNames")
        .action((value, conf) => conf.copy(tableNames = value.split(",").map(_.trim).toSeq))
        .text("target table names, e.g. t1,t2...")
        .required()

      opt[String]("excludeColumns")
        .action((value, conf) =>
          conf.copy(excludeColumns =
            value
              .split(",")
              .map { line =>
                val tableAndColumn = line.trim.split("\\.")
                (tableAndColumn(0), tableAndColumn(1))
              }
              .groupBy(_._1)
              .map {
                case (k, v) =>
                  (k, v.map(_._2).toSet)
              }
          )
        )
        .text("columns need to be excluded, e.g. t1.c1, t2.c2, t2.c3")
        .required()

      opt[String]("jdbcOptions")
        .action((value, conf) => conf.copy( jdbcOptions =
          value.split(",").map { line =>
            val kv = line.trim.split("=")
            (kv(0).trim, kv(1).trim)
          }.toMap
        ))
        .text("jdbc options, e.g. readTimeout=1000,...")

      opt[String]("splitColumn")
        .action((value, conf) => conf.copy(maybeSplitColumn = Option(value)))
        .text("column to split, e.g id")

      opt[Long]("splitCount")
        .action((value, conf) => conf.copy(maybeSplitCount = Option(value)))
        .text("count number you want to split, e.g. 1000")

    }

  def parse(args: Array[String]): MysqlSourceConfig = {
    parser
      .parse(args, defaultConfig)
      .getOrElse(throw new RuntimeException("Unable to parse config"))
  }
}
