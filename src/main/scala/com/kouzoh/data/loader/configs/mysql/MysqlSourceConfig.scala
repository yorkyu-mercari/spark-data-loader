package com.kouzoh.data.loader.configs.mysql

import scopt.OptionParser

case class MysqlSourceConfig(url: String,
                             port: Int,
                             username: String,
                             password: String,
                             dbName: String,
                             tableNames: Seq[String],
                             excludeColumns: Map[String, Set[String]])

object MysqlSourceConfig {

  private val defaultConfig = MysqlSourceConfig(
    url = null,
    port = -1,
    username = null,
    password = null,
    dbName = null,
    tableNames = null,
    excludeColumns = null
  )

  private val parser: OptionParser[MysqlSourceConfig] =
    new OptionParser[MysqlSourceConfig]("MysqlSourceConfig") {
      override def errorOnUnknownArgument = false

      opt[String]("url")
        .action((value, conf) => conf.copy(url = value))
        .text("url of db")
        .required()

      opt[Int]("port")
        .action((value, conf) => conf.copy(port = value))
        .text("port of db")
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
        .action((value, conf) => conf.copy(excludeColumns =
          value
            .split(",")
            .map { line =>
              val tableAndColumn = line.trim.split("\\.")
              (tableAndColumn(0), tableAndColumn(1))
            }
            .groupBy(_._1)
            .map { case (k, v) =>
              (k, v.map(_._2).toSet)
            }
        ))
        .text("columns need to be excluded, e.g. t1.c1, t2,c2, t2,c3")
        .required()

    }

  def parse(args: Array[String]): MysqlSourceConfig = {
    parser.parse(args, defaultConfig).getOrElse(throw new RuntimeException("Unable to parse config"))
  }
}