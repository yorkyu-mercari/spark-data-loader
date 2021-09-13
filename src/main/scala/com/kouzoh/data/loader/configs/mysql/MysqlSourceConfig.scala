package com.kouzoh.data.loader.configs.mysql

case class MysqlSourceConfig(url: String,
                             port: Int,
                             username: String,
                             password: String,
                             dbName: String,
                             tableNames: Seq[String],
                             // should be like ("tableName", set["columnA", "columnB"])
                             excludeColumns: Map[String, Set[String]])
