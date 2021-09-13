package com.kouzoh.data.loader.configs.mysql

case class MysqlSourceConfig(url: String,
                             port: Int,
                             username: String,
                             password: String,
                             dbName: String,
                             tableName: String)
