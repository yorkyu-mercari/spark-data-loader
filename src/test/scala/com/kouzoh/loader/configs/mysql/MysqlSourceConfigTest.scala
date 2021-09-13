package com.kouzoh.loader.configs.mysql

import com.kouzoh.data.loader.configs.mysql.MysqlSourceConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MysqlSourceConfigTest extends AnyFlatSpec with Matchers {
  it should "parse correctly" in {
    val args = Array(
      "--url", "url",
      "--port", "1",
      "--username", "u",
      "--password", "p",
      "--dbName", "d",
      "--tableNames", "t1, t2",
      "--excludeColumns", "t1.c1, t2.c2,t2.c3, t1.c1"
    )

    val result = MysqlSourceConfig.parse(args)

    val expected = MysqlSourceConfig(
      url = "url",
      port = 1,
      username = "u",
      password = "p",
      dbName = "d",
      tableNames = Seq("t1", "t2"),
      excludeColumns = Map(
        ("t1", Set("c1")),
        ("t2", Set("c2", "c3"))
      )
    )

    result shouldBe expected
  }
}
