package com.kouzoh.data.loader.configs.mysql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MysqlSourceConfigTest extends AnyFlatSpec with Matchers {
  it should "parse correctly" in {
    val args = Array(
      "--cloudSqlInstance", "aaa:ccc",
      "--credentialFile", "aa",
      "--username", "u",
      "--password", "p",
      "--dbName", "d",
      "--tableNames", "t1, t2",
      "--excludeColumns", "t1.c1, t2.c2,t2.c3, t1.c1",
      "--jdbcOptions", "a=a, b=b"
    )

    val result = MysqlSourceConfig.parse(args)

    val expected = MysqlSourceConfig(
      cloudSqlInstance = "aaa:ccc",
      credentialFile = "aa",
      username = "u",
      password = "p",
      dbName = "d",
      tableNames = Seq("t1", "t2"),
      excludeColumns = Map(
        ("t1", Set("c1")),
        ("t2", Set("c2", "c3"))
      ),
      jdbcOptions = Map(
        "a" -> "a",
        "b" -> "b"
      ),
      maybeSplitColumn = None,
      maybeSplitCount = None
    )

    result shouldBe expected
  }
}
