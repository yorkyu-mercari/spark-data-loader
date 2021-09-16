package com.kouzoh.data.loader.source.mysql

import com.kouzoh.data.loader.source.mysql.MysqlDataLoader.{MAX_COL, MIN_COL}
import com.kouzoh.data.loader.SparkTestBase
import org.apache.spark.sql.DataFrame
import org.scalatest.matchers.should.Matchers

class MysqlDataLoaderTest extends SparkTestBase with Matchers {

  private var testDF: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val testDF = spark.createDataFrame(
      rows(
        (1, 1L, timestamp("2021-01-01 00:00:01"), date("2021-01-01"), 0.1d, 0.1f),
        (2, 2L, timestamp("2021-01-01 00:00:02"), date("2021-01-02"), 0.2d, 0.2f),
        (3, 3L, timestamp("2021-01-01 00:00:03"), date("2021-01-03"), 0.3d, 0.3f)
      ),
      schema(
        "intField" -> "integer",
        "longField" -> "long",
        "timestampField" -> "timestamp",
        "dateField" -> "date",
        "doubleField" -> "double",
        "floatField" -> "float"
      )
    )
    testDF.createOrReplaceTempView("table")
  }

  "determineTypeAndCreatePredicates" should "determine int type" in {
    val colName: String = "intField"
    val df: DataFrame = spark.sql(
      s"""
        | SELECT MAX($colName) as $MAX_COL, MIN($colName) as $MIN_COL FROM table
        |""".stripMargin)

    val result = MysqlDataLoader.determineTypeAndCreatePredicates(df, 1, colName)
    val expected = Seq(s"$colName >= 1 AND $colName < 4")
    result shouldBe expected
  }

  "determineTypeAndCreatePredicates" should "determine long type" in {
    val colName: String = "longField"
    val df: DataFrame = spark.sql(
      s"""
         | SELECT MAX($colName) as $MAX_COL, MIN($colName) as $MIN_COL FROM table
         |""".stripMargin)

    val result = MysqlDataLoader.determineTypeAndCreatePredicates(df, 1, colName)
    val expected = Seq(s"$colName >= 1 AND $colName < 4")
    result shouldBe expected
  }

  "determineTypeAndCreatePredicates" should "determine float type" in {
    val colName: String = "floatField"
    val df: DataFrame = spark.sql(
      s"""
         | SELECT MAX($colName) as $MAX_COL, MIN($colName) as $MIN_COL FROM table
         |""".stripMargin)

    val result = MysqlDataLoader.determineTypeAndCreatePredicates(df, 1, colName)
    val expected = Seq(s"$colName >= ${"%.6f".format(0.1d)} AND $colName < ${"%.6f".format(1.3d)}")
    result shouldBe expected
  }

  "determineTypeAndCreatePredicates" should "determine double type" in {
    val colName: String = "doubleField"
    val df: DataFrame = spark.sql(
      s"""
         | SELECT MAX($colName) as $MAX_COL, MIN($colName) as $MIN_COL FROM table
         |""".stripMargin)

    val result = MysqlDataLoader.determineTypeAndCreatePredicates(df, 1, colName)
    val expected = Seq(s"$colName >= ${"%.6f".format(0.1d)} AND $colName < ${"%.6f".format(1.3d)}")
    result shouldBe expected
  }

  "determineTypeAndCreatePredicates" should "determine timestamp type" in {
    val colName: String = "timestampField"
    val df: DataFrame = spark.sql(
      s"""
         | SELECT MAX($colName) as $MAX_COL, MIN($colName) as $MIN_COL FROM table
         |""".stripMargin)

    val result = MysqlDataLoader.determineTypeAndCreatePredicates(df, 1, colName)
    val expected = Seq(s"$colName >= FROM_UNIXTIME(1609426801) AND $colName < FROM_UNIXTIME(1609427803)")
    result shouldBe expected
  }

  "determineTypeAndCreatePredicates" should "determine date type" in {
    val colName: String = "dateField"
    val df: DataFrame = spark.sql(
      s"""
         | SELECT MAX($colName) as $MAX_COL, MIN($colName) as $MIN_COL FROM table
         |""".stripMargin)

    val result = MysqlDataLoader.determineTypeAndCreatePredicates(df, 1, colName)
    val expected = Seq(s"$colName >= FROM_UNIXTIME(1609426800) AND $colName < FROM_UNIXTIME(1609600600)")
    result shouldBe expected
  }
}
