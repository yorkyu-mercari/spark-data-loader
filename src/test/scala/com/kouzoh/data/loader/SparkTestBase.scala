package com.kouzoh.data.loader

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._

class SparkTestBase extends AnyFlatSpec with BeforeAndAfterAll {
  private var _spark: SparkSession = _

  protected def spark: SparkSession = _spark

  override def beforeAll(): Unit = {
    _spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .appName(getClass.getName)
      .getOrCreate()

    _spark.sparkContext.setLogLevel("warn")
  }

  override def afterAll(): Unit = _spark.stop()

  def schema(fields: (String, Any)*): StructType = StructType(
    fields.map { case(name, t) =>
      StructField(
        name,
        t match {
          case t: DataType => t
          case n: String => DataType.fromDDL(n)
        }
      )
    }
  )

  def rows(values: Product*): java.util.List[Row] = (values.map { tuple =>
    Row(tuple.productIterator.toSeq: _*)
  }).asJava

  def timestamp(value: String): Timestamp = Timestamp.valueOf(value)
  def date(value: String): Date = Date.valueOf(value)
}
