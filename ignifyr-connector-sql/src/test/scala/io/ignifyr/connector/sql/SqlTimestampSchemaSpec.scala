package io.ignifyr.connector.sql

import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SqlTimestampSchemaSpec extends AnyFlatSpec with Matchers {

  "SQL schema enforcement" should "retain native timestamps requested as text for mapping compatibility" in {
    val requested = new StructType().add("event_time", StringType)
    val native = new StructType().add("event_time", TimestampType)
    SqlSourceReader.toCustomSchema(requested, native, caseSensitive = false) shouldBe "event_time TIMESTAMP"
  }

  it should "keep real text columns as text and enforce other requested types" in {
    val requested = new StructType().add("description", StringType).add("amount", DoubleType)
    val native = new StructType().add("description", StringType).add("amount", IntegerType)
    SqlSourceReader.toCustomSchema(requested, native, caseSensitive = false) shouldBe
      "description STRING, amount DOUBLE"
  }

  it should "match JDBC column casing according to Spark case sensitivity" in {
    val requested = new StructType().add("event_time", StringType)
    val native = new StructType().add("EVENT_TIME", TimestampType)
    SqlSourceReader.toCustomSchema(requested, native, caseSensitive = false) shouldBe "event_time TIMESTAMP"
    SqlSourceReader.toCustomSchema(requested, native, caseSensitive = true) shouldBe "event_time STRING"
  }

  it should "leave explicit timestamp requests and unrelated columns unchanged" in {
    val requested = new StructType().add("event_time", TimestampType).add("missing", StringType)
    val native = new StructType().add("event_time", TimestampType)
    SqlSourceReader.toCustomSchema(requested, native, caseSensitive = false) shouldBe
      "event_time TIMESTAMP, missing STRING"
  }
}
