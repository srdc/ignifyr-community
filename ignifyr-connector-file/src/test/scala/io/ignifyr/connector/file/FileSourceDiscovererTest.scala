package io.ignifyr.connector.file

import io.ignifyr.connector.file.format.FileDescription
import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.model.{FileSystemSource, FileSystemSourceSettings}
import io.ignifyr.engine.spi._
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.StringReader
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.jdk.CollectionConverters._

/**
 * The file source discoverer on a temporary data folder: the files of each content type with their columns (from a
 * CSV or TSV header and first record, from a Parquet file's own schema), the files it cannot describe, and the
 * folder walk (sub-folders, Spark's Parquet folders, hidden entries). No Docker; Spark runs locally for Parquet.
 */
class FileSourceDiscovererTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = IgnifyrConfig.sparkSession
  private val root: Path = Files.createTempDirectory("file-discovery")
  private val settings = FileSystemSourceSettings("registry", "https://example.org/registry", root.toString)
  private val discoverer = new FileSourceDiscoverer(() => spark)

  private def write(name: String, content: String): Unit = {
    val path = root.resolve(name)
    Files.createDirectories(path.getParent)
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  override def beforeAll(): Unit = {
    // a BOM, a quoted separator, a code with a leading zero, an ISO date, a non-ISO date, a decimal and a boolean
    write(
      "patients.csv",
      "﻿pid,name,code,birth_date,visit_day,weight,active,seen_at\r\n" +
        "P-1,\"Doe, Jane\",007,1970-01-31,31/01/2024,72.5,true,2024-01-31T10:15:00Z\r\n" +
        "P-2,\"Roe, Rick\",12,1981-02-01,01/02/2024,80,false,2024-02-01 08:00\r\n"
    )
    write("labs/results.tsv", "visit_id\tcode\tvalue\n17\tGLU\t5.4\n")
    write("header-only.csv", "a,b\n")
    write("empty.csv", "")
    write("broken.csv", "a,b\n\"unclosed,2\n")
    write("events.json", """{"id": 1, "kind": "visit"}""" + "\n")
    write("notes.txt", "not a source")
    write(".hidden.csv", "x\n1\n")
    write("_staging/skip.csv", "x\n1\n")
    // a Parquet folder as Spark writes it, holding part files and a _SUCCESS marker
    import spark.implicits._
    Seq((1L, "I21.9", 12.5, java.sql.Date.valueOf("2024-01-31")))
      .toDF("visit_id", "dx", "cost", "visit_date")
      .coalesce(1)
      .write
      .parquet(root.resolve("visits.parquet").toString)
  }

  override def afterAll(): Unit =
    Files.walk(root).sorted(Comparator.reverseOrder[Path]()).iterator().asScala.foreach(Files.delete)

  private def discover(options: SourceDiscoveryOptions = SourceDiscoveryOptions()): SourceDiscovery =
    discoverer.discover(settings, options)

  private def table(discovery: SourceDiscovery, name: String): DiscoveredTable =
    discovery.tables.find(_.name == name).getOrElse(fail(s"No file $name in ${discovery.tables.map(_.name)}"))

  "The file source discoverer" should "list the files of every content type, in name order, with their bindings" in {
    val discovery = discover()
    discovery.sourceKind shouldBe "file"
    discovery.truncated shouldBe false
    // not the .txt file, the hidden file, the _-folder, nor the Parquet folder's part files
    discovery.tables.map(_.name) shouldBe Seq(
      "broken.csv",
      "empty.csv",
      "events.json",
      "header-only.csv",
      "labs/results.tsv",
      "patients.csv",
      "visits.parquet"
    )
    discovery.tables.map(_.kind).distinct shouldBe Seq("file")
    table(discovery, "labs/results.tsv").binding shouldBe FileSystemSource(path = "labs/results.tsv", contentType = "tsv")
    table(discovery, "visits.parquet").binding shouldBe FileSystemSource(path = "visits.parquet", contentType = "parquet")
    discovery.tables.flatMap(t => t.primaryKey ++ t.foreignKeys.map(_.toTable)) shouldBe empty
  }

  it should "take a CSV file's columns from its header and their types from its first record" in {
    val patients = table(discover(), "patients.csv")
    patients.unreadable shouldBe None
    patients.columns.map(c => (c.name, c.`type`)) shouldBe Seq(
      "pid" -> "string",
      "name" -> "string",
      "code" -> "string", // 007 is a code: a number would lose its zero
      "birth_date" -> "date",
      "visit_day" -> "string", // 31/01/2024 is not ISO: day and month cannot be told apart in general
      "weight" -> "decimal",
      "active" -> "boolean",
      "seen_at" -> "dateTime"
    )
    patients.columns.map(_.typeOrigin).distinct shouldBe Seq("sampled")
    patients.columns.flatMap(_.nullable) shouldBe empty
  }

  it should "read a TSV file with a tab, and a header without a record as strings" in {
    val discovery = discover()
    table(discovery, "labs/results.tsv").columns.map(c => (c.name, c.`type`)) shouldBe
      Seq("visit_id" -> "integer", "code" -> "string", "value" -> "decimal")
    table(discovery, "header-only.csv").columns.map(c => (c.name, c.`type`)) shouldBe Seq("a" -> "string", "b" -> "string")
  }

  it should "take a Parquet file's columns and types from its own schema" in {
    val visits = table(discover(), "visits.parquet")
    visits.columns.map(c => (c.name, c.`type`, c.nativeType, c.typeOrigin)) shouldBe Seq(
      ("visit_id", "integer", Some("bigint"), "declared"),
      ("dx", "string", Some("string"), "declared"),
      ("cost", "decimal", Some("double"), "declared"),
      ("visit_date", "date", Some("date"), "declared")
    )
  }

  it should "list a file it cannot describe with the reason and no column" in {
    val discovery = discover()
    table(discovery, "empty.csv").unreadable.map(_.code) shouldBe Some(DiscoveryProblem.Empty)
    table(discovery, "broken.csv").unreadable.map(_.code) shouldBe Some(DiscoveryProblem.Malformed)
    // the community edition has no JSON source format
    val events = table(discovery, "events.json")
    events.unreadable.map(_.code) shouldBe Some(DiscoveryProblem.Unsupported)
    events.columns shouldBe empty
  }

  it should "describe a headerless CSV file as Spark reads it, and honour the separator option" in {
    write("raw/no-header.csv", "1;x\n")
    val format = new io.ignifyr.connector.file.format.source.CsvSourceFormat
    val described = format.describe(
      io.ignifyr.connector.file.format.FileDescribeContext(
        root.resolve("raw/no-header.csv"),
        FileSystemSource("raw/no-header.csv", "csv", options = Map("header" -> "false", "sep" -> ";")),
        () => spark
      )
    )
    described.map(_.map(c => (c.name, c.`type`))) shouldBe Right(Seq("_c0" -> "integer", "_c1" -> "string"))
    Files.delete(root.resolve("raw/no-header.csv"))
    Files.delete(root.resolve("raw"))
  }

  it should "keep to the files asked for, whatever their case or slashes, and list at most the given number" in {
    discover(SourceDiscoveryOptions(include = Seq("PATIENTS.csv", "labs\\results.tsv", "missing.csv"))).tables
      .map(_.name) shouldBe Seq("labs/results.tsv", "patients.csv")
    val truncated = discover(SourceDiscoveryOptions(maxTables = 2))
    truncated.tables.map(_.name) shouldBe Seq("broken.csv", "empty.csv")
    truncated.truncated shouldBe true
  }

  it should "refuse a missing data folder and a Hadoop one" in {
    val missing = the[SourceDiscoveryException] thrownBy
      discoverer.discover(settings.copy(dataFolderPath = root.resolve("nowhere").toString), SourceDiscoveryOptions())
    missing.getMessage should include("does not exist")
    an[SourceDiscoveryException] should be thrownBy
      discoverer.discover(settings.copy(dataFolderPath = "hdfs://namenode/data"), SourceDiscoveryOptions())
  }

  "The record reader" should "read quoted fields, doubled quotes and line breaks in quotes, and stop after the records asked for" in {
    FileDescription.readRecords(new StringReader("a,\"b,\"\"c\"\"\nd\",e\r\nf,g\n"), ',', '"', 1) shouldBe
      Seq(Seq("a", "b,\"c\"\nd", "e"))
    FileDescription.readRecords(new StringReader("a,b\r\n\r\nc,\n"), ',', '"', 5) shouldBe
      Seq(Seq("a", "b"), Seq("c", ""))
    an[IllegalArgumentException] should be thrownBy
      FileDescription.readRecords(new StringReader("a,\"b\n"), ',', '"', 2)
  }

  "Sampled types" should "count only unambiguous forms" in {
    FileDescription.sampledType("-12") shouldBe "integer"
    FileDescription.sampledType("0") shouldBe "integer"
    FileDescription.sampledType("0012") shouldBe "string"
    FileDescription.sampledType("3.") shouldBe "decimal"
    FileDescription.sampledType("1e5") shouldBe "string"
    FileDescription.sampledType("FALSE") shouldBe "boolean"
    FileDescription.sampledType("10:30") shouldBe "time"
    FileDescription.sampledType("") shouldBe "string"
  }

  "The file connector extension" should "register the file source discoverer" in {
    ExtensionRegistry.sourceDiscoverers.get(classOf[FileSystemSourceSettings]).map(_.id) shouldBe Some("file")
  }
}
