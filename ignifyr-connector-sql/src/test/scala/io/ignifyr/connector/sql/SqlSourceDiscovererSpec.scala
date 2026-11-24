package io.ignifyr.connector.sql

import io.ignifyr.engine.model.{SqlSource, SqlSourceSettings}
import io.ignifyr.engine.spi._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, DriverManager}
import java.util.UUID

/**
 * The SQL source discoverer on an in-memory H2 database (no Docker): tables and views of the non-system schemas with
 * their columns, primary keys, foreign keys and comments, from the catalogue only. The same on PostgreSQL, and that no
 * row is read, is shown by `SqlSourceDiscoveryTest` in the integration tier.
 */
class SqlSourceDiscovererSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val url = s"jdbc:h2:mem:discovery-${UUID.randomUUID()};DB_CLOSE_DELAY=-1"
  private val settings = SqlSourceSettings("registry", "https://example.org/registry", url, "sa", "")

  /** Keeps the in-memory database alive for the suite. */
  private var keeper: Connection = _

  override def beforeAll(): Unit = {
    keeper = DriverManager.getConnection(url, "sa", "")
    val statement = keeper.createStatement()
    Seq(
      "CREATE SCHEMA registry",
      """CREATE TABLE registry.patients (
        |  pid VARCHAR(20) PRIMARY KEY,
        |  birth_date DATE,
        |  gender CHAR(1) NOT NULL
        |)""".stripMargin,
      "COMMENT ON TABLE registry.patients IS 'One row per registry patient'",
      "CREATE TABLE public.wards (ward_code NUMERIC(4, 0) PRIMARY KEY, title VARCHAR(50))",
      """CREATE TABLE registry.visits (
        |  visit_id INTEGER PRIMARY KEY,
        |  pid VARCHAR(20) NOT NULL,
        |  dx VARCHAR(10),
        |  weight NUMERIC(5, 2),
        |  visited_at TIMESTAMP,
        |  visit_time TIME,
        |  emergency BOOLEAN,
        |  ward_code NUMERIC(4, 0),
        |  CONSTRAINT visits_pid_fkey FOREIGN KEY (pid) REFERENCES registry.patients (pid),
        |  CONSTRAINT visits_ward_fkey FOREIGN KEY (ward_code) REFERENCES public.wards (ward_code)
        |)""".stripMargin,
      "COMMENT ON COLUMN registry.visits.dx IS 'Main diagnosis, ICD-10-CM'",
      """CREATE TABLE registry.lab_results (
        |  visit_id INTEGER NOT NULL,
        |  code VARCHAR(10) NOT NULL,
        |  result_value DOUBLE PRECISION,
        |  big_count BIGINT,
        |  PRIMARY KEY (code, visit_id),
        |  CONSTRAINT lab_results_visit_fkey FOREIGN KEY (visit_id) REFERENCES registry.visits (visit_id)
        |)""".stripMargin,
      "CREATE VIEW registry.visit_summary AS SELECT pid, COUNT(*) AS visits FROM registry.visits GROUP BY pid",
      // a row that must never be read
      "INSERT INTO registry.patients VALUES ('P-SECRET-1', DATE '1970-01-01', 'F')"
    ).foreach(statement.execute)
    statement.close()
  }

  override def afterAll(): Unit = keeper.close()

  private val discoverer = new SqlSourceDiscoverer

  private def discover(options: SourceDiscoveryOptions = SourceDiscoveryOptions()): SourceDiscovery =
    discoverer.discover(settings, options)

  private def table(discovery: SourceDiscovery, name: String): DiscoveredTable =
    discovery.tables.find(_.name == name).getOrElse(fail(s"No table $name in ${discovery.tables.map(_.name)}"))

  "The SQL source discoverer" should "list the tables and views of every non-system schema, in name order" in {
    val discovery = discover()
    discovery.sourceKind shouldBe "sql"
    discovery.truncated shouldBe false
    // H2 folds unquoted names to upper case; the names are the catalogue's
    discovery.tables.map(t => (t.name, t.kind)) shouldBe Seq(
      "PUBLIC.WARDS" -> "table",
      "REGISTRY.LAB_RESULTS" -> "table",
      "REGISTRY.PATIENTS" -> "table",
      // "_" sorts before "s"
      "REGISTRY.VISIT_SUMMARY" -> "view",
      "REGISTRY.VISITS" -> "table"
    )
    table(discovery, "REGISTRY.PATIENTS").description shouldBe Some("One row per registry patient")
    table(discovery, "REGISTRY.PATIENTS").binding shouldBe SqlSource(tableName = Some("REGISTRY.PATIENTS"))
  }

  it should "describe each column with its declared type, native type, nullability and comment, in table order" in {
    val visits = table(discover(), "REGISTRY.VISITS")
    visits.columns.map(c => (c.name, c.`type`, c.nullable)) shouldBe Seq(
      ("VISIT_ID", "integer", Some(false)),
      ("PID", "string", Some(false)),
      ("DX", "string", Some(true)),
      ("WEIGHT", "decimal", Some(true)),
      ("VISITED_AT", "dateTime", Some(true)),
      ("VISIT_TIME", "time", Some(true)),
      ("EMERGENCY", "boolean", Some(true)),
      ("WARD_CODE", "integer", Some(true))
    )
    visits.columns.map(_.typeOrigin).distinct shouldBe Seq("declared")
    val byName = visits.columns.map(c => c.name -> c).toMap
    byName("DX").description shouldBe Some("Main diagnosis, ICD-10-CM")
    byName("DX").nativeType shouldBe Some("CHARACTER VARYING(10)")
    byName("WEIGHT").nativeType shouldBe Some("NUMERIC(5,2)")
    byName("VISIT_ID").nativeType shouldBe Some("INTEGER")

    val lab = table(discover(), "REGISTRY.LAB_RESULTS").columns.map(c => c.name -> c.`type`).toMap
    lab("RESULT_VALUE") shouldBe "decimal"
    lab("BIG_COUNT") shouldBe "integer"
    table(discover(), "REGISTRY.PATIENTS").columns.find(_.name == "BIRTH_DATE").map(_.`type`) shouldBe Some("date")
  }

  it should "give the declared primary keys in key order, and none for a view" in {
    val discovery = discover()
    table(discovery, "REGISTRY.PATIENTS").primaryKey shouldBe Seq("PID")
    table(discovery, "REGISTRY.LAB_RESULTS").primaryKey shouldBe Seq("CODE", "VISIT_ID")
    table(discovery, "REGISTRY.VISIT_SUMMARY").primaryKey shouldBe empty
  }

  it should "give the declared foreign keys, naming the referenced table as the discovery does, across schemas" in {
    val discovery = discover()
    table(discovery, "REGISTRY.VISITS").foreignKeys shouldBe Seq(
      DiscoveredForeignKey(Some("VISITS_WARD_FKEY"), Seq("WARD_CODE"), "PUBLIC.WARDS", Seq("WARD_CODE")),
      DiscoveredForeignKey(Some("VISITS_PID_FKEY"), Seq("PID"), "REGISTRY.PATIENTS", Seq("PID"))
    )
    table(discovery, "REGISTRY.LAB_RESULTS").foreignKeys.map(k => (k.columns, k.toTable, k.toColumns)) shouldBe
      Seq((Seq("VISIT_ID"), "REGISTRY.VISITS", Seq("VISIT_ID")))
    table(discovery, "REGISTRY.PATIENTS").foreignKeys shouldBe empty
  }

  it should "keep to the database schemas and tables asked for, whatever their case" in {
    discover(SourceDiscoveryOptions(databaseSchemas = Seq("public"))).tables.map(_.name) shouldBe Seq("PUBLIC.WARDS")
    discover(SourceDiscoveryOptions(include = Seq("registry.visits", "Public.Wards", "registry.missing"))).tables
      .map(_.name) shouldBe Seq("PUBLIC.WARDS", "REGISTRY.VISITS")
    // the system schema only when asked for
    discover().tables.map(_.name).exists(_.startsWith("INFORMATION_SCHEMA.")) shouldBe false
    discover(SourceDiscoveryOptions(databaseSchemas = Seq("information_schema"))).tables should not be empty
  }

  it should "list at most the given number of tables, and say so" in {
    val discovery = discover(SourceDiscoveryOptions(maxTables = 2))
    discovery.tables.map(_.name) shouldBe Seq("PUBLIC.WARDS", "REGISTRY.LAB_RESULTS")
    discovery.truncated shouldBe true
  }

  it should "fail with a SourceDiscoveryException that quotes no password when the database refuses the connection" in {
    val wrong = settings.copy(password = "not-the-password-42")
    val e = the[SourceDiscoveryException] thrownBy discoverer.discover(wrong, SourceDiscoveryOptions())
    e.getMessage should startWith("Cannot connect to the database")
    e.getMessage should not include "not-the-password-42"
  }

  it should "map JDBC types to schema-definition types, and show bounded lengths only" in {
    import java.sql.Types
    SqlSourceDiscoverer.schemaType(Types.NUMERIC, Some(0)) shouldBe "integer"
    SqlSourceDiscoverer.schemaType(Types.NUMERIC, None) shouldBe "decimal"
    SqlSourceDiscoverer.schemaType(Types.TIMESTAMP_WITH_TIMEZONE, None) shouldBe "dateTime"
    SqlSourceDiscoverer.schemaType(Types.OTHER, None) shouldBe "string"
    SqlSourceDiscoverer.nativeType("varchar", Types.VARCHAR, Some(Int.MaxValue), None) shouldBe "varchar"
    SqlSourceDiscoverer.nativeType("varchar", Types.VARCHAR, Some(20), None) shouldBe "varchar(20)"
    SqlSourceDiscoverer.nativeType("int4", Types.INTEGER, Some(10), Some(0)) shouldBe "int4"
    SqlSourceDiscoverer.qualifiedName(Some("shop"), None, "orders") shouldBe "shop.orders"
    SqlSourceDiscoverer.isSystemSchema("pg_temp_3") shouldBe true
  }
}
