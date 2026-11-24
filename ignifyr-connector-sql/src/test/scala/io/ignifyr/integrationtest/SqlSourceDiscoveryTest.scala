package io.ignifyr.integrationtest

import io.ignifyr.connector.sql.SqlSourceDiscoverer
import io.ignifyr.engine.model.{SqlSource, SqlSourceSettings}
import io.ignifyr.engine.spi.{DiscoveredForeignKey, SourceDiscovery, SourceDiscoveryOptions}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

import java.sql.{DriverManager, SQLException}
import scala.util.Using

/**
 * The SQL source discoverer on a real PostgreSQL (in a container): tables, views and materialized views with their
 * columns, PostgreSQL's native types, comments, primary keys and foreign keys (a composite one included).
 *
 * Discovery runs as a user that may connect but not select from any table, which shows that it reads the catalogue
 * only and never a row.
 */
class SqlSourceDiscoveryTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val postgres: PostgreSQLContainer[_] = {
    val container = new PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"))
    container.start()
    container
  }

  private val readerPassword = "catalogue-only-7"

  override def beforeAll(): Unit =
    Using.resource(DriverManager.getConnection(postgres.getJdbcUrl, postgres.getUsername, postgres.getPassword)) {
      connection =>
        val statement = connection.createStatement()
        Seq(
          "CREATE SCHEMA registry",
          """CREATE TABLE registry.patients (
            |  pid varchar(20) PRIMARY KEY,
            |  birth_date date,
            |  notes text
            |)""".stripMargin,
          "COMMENT ON TABLE registry.patients IS 'One row per registry patient'",
          """CREATE TABLE registry.visits (
            |  visit_id serial PRIMARY KEY,
            |  pid varchar(20) NOT NULL REFERENCES registry.patients (pid),
            |  dx varchar(10),
            |  weight numeric(5, 2),
            |  visited_at timestamptz,
            |  emergency boolean,
            |  UNIQUE (visit_id, pid)
            |)""".stripMargin,
          "COMMENT ON COLUMN registry.visits.dx IS 'Main diagnosis, ICD-10-CM'",
          """CREATE TABLE public.procedures (
            |  visit_id integer NOT NULL,
            |  pid varchar(20) NOT NULL,
            |  seq smallint NOT NULL,
            |  code varchar(10),
            |  PRIMARY KEY (visit_id, seq),
            |  CONSTRAINT procedures_visit_fkey FOREIGN KEY (visit_id, pid) REFERENCES registry.visits (visit_id, pid)
            |)""".stripMargin,
          "CREATE VIEW registry.visit_counts AS SELECT pid, count(*) AS visits FROM registry.visits GROUP BY pid",
          "CREATE MATERIALIZED VIEW registry.dx_counts AS SELECT dx, count(*) AS n FROM registry.visits GROUP BY dx",
          "INSERT INTO registry.patients VALUES ('P-SECRET-1', DATE '1970-01-01', 'never to be read')",
          // a user that may connect, but not select from any table
          s"CREATE USER catalogue_reader PASSWORD '$readerPassword'",
          "REVOKE ALL ON ALL TABLES IN SCHEMA registry, public FROM PUBLIC, catalogue_reader"
        ).foreach(statement.execute)
        statement.close()
    }

  override def afterAll(): Unit = postgres.stop()

  private def settings(user: String = "catalogue_reader", password: String = readerPassword) =
    SqlSourceSettings("registry", "https://example.org/registry", postgres.getJdbcUrl, user, password)

  private def discover(options: SourceDiscoveryOptions = SourceDiscoveryOptions()): SourceDiscovery =
    new SqlSourceDiscoverer().discover(settings(), options)

  "The SQL source discoverer" should "discover a PostgreSQL database as a user that may not read a single row" in {
    // the user cannot read the data
    Using.resource(DriverManager.getConnection(postgres.getJdbcUrl, "catalogue_reader", readerPassword)) { c =>
      an[SQLException] should be thrownBy c.createStatement().executeQuery("SELECT * FROM registry.patients")
    }
    val discovery = discover()
    discovery.tables.map(t => (t.name, t.kind)) shouldBe Seq(
      "public.procedures" -> "table",
      "registry.dx_counts" -> "view",
      "registry.patients" -> "table",
      "registry.visit_counts" -> "view",
      "registry.visits" -> "table"
    )
    discovery.truncated shouldBe false
    discovery.tables.find(_.name == "registry.patients").get.binding shouldBe
      SqlSource(tableName = Some("registry.patients"))
  }

  it should "give PostgreSQL's native types, the schema-definition types, nullability and comments" in {
    val discovery = discover()
    val patients = discovery.tables.find(_.name == "registry.patients").get
    patients.description shouldBe Some("One row per registry patient")
    patients.columns.map(c => (c.name, c.`type`, c.nativeType)) shouldBe Seq(
      ("pid", "string", Some("varchar(20)")),
      ("birth_date", "date", Some("date")),
      ("notes", "string", Some("text"))
    )
    val visits = discovery.tables.find(_.name == "registry.visits").get
    visits.columns.map(c => (c.name, c.`type`, c.nativeType, c.nullable)) shouldBe Seq(
      ("visit_id", "integer", Some("serial"), Some(false)),
      ("pid", "string", Some("varchar(20)"), Some(false)),
      ("dx", "string", Some("varchar(10)"), Some(true)),
      ("weight", "decimal", Some("numeric(5,2)"), Some(true)),
      ("visited_at", "dateTime", Some("timestamptz"), Some(true)),
      ("emergency", "boolean", Some("bool"), Some(true))
    )
    visits.columns.find(_.name == "dx").flatMap(_.description) shouldBe Some("Main diagnosis, ICD-10-CM")
  }

  it should "give the primary keys in key order and the foreign keys, a composite one across schemas included" in {
    val discovery = discover()
    val procedures = discovery.tables.find(_.name == "public.procedures").get
    procedures.primaryKey shouldBe Seq("visit_id", "seq")
    procedures.foreignKeys shouldBe Seq(
      DiscoveredForeignKey(
        Some("procedures_visit_fkey"),
        Seq("visit_id", "pid"),
        "registry.visits",
        Seq("visit_id", "pid")
      )
    )
    discovery.tables
      .find(_.name == "registry.visits")
      .get
      .foreignKeys
      .map(k => (k.columns, k.toTable, k.toColumns)) shouldBe
      Seq((Seq("pid"), "registry.patients", Seq("pid")))
  }

  it should "keep to the schemas asked for, and leave the system schemas out" in {
    discover(SourceDiscoveryOptions(databaseSchemas = Seq("PUBLIC"))).tables.map(_.name) shouldBe Seq(
      "public.procedures"
    )
    discover().tables
      .map(_.name)
      .filter(n => n.startsWith("pg_catalog.") || n.startsWith("information_schema.")) shouldBe empty
  }
}
