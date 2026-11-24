package io.ignifyr.connector.sql

import io.ignifyr.engine.model.{MappingJobSourceSettings, SqlSource, SqlSourceSettings}
import io.ignifyr.engine.spi._

import java.sql.{Connection, DatabaseMetaData, DriverManager, ResultSet, SQLException, Types}
import java.util.Locale
import scala.collection.mutable
import scala.util.Using

/**
 * Discovers the tables and views of a SQL source from the database's catalogue (JDBC `DatabaseMetaData`): their
 * columns with declared types, nullability and comments, their primary keys and their foreign keys. No statement is
 * executed on the data, so no row is read, and a user allowed to see the catalogue but not to select from a table can
 * still discover it.
 *
 * A table is named `schema.table`, or `catalog.table` for a database without schemas (e.g. MySQL), which is also the
 * table name of the [[SqlSource]] binding that reads it. The database's system schemas are left out unless asked for.
 */
class SqlSourceDiscoverer extends SourceDiscoverer {

  import SqlSourceDiscoverer._

  override val id: String = "sql"

  override val settingsClass: Class[_ <: MappingJobSourceSettings] = classOf[SqlSourceSettings]

  override def discover(settings: MappingJobSourceSettings, options: SourceDiscoveryOptions): SourceDiscovery = {
    val sql = settings.asInstanceOf[SqlSourceSettings]
    val connection =
      try DriverManager.getConnection(sql.databaseUrl, sql.username, sql.password)
      catch {
        // a driver's message can name the user or the host, never the password; still, keep only the state
        case e: SQLException =>
          throw new SourceDiscoveryException(s"Cannot connect to the database${state(e)}", e)
      }
    try Using.resource(connection)(discover(_, options))
    catch {
      case e: SQLException =>
        throw new SourceDiscoveryException(s"The database's catalogue could not be read${state(e)}", e)
    }
  }

  private def discover(connection: Connection, options: SourceDiscoveryOptions): SourceDiscovery = {
    val metaData = connection.getMetaData
    val schemas = options.databaseSchemas.map(lower).toSet
    val include = options.include.map(lower).toSet

    val all = rows(metaData.getTables(null, null, "%", null)) { rs =>
      TableRef(
        Option(rs.getString("TABLE_CAT")),
        Option(rs.getString("TABLE_SCHEM")),
        rs.getString("TABLE_NAME"),
        Option(rs.getString("TABLE_TYPE")).getOrElse(""),
        text(rs.getString("REMARKS"))
      )
    }
    val candidates = all
      .filter(t => TableTypes.contains(t.tableType.toUpperCase(Locale.ROOT)))
      .filter(t =>
        if (schemas.nonEmpty) t.schema.orElse(t.catalog).map(lower).exists(schemas.contains)
        else !t.schema.exists(isSystemSchema)
      )
      .filter(t => include.isEmpty || include.contains(lower(t.name)))
      .sortBy(t => (lower(t.name), t.name))

    val listed = candidates.take(options.maxTables)
    // a foreign key names the referenced table as the catalogue does; name it as this discovery does
    val names = all.map(t => (t.catalog, t.schema, t.table) -> t.name).toMap
    SourceDiscovery(SourceKind, listed.map(describe(metaData, _, names)), truncated = candidates.size > listed.size)
  }

  private def describe(
      metaData: DatabaseMetaData,
      table: TableRef,
      names: Map[(Option[String], Option[String], String), String]
  ): DiscoveredTable = {
    val (catalog, schema) = (table.catalog.orNull, table.schema.orNull)
    val columns = rows(metaData.getColumns(catalog, schema, table.table, "%")) { rs =>
      (
        rs.getInt("ORDINAL_POSITION"),
        DiscoveredColumn(
          name = rs.getString("COLUMN_NAME"),
          `type` = schemaType(rs.getInt("DATA_TYPE"), intOption(rs, "DECIMAL_DIGITS")),
          nativeType = Some(
            nativeType(
              rs.getString("TYPE_NAME"),
              rs.getInt("DATA_TYPE"),
              intOption(rs, "COLUMN_SIZE"),
              intOption(rs, "DECIMAL_DIGITS")
            )
          ),
          nullable = rs.getInt("NULLABLE") match {
            case DatabaseMetaData.columnNoNulls => Some(false)
            case DatabaseMetaData.columnNullable => Some(true)
            case _ => None
          },
          description = text(rs.getString("REMARKS")),
          typeOrigin = DiscoveredColumn.Declared
        )
      )
    }.sortBy(_._1).map(_._2)

    val primaryKey = rows(metaData.getPrimaryKeys(catalog, schema, table.table)) { rs =>
      (rs.getInt("KEY_SEQ"), rs.getString("COLUMN_NAME"))
    }.sortBy(_._1).map(_._2)

    val foreignKeys = rows(metaData.getImportedKeys(catalog, schema, table.table)) { rs =>
      ImportedKeyColumn(
        name = text(rs.getString("FK_NAME")),
        toTable =
          (Option(rs.getString("PKTABLE_CAT")), Option(rs.getString("PKTABLE_SCHEM")), rs.getString("PKTABLE_NAME")),
        seq = rs.getInt("KEY_SEQ"),
        column = rs.getString("FKCOLUMN_NAME"),
        toColumn = rs.getString("PKCOLUMN_NAME")
      )
    }
    DiscoveredTable(
      name = table.name,
      kind =
        if (table.tableType.toUpperCase(Locale.ROOT).contains("VIEW")) DiscoveredTable.View else DiscoveredTable.Table,
      description = table.remarks,
      binding = SqlSource(tableName = Some(table.name)),
      columns = columns,
      primaryKey = primaryKey,
      foreignKeys = groupForeignKeys(foreignKeys)
        .map { key =>
          val (cat, sch, tbl) = key.head.toTable
          DiscoveredForeignKey(
            name = key.head.name,
            columns = key.map(_.column),
            toTable = names.getOrElse((cat, sch, tbl), qualifiedName(cat, sch, tbl)),
            toColumns = key.map(_.toColumn)
          )
        }
        .sortBy(k => (k.toTable, k.columns.mkString(",")))
    )
  }
}

object SqlSourceDiscoverer {

  val SourceKind = "sql"

  /** The catalogue's table types that are listed: tables and views, as the common drivers name them. */
  val TableTypes: Set[String] = Set("TABLE", "BASE TABLE", "PARTITIONED TABLE", "VIEW", "MATERIALIZED VIEW")

  /** Schemas that hold the database's own catalogue, never data to map. */
  private val SystemSchemas: Set[String] =
    Set("information_schema", "pg_catalog", "pg_toast", "sys", "mysql", "performance_schema")

  def isSystemSchema(schema: String): Boolean = {
    val s = lower(schema)
    SystemSchemas.contains(s) || s.startsWith("pg_temp_") || s.startsWith("pg_toast_temp_")
  }

  /**
   * The schema-definition type of a JDBC type. A NUMERIC or DECIMAL without decimal digits is an integer (as an
   * Oracle `NUMBER(10)` id is); an unknown type is a string.
   */
  def schemaType(sqlType: Int, decimalDigits: Option[Int]): String = sqlType match {
    case Types.TINYINT | Types.SMALLINT | Types.INTEGER | Types.BIGINT => "integer"
    case Types.NUMERIC | Types.DECIMAL => if (decimalDigits.contains(0)) "integer" else "decimal"
    case Types.FLOAT | Types.REAL | Types.DOUBLE => "decimal"
    case Types.BOOLEAN | Types.BIT => "boolean"
    case Types.DATE => "date"
    case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE => "dateTime"
    case Types.TIME | Types.TIME_WITH_TIMEZONE => "time"
    case _ => "string"
  }

  /** The database's type with its length or precision, e.g. `varchar(20)`, `numeric(10,2)`, `int4`. */
  def nativeType(typeName: String, sqlType: Int, size: Option[Int], decimalDigits: Option[Int]): String = {
    val name = Option(typeName).getOrElse("")
    // an unbounded length is reported as a huge size
    val bounded = size.filter(s => s > 0 && s < Int.MaxValue)
    sqlType match {
      case Types.CHAR | Types.VARCHAR | Types.NCHAR | Types.NVARCHAR =>
        bounded.fold(name)(s => s"$name($s)")
      case Types.NUMERIC | Types.DECIMAL =>
        bounded.fold(name)(s => decimalDigits.fold(s"$name($s)")(d => s"$name($s,$d)"))
      case _ => name
    }
  }

  def qualifiedName(catalog: Option[String], schema: Option[String], table: String): String =
    schema.filter(_.nonEmpty).orElse(catalog.filter(_.nonEmpty)).fold(table)(prefix => s"$prefix.$table")

  /** A table as the catalogue lists it. */
  private case class TableRef(
      catalog: Option[String],
      schema: Option[String],
      table: String,
      tableType: String,
      remarks: Option[String]
  ) {
    val name: String = qualifiedName(catalog, schema, table)
  }

  /** One column of an imported (foreign) key. */
  private case class ImportedKeyColumn(
      name: Option[String],
      toTable: (Option[String], Option[String], String),
      seq: Int,
      column: String,
      toColumn: String
  )

  /**
   * The columns of each foreign key, in key order. Keys are told apart by name; an unnamed key's columns are told
   * apart by the referenced table and by the key sequence starting over.
   */
  private def groupForeignKeys(columns: Seq[ImportedKeyColumn]): Seq[Seq[ImportedKeyColumn]] = {
    val (named, unnamed) = columns.partition(_.name.isDefined)
    val byName = named.groupBy(c => (c.name, c.toTable)).values.map(_.sortBy(_.seq)).toSeq
    val anonymous = mutable.ArrayBuffer.empty[mutable.ArrayBuffer[ImportedKeyColumn]]
    unnamed.foreach { c =>
      anonymous.lastOption match {
        case Some(key) if key.last.toTable == c.toTable && c.seq > key.last.seq => key += c
        case _ => anonymous += mutable.ArrayBuffer(c)
      }
    }
    byName ++ anonymous.map(_.toSeq)
  }

  private def rows[T](resultSet: => ResultSet)(read: ResultSet => T): Seq[T] =
    Using.resource(resultSet) { rs =>
      val result = Seq.newBuilder[T]
      while (rs.next()) result += read(rs)
      result.result()
    }

  private def intOption(rs: ResultSet, column: String): Option[Int] = {
    val value = rs.getInt(column)
    if (rs.wasNull()) None else Some(value)
  }

  private def text(value: String): Option[String] = Option(value).map(_.trim).filter(_.nonEmpty)

  private def lower(s: String): String = s.toLowerCase(Locale.ROOT)

  private def state(e: SQLException): String = Option(e.getSQLState).fold("")(s => s" (SQL state $s)")
}
