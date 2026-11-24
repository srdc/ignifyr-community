package io.ignifyr.engine.spi

import io.ignifyr.engine.model.{MappingJobSourceSettings, MappingSourceBinding}

/**
 * Lets a connector list what a data source holds — its tables (or files) with their columns, primary keys and foreign
 * keys — so that a user or a tool can choose what to map and see how the tables relate, before any schema exists.
 *
 * Discovery reads the source's own description of itself, never its data: a SQL connector asks the database's
 * catalogue (JDBC `DatabaseMetaData`) and reads no row. A connector contributes a discoverer keyed by its settings
 * class; callers (e.g. the server's source-discovery endpoint) look it up in [[ExtensionRegistry.sourceDiscoverers]],
 * and a source kind without one cannot be discovered.
 */
trait SourceDiscoverer {

  /** Short identifier of the providing connector, e.g. "sql" (used in error reporting). */
  def id: String

  /** The source-settings class this discoverer handles; the registry key. */
  def settingsClass: Class[_ <: MappingJobSourceSettings]

  /**
   * Discovers the tables of a source.
   *
   * @return the tables in name order, at most `options.maxTables` of them
   * @throws SourceDiscoveryException when the source cannot be reached or does not answer; its message quotes no
   *                                  credentials
   */
  def discover(settings: MappingJobSourceSettings, options: SourceDiscoveryOptions): SourceDiscovery
}

/**
 * What to discover.
 *
 * @param include         only these tables (`schema.table`) or files; empty means all. Matched without regard to case,
 *                        since databases differ in how they fold unquoted names
 * @param databaseSchemas SQL: only the tables of these database schemas, matched without regard to case; empty means
 *                        every schema but the database's system schemas
 * @param maxTables       the most tables listed; the result is marked truncated when the source has more
 */
case class SourceDiscoveryOptions(
    include: Seq[String] = Nil,
    databaseSchemas: Seq[String] = Nil,
    maxTables: Int = SourceDiscoveryOptions.DefaultMaxTables
)

object SourceDiscoveryOptions {
  val DefaultMaxTables = 1000
}

/**
 * The result of a discovery (`SourceDiscoveryResult` of the server's API).
 *
 * @param sourceKind `sql` or `file`
 * @param truncated  whether the source has more tables than were listed
 */
case class SourceDiscovery(sourceKind: String, tables: Seq[DiscoveredTable], truncated: Boolean)

/**
 * A table, view or file of a source (`DiscoveredTable`).
 *
 * @param name        SQL: `schema.table` (`catalog.table` for a database without schemas); files: the path relative to
 *                    the data folder
 * @param kind        `table`, `view` or `file`
 * @param description SQL: the table's comment
 * @param binding     the source binding that reads it in a mapping task
 * @param primaryKey  the declared primary key's columns, in key order; empty when none is declared
 * @param unreadable  a file whose columns could not be read
 */
case class DiscoveredTable(
    name: String,
    kind: String,
    description: Option[String] = None,
    binding: MappingSourceBinding,
    columns: Seq[DiscoveredColumn],
    primaryKey: Seq[String] = Nil,
    foreignKeys: Seq[DiscoveredForeignKey] = Nil,
    unreadable: Option[DiscoveryProblem] = None
)

object DiscoveredTable {
  val Table = "table"
  val View = "view"
  val File = "file"
}

/**
 * A column (`DiscoveredColumn`).
 *
 * @param `type`     as in a schema definition: string, integer, decimal, boolean, date, dateTime or time
 * @param nativeType SQL: the database's type, e.g. `varchar(20)`
 * @param nullable   SQL: whether the column is declared nullable
 * @param typeOrigin `declared` (from the source's catalogue or the file's own schema) or `sampled` (from a record)
 */
case class DiscoveredColumn(
    name: String,
    `type`: String,
    nativeType: Option[String] = None,
    nullable: Option[Boolean] = None,
    description: Option[String] = None,
    typeOrigin: String = DiscoveredColumn.Declared
)

object DiscoveredColumn {
  val Declared = "declared"
  val Sampled = "sampled"
}

/**
 * A declared foreign key (`DiscoveredForeignKey`).
 *
 * @param name    the constraint's name, when the database has one
 * @param toTable the referenced table, named as [[DiscoveredTable.name]]
 */
case class DiscoveredForeignKey(
    name: Option[String],
    columns: Seq[String],
    toTable: String,
    toColumns: Seq[String]
)

/**
 * Why a file's columns could not be read.
 *
 * @param code    `empty`, `malformed` or `unsupported`
 * @param message quotes no file content
 */
case class DiscoveryProblem(code: String, message: String)

object DiscoveryProblem {
  val Empty = "empty"
  val Malformed = "malformed"
  val Unsupported = "unsupported"
}

/** The source could not be reached or did not answer. The message quotes no credentials. */
class SourceDiscoveryException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)
