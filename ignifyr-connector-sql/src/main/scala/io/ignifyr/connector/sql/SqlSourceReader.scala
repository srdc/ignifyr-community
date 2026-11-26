package io.ignifyr.connector.sql

import com.typesafe.scalalogging.Logger
import io.ignifyr.engine.data.read.BaseDataSourceReader
import io.ignifyr.engine.model.exception.FhirMappingException
import io.ignifyr.engine.model.{SqlSource, SqlSourceSettings}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

/**
 *
 * @param spark
 */
class SqlSourceReader(spark: SparkSession) extends BaseDataSourceReader[SqlSource, SqlSourceSettings] {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Read the source data from relational database systems through SQL
   *
   * @param mappingSourceBinding     Configuration information for the mapping source
   * @param mappingJobSourceSettings Common settings for the source system
   * @param schema                   Schema for the source data
   * @param timeRange                Time range for the data to read if given
   * @param jobId                    The identifier of mapping job which executes the mapping
   * @return
   */
  override def read(
      mappingSourceBinding: SqlSource,
      mappingJobSourceSettings: SqlSourceSettings,
      schema: Option[StructType],
      timeRange: Option[(LocalDateTime, LocalDateTime)] = Option.empty,
      jobId: Option[String]
  ): DataFrame = {

    if (mappingSourceBinding.tableName.isDefined && mappingSourceBinding.query.isDefined) {
      throw FhirMappingException(
        s"Both table name: ${mappingSourceBinding.tableName.get} and query: ${mappingSourceBinding.query.get} should not be specified at the same time."
      )
    }
    if (mappingSourceBinding.tableName.isEmpty && mappingSourceBinding.query.isEmpty) {
      throw FhirMappingException(
        s"Both table name and query cannot be empty at the same time. One of them must be provided."
      )
    }

    // As in spark jdbc read docs, instead of a full table you could also use a sub-query in parentheses.
    val dbTable: String = mappingSourceBinding.tableName.getOrElse({
      var query: String = mappingSourceBinding.query.get
      if (timeRange.isDefined) {
        val (fromTs, toTs) = timeRange.get
        query = query.replace("$fromTs", "'" + fromTs.toString + "'").replace("$toTs", "'" + toTs.toString + "'")
      }
      s"( $query ) queryGeneratedTable"
    })

    val reader = spark.read
      .format("jdbc")
      .option("url", mappingJobSourceSettings.databaseUrl)
      .option("dbtable", dbTable)
      .option("user", mappingJobSourceSettings.username)
      .option("password", mappingJobSourceSettings.password)
      .options(mappingSourceBinding.options)

    // Apply schema via customSchema if provided
    val readerWithSchema = schema match {
      case Some(s) =>
        // Source dateTime fields are represented as strings by SchemaConverter. Reading a native
        // JDBC timestamp as STRING delegates formatting to the driver (DB2 appends ".0"). Keep
        // those fields as timestamps so the existing Row-to-JSON conversion retains its format.
        // load().schema resolves JDBC metadata; it does not collect source records.
        val jdbcSchema =
          if (s.fields.exists(_.dataType == StringType)) reader.option("customSchema", "").load().schema
          else StructType(Nil)
        reader.option(
          "customSchema",
          SqlSourceReader.toCustomSchema(s, jdbcSchema, spark.conf.get("spark.sql.caseSensitive", "false").toBoolean)
        )
      case None =>
        reader
    }
    readerWithSchema.load()
  }

}

object SqlSourceReader {

  /**
   * Converts a Spark {@link StructType} schema into a JDBC-compatible
   * {@code customSchema} definition string.
   *
   * <p>
   * Spark's JDBC data source does not have {@code schema(...)}
   * when reading from a database. Instead, it allows enforcing column types
   * at read time via the {@code customSchema} option. This method translates
   * a {@link StructType} into the format expected by that option.
   * </p>
   *
   * <p>
   * Example:
   * <pre>
   * StructType(
   * StructField("id", IntegerType),
   * StructField("value", StringType),
   * StructField("event_time", TimestampType)
   * )
   * </pre>
   *
   * will be converted to:
   *
   * <pre>
   * "id INT, value STRING, event_time TIMESTAMP"
   * </pre>
   * </p>
   *
   * @param schema the Spark {@link StructType} defining the expected column names
   *               and data types
   * @param jdbcSchema native JDBC column types, before applying the source schema
   * @param caseSensitive whether column names are resolved case-sensitively
   * @return a string suitable for Spark JDBC reads, preserving native timestamps requested as text
   */
  private[sql] def toCustomSchema(schema: StructType, jdbcSchema: StructType, caseSensitive: Boolean): String =
    schema.fields
      .map { f =>
        val nativeField = jdbcSchema.fields.find(native =>
          if (caseSensitive) native.name == f.name else native.name.equalsIgnoreCase(f.name)
        )
        val readType =
          if (f.dataType == StringType && nativeField.exists(_.dataType == TimestampType)) TimestampType
          else f.dataType
        s"${f.name} ${readType.sql}"
      }
      .mkString(", ")

}
