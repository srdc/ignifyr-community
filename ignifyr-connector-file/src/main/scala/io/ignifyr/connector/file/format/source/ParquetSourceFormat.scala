package io.ignifyr.connector.file.format.source

import io.ignifyr.connector.file.format.{FileDescribeContext, FileDescription, FileSourceFormat, FileSourceReadContext}
import io.ignifyr.engine.model.SourceContentTypes
import io.ignifyr.engine.spi.{DiscoveredColumn, DiscoveryProblem}
import org.apache.spark.sql.DataFrame

/**
 * Community Parquet file source format. (No zip handling - matching the original reader.)
 */
class ParquetSourceFormat extends FileSourceFormat {

  override val contentTypes: Seq[String] = Seq(SourceContentTypes.PARQUET)

  /** The columns of a Parquet file (or of a folder of them, as Spark writes it) from its own schema: its footer. */
  override def describe(ctx: FileDescribeContext): Either[DiscoveryProblem, Seq[DiscoveredColumn]] = {
    val schema = ctx.spark().read.options(ctx.mappingSourceBinding.options).parquet(ctx.path.toString).schema
    if (schema.isEmpty) Left(DiscoveryProblem(DiscoveryProblem.Empty, "The file declares no column"))
    else Right(FileDescription.declaredColumns(schema))
  }

  override def read(ctx: FileSourceReadContext): DataFrame = {
    if (ctx.mappingJobSourceSettings.asStream) {
      ctx.spark.readStream
        .options(ctx.mappingSourceBinding.options)
        .schema(ctx.schema.orNull)
        .parquet(ctx.finalPath)
    } else {
      ctx.spark.read.options(ctx.mappingSourceBinding.options).schema(ctx.schema.orNull).parquet(ctx.finalPath)
    }
  }
}
