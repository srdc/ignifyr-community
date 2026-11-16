package io.ignifyr.engine.execution.processing

import io.ignifyr.engine.model.{FhirMappingErrorCodes, FhirMappingJobExecution, FhirMappingResult}
import io.ignifyr.engine.util.FileUtils.FileExtensions
import org.apache.spark.sql.functions.{col, to_json}
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.json4s.jackson.JsonMethods
import org.json4s.{JArray, JObject}

import java.io.File
import java.util
import scala.util.Try

/**
 * This class persists [[FhirMappingResult]]s that produced some error during the mapping process.
 */
object ErroneousRecordWriter {

  /**
   * Archive data sources row by row based on error types (e.g. invalid input, mapping error, invalid resource) to configured folder
   *
   * @param spark                 spark session to create dataset from not written resources
   * @param mappingJobExecution   job execution to get output directory of data sources with errors
   * @param mappingTaskName       to create directory for each mappingTask name within the job execution
   * @param notWrittenResources   created fhir resources that cannot be written to fhir server
   * @param mappingErrors         data sources that got error because of the mapping definition
   * @param invalidInputs         data sources that got error because of invalid input
   */
  def saveErroneousRecords(
      spark: SparkSession,
      mappingJobExecution: FhirMappingJobExecution,
      mappingTaskName: String,
      notWrittenResources: util.List[FhirMappingResult],
      mappingErrors: Dataset[FhirMappingResult],
      invalidInputs: Dataset[FhirMappingResult]
  ): Unit = {
    if (mappingJobExecution.saveErroneousRecords) {
      if (!invalidInputs.isEmpty) {
        this.writeErroneousDataset(
          mappingJobExecution,
          invalidInputs,
          mappingTaskName,
          FhirMappingErrorCodes.INVALID_INPUT
        )
      }
      if (!mappingErrors.isEmpty) {
        this.writeErroneousDataset(
          mappingJobExecution,
          mappingErrors,
          mappingTaskName,
          FhirMappingErrorCodes.MAPPING_ERROR
        )
      }
      if (!notWrittenResources.isEmpty) {
        import spark.implicits._
        val notWrittenResourcesDs = spark.createDataset[FhirMappingResult](notWrittenResources)
        this.writeErroneousDataset(
          mappingJobExecution,
          notWrittenResourcesDs,
          mappingTaskName,
          FhirMappingErrorCodes.INVALID_RESOURCE
        )
      }
    }
  }

  /**
   * Writes the dataset to the error output directory based on the error type and job details.
   * Each source (e.g., "mainSource") within the "source" field is extracted dynamically,
   * and its data is written into a separate subdirectory under the output path. The directory structure is as follows:
   *
   * error-folder-path\<error-type>\job-<jobId>\execution-<executionId>\<mappingTaskName>\<sourceName>\<random-generated-name-by-spark>.csv
   *
   * The rows of a joined source do not have one shape: a result carries no entry for a joined source that matched
   * nothing, an object when it matched one row and an array when it matched several, and a null column is left out
   * of the serialized row altogether. So the columns of each source are taken from all of its rows rather than from
   * one sampled result, which is what failed with FIELD_NOT_FOUND when the sample happened to lack a joined source.
   *
   * @param mappingJobExecution The job execution object, used to get the output directory of data sources with errors.
   * @param dataset             The filtered dataset of data sources with errors to be written to the configured folder.
   * @param mappingTaskName     The name of the mapping task, used to create a directory for each mapping within the job execution.
   * @param errorType           The type of error (e.g., invalid_input, mapping_error, invalid_resource).
   */
  private def writeErroneousDataset(
      mappingJobExecution: FhirMappingJobExecution,
      dataset: Dataset[FhirMappingResult],
      mappingTaskName: String,
      errorType: String
  ): Unit = {
    val outputPath = mappingJobExecution.getErrorOutputDirectory(mappingTaskName, errorType)
    val spark = dataset.sparkSession
    import spark.implicits._

    // one (source name, serialized row) pair per source row of every result
    val sourceRows: Dataset[(String, String)] = dataset.flatMap(result => extractSourceRows(result.source))
    sourceRows.cache()
    try {
      // for each source, create and write a separate CSV file
      sourceRows.map(_._1).distinct().collect().foreach { sourceKey =>
        val rowsOfSource = sourceRows.filter(_._1 == sourceKey).map(_._2)
        // The columns are the union of the columns of all rows. The values are kept as they were serialized
        // (no type inference), so the archive shows the input as the mapping saw it.
        val sourceDataset = stringifyNestedColumns(spark.read.option("primitivesAsString", "true").json(rowsOfSource))
        // rows without any column (e.g. only null values) leave nothing to write
        if (sourceDataset.columns.nonEmpty)
          // write the extracted source data as a separate CSV file into the directory for that sourceKey
          sourceDataset
            .coalesce(1)
            .write
            .mode(SaveMode.Append)
            .option("header", "true")
            .csv(s"$outputPath/$sourceKey")
      }
    } finally sourceRows.unpersist()

    // remove all files except the CSV file (to remove .crc files)
    deleteNonCsvFiles(new File(outputPath))
  }

  /**
   * Splits the serialized source of a mapping result (see [[FhirMappingResult.source]]) into its source rows.
   *
   * @param source JSON object whose keys are the source names ("mainSource" and the aliases of the joined sources)
   *               and whose values are a row (JSON object) or, for a joined source matching several rows, an array of rows
   * @return The name of the source and the serialized row, for every row; nothing if the source is not such an object
   */
  private[processing] def extractSourceRows(source: String): Seq[(String, String)] =
    Try(JsonMethods.parse(source)).toOption match {
      case Some(JObject(sources)) =>
        sources.flatMap {
          case (sourceName, row: JObject) => Seq(sourceName -> JsonMethods.compact(JsonMethods.render(row)))
          case (sourceName, JArray(rows)) =>
            rows.collect { case row: JObject => sourceName -> JsonMethods.compact(JsonMethods.render(row)) }
          case _ => Seq.empty
        }
      case _ => Seq.empty
    }

  /**
   * CSV has no representation for nested values, so the columns holding an object or an array are written as JSON text.
   */
  private def stringifyNestedColumns(df: DataFrame): DataFrame =
    df.schema.fields.foldLeft(df) { (current, field) =>
      field.dataType match {
        case _: StructType | _: ArrayType | _: MapType =>
          current.withColumn(field.name, to_json(col(s"`${field.name}`")))
        case _ => current
      }
    }

  /**
   * Recursively deletes non-CSV files (like .crc files) from the directory.
   *
   * @param dir The directory to clean up by removing non-CSV files.
   */
  private def deleteNonCsvFiles(dir: File): Unit = {
    if (dir.exists() && dir.isDirectory) {
      // get a list of files to delete, excluding CSV files
      val filesToDelete = dir
        .listFiles()
        .filterNot(f => f.getPath.endsWith(FileExtensions.CSV.toString))
      // process each file in the current directory
      filesToDelete.foreach(file => {
        if (file.isFile) {
          // delete the file if it's a regular file
          file.delete()
        } else if (file.isDirectory) {
          // if it's a subdirectory, recursively clean it up
          deleteNonCsvFiles(file)
        }
      })
    }
  }
}
