package io.ignifyr.test.engine.execution

import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.execution.processing.ErroneousRecordWriter
import io.ignifyr.engine.model._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import java.time.Instant
import java.util.Collections

/**
 * The erroneous records of a mapping task are archived per source. With joined sources the serialized results do
 * not share one shape - a joined source is absent when it matched nothing, an object when it matched one row and an
 * array when it matched several, and null columns are left out - so the archive must be built from all results,
 * not from the shape of one of them.
 */
class ErroneousRecordWriterTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val sparkSession: SparkSession = IgnifyrConfig.sparkSession

  private val execution: FhirMappingJobExecution = FhirMappingJobExecution(
    id = "erroneous-records-execution",
    job = FhirMappingJob(
      id = "erroneous-records-job",
      sourceSettings = Map.empty,
      sinkSettings = FileSystemSinkSettings("./out", SinkContentTypes.NDJSON),
      mappings = Seq.empty,
      dataProcessingSettings = DataProcessingSettings(saveErroneousRecords = true)
    )
  )

  private val taskName = "patient-mapping"

  private def outputFolder(errorType: String): File = new File(execution.getErrorOutputDirectory(taskName, errorType))

  override protected def afterEach(): Unit = {
    Seq(
      FhirMappingErrorCodes.INVALID_INPUT,
      FhirMappingErrorCodes.MAPPING_ERROR,
      FhirMappingErrorCodes.INVALID_RESOURCE
    )
      .foreach(errorType => FileUtils.deleteQuietly(outputFolder(errorType)))
    super.afterEach()
  }

  private def failed(source: String, errorCode: String): FhirMappingResult =
    FhirMappingResult(
      jobId = execution.jobId,
      mappingTaskName = taskName,
      timestamp = Timestamp.from(Instant.now()),
      source = source,
      error = Some(FhirMappingError(errorCode, "test error"))
    )

  private def save(mappingErrors: Seq[FhirMappingResult], invalidInputs: Seq[FhirMappingResult] = Seq.empty): Unit = {
    import sparkSession.implicits._
    ErroneousRecordWriter.saveErroneousRecords(
      sparkSession,
      execution,
      taskName,
      Collections.emptyList[FhirMappingResult](),
      mappingErrors.toDS(),
      invalidInputs.toDS()
    )
  }

  private def readArchive(errorType: String, sourceName: String): DataFrame =
    sparkSession.read.option("header", "true").csv(new File(outputFolder(errorType), sourceName).getPath)

  "saveErroneousRecords" should "archive the rows of a joined source that only some results carry" in {
    save(
      mappingErrors = Seq(
        // the joined source matched nothing: the result has no entry for it
        failed("""{"mainSource":{"pid":"p1","gender":"male"}}""", FhirMappingErrorCodes.MAPPING_ERROR),
        // the joined source matched one row: an object
        failed(
          """{"mainSource":{"pid":"p2","gender":"female"},"admissions":{"pid":"p2","hadm":"a1"}}""",
          FhirMappingErrorCodes.MAPPING_ERROR
        ),
        // the joined source matched several rows: an array
        failed(
          """{"mainSource":{"pid":"p3","gender":"male"},"admissions":[{"pid":"p3","hadm":"a2"},{"pid":"p3","hadm":"a3"}]}""",
          FhirMappingErrorCodes.MAPPING_ERROR
        )
      )
    )

    val mainSource = readArchive(FhirMappingErrorCodes.MAPPING_ERROR, "mainSource")
    mainSource.select("pid").collect().map(_.getString(0)).toSet shouldBe Set("p1", "p2", "p3")

    val admissions = readArchive(FhirMappingErrorCodes.MAPPING_ERROR, "admissions")
    admissions.select("hadm").collect().map(_.getString(0)).toSet shouldBe Set("a1", "a2", "a3")
  }

  it should "keep a column that is null, and therefore missing, in some of the rows" in {
    save(
      mappingErrors = Seq.empty,
      invalidInputs = Seq(
        failed("""{"mainSource":{"pid":"p1"}}""", FhirMappingErrorCodes.INVALID_INPUT),
        failed("""{"mainSource":{"pid":"p2","deathDate":"2020-01-01"}}""", FhirMappingErrorCodes.INVALID_INPUT)
      )
    )

    val mainSource = readArchive(FhirMappingErrorCodes.INVALID_INPUT, "mainSource")
    mainSource.columns.toSet shouldBe Set("pid", "deathDate")
    mainSource.collect().map(row => row.getAs[String]("pid") -> row.getAs[String]("deathDate")).toSet shouldBe
      Set("p1" -> null, "p2" -> "2020-01-01")
  }

  it should "archive the values as they were serialized, without re-typing them" in {
    save(mappingErrors =
      Seq(
        failed("""{"mainSource":{"code":"00123","count":7,"nested":{"a":1}}}""", FhirMappingErrorCodes.MAPPING_ERROR)
      )
    )

    val row = readArchive(FhirMappingErrorCodes.MAPPING_ERROR, "mainSource").collect().head
    row.getAs[String]("code") shouldBe "00123"
    row.getAs[String]("count") shouldBe "7"
    // a nested value has no CSV representation and is kept as JSON text
    row.getAs[String]("nested") shouldBe """{"a":"1"}"""
  }

  it should "write nothing when the execution does not save erroneous records" in {
    import sparkSession.implicits._
    val notSaving = execution.copy(saveErroneousRecords = false)
    ErroneousRecordWriter.saveErroneousRecords(
      sparkSession,
      notSaving,
      taskName,
      Collections.emptyList[FhirMappingResult](),
      Seq(failed("""{"mainSource":{"pid":"p1"}}""", FhirMappingErrorCodes.MAPPING_ERROR)).toDS(),
      sparkSession.emptyDataset[FhirMappingResult]
    )
    outputFolder(FhirMappingErrorCodes.MAPPING_ERROR).exists() shouldBe false
  }
}
