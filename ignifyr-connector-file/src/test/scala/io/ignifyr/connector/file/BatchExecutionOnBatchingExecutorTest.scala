package io.ignifyr.connector.file

import akka.actor.ActorSystem
import io.ignifyr.IgnifyrTestSpec
import io.ignifyr.engine.mapping.job.FhirMappingJobManager
import io.ignifyr.engine.model._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

/**
 * A batch execution reads and counts its source on the thread that runs it. On a batching executor (an Akka
 * dispatcher, which is what the server launches executions on) the callbacks queued behind that work on the same
 * thread wait for it, unless the work is marked as blocking. The reply to the request that launched an execution is
 * such a callback: without the marking it is sent only after the source is read, which for a big source is after the
 * request has timed out.
 */
class BatchExecutionOnBatchingExecutorTest extends AnyFlatSpec with IgnifyrTestSpec with BeforeAndAfterAll {

  private val actorSystem: ActorSystem = ActorSystem("batch-execution-on-batching-executor-test")
  // The Akka dispatcher batches the callbacks scheduled from one of its threads and runs them on that thread
  private implicit val dispatcher: ExecutionContext = actorSystem.dispatcher

  override protected def afterAll(): Unit = {
    Await.result(actorSystem.terminate(), 30.seconds)
    super.afterAll()
  }

  // The fixture has 10 patients and each one takes a while to read, so reading the source takes seconds
  private val numOfRows = 10

  "a batch execution launched from a callback on a batching executor" should
    "not keep the callbacks queued behind it waiting until its source is read" in {
      BatchExecutionOnBatchingExecutorTest.rowsRead.set(0)
      sparkSession.udf.register("slow_identity", BatchExecutionOnBatchingExecutorTest.slowIdentity)

      val slowPatientMappingTask = FhirMappingTask(
        name = "slow-patient-mapping",
        mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
        sourceBinding = Map(
          "source" -> FileSystemSource(
            path = "patients.csv",
            contentType = SourceContentTypes.CSV,
            // the SQL only filters the rows, so the schema of the mapping describes the file as it is
            options = Map("inferSchema" -> "false"),
            preprocessSql = Some("SELECT * FROM source WHERE slow_identity(pid) IS NOT NULL")
          )
        )
      )
      val job = FhirMappingJob(
        id = "batching-executor-test-job",
        sourceSettings = Map(
          "source" -> FileSystemSourceSettings(
            "test-source",
            "https://aiccelerate.eu/data-integration-suite/test-data",
            Paths.get(getClass.getResource("/test-data").toURI).normalize().toAbsolutePath.toString
          )
        ),
        // never contacted: the execution skips the write
        sinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://localhost:1/fhir"),
        mappings = Seq(slowPatientMappingTask)
      )
      val execution = FhirMappingJobExecution(job = job, mappingTasks = Seq(slowPatientMappingTask), skipWrite = true)
      val jobManager =
        new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map.empty, sparkSession)

      // What a server does: it launches the execution inside a callback and replies in callbacks chained to it
      val completion = Promise[Future[Unit]]()
      val rowsReadAtReply: Future[Int] = Future.unit
        .map { _ =>
          completion.success(jobManager.executeMappingJob(execution, job.sourceSettings, job.sinkSettings))
          ()
        }
        .map(identity)
        .map(identity)
        .map(_ => BatchExecutionOnBatchingExecutorTest.rowsRead.get())

      // The reply must not have waited for the source: it is sent before the rows are read
      Await.result(rowsReadAtReply, 60.seconds) should be < numOfRows

      // and the execution itself runs to its end
      Await.result(completion.future.flatten, 120.seconds)
      BatchExecutionOnBatchingExecutorTest.rowsRead.get() should be >= numOfRows
    }
}

object BatchExecutionOnBatchingExecutorTest {
  // Counted by the UDF, which Spark serializes into its tasks; a JVM-wide counter survives that in local mode
  val rowsRead: AtomicInteger = new AtomicInteger(0)

  private val delayPerRowInMillis = 300L

  /** Returns its input after a delay; defined here so that it does not drag the suite into the Spark task. */
  val slowIdentity: UserDefinedFunction = udf { (value: String) =>
    Thread.sleep(delayPerRowInMillis)
    rowsRead.incrementAndGet()
    value
  }.asNondeterministic()
}
