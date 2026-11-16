package io.ignifyr.connector.file

import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.model.{FileSystemSource, FileSystemSourceSettings, SourceContentTypes}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

/**
 * A source with a `preprocessSql` is read with Spark's type inference, because the SQL may reshape the file and the
 * provided schema describes the result of the SQL. Inference must not change the value of a column that the schema
 * declares as text: a code such as "00169750111" or "038.90" read as a number loses its zeros for good.
 */
class CsvDeclaredStringColumnsTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = IgnifyrConfig.sparkSession
  private val reader = new FileDataSourceReader(sparkSession)

  private val dataFolder: File = Files.createTempDirectory("csv-declared-strings").toFile

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.writeStringToFile(
      new File(dataFolder, "prescriptions.csv"),
      """pid,ndc,icd,amount,undeclared,active
        |1,00169750111,038.90,2.50,007,true
        |2,00002322730,250.00,10,008,false
        |""".stripMargin,
      StandardCharsets.UTF_8
    )
  }

  override protected def afterAll(): Unit = {
    FileUtils.deleteQuietly(dataFolder)
    super.afterAll()
  }

  // What the mapping declares for the source: the codes are text, the amount is a number
  private val declaredSchema = StructType(
    Seq(
      StructField("pid", IntegerType),
      StructField("ndc", StringType),
      StructField("icd", StringType),
      StructField("amount", DoubleType),
      StructField("active", StringType)
    )
  )

  private def read(
      preprocessSql: Option[String],
      options: Map[String, String] = Map.empty,
      schema: Option[StructType] = Some(declaredSchema)
  ): DataFrame =
    reader.read(
      mappingSourceBinding = FileSystemSource(
        path = "prescriptions.csv",
        contentType = SourceContentTypes.CSV,
        options = options,
        preprocessSql = preprocessSql
      ),
      mappingJobSourceSettings = FileSystemSourceSettings(
        name = "test",
        sourceUri = "urn:test",
        dataFolderPath = dataFolder.getAbsolutePath
      ),
      schema = schema
    )

  private def typeOf(df: DataFrame, column: String): DataType = df.schema(column).dataType

  private def valuesOf(df: DataFrame, column: String): Seq[String] =
    df.orderBy("pid").select(column).collect().map(_.get(0).toString).toSeq

  private val anyPreprocessSql = Some("SELECT * FROM source")

  "a csv source with a preprocessSql" should "keep the columns the schema declares as strings as they are in the file" in {
    val df = read(anyPreprocessSql)
    typeOf(df, "ndc") shouldBe StringType
    typeOf(df, "icd") shouldBe StringType
    valuesOf(df, "ndc") shouldBe Seq("00169750111", "00002322730")
    valuesOf(df, "icd") shouldBe Seq("038.90", "250.00")
  }

  it should "still infer the types of the other columns" in {
    val df = read(anyPreprocessSql)
    // declared as numbers
    typeOf(df, "pid") shouldBe IntegerType
    typeOf(df, "amount") shouldBe DoubleType
    // not in the provided schema at all (e.g. consumed by the preprocessSql)
    typeOf(df, "undeclared") shouldBe IntegerType
    // only numeric inferences are overridden
    typeOf(df, "active") shouldBe BooleanType
  }

  it should "infer every type when the source asks for the inference explicitly" in {
    val df = read(anyPreprocessSql, options = Map("inferSchema" -> "true"))
    typeOf(df, "ndc") shouldBe a[NumericType]
    typeOf(df, "icd") shouldBe DoubleType
    valuesOf(df, "icd") shouldBe Seq("38.9", "250.0")
  }

  it should "infer every type when no schema is provided" in {
    val df = read(anyPreprocessSql, schema = None)
    typeOf(df, "ndc") shouldBe a[NumericType]
  }

  "a csv source without a preprocessSql" should "be read with the provided schema as before" in {
    // without a preprocessSql the provided schema describes the file itself, column by column
    val fileSchema = StructType(
      Seq(
        StructField("pid", IntegerType),
        StructField("ndc", StringType),
        StructField("icd", StringType),
        StructField("amount", DoubleType),
        StructField("undeclared", StringType),
        StructField("active", StringType)
      )
    )
    val df = read(preprocessSql = None, schema = Some(fileSchema))
    df.schema shouldBe fileSchema
    valuesOf(df, "ndc") shouldBe Seq("00169750111", "00002322730")
    valuesOf(df, "undeclared") shouldBe Seq("007", "008")
  }
}
