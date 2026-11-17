package io.ignifyr.common.util

import io.onfhir.definitions.common.model.{DataTypeWithProfiles, SchemaDefinition, SimpleStructureDefinition}
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * `convertToStructureDefinitionResource` builds the StructureDefinition that the engine turns into the Spark schema of
 * a source, so the element paths it writes become the column names Spark expects in the source file.
 */
class SchemaUtilTest extends AnyFlatSpec with Matchers {

  private implicit val formats: Formats = DefaultFormats

  private def field(path: String, children: Option[Seq[SimpleStructureDefinition]] = None): SimpleStructureDefinition =
    SimpleStructureDefinition(
      id = path.split('.').last,
      path = path,
      dataTypes = Some(
        Seq(DataTypeWithProfiles(dataType = if (children.isDefined) "BackboneElement" else "string", profiles = None))
      ),
      isPrimitive = children.isEmpty,
      isChoiceRoot = false,
      isArray = children.isDefined,
      minCardinality = 0,
      maxCardinality = if (children.isDefined) None else Some(1),
      boundToValueSet = None,
      isValueSetBindingRequired = None,
      referencableProfiles = None,
      constraintDefinitions = None,
      sliceDefinition = None,
      sliceName = None,
      fixedValue = None,
      patternValue = None,
      referringTo = None,
      short = None,
      definition = None,
      comment = None,
      elements = children
    )

  private def schema(id: String, `type`: String, fields: Seq[SimpleStructureDefinition]): SchemaDefinition =
    SchemaDefinition(
      id = id,
      url = s"https://example.com/fhir/schemas/$id",
      version = SchemaDefinition.VERSION_LATEST,
      `type` = `type`,
      name = `type`,
      description = None,
      rootDefinition = None,
      fieldDefinitions = Some(fields)
    )

  private def elementPaths(schemaDefinition: SchemaDefinition): Seq[String] =
    (SchemaUtil.convertToStructureDefinitionResource(schemaDefinition, "R5") \ "differential" \ "element" \ "path")
      .extract[Seq[String]]

  "convertToStructureDefinitionResource" should "keep a column whose name starts with the schema id" in {
    val medication = schema(
      "medication",
      "Medication",
      Seq(field("medication.CODICE_SANITARIO"), field("medication.medication_name"), field("medication.medication_id"))
    )
    elementPaths(medication) shouldBe Seq(
      "Medication",
      "Medication.CODICE_SANITARIO",
      "Medication.medication_name",
      "Medication.medication_id"
    )
  }

  it should "keep a column named like the schema id or type" in {
    val nyha = schema("nyha", "Nyha", Seq(field("nyha.nyha"), field("Nyha.Nyha")))
    elementPaths(nyha) shouldBe Seq("Nyha", "Nyha.nyha", "Nyha.Nyha")
  }

  it should "prefix the paths with the type whether they start with the id, the type or neither" in {
    val patient = schema("patient", "Patient", Seq(field("patient.a"), field("Patient.b"), field("c")))
    elementPaths(patient) shouldBe Seq("Patient", "Patient.a", "Patient.b", "Patient.c")
  }

  it should "keep a nested column whose name starts with the schema id" in {
    val nlp = schema("nlp", "Nlp", Seq(field("nlp.nlp_output", Some(Seq(field("nlp.nlp_output.nlp_text"))))))
    elementPaths(nlp) shouldBe Seq("Nlp", "Nlp.nlp_output", "Nlp.nlp_output.nlp_text")
  }
}
