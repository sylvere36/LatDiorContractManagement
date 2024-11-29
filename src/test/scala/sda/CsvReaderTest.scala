package sda.reader

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class CsvReaderTest extends AnyFunSuite {

  // Configuration SparkSession pour les tests
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("CsvReaderTest")
    .master("local[*]")
    .getOrCreate()

  // Test de lecture du fichier CSV
  test("CsvReader should read CSV file correctly") {
    // Données CSV d'exemple
    val csvPath = "src/test/resources/data.csv"

    // Création d'une instance de CsvReader
    val csvReader = CsvReader(csvPath, Some("#"), Some(true))

    // Lecture du DataFrame
    val df = csvReader.read()

    // Schéma attendu
    val expectedSchema = StructType(Seq(
      StructField("Id_Client", StringType, true),
      StructField("HTT_TVA", StringType, true),
      StructField("MetaData", StringType, true)
    ))

    // Assertions
    assert(df.schema === expectedSchema)
    assert(df.count() === 4)

    val firstRow = df.head()
    assert(firstRow.getAs[String]("Id_Client") === "1")
    assert(firstRow.getAs[String]("HTT_TVA") === "100,5|0,19")
    assert(firstRow.getAs[String]("MetaData").contains("MetaTransaction"))
  }

  // Fermeture de la SparkSession après les tests
  after {
    spark.stop()
  }
}
