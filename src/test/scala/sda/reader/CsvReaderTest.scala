package sda.reader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class CsvReaderTest extends AnyFunSuite with BeforeAndAfter {

  // Configuration SparkSession pour les tests
  implicit var spark: SparkSession = _

  // Initialisation de SparkSession avant chaque test
  before {
    spark = SparkSession.builder()
      .appName("CsvReaderTest")
      .master("local[*]")
      .getOrCreate()
  }

  // Test de lecture du fichier CSV
  test("CsvReader should read CSV file correctly") {
    // Données CSV d'exemple
    val csvPath = "src/main/resources/DataforTest/data.csv"

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

  // Fermeture de la SparkSession après chaque test
  after {
    if (spark != null) {
      spark.stop()
    }
  }
}
