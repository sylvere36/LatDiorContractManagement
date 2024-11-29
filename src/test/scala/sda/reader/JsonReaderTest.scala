package sda.reader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

// Import de JsonReader

class JsonReaderTest extends AnyFunSuite with BeforeAndAfter {

  // Configuration SparkSession pour les tests
  implicit var spark: SparkSession = _

  // Initialisation de SparkSession avant chaque test
  before {
    spark = SparkSession.builder()
      .appName("JsonReaderTest")
      .master("local[*]")
      .getOrCreate()
  }

  // Test de lecture du fichier JSON
  test("JsonReader should read JSON file correctly") {
    // Données JSON d'exemple
    val jsonPath = "src/main/resources/DataforTest/data.json"

    // Création d'une instance de JsonReader
    val jsonReader = new JsonReader(jsonPath)

    // Lecture du DataFrame
    val df = jsonReader.read()

    // Schéma attendu
    val expectedSchema = StructType(Seq(
      StructField("Id_Client", StringType, true),
      StructField("HTT_TVA", StringType, true),
      StructField("MetaData", StringType, true)
    ))

    // Assertions sur le schéma
    assert(df.schema === expectedSchema)

    // Nombre de lignes attendu
    assert(df.count() === 4)

    // Vérification du contenu de la première ligne
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
