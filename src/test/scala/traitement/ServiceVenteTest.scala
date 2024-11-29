package sda.traitement

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

class ServiceVenteTest extends AnyFunSuite with BeforeAndAfter {

  // Configuration SparkSession pour les tests
  implicit var spark: SparkSession = _

  // Initialisation de SparkSession avant chaque test
  before {
    spark = SparkSession.builder()
      .appName("ServiceVenteTest")
      .master("local[*]")
      .getOrCreate()
  }

  // Fermeture de la SparkSession après chaque test
  after {
    if (spark != null) {
      spark.stop()
    }
  }

  test("formatter should split HTT_TVA into HTT and TVA correctly") {
    import sda.traitement.ServiceVente._

    // Création d'un DataFrame d'exemple
    val data = Seq(
      Row("1", "100,5|0,19", "SomeMetaData"),
      Row("2", "120,546|0,20", "SomeMetaData")
    )
    val schema = StructType(Seq(
      StructField("Id_Client", StringType, true),
      StructField("HTT_TVA", StringType, true),
      StructField("MetaData", StringType, true)
    ))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Application de la fonction formatter()
    val formattedDf = df.formatter()

    // Vérification des colonnes et du contenu
    assert(formattedDf.columns.contains("HTT"))
    assert(formattedDf.columns.contains("TVA"))
    val firstRow = formattedDf.head()
    assert(firstRow.getAs[Double]("HTT") === 100.5)
    assert(firstRow.getAs[Double]("TVA") === 0.19)
  }

  test("calculTTC should calculate TTC correctly") {
    import sda.traitement.ServiceVente._

    // Création d'un DataFrame d'exemple
    val data = Seq(
      Row("1", "100,5|0,19", "SomeMetaData"),
      Row("2", "120,546|0,20", "SomeMetaData")
    )
    val schema = StructType(Seq(
      StructField("Id_Client", StringType, true),
      StructField("HTT_TVA", StringType, true),
      StructField("MetaData", StringType, true)
    ))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).formatter()

    // Application de la fonction calculTTC()
    val ttcDf = df.calculTTC()

    // Vérification des colonnes et du contenu
    assert(ttcDf.columns.contains("TTC"))
    val firstRow = ttcDf.head()
    assert(firstRow.getAs[Double]("TTC") === 119.6)
  }

  test("extractDateEndContratVille should extract Date_End_contrat and Ville correctly") {
    import sda.traitement.ServiceVente._

    // Création d'un DataFrame d'exemple
    val data = Seq(
      Row("1", "100,5|0,19", "{\"MetaTransaction\":[{\"Ville\":\"Paris\",\"Date_End_contrat\":\"2020-12-23 00:00:00\"}]}"),
      Row("2", "120,546|0,20", "{\"MetaTransaction\":[{\"Ville\":\"Alger\",\"Date_End_contrat\":\"2023-12-23 00:00:00\"}]}")
    )
    val schema = StructType(Seq(
      StructField("Id_Client", StringType, true),
      StructField("HTT_TVA", StringType, true),
      StructField("MetaData", StringType, true)
    ))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).formatter()

    // Application de la fonction extractDateEndContratVille()
    val extractedDf = df.extractDateEndContratVille()

    // Vérification des colonnes et du contenu
    assert(extractedDf.columns.contains("Date_End_contrat"))
    assert(extractedDf.columns.contains("Ville"))
    val firstRow = extractedDf.head()
    assert(firstRow.getAs[String]("Ville") === "Paris")
    assert(firstRow.getAs[String]("Date_End_contrat") === "2020-12-23 00:00:00")
  }

  test("contratStatus should add Contrat_Status column correctly") {
    import sda.traitement.ServiceVente._

    // Création d'un DataFrame d'exemple
    val data = Seq(
      Row("1", "100,5|0,19", "{\"MetaTransaction\":[{\"Ville\":\"Paris\",\"Date_End_contrat\":\"2020-12-23 00:00:00\"}]}"),
      Row("2", "120,546|0,20", "{\"MetaTransaction\":[{\"Ville\":\"Alger\",\"Date_End_contrat\":\"2023-12-23 00:00:00\"}]}")
    )
    val schema = StructType(Seq(
      StructField("Id_Client", StringType, true),
      StructField("HTT_TVA", StringType, true),
      StructField("MetaData", StringType, true)
    ))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).formatter().extractDateEndContratVille()

    // Application de la fonction contratStatus()
    val statusDf = df.contratStatus()

    // Vérification des colonnes et du contenu
    assert(statusDf.columns.contains("Contrat_Status"))
    val firstRow = statusDf.head()
    assert(firstRow.getAs[String]("Contrat_Status") === "Expired")
  }
}
