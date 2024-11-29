package sda.main
import org.apache.spark.sql.SparkSession
import sda.args._
import sda.parser.ConfigurationParser
import sda.traitement.ServiceVente._
object MainBatch {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder
      .appName("SDA")
      .config("spark.master", "local")
      .getOrCreate()
    Args.parseArguments(args)
    val reader = Args.readertype match {
      case "csv" => {
        ConfigurationParser.getCsvReaderConfigurationFromJson(Args.readerConfigurationFile)
      }
      case "json" => {
        ConfigurationParser.getJsonReaderConfigurationFromJson(Args.readerConfigurationFile)
      }
      case "xml" => {
        ConfigurationParser.getXmlReaderConfigurationFromJson(Args.readerConfigurationFile)
      }
      case _ => throw new Exception("Invalid reader type. Supported reader format : csv, json and xml in feature")
    }
    val df=reader.read().formatter()

    println("***********************Resultat Question 1*****************************")
    df.show(20)

    println("***********************Resultat Question 2*****************************")
    val dfWithTTC = df.calculTTC()
    dfWithTTC.show(20)

    println("***********************Resultat Question 3*****************************")
    val dfWithDateAndVille = dfWithTTC.extractDateEndContratVille()
    dfWithDateAndVille.show

    println("***********************Resultat Question 4*****************************")
    val dfWithStatus = dfWithDateAndVille.contratStatus()
    dfWithStatus.show(20)
  }
}