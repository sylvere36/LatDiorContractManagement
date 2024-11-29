package sda.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

// Classe pour lire les fichiers JSON
class JsonReader(path: String)
  extends Reader {
    val format = "json"

    def read()(implicit spark: SparkSession): DataFrame = {
      spark.read.format(format)
        .option("multiline", true)
        .load(path)
    }

    private def getMetaDataSchema() = {
      import org.apache.spark.sql.types._
      StructType(Seq(
        StructField("MetaTransaction", ArrayType(StructType(Seq(
          StructField("Ville", StringType, true),
          StructField("Date_End_contrat", StringType, true),
          StructField("TypeProd", StringType, true),
          StructField("produit", ArrayType(StringType), true)
        )), true))
      ))
    }
  }
