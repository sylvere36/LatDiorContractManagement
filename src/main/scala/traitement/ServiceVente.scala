package sda.traitement
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {

    def formatter() = {
      dataFrame
        .withColumn("HTT_TVA_Corrected", regexp_replace(col("HTT_TVA"), ",", "."))
        .withColumn("HTT", split(col("HTT_TVA_Corrected"), "\\|").getItem(0).cast("double"))
        .withColumn("TVA", split(col("HTT_TVA_Corrected"), "\\|").getItem(1).cast("double"))
        .drop("HTT_TVA_Corrected")
    }

    def calculTTC(): DataFrame = {
      dataFrame.withColumn("TTC", round(col("HTT") + (col("HTT") * col("TVA")), 2))
        .drop("TVA", "HTT_TVA")
    }

    def extractDateEndContratVille(): DataFrame = {
      val schema_MetaTransaction = new StructType()
        .add("Ville", StringType, true)
        .add("Date_End_contrat", StringType, true)
        .add("TypeProd", StringType, true)
        .add("produit", ArrayType(StringType, true), true)

      val schema = new StructType()
        .add("MetaTransaction", ArrayType(schema_MetaTransaction), true)

      val metaDataColumn = dataFrame.schema.fields.find(_.name == "MetaData")

      val updatedDataFrame = metaDataColumn match {
        case Some(field) if field.dataType == StringType =>
          dataFrame.withColumn("MetaData", from_json(col("MetaData"), schema))
        case _ =>
          dataFrame
      }

      updatedDataFrame
        .withColumn("Date_End_contrat", expr("MetaData.MetaTransaction[0].Date_End_contrat"))
        .withColumn("Ville", expr("MetaData.MetaTransaction[0].Ville"))
        .drop("MetaData")
    }

    def contratStatus(): DataFrame = {
      dataFrame.withColumn(
        "Contrat_Status",
        when(
          to_timestamp(col("Date_End_contrat"), "yyyy-MM-dd HH:mm:ss").lt(current_timestamp()),
          "Expired"
        ).otherwise("Actif")
      )
    }
  }

}
