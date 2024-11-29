package sda.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.databricks.spark.xml._

case class XmlReader(path: String,
                     rowTag: Option[String] = None
                    ) extends Reader {
  val format = "com.databricks.spark.xml"

  def read()(implicit spark: SparkSession): DataFrame = {
    spark.read
      .format(format)
      .option("rowTag", rowTag.getOrElse("client"))
      .load(path)
  }
}