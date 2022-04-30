package etl

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Hands on ETL").getOrCreate()

    val transformer = new Transformer(spark)
    transformer.process()

  }
}
