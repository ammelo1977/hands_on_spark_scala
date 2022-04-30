package etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, length, sum, udf}

class Transformer (spark:SparkSession) extends Serializable{
  val input_path = "/opt/bitnami/spark/work/bronze/testset.csv"
  val output_path = "/opt/bitnami/spark/work/silver/testset"

  def hasSmoke(x: String): Int = if (x=="Smoke") 1 else 0

  def process():Unit = {
    val rawDataDF = spark.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv(input_path)
    val columnsDroppedDF = rawDataDF.select(
      col("datetime_utc"), col(" _conds"), col(" _hum"), col(" _pressurem")
    )
    val columnsRenamedDF = columnsDroppedDF.withColumnRenamed("  datetime_utc", "datetime_utc")
      .withColumnRenamed(" _conds", "condition")
      .withColumnRenamed(" _hum", "humidity")
      .withColumnRenamed(" _pressurem", "pressure")
    val filteredDF = columnsRenamedDF
      .filter(col("datetime_utc").isNotNull.and(length(col("datetime_utc")).geq(8))
        .and(col("condition").isNotNull)
        .and(col("humidity").isNotNull).and(col("humidity").notEqual("N/A"))
        .and(col("pressure").isNotNull).and(col("pressure").notEqual("-9999")))
    val removeTime: String => String = _.substring(0, 8)
    val removeTimeUDF = udf(removeTime)
    val hasSmokeObj: String => Int = hasSmoke(_)
    val hasSmokeUDF = udf(hasSmokeObj)
    val transformedDF = filteredDF.withColumn("smoke", hasSmokeUDF(col("condition")))
      .withColumn("datetime", removeTimeUDF(col("datetime_utc")))
      .drop(col("condition")).drop(col("datetime_utc"))
    val aggregatedDF = transformedDF.groupBy("datetime").agg(
      sum("smoke").alias("hours_smoke"),
      avg("humidity").alias("avg_humidity"),
      avg("pressure").alias("avg_pressure")
    )
    aggregatedDF.write.mode("overwrite")
      .option("header", "true").option("delimiter", ";").option("compression", "gzip")
      .format("csv").save(output_path)
  }
}
