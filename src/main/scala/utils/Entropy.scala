package utils

import org.apache.spark.sql.DataFrame

object Entropy {

  implicit class DataFrameMathExt(df: DataFrame) {

    import df.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val probabilityColName = "p"

    def calculateEntropy(columnName: String): Double = {
      val dfSize = df.count()
      df.groupBy(columnName)
        .count()
        .withColumn(probabilityColName, $"count" / dfSize)
        .withColumn(probabilityColName, -col(probabilityColName) * log2(probabilityColName))
        .agg(sum(probabilityColName))
        .as[Double]
        .collect()
        .head
    }

  }

}
