package utils

import org.apache.spark.sql.DataFrame
import testutils.SparkSpec

class EntropySpec extends SparkSpec {

  import org.apache.spark.sql.functions._
  import sparkSession.implicits._
  import utils.Entropy.DataFrameMathExt

  "Entropy function" should {

    "return entropy for geospatial data" in {

      val expectedEntropy = 16.9861

      val filePath: String = getClass.getResource("/geospatialdata.csv").getPath

      val inputDf = sparkSession.read
        .option("header", value = true)
        .csv(filePath)
        .select(concat($"LONGITUDE", lit(", "), $"LATITUDE") as "location")

      val actualEntropy = inputDf.calculateEntropy("location")
      roundDouble(actualEntropy) should beEqualTo(expectedEntropy)
    }

    "return entropy for string symbols" in {

      val expectedEntropy = 1.8464
      val inputDf: DataFrame =
        Seq(
          "1",
          "2",
          "2",
          "3",
          "3",
          "3",
          "4",
          "4",
          "4",
          "4"
        ).toDF("i")

      val actualEntropy = inputDf.calculateEntropy("i")

      roundDouble(actualEntropy) should beEqualTo(expectedEntropy)

    }
  }

  private def roundDouble(value: Double, scale: Int = 4): Double = {
    BigDecimal(value)
      .setScale(scale, BigDecimal.RoundingMode.HALF_UP)
      .toDouble
  }

}
