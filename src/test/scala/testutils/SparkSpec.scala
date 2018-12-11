package testutils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.specs2.mutable.Specification

/**
  * Created by olha on 11/12/18
  */
trait SparkSpec extends Specification {

  sequential

  private val conf: SparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("Local Test")
    .set("spark.sql.shuffle.partitions", "4")

  val sparkSession: SparkSession =
    SparkSession.builder().config(conf).getOrCreate()
  sparkSession.sparkContext.setLogLevel("INFO")

}
