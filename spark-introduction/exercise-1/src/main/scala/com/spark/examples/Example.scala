package com.spark.examples

import org.apache.spark.sql.SparkSession

object Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("exercise-1")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val ds = Seq(1, 2, 3, 4, 5).toDS
    ds.show

    spark.stop()
  }


}
