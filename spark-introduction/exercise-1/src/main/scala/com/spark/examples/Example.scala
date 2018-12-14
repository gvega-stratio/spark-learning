package com.spark.examples

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("exercise-1")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val data: RDD[String] = spark.sparkContext.textFile(path = s"file://${args(0)}")
    if (args.length != 1) spark.stop()

    println("#################### RDD ####################")
    val countLines: Long = data.count()
    val countCharacters: Long = data.flatMap(v => v.toCharArray).count()

    println(s"N. Lineas: ${countLines}")
    println(s"N. Caracteres: ${countCharacters}")
    println(s"Primeras lineas: ")

    val firstLines: Array[String] = data.take(5)
    firstLines.foreach(println)

    println(s"Primeras lineas: ")
    val tupleRDD: RDD[(String, String, String, String)] = data.map(v => v.split("\\|")).map(x => (x(0), x(1), x(2), x(3)))
    tupleRDD.take(5).foreach(println)

    val countLogsCountry: Array[(String, Int)] = tupleRDD.map(v => (v._3,1)).groupByKey().map(v => (v._1,(v._2).sum)).map(v => (v._2,v._1)).top(3).map(v => (v._2,v._1))
    val countLogsCountry2: Array[(String, Int)] = tupleRDD.map(v => (v._3,1)).reduceByKey((v, v1) => v+v1).map(v => (v._2,v._1)).top(3).map(v => (v._2,v._1))
    println("N. Logs por ciudad (1): ")
    countLogsCountry.foreach(println)
    println("N. Logs por ciudad (2): ")
    countLogsCountry2.foreach(println)

    val countLogsType: RDD[(String, Int)] = tupleRDD.map(v => (v._4,1)).groupByKey().map(v => (v._1,(v._2).sum))
    val countLogsType2: RDD[(String, Int)] = tupleRDD.map(v => (v._4,1)).reduceByKey((v, v1) => v+v1)
    println("N. Errores y Aciertos (1): ")
    countLogsType.foreach(println)
    println("N. Errores y Aciertos (2): ")
    countLogsType2.foreach(println)

    val startErrorDate: Array[(String, String)] = tupleRDD.filter(v => v._4 == "ERROR").top(1).map(v => (v._1,v._3))
    println("Fecha de primer error y ciudad: ")
    startErrorDate.foreach(println)

    val mostErrorsCountry: Array[(String, Int)] = tupleRDD.filter(v => v._4 == "ERROR").map(v => (v._3,1)).groupByKey().map(v => (v._1,(v._2).sum)).map(v => (v._2,v._1)).top(1).map(v => (v._2,v._1))
    println("La ciudad con más errores es: ")
    mostErrorsCountry.foreach(println)

    println("#############################################")

    println("################### DF/DS ###################")

    

    println("#############################################")

    //    val ds: Dataset[Int] = Seq(1, 2, 3, 4, 5).toDS
    //    ds.show

    spark.stop()
  }


}
