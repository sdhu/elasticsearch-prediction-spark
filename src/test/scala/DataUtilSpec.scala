package com.sdhu.elasticsearchprediction.spark
package test

import org.scalatest._
import com.holdenkarau.spark.testing._

class DataUtilSpec extends FlatSpec with MustMatchers with SharedSparkContext {

  "RichArrayString" should "get proper conversions" in {
    import CsvUtil._
    val a = Array("3", "1.0", "boo")
    val cm0 = Map("boo" -> 10.0)
    val cm1 = Map[String, Double]()

    a.toDoubleOpt(0) must equal(Option(3.0))
    a.toDoubleOpt(2) must equal(None)
    a.toDoubleEither(1) must equal(Left(1.0))
    a.toDoubleEither(2) must equal(Right("boo"))
    a.toDoubleArray(cm0) must equal(Array(3.0, 1.0, 10.0))
    a.toDoubleArray(cm1) must equal(Array(3.0, 1.0, 0.0))
  }

  
  "ReadUtils" should "read CSV" in {
    val cols = Set("sex","probability")
    val label = "native-country"
    val path = getClass.getResource("/mini.csv").toURI.toString 
    println(path)
   
    sc.textFile(path)

    val (rdd, cm) = ReadUtil.csv2RDD(sc, path, cols, label)
    val data = rdd.collect
    println(cm.mkString(", "))
    println(data(0).features.toArray.mkString(", "))
    println(data(0).label)
    println(data.size)
/*
    cm.keys must contain allOf ("Male", "Female", "United-States", "England")
    data.size must equal(1000)
    data(0).features.size must equal(2)
    data(0).label must equal(cm.getOrElse("United-States", 0.0)) 
  */
  }
}
