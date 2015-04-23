package com.sdhu.elasticsearchprediction.spark
package test

import com.mahisoft.elasticsearchprediction._
import utils.DataProperties
import plugin.domain.IndexValue
import plugin.exception.PredictionException
import plugin.engine.PredictorEngine

import org.apache.spark._
import rdd.RDD
import mllib.regression._
import mllib.classification._

import org.scalatest._
import com.holdenkarau.spark.testing._
import java.io.File
import java.util.Collection

import scala.collection.JavaConversions._


class SparkPredictorEngineSpec extends FlatSpec with MustMatchers {
  val pconf = getClass.getResource("/prop1.conf").getPath
  val dataP = getClass.getResource("/mini.csv").toURI.toString
  val dp = new DataProperties(pconf)
  val modelP = getClass.getResource("/spark-clf-test.model").getPath 
  val clf_type = "spark.logistic-regression"

  "Predictor Engine" should "throw empty model exception" in {
    val eng = new SparkPredictorEngine(modelP, SVM_Helper)
    evaluating {eng.getPrediction(List[IndexValue]())} must produce [PredictionException]
  }

//  "Spark_PredictorEngine" should "return sparkPredictorEngine of svm type" in {
//    val speng = new Spark_PredictorEngine(modelP, "spark.svm")
//    speng.getSparkPredictorEngine mustBe a [SparkPredictorEngine[_]]
//    
//  }

  it should "return a generic PredictorEngine" in {
    val speng = new Spark_PredictorEngine(modelP, "spark.svm")
    speng.getPredictorEngine mustBe a [PredictorEngine]
  }

  it should "load the classifier" in {
    val speng = new Spark_PredictorEngine(modelP, clf_type)
    val eng = speng.getSparkPredictorEngine
    val m = eng.getModel
    val cm = m.categoriesMap.getOrElse(Map[String, Double]())

    m.clf must not be empty
    //m.numClasses must be(Some(2))
    //m.binThreshold must be(Some(0.5))
    cm.keys must contain allOf("Female", "Male", "United-States", "China")
  }

  it should "evaluate values" in { 
    val speng = new Spark_PredictorEngine(modelP, clf_type)
    val eng = speng.getSparkPredictorEngine

    val p0 = Array("50", "Self-emp-not-inc", "Male", "0", "0", "United-States")
    val cindv = ReadUtil.arr2CIndVal(p0)
    
    val check = eng.getPrediction(cindv) 
    
    check must equal(0.0)
    check mustBe a [java.lang.Double]
  }
  
  it should "evaluate values using generic Predictor engine" in { 
    val speng = new Spark_PredictorEngine(modelP, clf_type)
    val eng = speng.getPredictorEngine

    val p0 = Array("50", "Self-emp-not-inc", "Male", "0", "0", "United-States")
    val cindv = ReadUtil.arr2CIndVal(p0)
    
    val check = eng.getPrediction(cindv) 
    
    check must equal(0.0)
    check mustBe a [java.lang.Double]
  }
}
