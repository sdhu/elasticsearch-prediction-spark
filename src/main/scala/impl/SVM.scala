package com.sdhu.elasticsearchprediction.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.linalg.Vectors

object SVM_Helper extends Linear_SparkModelHelpers[SVMModel]{
  val name = "SVM_Helper"
  
  override def sparkTrainClf(data: RDD[LabeledPoint], params: Map[String,String]): SVMModel = {
    SVMWithSGD.train(
      data,
      params.getOrElse("numIterations","3").toInt,
      params.getOrElse("stepSize","1.0").toDouble,
      params.getOrElse("regParam","0.01").toDouble,
      params.getOrElse("minBatchFraction","1.0").toDouble
      )
  }
  
  override def getOptClf(lm: LinearModelData): Option[SVMModel] = {
    Some(new SVMModel(Vectors.dense(lm.weights), lm.intercept))
  }
}


