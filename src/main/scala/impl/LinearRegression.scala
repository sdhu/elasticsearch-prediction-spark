package com.sdhu.elasticsearchprediction.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.linalg.Vectors

object LinearRegression_Helper extends Linear_SparkModelHelpers[LinearRegressionModel]{
  val name = "LinearRegression_Helper"

  override def sparkTrainClf(data: RDD[LabeledPoint], params: Map[String,String]): LinearRegressionModel = {
    LinearRegressionWithSGD.train(
      data,
      params.getOrElse("numIterations","3").toInt,
      params.getOrElse("stepSize","1.0").toDouble,
      params.getOrElse("minBatchFraction","1.0").toDouble
      )
  }
  
  override def getOptClf(lm: LinearModelData): Option[LinearRegressionModel] = {
    Some(new LinearRegressionModel(Vectors.dense(lm.weights), lm.intercept))
  }
}

