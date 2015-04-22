package com.sdhu.elasticsearchprediction.spark

import com.mahisoft.elasticsearchprediction._
import utils.DataProperties
import classifier.GenericClassifier
import plugin.engine.PredictorEngine

class Spark_Trainer(dp: DataProperties) {
  
  def getSparkGenericTrainer: SparkGenericTrainer[_] = {
    val config = SparkClassifierConfig().readDataProperties(dp)
  
    if (config.clf_type.nonEmpty) {
      config.clf_type.get match {
        case "linear-regression" ⇒ new SparkGenericTrainer(LinearRegression_Helper)
        case "logistic-regression" ⇒ new SparkGenericTrainer(LogisticRegression_Helper)
        case "ridge-regression" ⇒ new SparkGenericTrainer(RidgeRegression_Helper)
        case "svm" ⇒ new SparkGenericTrainer(SVM_Helper)
        case _ ⇒  throw new IllegalArgumentException(s"Invalid spark.clf_type")
      }
    } else {
     throw new IllegalArgumentException("spark.clf_type must be provided")
    }
  }

  def getGenericClassifier: GenericClassifier = this.getSparkGenericTrainer
}


class Spark_PredictorEngine(readP: String, clf_type: String) {
  
  def getSparkPredictorEngine: SparkPredictorEngine[_] = {
    val predEng = clf_type match {
      case "spark.linear-regression" ⇒ new SparkPredictorEngine(readP, LinearRegression_Helper)
      case "spark.logistic-regression" ⇒ new SparkPredictorEngine(readP, LogisticRegression_Helper)
      case "spark.ridge-regression" ⇒ new SparkPredictorEngine(readP, RidgeRegression_Helper)
      case "spark.svm" ⇒ new SparkPredictorEngine(readP, SVM_Helper)
      case _ ⇒  throw new IllegalArgumentException(s"Invalid spark.clf_type")
    }
    predEng.readModel()
    predEng
  }

  def getPredictorEngine: PredictorEngine = this.getSparkPredictorEngine
}
