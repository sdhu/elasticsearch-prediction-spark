package com.sdhu.elasticsearchprediction.spark

import com.mahisoft.elasticsearchprediction.utils.DataProperties

object SparkLinear_Trainer extends Serializable {
  def apply(dp: DataProperties): SparkGenericTrainer[_] = {
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
}


object SparkLinear_PredictorEngine extends Serializable {
  def apply(readP: String, clf_type: String): SparkPredictorEngine[_] = {
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
}
