package com.sdhu.elasticsearchprediction.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.{ LabeledPoint, GeneralizedLinearModel }

import org.json4s._
import org.json4s.jackson.Serialization

import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets


/*
 * Note: This can be cleaned up a bit, and can be expanded beyond linear classifiers easily 
 * once spark.ml graduates out of beta and useful traits like Estimator become public.
 * Right now only supported typed of models will be linear....
 *
 * So everything is just inheriting off of GeneralizedLinearModel which is not Ideal
 * instead it should have an "sparkModel" so any algorithm can be used
 * as long as we can seriliaze it
sealed trait SparkModel extends Serializable {
 
  def predict(d: RDD[Vector]): RDD[Double]

  def predict(d: Vector): Double
  
}
*/


case class ModelData[M <: GeneralizedLinearModel](
    clf: Option[M] = None,     
    categoriesMap: Option[Map[String, Double]] = None,
    binThreshold: Option[Double] = None,
    numClasses: Option[Int] = None
) {
  def setClf(m: Option[M]) = this.copy(clf = m)
  def setCategoriesMap(cm: Option[Map[String, Double]]) = this.copy(categoriesMap = cm)
  def setProperties(tOpt: Option[Double], nOpt: Option[Int]) = this.copy(binThreshold = tOpt, numClasses = nOpt)
}


case class LinearModelData(
  modelName: String, 
  numFeatures: Int, 
  intercept: Double, 
  weights: Array[Double],
  categoriesMap: Option[Map[String, Double]],
  binThreshold: Option[Double],
  numClasses: Option[Int]
)

  
trait SparkModelHelpers[M <: GeneralizedLinearModel] {
  
  val name: String

  def sparkTrainClf(data: RDD[LabeledPoint], params: Map[String,String]): M
  
  def saveSparkModel(path: String, modelName: String, model: ModelData[M]): Boolean
  
  def readSparkModel(path: String): ModelData[M]
}


trait Linear_SparkModelHelpers[M <: GeneralizedLinearModel] extends SparkModelHelpers[M] {
  def getOptClf(lm: LinearModelData): Option[M]
  
  /*
   * Read into format usable by sparkGenericTrainer
   */
  def readSparkModel(path: String): ModelData[M] = {
    val lm = readModelFromJson(path)
    ModelData(
      getOptClf(lm),
      lm.categoriesMap,
      lm.binThreshold,
      lm.numClasses)
  }
  
  /*
   * Current support is only for Linear, so saving intercepts and weights is enough
   */
  def saveSparkModel(path: String, modelName: String, model: ModelData[M]): Boolean = {
    model.clf match {
      case Some(m) ⇒ {
        saveModel2Json(path, modelName, m, model.categoriesMap, model.binThreshold, model.numClasses)
        Files.exists(Paths.get(path)) //check if file was created...
      }
      case None ⇒ false
    }
  }

  /* 
   * Custom Serialization of linear models without using spark context
   * using json for the weights and intercepts
   */
  def saveModel2Json(
    path: String, 
    modelName: String, 
    m: M,
    catMap: Option[Map[String, Double]],
    thresh: Option[Double],
    numClasses: Option[Int]): Unit = {
      implicit val formats = Serialization.formats(NoTypeHints)
      val jx_str = Serialization.write(
        LinearModelData(
          modelName, 
          m.weights.size, 
          m.intercept, 
          m.weights.toArray,
          catMap,
          thresh,
          numClasses))
      
      Files.write(Paths.get(path), jx_str.getBytes(StandardCharsets.UTF_8))
  }

  def readModelFromJson(path: String): LinearModelData = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val s = scala.io.Source.fromFile(path).getLines.mkString
    Serialization.read[LinearModelData](s)
  }
}
