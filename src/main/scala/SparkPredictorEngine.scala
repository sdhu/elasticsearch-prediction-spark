package com.sdhu.elasticsearchprediction.spark

import com.mahisoft.elasticsearchprediction.plugin.engine.PredictorEngine
import com.mahisoft.elasticsearchprediction.plugin.domain.IndexValue
import com.mahisoft.elasticsearchprediction.plugin.exception.PredictionException

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.GeneralizedLinearModel

import java.util.Collection

class SparkPredictorEngine[M <: GeneralizedLinearModel](val readPath: String, val spHelp: SparkModelHelpers[M]) extends PredictorEngine {
  
  private var _model: ModelData[M] = ModelData[M]()

  override def getPrediction(values: Collection[IndexValue]): Double = {
    if (_model.clf.nonEmpty) { 
      val v = ReadUtil.cIndVal2Vector(
        values, //a bit ugly 
        _model.categoriesMap.getOrElse(Map[String, Double]()))
      
      _model.clf.get.predict(v)
    } else {
      throw new PredictionException("Empty model");
    }
  }
  
  def readModel(): ModelData[M] = {
    _model = spHelp.readSparkModel(readPath)
    _model
  }

  def getModel: ModelData[M] = _model
}

