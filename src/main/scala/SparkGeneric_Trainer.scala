package com.sdhu.elasticsearchprediction.spark

import com.mahisoft.elasticsearchprediction._
import classifier.GenericClassifier
import utils.DataProperties
import domain.{ CrossDataSetResult, SimpleDataSetResult }
//import exception._

import org.apache.spark._
import rdd.RDD
import mllib.util.MLUtils
import mllib.regression.{ LabeledPoint, GeneralizedLinearModel }

import org.apache.log4j.{ Logger, LogManager }

import java.io.File
import java.lang.{Boolean ⇒ JBoolean, Integer ⇒ JInt}


/*
 * Specific format for sparkClassifier properties:
 *
 * data.filename = </full/path> of training data
 * data.type = <type>  (csv supported only as of now. Future work on json, parquet, etc...)
 * data.columns = <feat1,feat2,...> feature dimensions to select from data
 * data.column = <targetVariableColName>
 *
 * train.percentage = <int> 0-100
 * validate.numFolds = <int> 
 * model.filename = </full/path> of location to save/read serialized trained model
 *
 * spark.model.type = <string> currently only linear models supported
 * spark.model.parmas = <param1:val1,param2:val2,...> params for model training
 * spark.model.isregression = <boolean>
 * spark.model.binThreshold = <double> specify threshold to binarize target variable
 * spark.conf = <sparkConfKey1:sparkConfVal1,sparkConfKey2:sparkConfVal2> set spark conf values such
 *              as master, parallelism, etc...
 */
case class SparkClassifierConfig(
  data_filename: Option[String] = None, 
  data_columns: Option[IndexedSeq[String]] = None,
  data_column_label: Option[String] = None,
  model_filename: Option[String] = None,
  train_percentage: Option[Int] = Some(75),
  validate_kfolds: Option[Int] = Some(0),
  clf_type: Option[String] = None,
  clf_params: Option[Map[String,String]] = None,
  clf_isRegression: Option[Boolean] = None,
  clf_binThreshold: Option[Double] = None,
  clf_numClasses: Option[Int] = None,
  spark_conf: Option[Map[String, String]] = None
) {
  def readDataProperties(dp: DataProperties) = {
    this.copy(
      data_filename = Option(dp.getValue("data.filename")),
      data_columns = Option(dp.getValue("data.columns")).map(_.split(",").toIndexedSeq),
      data_column_label = Option(dp.getValue("data.column.label")),
      model_filename = Option(dp.getValue("model.filename")),
      train_percentage = Option(dp.getValue("train.percentage")).map(_.toInt),
      validate_kfolds = Option(dp.getValue("validate.numFolds")).map(_.toInt),
      clf_type = Option(dp.getValue("spark.model.type")),
      clf_params = Option(dp.getValue("spark.model.params"))
        .map(_.split(",").map(_.split(":")).collect { case Array(p,v) ⇒ (p,v) }.toMap),
      clf_isRegression = Option(dp.getValue("spark.model.isregression")).map(_.toBoolean),
      clf_binThreshold = Option(dp.getValue("spark.model.binThreshold")).map(_.toDouble),
      clf_numClasses = Option(dp.getValue("spark.model.numClasses")).map(_.toInt),
      spark_conf = Option(dp.getValue("spark.conf"))
        .map(_.split(",").map(_.split(":")).collect { case Array(p,v) ⇒ (p,v) }.toMap)
      )    
  }

  def checkValid: Boolean = {
    this.data_filename.nonEmpty &&
    this.data_columns.nonEmpty &&
    this.model_filename.nonEmpty &&
    this.clf_type.nonEmpty &&
    this.spark_conf.nonEmpty
  }
}


object Labels extends Enumeration {
    type Labels = Value
    val POS, NEG, TP, TN, FN, FP = Value
}


case class Result(size: Int, result: String)


/*
 *
 * Trait to operate with GenericClassifier Java interface with additional utils specific for
 * Spark classifier types in mllib
 *
 * TODO: will be cleaned out once spark.ml interface is out of beta... to support
 * models beyond linear
 * - a cleanup method to sc.stop().....
 */
class SparkGenericTrainer[M <: GeneralizedLinearModel](val sparkModelHelper: SparkModelHelpers[M]) extends GenericClassifier {
  private var _sparkConf: SparkConf = new SparkConf() 
 
  @transient
  var _scOpt: Option[SparkContext] = None

  @transient
  private val log: Logger = LogManager.getLogger(this.getClass.getSimpleName)

  //should be private for debug will make visible
  private var _model: ModelData[M] = ModelData()
  private var _data: Option[RDD[LabeledPoint]] = None
  private var _config: SparkClassifierConfig = SparkClassifierConfig()

  def cleanUp(): Unit = {
    if (_scOpt.nonEmpty) {
      _scOpt.get.stop()
      _scOpt = None
    }
  }
  
  def getModelData = this._model
  
  def getData = this._data
  
  def getConfig = this._config

  /*
   * Calculate simple metrics. Assume y input is tuple (truth, prediction)
   */
  def evalResults(y: Array[(Double, Double)], isRegression: Boolean, binTr: Option[Double]): Result = {
    import Labels._
    import scala.math._
    
    val res = if (isRegression) {
      val diff = y.map{ case(a, b) ⇒ a - b}
      val diff2 = diff.map(x ⇒ x * x)
      val n = y.size.toDouble
      val u_truth = y.map(_._1).sum / n
      val u_pred = y.map(_._2).sum / n

      val mse = diff2.sum / n
      val rmse = sqrt(mse)
      val mae = diff.map(x ⇒ abs(x)).sum / n
      val rse = diff2.sum / y.map(x ⇒ pow(x._1 - u_truth, 2)).sum

      s"MSE: $mse, RMSE: $rmse, MAE: $mae, RSE: $rse \n N: $n, u_truth: $u_truth, u_pred: $u_pred"
    } else {
      val yBin = y.map{ case(a, b) ⇒ {
        val btr = binTr.getOrElse(0.5)
        val tr = if(a > btr)  POS else NEG
        val pred = if(b > btr) POS else NEG
        (tr, pred) match {
          case (POS, POS) ⇒ TP
          case (POS, NEG) ⇒ FN
          case (NEG, POS) ⇒ FP
          case (NEG, NEG) ⇒ TN
        }
      }}

      val tp = yBin.filter(_ == TP).size.toDouble
      val tn = yBin.filter(_ == TN).size.toDouble
      val fp = yBin.filter(_ == FP).size.toDouble
      val fn = yBin.filter(_ == FN).size.toDouble
      val n = yBin.size
      val prc = if ((tp + fp) > 0.0) tp / (tp + fp) else 0.0
      val rec = if ((tp + fn) > 0.0) tp / (tp + fn) else 0.0
      val f1 = if ((prc + rec) > 0.0) (2 * prc * rec ) / (prc + rec) else 0.0

      s"Precision: $prc, Recall: $rec, F1-Score: $f1 \n N: $n, TP: $tp, TN: $tn, FP: $fp, FN: $fn"
    }
    Result(y.size, res)
  }

//  @throws(classOf[ModelException])
  override def loadClassifier(): JBoolean = {
    _config.model_filename match {
      case Some(path) ⇒ loadClassifier(path)
      case None ⇒ false
    }
  }

  def loadClassifier(path: String): Boolean = {
    _model = sparkModelHelper.readSparkModel(path)
    _model.clf.nonEmpty
  }


  //TODO support for other formats other than CSV
  override def loadData(dataFile: File): JBoolean = {
    if (_scOpt.isEmpty)
      _scOpt = Some(new SparkContext(_sparkConf))
    
    val ret = _config.data_columns.map(cols ⇒ {
        val lb: String = _config.data_column_label.getOrElse(cols.last)
        val c = if(cols.last == lb) cols.dropRight(1) else cols
        ReadUtil.csv2RDD(
          _scOpt.get, 
          dataFile.getPath, 
          c, 
          lb
        )
    })

       //   _config.clf_isRegression.getOrElse(true),
        //  _config.clf_binThreshold.getOrElse(0.5)))
    
    _data = ret.map(_._1)
    _model = _model.setCategoriesMap(ret.map(_._2))
    _data.nonEmpty
  }


  // right now jsut training on full set....
  override def trainModel(): Unit = {
    val p = _config.clf_params.getOrElse(Map[String,String]())
    _model = _model.setClf(_data.map(d ⇒ sparkModelHelper.sparkTrainClf(d, p)))

    if (_model.clf.nonEmpty)
      log.info(s"Trained Model for: ${sparkModelHelper.name}")
    else
      log.info(s"Bad Training Model for: ${sparkModelHelper.name}")
  }

  // not implemented
  override def splitDataSet(trainingPercentage: JInt): JBoolean = {
    true
  }

  override def simpleValidation(): SimpleDataSetResult = {
    val ropt = if (_data.nonEmpty && _model.clf.nonEmpty && _config.clf_isRegression.nonEmpty) {
      val data = _data.get.cache
      val y_truth = data.map(_.label).collect
      val y_pred = _model.clf.get.predict(data.map(_.features)).collect

      Some(evalResults(y_truth.zip(y_pred), _config.clf_isRegression.get, _config.clf_binThreshold))  
    } else None

    val res = new SimpleDataSetResult()
    
    if (ropt.nonEmpty) { 
      val r = ropt.get
      res.setTrainDataSetSize(r.size)
      res.setTestDataSetSize(r.size)
      res.setResults(r.result)
    } else {
      log.error("Error validating results")
    }

    res
  }

  // try set of params????
  override def crossValidation(numFolds: JInt): CrossDataSetResult = {
    val res = new CrossDataSetResult(numFolds)
    
    if (_data.nonEmpty && _config.clf_isRegression.nonEmpty){
      val rg = _config.clf_isRegression.get
      val bopt = _config.clf_binThreshold

      val data = _data.get.cache
      val kdata = MLUtils.kFold(data, numFolds, 0).zipWithIndex
      val p = _config.clf_params.getOrElse(Map[String,String]())
      
      res.setDataSetSize(data.count.toInt)
      
      for (((tr,te),i) ← kdata) {
        val clf = sparkModelHelper.sparkTrainClf(tr, p)
        
        val y_tr_truth = tr.map(_.label).collect
        val y_te_truth = te.map(_.label).collect
        val y_tr_pred = clf.predict(tr.map(_.features)).collect
        val y_te_pred = clf.predict(te.map(_.features)).collect
        
        val result_tr = evalResults(y_tr_truth.zip(y_tr_pred), rg, bopt).result
        val result_te = evalResults(y_te_truth.zip(y_te_pred), rg, bopt).result

        res.addResult(i, s"Training: ${result_tr}\nTesting: ${result_te}")
      }
    } 

    return res
  } 
  
  override def saveModel(modelName: String): JBoolean = {
    _config.model_filename match {
      case Some(path) ⇒ saveModel(modelName, path) 
      case None ⇒ false
    }
  }

  def saveModel(modelName: String, path: String): Boolean = {
    sparkModelHelper.saveSparkModel(path, modelName, _model) 
  } 
    
  override def setDataProperties(dataProperties: DataProperties) = {
    _config = _config.readDataProperties(dataProperties)
    _model = _model.setProperties(_config.clf_binThreshold, _config.clf_numClasses)

    //validate that required fields have been passed....
    if (_config.checkValid){
      
     // log.info(s"Setting ${sparkModelHelper.name} DataProperties")
      val spc = _config.spark_conf.get
      _sparkConf = _sparkConf.setAppName(sparkModelHelper.name)
        .setMaster(spc.getOrElse("spark.master","local[4]"))
      
      // configure all other spark settings
      val spc2 = if (spc.contains("spark.master")) spc - "spark.master" else spc
      for ( (spK, spV) ← spc2.toSeq){
        _sparkConf = _sparkConf.set(spK, spV)
      }
    }
    else{
      log.error(s"Invalid Setting ${sparkModelHelper.name} DataProperties")
    }
  }

}
