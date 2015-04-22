package com.sdhu.elasticsearchprediction.spark
package test

import com.mahisoft.elasticsearchprediction.utils.DataProperties

import org.apache.spark._
import rdd.RDD
import mllib.regression.LabeledPoint

import org.scalatest._
import com.holdenkarau.spark.testing._
import java.io.File

class SparkTrainerSpec extends FlatSpec with MustMatchers with BeforeAndAfterAll {
  val pconf = getClass.getResource("/prop1.conf").getPath
  val dataP = getClass.getResource("/mini.csv").toURI.toString
  val dp = new DataProperties(pconf)
  val trainer = (new Spark_Trainer(dp)).getSparkGenericTrainer
  val correctSPC = SparkClassifierConfig(
      Some("./src/test/resource/mini.csv"),
      Some(Set("age","sex","capital_gain","capital_loss","native-country")),
      Some("probability"),
      Some("./src/test/resource/sparkmodel.test"),
      Some(80),
      Some(5),
      Some("logistic-regression"),
      Some(Map("numIterations" -> "3")),
      Some(false),
      Some(0.5),
      Some(2),
      Some(Map("spark.master"->"local[4]", "spark.driver.memory"->"512m")))
 
  val modelPath = "sparkmodel.test"

  override def afterAll(){
    val f = new File(modelPath)
    f.delete()
  }


  "DataProperties" should "read into SparkClassifierConfig class" in {
    val spc = SparkClassifierConfig().readDataProperties(dp)
    
    spc must equal (correctSPC)

    spc.checkValid must equal(true)
  }

  "Spark Linear Trainer for Classification" should "select logistic-regression" in {
    trainer.sparkModelHelper.name must equal ("LogisticRegression_Helper")
  }

  it should "set properties" in {
    trainer.setDataProperties(dp)
    trainer.getConfig must equal(correctSPC)
  }

  it should "load data" in {
    trainer.setDataProperties(dp)
    val success = trainer.loadData(new File(dataP))
    val data = trainer.getData.map(_.collect)

    success must equal(true)
    data.map(_.size) must equal(Some(1000))
    data.map(_(0).features.size) must equal(Some(5))
  }

  it should "train and do simple validation" in {
    trainer.setDataProperties(dp)
    trainer.loadData(new File(dataP))
    trainer.trainModel()
    val res = trainer.simpleValidation()
    val model = trainer.getModelData
    val cm = model.categoriesMap.getOrElse(Map[String,Double]())
    
    model.binThreshold must equal(Some(0.5))
    model.clf must not be empty
    cm.keys must contain allOf("Female", "Male", "United-States", "China")

    res.getTrainDataSetSize must equal(1000)
    res.getTestDataSetSize must equal(1000)
    println(res.getResults)
  }

  it should "Cross Validate" in {
    trainer.setDataProperties(dp)
    trainer.loadData(new File(dataP))
    trainer.trainModel()
    val res = trainer.crossValidation(5)
   
    res.getNumFolds must equal(5)
    res.getDataSetSize must equal(1000)
    println(res.getResults)
  }

  it should "save the Model" in {
    trainer.setDataProperties(dp)
    trainer.loadData(new File(dataP))
    trainer.trainModel()

    val success = trainer.saveModel("testModel", modelPath)

    success must equal(true)
  }

  it should "load a new Model" in {
    val tr2 = (new Spark_Trainer(dp)).getSparkGenericTrainer
    tr2.setDataProperties(dp)
    tr2.loadClassifier(modelPath)
    val model = tr2.getModelData
    val cm = model.categoriesMap.getOrElse(Map[String,Double]())
    
    model.binThreshold must equal(Some(0.5))
    model.clf must not be empty
    cm.keys must contain allOf("Female", "Male", "United-States", "China")
  }
}
