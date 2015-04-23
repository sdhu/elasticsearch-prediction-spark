package com.sdhu.elasticsearchprediction.spark

import com.mahisoft.elasticsearchprediction._
import plugin.domain.{ IndexValue, IndexAttributeDefinition }
import domain.DataType

import org.apache.spark._
import rdd.RDD
import mllib.linalg.{ Vectors, Vector ⇒ spV }
import mllib.regression.LabeledPoint

import java.util.Collection

import scala.util.control.Exception._
import scala.collection.JavaConversions._


object CsvUtil extends Serializable {

  implicit class RichArrayString(val a: Array[String]) extends Serializable {
    def toDoubleOpt(i: Int): Option[Double] =  
      catching(classOf[NumberFormatException]).opt(a(i).toDouble)
    
    def toDoubleEither(i: Int): Either[Double, String] = {
      this.toDoubleOpt(i) match {
        case Some(d) ⇒ Left(d)
        case None ⇒ Right(a(i))
      }
    }

    def toDoubleArray(cm: Map[String, Double]): Array[Double] = {
      a.zipWithIndex.map{ case (v,i) ⇒ {
        this.toDoubleOpt(i) match {
          case Some(d) ⇒ d
          case None ⇒ cm.getOrElse(v, 0.0)
        }
      }}
    }
  }
}


object ReadUtil extends Serializable {
  import CsvUtil._
  
  /*
   * Reads a csv file using and selects appropiate columns for features,
   * and given column as label
   */
  def csv2RDD(
    sc: SparkContext, 
    path: String, 
    cols: IndexedSeq[String], 
    label: String,
    isRegression: Boolean = true,
    thresh: Double = 0.5
  ): (RDD[LabeledPoint], Map[String, Double]) = {
      val csv = sc.textFile(path)
        .map(_.split(",").map(_.trim))
      val header = csv.first.zipWithIndex
      val feat_ind = header.filter(x ⇒ cols.contains(x._1)).map(_._2)
      val label_ind = header.filter(_._1 == label).map(_._2)
      val ind = label_ind ++ feat_ind

      //convert to numeric and get all categorical values
      val select_csv = csv.filter(_.head != header.head._1)
        .map(x ⇒ ind.map(i ⇒ x.toDoubleEither(i)))
        
      val categories = select_csv.flatMap(_.zipWithIndex.filter(_._1.isRight).map(x ⇒ (x._1.right.get, x._2)))
        .countByKey
    
     // log.debug(s"csv2RDD categories: ${categories}")

      //start index on 1.0 since 0.0 is left for sparseness
      val cats = categories.keys.zipWithIndex.map(x ⇒ (x._1, x._2.toDouble + 1.0)).toMap
      val bcCats = sc.broadcast(cats)

      (select_csv.map(x ⇒ {
        val dv = x.map(_ match {
          case Left(d) ⇒ d
          case Right(s) ⇒ bcCats.value.getOrElse(s, 0.0)
        })

        val lb = if (isRegression) {
          dv(0)
        } else {
          if (dv(0) > thresh) 1.0 else 0.0
        }

        LabeledPoint(lb, Vectors.dense(dv.drop(1)))
      }),
      cats)
  }


  /*
   * Take an IndexedValue from the pplugin and convert it with appropiate categorical numerical
   * mapp]ng into a spark vector
   *
   * Uggly need to fix type conversion
   */
  def cIndVal2Vector(v: Collection[IndexValue], cm: Map[String, Double]): spV = {
    val a = v.map(_.getValue.asInstanceOf[String]).toArray
    Vectors.dense(a.toDoubleArray(cm))
  }

  // not using IdexAttributeDefinition ... just set it to double
  def arr2CIndVal(v: Array[String]): Collection[IndexValue] = {
    val ret = v.map(s ⇒ new IndexValue(
        new IndexAttributeDefinition("notUsed", DataType.STRING),
        s))
    
    asJavaCollection[IndexValue](ret)
  }
}

