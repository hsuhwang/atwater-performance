package com.datastax.atwater.recommendations.ml

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.fpm.AssociationRules


/**
  * Build a prediction model based on purchased history,
  * Recommendations based on the current items in the
  * shopping cart.
  * @author Hsu-Kwang Hwang
  */

object AprioriRecommendations {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().
      setAppName("My App"))

    val data = sc.textFile("file:///opt/dse_ml/10_grocery.csv")

    val transactions: RDD[Array[String]] = data.
      map(s => s.trim.split(','))
    val minConfidence = 0.8

    model.generateAssociationRules(minConfidence).
      collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence
      )
    }


  }
  println("")
  println("")

  val ar = new AssociationRules().setMinConfidence(0.1)
  val results = ar.run(model.freqItemsets)

  results.collect().foreach { rule =>
    println("[" + rule.antecedent.mkString(",")
      + "=>"
      + rule.consequent.mkString(",") + "]," +
      rule.confidence
    )
  }
