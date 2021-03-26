package com.zozospider.spark.core.framework.f1.controller

import com.zozospider.spark.core.framework.f1.service.WordCountService
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WordCountController {

  private val service: WordCountService = new WordCountService

  def execute(context: SparkContext): Unit = {
    val rdd: RDD[(String, Int)] = service.execute(context)
    rdd.collect.foreach(println)
  }

}
