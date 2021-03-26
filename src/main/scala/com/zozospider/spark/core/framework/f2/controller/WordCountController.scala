package com.zozospider.spark.core.framework.f2.controller

import com.zozospider.spark.core.framework.f2.service.WordCountService
import org.apache.spark.rdd.RDD

class WordCountController {

  private val service: WordCountService = new WordCountService

  def execute(): Unit = {
    val rdd: RDD[(String, Int)] = service.execute()
    rdd.collect.foreach(println)
  }

}
