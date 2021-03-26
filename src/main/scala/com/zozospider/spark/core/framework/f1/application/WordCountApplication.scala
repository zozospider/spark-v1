package com.zozospider.spark.core.framework.f1.application

import com.zozospider.spark.core.framework.f1.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}

object WordCountApplication extends App {

  val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
  val context: SparkContext = new SparkContext(conf)

  private val controller: WordCountController = new WordCountController
  controller.execute(context)

  context.stop

}
