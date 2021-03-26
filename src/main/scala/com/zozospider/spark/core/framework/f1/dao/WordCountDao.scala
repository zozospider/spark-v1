package com.zozospider.spark.core.framework.f1.dao

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WordCountDao {

  def textFile(context: SparkContext, path: String): RDD[String] = {
    context.textFile(path)
  }

}
