package com.zozospider.spark.core.framework.f2.dao

import com.zozospider.spark.core.framework.f2.util.EnvUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WordCountDao {

  def textFile(path: String): RDD[String] = {
    val context: SparkContext = EnvUtil.getSparkContext
    context.textFile(path)
  }

}
