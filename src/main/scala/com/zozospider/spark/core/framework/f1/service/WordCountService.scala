package com.zozospider.spark.core.framework.f1.service

import com.zozospider.spark.core.framework.f1.dao.WordCountDao
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WordCountService {

  private val dao: WordCountDao = new WordCountDao

  def execute(context: SparkContext): RDD[(String, Int)] = {
    val rdd: RDD[String] = dao.textFile(context, "data-dir\\word-count")
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map((s: String) => (s, 1))
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    rdd4
  }

}
