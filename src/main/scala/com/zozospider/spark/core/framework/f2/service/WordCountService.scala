package com.zozospider.spark.core.framework.f2.service

import com.zozospider.spark.core.framework.f2.dao.WordCountDao
import org.apache.spark.rdd.RDD

class WordCountService {

  private val dao: WordCountDao = new WordCountDao

  def execute(): RDD[(String, Int)] = {
    val rdd: RDD[String] = dao.textFile("data-dir\\word-count")
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map((s: String) => (s, 1))
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    rdd4
  }

}
