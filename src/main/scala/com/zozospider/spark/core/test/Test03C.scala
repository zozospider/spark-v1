package com.zozospider.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 需求 C: 页面单跳转换率统计
object Test03C {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 读取原始日志, 缓存提高性能
    val rdd: RDD[String] = context.textFile("data-dir\\test\\user_visit_action_simple.txt")
    rdd.cache


    context.stop
  }

}
