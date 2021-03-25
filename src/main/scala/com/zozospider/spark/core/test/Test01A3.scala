package com.zozospider.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 需求 A: Top10 热门品类 - 实现方式 3
object Test01A3 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 问题:
    // 1. 存在 reduceByKey() 性能较低 (有 shuffle)

    // 读取原始日志
    val rdd: RDD[String] = context.textFile("data-dir\\test\\user_visit_action_simple.txt")

    // 将数据转换为:
    // 点击: (品类 ID, (1, 0, 0))
    // 下单: (品类 ID, (0, 1, 0))
    // 支付: (品类 ID, (0, 0, 1))
    val rdd2: RDD[(String, (Int, Int, Int))] = rdd.flatMap((s: String) => {
      val fields: Array[String] = s.split("_")
      if (fields(6) != "-1") {
        // 点击
        List((fields(6), (1, 0, 0)))
      } else if (fields(8) != "null") {
        // 下单
        val array: Array[String] = fields(8).split(",")
        array.map((s: String) => (s, (0, 1, 0)))
      } else if (fields(10) != "null") {
        // 支付
        val array: Array[String] = fields(10).split(",")
        array.map((s: String) => (s, (0, 0, 1)))
      } else {
        Nil
      }
    })
    rdd2.take(9).foreach(println)

    println("------")

    // 将数据转换为: (品类 ID, (点击数, 下单数, 支付数))
    val rdd3: RDD[(String, (Int, Int, Int))] = rdd2.reduceByKey((tuple1: (Int, Int, Int), tuple2: (Int, Int, Int)) =>
      (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2, tuple1._3 + tuple2._3))

    // 将品类排序 (按点击数, 下单数, 支付数排序), 取前 10 名, 采集并打印
    val array: Array[(String, (Int, Int, Int))] = rdd3
      .sortBy((tuple: (String, (Int, Int, Int))) => tuple._2, ascending = false)
      .take(10)
    array.foreach(println)

    context.stop
  }

}
