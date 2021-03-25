package com.zozospider.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 需求 A: Top10 热门品类 - 实现方式 2
object Test01A2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 问题:
    // 1. 存在大量不同数据的 reduceByKey() 性能较低 (有 shuffle)

    // 读取原始日志, 缓存提高性能
    val rdd: RDD[String] = context.textFile("data-dir\\test\\user_visit_action_simple.txt")
    rdd.cache

    // 统计品类点击数量: (品类 ID, (点击数, 0, 0))
    val rddA2: RDD[String] = rdd.filter((s: String) => {
      val fields: Array[String] = s.split("_")
      fields(6) != "-1"
    })
    val rddA3: RDD[(String, Int)] = rddA2.map((s: String) => {
      val fields: Array[String] = s.split("_")
      (fields(6), 1)
    })
    val rddA4: RDD[(String, Int)] = rddA3.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    val rddA5: RDD[(String, (Int, Int, Int))] = rddA4.mapValues((i: Int) => (i, 0, 0))
    rddA5.take(3).foreach(println)
    println("---")

    // 统计品类下单数量: (品类 ID, (0, 下单数, 0))
    val rddB2: RDD[String] = rdd.filter((s: String) => {
      val fields: Array[String] = s.split("_")
      fields(8) != "null"
    })
    val rddB3: RDD[(String, Int)] = rddB2.flatMap((s: String) => {
      val fields: Array[String] = s.split("_")
      val array: Array[String] = fields(8).split(",")
      val array2: Array[(String, Int)] = array.map((s: String) => (s, 1))
      array2
    })
    val rddB4: RDD[(String, Int)] = rddB3.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    val rddB5: RDD[(String, (Int, Int, Int))] = rddB4.mapValues((i: Int) => (0, i, 0))
    rddB5.take(3).foreach(println)
    println("---")

    // 统计品类支付数量: (品类 ID, (0, 0, 支付数))
    val rddC2: RDD[String] = rdd.filter((s: String) => {
      val fields: Array[String] = s.split("_")
      fields(10) != "null"
    })
    val rddC3: RDD[(String, Int)] = rddC2.flatMap((s: String) => {
      val fields: Array[String] = s.split("_")
      val array: Array[String] = fields(10).split(",")
      val array2: Array[(String, Int)] = array.map((s: String) => (s, 1))
      array2
    })
    val rddC4: RDD[(String, Int)] = rddC3.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    val rddC5: RDD[(String, (Int, Int, Int))] = rddC4.mapValues((i: Int) => (0, 0, i))
    rddC5.take(3).foreach(println)

    println("------")

    // 将数据转换为: (品类 ID, (点击数, 下单数, 支付数))
    val rdd2: RDD[(String, (Int, Int, Int))] = rddA5.union(rddB5).union(rddC5)
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
