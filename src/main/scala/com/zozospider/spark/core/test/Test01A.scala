package com.zozospider.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 需求 A: Top10 热门品类 - 实现方式 1
object Test01A {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 问题:
    // 1. rdd 重复使用
    // 2. cogroup() 性能较低 (可能会有 shuffle)

    // 读取原始日志
    val rdd: RDD[String] = context.textFile("data-dir\\test\\user_visit_action_simple.txt")

    // 统计品类点击数量: (品类 ID, 点击数)
    val rddA2: RDD[String] = rdd.filter((s: String) => {
      val fields: Array[String] = s.split("_")
      fields(6) != "-1"
    })
    val rddA3: RDD[(String, Int)] = rddA2.map((s: String) => {
      val fields: Array[String] = s.split("_")
      (fields(6), 1)
    })
    val rddA4: RDD[(String, Int)] = rddA3.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    rddA4.take(3).foreach(println)
    println("---")

    // 统计品类下单数量: (品类 ID, 下单数)
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
    rddB4.take(3).foreach(println)
    println("---")

    // 统计品类支付数量: (品类 ID, 支付数)
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
    rddC4.take(3).foreach(println)

    println("------")

    // 将数据转换为: (品类 ID, (点击数, 下单数, 支付数))
    val rdd2: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = rddA4.cogroup(rddB4, rddC4)
    val rdd3: RDD[(String, (Int, Int, Int))] = rdd2.mapValues((tuple: (Iterable[Int], Iterable[Int], Iterable[Int])) => {
      var aCount: Int = 0
      tuple._1.iterator.foreach((i: Int) => aCount += i)
      var bCount: Int = 0
      tuple._2.iterator.foreach((i: Int) => bCount += i)
      var cCount: Int = 0
      tuple._3.iterator.foreach((i: Int) => cCount += i)
      (aCount, bCount, cCount)
    })

    // 将品类排序 (按点击数, 下单数, 支付数排序), 取前 10 名, 采集并打印
    val array: Array[(String, (Int, Int, Int))] = rdd3
      .sortBy((tuple: (String, (Int, Int, Int))) => tuple._2, ascending = false)
      .take(10)
    array.foreach(println)

    context.stop
  }

}
