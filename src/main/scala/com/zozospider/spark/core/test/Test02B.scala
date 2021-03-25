package com.zozospider.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 需求 B: Top10 热门品类中每个品类的 Top10 活跃 Session 统计 - 实现方式 1
object Test02B {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 读取原始日志
    val rdd: RDD[String] = context.textFile("data-dir\\test\\user_visit_action_simple.txt")
    // 缓存提高性能
    rdd.cache

    // 获取 Top10 热门品类集合
    val top10CategoryIds: Array[String] = test01A3(rdd).map((tuple: (String, (Int, Int, Int))) => tuple._1)
    println(top10CategoryIds.mkString("Array(", ", ", ")"))
    println("------")

    // 过滤原始数据, 保留点击行为数据且为 Top10 热门品类 ID
    val rdd2: RDD[String] = rdd.filter((s: String) => {
      val fields: Array[String] = s.split("_")
      fields(6) != "-1" && top10CategoryIds.contains(fields(6))
    })

    // 根据品类 ID 和 session ID 进行点击量的统计: ((品类 ID, session ID), 1)
    val rdd3: RDD[((String, String), Int)] = rdd2.map((s: String) => {
      val fields: Array[String] = s.split("_")
      ((fields(6), fields(2)), 1)
    })

    // 聚合: ((品类 ID, session ID), count)
    val rdd4: RDD[((String, String), Int)] = rdd3.reduceByKey((i1: Int, i2: Int) => i1 + i2)

    // 结构转换: (品类 ID, (session ID, count))
    val rdd5: RDD[(String, (String, Int))] = rdd4.map((tuple: ((String, String), Int)) => {
      (tuple._1._1, (tuple._1._2, tuple._2))
    })

    // 相同品类 ID 进行分组
    val rdd6: RDD[(String, Iterable[(String, Int)])] = rdd5.groupByKey()

    // 点击量排序取前 10 名, 采集并打印
    val rdd7: RDD[(String, List[(String, Int)])] = rdd6.mapValues((iterable: Iterable[(String, Int)]) =>
      iterable
        .toList
        .sortBy((tuple: (String, Int)) => tuple._2)(Ordering.Int.reverse)
        .take(10))
    rdd7.collect.foreach(println)

    context.stop
  }

  // 获取 Top10 热门品类, 具体逻辑参考 Test01A3.scala
  def test01A3(rdd: RDD[String]): Array[(String, (Int, Int, Int))] = {
    val rdd2: RDD[(String, (Int, Int, Int))] = rdd.flatMap((s: String) => {
      val fields: Array[String] = s.split("_")
      if (fields(6) != "-1") {
        List((fields(6), (1, 0, 0)))
      } else if (fields(8) != "null") {
        val array: Array[String] = fields(8).split(",")
        array.map((s: String) => (s, (0, 1, 0)))
      } else if (fields(10) != "null") {
        val array: Array[String] = fields(10).split(",")
        array.map((s: String) => (s, (0, 0, 1)))
      } else {
        Nil
      }
    })
    val rdd3: RDD[(String, (Int, Int, Int))] = rdd2.reduceByKey((tuple1: (Int, Int, Int), tuple2: (Int, Int, Int)) =>
      (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2, tuple1._3 + tuple2._3))
    val array: Array[(String, (Int, Int, Int))] = rdd3
      .sortBy((tuple: (String, (Int, Int, Int))) => tuple._2, ascending = false)
      .take(10)
    array
  }

}
