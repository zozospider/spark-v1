package com.zozospider.spark.core.broadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

// 广播变量
object Broadcast01 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Broadcast").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 闭包数据, 都是以 Task 为单位发送的, 每个任务中包含闭包数据
    // 这样可能会导致, 一个 Executor 中含有大量重复的数据, 并且占用大量的内存

    val rdd: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 1), ("b", 2), ("c", 3)))
    val map: mutable.Map[String, Int] = mutable.Map(("a", 10), ("b", 20), ("c", 30))

    val rdd2: RDD[(String, (Int, Int))] = rdd.map((tuple: (String, Int)) =>
      (tuple._1, (tuple._2, map.getOrElse(tuple._1, 0))))

    rdd2.collect.foreach(println)

    context.stop
  }

}
