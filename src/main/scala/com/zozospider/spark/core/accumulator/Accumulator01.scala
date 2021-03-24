package com.zozospider.spark.core.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 累加器
object Accumulator01 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Accumulator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 因为 Spark 的闭包功能, Driver 端的 sum 会被传到 Executor 端分别计算, 计算完成后并不会传回到 Driver 端, 所以结果为 0

    val rdd: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))

    var sum: Long = 0
    rdd.foreach((i: Int) => sum += i)
    println(sum)

    context.stop
  }

}
