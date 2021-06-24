package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Key - Value 类型 - join() - 通过广播变量实现类似 join 的功能
object RDDOperator23Join2ByBroadcast {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rddA: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 1), ("b", 2), ("b", 20), ("c", 3)))
    val rddB: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 100), ("b", 200), ("a", 1000), ("d", 400)))

    // 广播变量
    val array2: Array[(String, Int)] = rddB.collect
    val broadcast2: Broadcast[Array[(String, Int)]] = context.broadcast(array2)

    val rdd2: RDD[List[(String, (Int, Int))]] = rddA.map((tuple: (String, Int)) => {
      var result: List[(String, (Int, Int))] = List()
      // 访问广播变量
      val array: Array[(String, Int)] = broadcast2.value
      array.foreach((tp: (String, Int)) => {
        if (tp._1 == tuple._1) {
          result = result :+ (tuple._1, (tuple._2, tp._2))
        }
      })
      result
    })
    val rdd22: RDD[(String, (Int, Int))] = rdd2.flatMap((list: List[(String, (Int, Int))]) => list)
    rdd22.collect.foreach(println)

    println("------")

    // 广播变量
    val array3: Array[(String, Int)] = rddA.collect
    val broadcast3: Broadcast[Array[(String, Int)]] = context.broadcast(array3)

    val rdd3: RDD[List[(String, (Int, Int))]] = rddB.map((tuple: (String, Int)) => {
      var result: List[(String, (Int, Int))] = List()
      // 访问广播变量
      val array: Array[(String, Int)] = broadcast3.value
      array.foreach((tp: (String, Int)) => {
        if (tp._1 == tuple._1) {
          result = result :+ (tuple._1, (tuple._2, tp._2))
        }
      })
      result
    })
    val rdd33: RDD[(String, (Int, Int))] = rdd3.flatMap((list: List[(String, (Int, Int))]) => list)
    rdd33.collect.foreach(println)

    context.stop
  }

}
