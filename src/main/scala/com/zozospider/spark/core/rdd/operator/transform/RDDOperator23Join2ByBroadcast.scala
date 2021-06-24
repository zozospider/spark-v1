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
    val arrayB: Array[(String, Int)] = rddB.collect
    val broadcastB: Broadcast[Array[(String, Int)]] = context.broadcast(arrayB)

    val rdd2: RDD[List[(String, (Int, Int))]] = rddA.map((tupleA: (String, Int)) => {
      // 访问广播变量
      val arrayB: Array[(String, Int)] = broadcastB.value
      // 找出 rddB (广播变量) 中具有和 rddA 相同的所有 key, 组合成 tuple 并添加到 list 中
      var result: List[(String, (Int, Int))] = List()
      arrayB.foreach((tupleB: (String, Int)) => {
        if (tupleB._1 == tupleA._1) {
          result = result :+ (tupleA._1, (tupleA._2, tupleB._2))
        }
      })
      result
    })
    // list(tuple) 转换成 tuple
    val rdd22: RDD[(String, (Int, Int))] = rdd2.flatMap((list: List[(String, (Int, Int))]) => list)
    rdd22.collect.foreach(println)

    println("------")

    // 广播变量
    val arrayA: Array[(String, Int)] = rddA.collect
    val broadcastA: Broadcast[Array[(String, Int)]] = context.broadcast(arrayA)

    val rdd3: RDD[List[(String, (Int, Int))]] = rddB.map((tupleB: (String, Int)) => {
      // 访问广播变量
      val arrayA: Array[(String, Int)] = broadcastA.value
      // 找出 rddA (广播变量) 中具有和 rddB 相同的所有 key, 组合成 tuple 并添加到 list 中
      var result: List[(String, (Int, Int))] = List()
      arrayA.foreach((tupleA: (String, Int)) => {
        if (tupleA._1 == tupleB._1) {
          result = result :+ (tupleB._1, (tupleB._2, tupleA._2))
        }
      })
      result
    })
    // list(tuple) 转换成 tuple
    val rdd33: RDD[(String, (Int, Int))] = rdd3.flatMap((list: List[(String, (Int, Int))]) => list)
    rdd33.collect.foreach(println)

    context.stop
  }

}
