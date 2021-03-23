package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Key - Value 类型 - cogroup()
object RDDOperator25Cogroup {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // cogroup():
    // 在类型为 (K, V) 和 (K, W) 的 RDD 上调用, 返回一个 (K, (Iterable<V>, Iterable<W>)) 类型的 RDD

    val rddA: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 1), ("b", 2), ("b", 20), ("c", 3)))
    val rddB: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 100), ("b", 200), ("a", 1000), ("d", 400)))
    val rddC: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", -100), ("b", -200), ("a", -1000), ("d", -400)))

    val rdd2: RDD[(String, (Iterable[Int], Iterable[Int]))] = rddA.cogroup(rddB)
    rdd2.collect.foreach(println)

    println("------")

    val rdd3: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = rddA.cogroup(rddB, rddC)
    rdd3.collect.foreach(println)

    context.stop
  }

}
