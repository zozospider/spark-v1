package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Key - Value 类型 - join()
object RDDOperator23Join {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // join():
    // 在类型为 (K, V) 和 (K, W) 的 RDD 上调用, 返回一个相同 key 对应的所有元素连接在一起的 (K, (V, W)) 的 RDD

    // 如果两个数据源中 key 没有匹配上, 那么数据不会出现在结果中
    // 如果两个数据源中有多个相同的 key, 会依次匹配 n * m 次, 可能出现笛卡尔积, 影响性能

    val rddA: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 1), ("b", 2), ("b", 20), ("c", 3)))
    val rddB: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 100), ("b", 200), ("a", 1000), ("d", 400)))

    val rdd2: RDD[(String, (Int, Int))] = rddA.join(rddB)
    val rdd3: RDD[(String, (Int, Int))] = rddB.join(rddA)

    rdd2.collect.foreach(println)
    println("------")
    rdd3.collect.foreach(println)

    context.stop
  }

}
