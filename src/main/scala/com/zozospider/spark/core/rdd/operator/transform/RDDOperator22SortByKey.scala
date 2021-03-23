package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Key - Value 类型 - sortByKey()
object RDDOperator22SortByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // sortByKey():
    // 在一个 (K, V) 的 RDD 上调用, K 必须实现 Ordered 接口 (特质), 返回一个按照 key 进行排序的

    val rdd: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 1), ("b", 2), ("b", 20), ("a", 10), ("a", 100), ("c", 3)), numSlices = 2)

    val rdd2: RDD[(String, Int)] = rdd.sortByKey(ascending = true)
    val rdd3: RDD[(String, Int)] = rdd.sortByKey(ascending = false)

    rdd2.collect.foreach(println)
    println("------")
    rdd3.collect.foreach(println)

    context.stop
  }

}
