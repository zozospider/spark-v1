package com.zozospider.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 行动算子 - countByValue() countByKey()
object RDDOperator10CountByValueAndCountByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // countByValue():
    // 统计每种 value 的个数

    // countByKey():
    // 统计每种 key 的个数

    // countByValue() 不要求数据类型, 统计的是整个元素的个数
    // countByKey() 要求数据的元素是 (k, v) 类型, 统计 k 的个数

    val rdd: RDD[Int] = context.makeRDD(
      List(10, 1, 10, 20, 20, 3))

    val map: collection.Map[Int, Long] = rdd.countByValue()
    println(map)

    println("---")

    val rdd2: RDD[(String, Int)] = context.makeRDD(
      List(("a", 10), ("b", 20), ("b", 20), ("a", 1), ("a", 10), ("c", 3)))

    val map2: collection.Map[(String, Int), Long] = rdd2.countByValue()
    println(map2)

    println("------")

    val map3: collection.Map[String, Long] = rdd2.countByKey()
    println(map3)

    context.stop
  }

}
