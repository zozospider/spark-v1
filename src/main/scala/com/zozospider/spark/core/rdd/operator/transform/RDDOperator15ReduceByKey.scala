package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Key - Value 类型 - reduceByKey()
object RDDOperator15ReduceByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // reduceByKey():
    // 可以将数据按照相同的 Key 对 Value 进行聚合

    val rdd: RDD[(String, Int)] = context.makeRDD(
      List(("a", 1), ("b", 2), ("b", 20), ("a", 10), ("a", 100), ("c", 3)))

    // Spark 和 Scala 语言中的聚合操作一样, 一般是两两聚合, 如: ((1 + 10) + 100) = 111
    // 如果 key 对应的 value 只有一个, 那么不会参与运算
    val rdd2: RDD[(String, Int)] = rdd.reduceByKey((i1: Int, i2: Int) => {
      println(s"$i1 + $i2 = ${i1 + i2}")
      i1 + i2
    })

    rdd2.collect.foreach(println)

    context.stop
  }

}
