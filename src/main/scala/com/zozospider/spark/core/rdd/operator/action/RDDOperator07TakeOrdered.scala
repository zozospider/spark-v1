package com.zozospider.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 行动算子 - takeOrdered()
object RDDOperator07TakeOrdered {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // takeOrdered():
    // 返回该 RDD 排序后的前 n 个元素组成的数组

    val rdd: RDD[Int] = context.makeRDD(List(10, 2, 3, 4))

    val array: Array[Int] = rdd.takeOrdered(2)

    println(array.mkString("Array(", ", ", ")"))

    context.stop
  }

}
