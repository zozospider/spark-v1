package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 算子 - Value 类型 - glom()
object RDDOperator05Glom {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // glom():
    // 将同一个分区的数据直接转换为相同类型的内存数组进行处理, 分区不变

    val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4), numSlices = 2)

    val rdd2: RDD[Array[Int]] = rdd.glom()

    rdd2.collect.foreach((array: Array[Int]) => println(array.mkString("Array(", ", ", ")")))

    context.stop
  }

}
