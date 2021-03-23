package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - 双 Value 类型 - zip()
object RDDOperator13IntersectionAndUnionAndSubtractAndZip2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // zip() 要求两个数据源的分区数量相同, 且每个分区的元素个数相同
    // zip() 的两个数据源的类型可以不相同
    val rddA: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4), numSlices = 2)
    val rddB: RDD[Int] = context.makeRDD(seq = List(10, 2, 30, 4), numSlices = 2)
    val rddC: RDD[String] = context.makeRDD(seq = List("a", "b", "c", "d"), numSlices = 2)

    val rdd2: RDD[(Int, Int)] = rddA.zip(rddB)
    val rdd3: RDD[(Int, String)] = rddA.zip(rddC)
    println(rdd2.collect.mkString("Array(", ", ", ")"))
    println(rdd3.collect.mkString("Array(", ", ", ")"))

    context.stop
  }

}
