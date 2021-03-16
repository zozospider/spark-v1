package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Value 类型 - map()
object RDDOperator01Map2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 分区不变性

    val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4), numSlices = 2)
    rdd.saveAsTextFile("output1")

    val rdd2: RDD[Int] = rdd.map(_ * 2)
    rdd2.saveAsTextFile("output2")

    val rdd3: RDD[Int] = rdd2.map(_ + 100)
    rdd3.saveAsTextFile("output3")

    context.stop
  }

}
