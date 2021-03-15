package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 算子 - Value 类型 - map()
object RDDOperator01MapParallelize {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // map() 方法:
    // 1. RDD 计算一个分区内的数据是一个一个的执行逻辑, 分区内数据的执行是有序的: 只有某一个数据全部逻辑执行完毕后, 才会执行下一个数据
    // 2. 不同分区数据计算是无序的

    // val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4))
    // val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4), numSlices = 1)
    val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4), numSlices = 2)

    val rdd2: RDD[Int] = rdd.map((i: Int) => {
      println(s">>> i = $i")
      i
    })
    val rdd3: RDD[Int] = rdd2.map((i: Int) => {
      println(s"### i = $i")
      i
    })

    rdd3.collect

    context.stop
  }

}
